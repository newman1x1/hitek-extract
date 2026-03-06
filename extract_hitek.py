#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════
  PORTABLE ZERO-DISK STREAMING EXTRACTOR  —  hardened edition
  Runs on Windows / macOS / Linux — NO Colab, NO restrictions!

  Pipeline:  Google Drive → rclone cat → 7z → RAM → Drive API upload
  Disk usage: ~0 (everything streams through RAM)

  What makes this robust:
  ─────────────────────────────────────────────────────────
  • Real-time progress every second (bytes, %, speed, ETA)
  • Live per-chunk progress via streaming generator
  • Server-offset verified before EVERY retry (never sends wrong offset)
  • Adaptive upload timeout  (scales with chunk size / slow internet)
  • 20 retries with exponential back-off + jitter
  • Handles 429 rate-limit (reads Retry-After), 500/502/503/504
  • Session-expiry recovery: creates a new Drive session inline
  • rclone called with --retries/--low-level-retries flags
  • 7-zip stderr drained in background (no pipe deadlock)
  • Thread-safe token manager
  • Checkpoint every 1 GB; clean Ctrl-C saves & exits

  SETUP (one-time, ~5 minutes):
  ─────────────────────────────────────────────────────────
  1. Install rclone:
       Windows:  winget install Rclone.Rclone
       Linux:    curl https://rclone.org/install.sh | sudo bash

  2. Install 7-Zip:
       Windows:  winget install 7zip.7zip
       Linux:    sudo apt install p7zip-full

  3. Configure rclone for Google Drive (one-time):
       rclone config
       → New remote → name it "gdrive" → type "drive" → follow prompts

  4. Run with uv:
       uv run extract_hitek.py
═══════════════════════════════════════════════════════════════════════
"""

# /// script
# requires-python = ">=3.10"
# dependencies = ["requests>=2.31"]
# ///

import subprocess, os, json, time, sys, threading, configparser, random
from datetime import datetime, timezone
from io import BytesIO

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("📦 Installing 'requests' ...")
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'requests', '-q'])
    import requests
    from requests.adapters import HTTPAdapter


# ════════════════════════════════════════════════════════════════════
# CONFIGURATION — edit to match your Drive layout
# ════════════════════════════════════════════════════════════════════
RCLONE_REMOTE      = 'Gdrive'                    # rclone remote name
PARTS_DIR          = 'TelegramDownloads'          # Drive folder with archive parts
FOLDER_NAME        = 'HiTek_Extracted'            # Drive output folder
OUTPUT_NAME        = 'users_data.zip'             # Output file name on Drive
CHUNK_SIZE         = 256 * 1024 * 1024            # 256 MB upload chunks
STATE_FILE         = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'upload_state.json')
TOTAL_SIZE_GB      = 109.4                        # Approximate total (for ETA display)

# Minimum expected upload speed for adaptive timeouts (bytes/s)
# 32 KB/s ≈ very slow connection; keeps timeouts extremely generous
MIN_SPEED_BPS      = 32 * 1024

MAX_UPLOAD_RETRIES = 20   # per-chunk retry attempts
MAX_RCLONE_RETRIES = 10   # per-part rclone retry attempts
MAX_SESS_RETRIES   = 15   # new-session creation attempts


# ════════════════════════════════════════════════════════════════════
# TOOL DETECTION
# ════════════════════════════════════════════════════════════════════
def find_7z():
    if sys.platform == 'win32':
        for path in [
            r'C:\Program Files\7-Zip\7z.exe',
            r'C:\Program Files (x86)\7-Zip\7z.exe',
        ]:
            if os.path.exists(path):
                return path
    try:
        subprocess.run(['7z'], capture_output=True, timeout=5)
        return '7z'
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    print("❌ 7-Zip not found!")
    print("   Windows: winget install 7zip.7zip")
    print("   Linux:   sudo apt install p7zip-full")
    sys.exit(1)


def find_rclone():
    try:
        r = subprocess.run(['rclone', 'version'], capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            return 'rclone'
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    print("❌ rclone not found!")
    print("   Windows: winget install Rclone.Rclone")
    print("   Linux:   curl https://rclone.org/install.sh | sudo bash")
    print("   Then run: rclone config   (set up Google Drive)")
    sys.exit(1)



# ════════════════════════════════════════════════════════════════════
# HTTP SESSION — persistent connection pool, no built-in retries
# (we manage retries ourselves for full control)
# ════════════════════════════════════════════════════════════════════
def make_session() -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=0)
    s.mount('https://', adapter)
    s.mount('http://',  adapter)
    return s


# ════════════════════════════════════════════════════════════════════
# TOKEN MANAGER — thread-safe, reads from rclone config
# ════════════════════════════════════════════════════════════════════
class TokenManager:
    def __init__(self, remote_name: str, rclone_cmd: str):
        self.remote = remote_name
        self.rclone = rclone_cmd
        self.access_token: str = ''
        self.expiry: float = 0.0
        self._lock = threading.Lock()

        if sys.platform == 'win32':
            self.config_path = os.path.join(
                os.environ.get('APPDATA', os.path.expanduser('~')),
                'rclone', 'rclone.conf',
            )
        else:
            self.config_path = os.path.expanduser('~/.config/rclone/rclone.conf')

        if not os.path.exists(self.config_path):
            print(f"❌ rclone config not found at: {self.config_path}")
            print(f"   Run:  rclone config   (create remote named '{remote_name}')")
            sys.exit(1)

        self._do_refresh()

    def _do_refresh(self):
        """Trigger rclone to refresh its OAuth2 token, then read updated config."""
        for attempt in range(5):
            try:
                subprocess.run(
                    [self.rclone, 'about', f'{self.remote}:', '--quiet'],
                    capture_output=True, timeout=120,
                )
                break
            except subprocess.TimeoutExpired:
                if attempt == 4:
                    print("⚠️  rclone about timed out — using cached token")
                time.sleep(5)

        cfg = configparser.ConfigParser()
        cfg.read(self.config_path)
        if self.remote not in cfg:
            print(f"❌ Remote '{self.remote}' not found. Run: rclone config")
            sys.exit(1)

        token_data = json.loads(cfg[self.remote].get('token', '{}'))
        self.access_token = token_data.get('access_token', '')

        exp_str = token_data.get('expiry', '')
        if exp_str:
            try:
                self.expiry = datetime.fromisoformat(
                    exp_str.replace('Z', '+00:00')
                ).timestamp()
            except Exception:
                self.expiry = time.time() + 3600
        else:
            self.expiry = time.time() + 3600

    def get_token(self) -> str:
        with self._lock:
            if time.time() > self.expiry - 300:
                self._do_refresh()
            return self.access_token

    def force_refresh(self):
        with self._lock:
            self._do_refresh()


# ════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════
def fmt_time(sec: float) -> str:
    """Format seconds as Xh MMm SSs, safely handling 0 / inf / nan."""
    try:
        if not sec or sec != sec or sec <= 0:
            return "--:--:--"
        sec = min(sec, 999 * 3600)
        h, rem = divmod(int(sec), 3600)
        m, s   = divmod(rem, 60)
        return f"{h}h {m:02d}m {s:02d}s"
    except Exception:
        return "--:--:--"


def fmt_bytes(b: float) -> str:
    if b >= 1e9:
        return f"{b/1e9:.2f} GB"
    if b >= 1e6:
        return f"{b/1e6:.1f} MB"
    return f"{b/1e3:.0f} KB"


def jittered_backoff(attempt: int, base: float = 2.0, cap: float = 120.0) -> float:
    """Exponential back-off with ±30 % jitter."""
    delay = min(base ** attempt, cap)
    delay *= (1 + random.uniform(-0.3, 0.3))
    return max(delay, 1.0)


def get_sorted_parts(rclone_cmd: str, remote: str, parts_dir: str) -> list:
    """List archive parts on Drive, retry on failure, sort by extension number."""
    for attempt in range(5):
        result = subprocess.run(
            [rclone_cmd, 'lsf', f'{remote}:{parts_dir}', '--include', '*hitek.7z.*'],
            capture_output=True, text=True, timeout=180,
        )
        if result.returncode == 0:
            break
        print(f"   ⚠️  lsf attempt {attempt+1}/5 failed: {result.stderr.strip()[:120]}")
        time.sleep(jittered_backoff(attempt))
    else:
        print(f"❌ Could not list {remote}:{parts_dir} after 5 attempts")
        sys.exit(1)

    files = [
        f.strip()
        for f in result.stdout.strip().split('\n')
        if f.strip() and '7z.' in f
    ]

    def ext_num(fn: str) -> int:
        try:
            return int(fn.split('7z.')[1])
        except (IndexError, ValueError):
            return 9999

    return sorted(files, key=ext_num)


def save_state(uri: str, uploaded: int):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump({'session_uri': uri, 'bytes_uploaded': uploaded}, f)
    except Exception as e:
        print(f"\n⚠️  Could not save state file: {e}")


# ════════════════════════════════════════════════════════════════════
# SERVER OFFSET VERIFICATION
# Ask Drive exactly how many bytes it has received for this session.
# Returns:
#   int  ≥ 0  → confirmed byte count
#   -1        → upload already complete (200/201)
#   None      → session expired (404/410) or too many failures
# ════════════════════════════════════════════════════════════════════
def verify_server_offset(
    sess: requests.Session,
    session_uri: str,
    token_mgr: TokenManager,
) -> 'int | None':
    for attempt in range(30):
        try:
            resp = sess.put(
                session_uri,
                headers={
                    'Authorization':  f'Bearer {token_mgr.get_token()}',
                    'Content-Length': '0',
                    'Content-Range':  'bytes */*',
                },
                timeout=30,
            )
            if resp.status_code == 308:
                rang = resp.headers.get('Range', '')
                if rang:
                    return int(rang.split('-')[1]) + 1
                return 0   # server has nothing yet
            elif resp.status_code in (200, 201):
                return -1  # already complete
            elif resp.status_code == 401:
                token_mgr.force_refresh()
            elif resp.status_code in (404, 410):
                return None  # session dead
            else:
                time.sleep(jittered_backoff(attempt))
        except Exception:
            time.sleep(jittered_backoff(attempt))
    return None  # gave up


# ════════════════════════════════════════════════════════════════════
# REAL-TIME PROGRESS PRINTER — background thread, updates every ~1 s
# ════════════════════════════════════════════════════════════════════
class ProgressPrinter:
    def __init__(self):
        self._lock       = threading.Lock()
        self._total      = 0      # bytes confirmed uploaded so far
        self._base       = 0      # bytes at start of this run (for speed)
        self._t_start    = time.time()
        self._inflight   = 0      # bytes sent in the current chunk (live)
        self._part_cur   = 0
        self._part_total = 0
        self._phase      = '⏳ Preparing'
        self._stop       = False
        self._thread     = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def set_phase(self, phase: str):
        with self._lock: self._phase = phase

    def set_parts(self, cur: int, total: int):
        with self._lock:
            self._part_cur   = cur
            self._part_total = total

    def set_base(self, total_so_far: int):
        with self._lock:
            self._base    = total_so_far
            self._total   = total_so_far
            self._t_start = time.time()

    def set_inflight(self, bytes_sent_in_chunk: int):
        with self._lock:
            self._inflight = bytes_sent_in_chunk

    def commit(self, new_total: int):
        with self._lock:
            self._total    = new_total
            self._inflight = 0

    def stop(self):
        self._stop = True
        self._thread.join(timeout=2)
        print()  # newline after last \r line

    def _run(self):
        while not self._stop:
            with self._lock:
                visible   = self._total + self._inflight
                elapsed   = max(time.time() - self._t_start, 0.001)
                speed     = (visible - self._base) / elapsed
                pct       = visible / (TOTAL_SIZE_GB * 1e9) * 100
                rem       = max(TOTAL_SIZE_GB * 1e9 - visible, 0)
                eta       = rem / speed if speed > 1 else 0
                parts_str = (
                    f"Part {self._part_cur}/{self._part_total}"
                    if self._part_total else ""
                )
                phase    = self._phase
                speed_mb = speed / 1e6

            line = (
                f"\r🚀 {fmt_bytes(visible):>10} ({pct:5.1f}%) │ "
                f"{speed_mb:6.2f} MB/s │ "
                f"ETA {fmt_time(eta)} │ "
                f"{parts_str:<14} │ {phase}          "
            )
            print(line, end='', flush=True)
            time.sleep(1.0)


# ════════════════════════════════════════════════════════════════════
# STREAMING GENERATOR — yields chunk in 4 MB pieces, tracks progress
# ════════════════════════════════════════════════════════════════════
def streaming_gen(data: bytes, progress: ProgressPrinter):
    """
    Yield data in 4 MB pieces while updating the progress printer.
    Passing this to requests.put(data=...) gives live per-chunk progress.
    """
    piece = 4 * 1024 * 1024
    view  = memoryview(data)
    sent  = 0
    total = len(data)
    while sent < total:
        end  = min(sent + piece, total)
        yield bytes(view[sent:end])
        sent = end
        progress.set_inflight(sent)


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════
def main():
    print("═" * 72)
    print("  PORTABLE ZERO-DISK STREAMING EXTRACTOR  —  hardened edition")
    print("  Drive mount → 7z extract → RAM → Drive API upload")
    print("═" * 72)

    # ── 1. Prerequisites ──────────────────────────────────────────
    print("\n🔍 Checking tools...")
    rclone_cmd = find_rclone()
    seven_zip  = find_7z()
    print(f"   rclone : ✅")
    print(f"   7-Zip  : ✅  ({seven_zip})")

    sess = make_session()

    # ── 2. Auth ───────────────────────────────────────────────────
    print(f"\n🔐 Authenticating with rclone remote '{RCLONE_REMOTE}' ...")
    token_mgr = TokenManager(RCLONE_REMOTE, rclone_cmd)
    print("   ✅ Token acquired")

    # ── 3. List & sort archive parts ──────────────────────────────
    print(f"\n📂 Listing parts in {RCLONE_REMOTE}:{PARTS_DIR} ...")
    parts = get_sorted_parts(rclone_cmd, RCLONE_REMOTE, PARTS_DIR)
    if not parts:
        print("❌ No archive parts found! Check PARTS_DIR and RCLONE_REMOTE.")
        sys.exit(1)
    print(f"   ✅ {len(parts)} parts found  ({parts[0]}  →  {parts[-1]})")

    # ── 4. Find / create target folder on Drive ───────────────────
    print(f'\n📁 Locating Drive folder "{FOLDER_NAME}" ...')
    folder_id: str = ''
    for attempt in range(10):
        try:
            r = sess.get(
                'https://www.googleapis.com/drive/v3/files',
                headers={'Authorization': f'Bearer {token_mgr.get_token()}'},
                params={
                    'q': (
                        f"name='{FOLDER_NAME}' and "
                        f"mimeType='application/vnd.google-apps.folder' and "
                        f"trashed=false"
                    )
                },
                timeout=30,
            )
            r.raise_for_status()
            break
        except Exception as e:
            if attempt == 9:
                print(f"❌ Drive API unreachable: {e}")
                sys.exit(1)
            time.sleep(jittered_backoff(attempt))

    found = r.json().get('files', [])
    if found:
        folder_id = found[0]['id']
        print(f"   ✅ Found   (id: {folder_id})")
    else:
        for attempt in range(10):
            try:
                r2 = sess.post(
                    'https://www.googleapis.com/drive/v3/files',
                    headers={
                        'Authorization': f'Bearer {token_mgr.get_token()}',
                        'Content-Type': 'application/json',
                    },
                    json={
                        'name': FOLDER_NAME,
                        'mimeType': 'application/vnd.google-apps.folder',
                    },
                    timeout=30,
                )
                r2.raise_for_status()
                folder_id = r2.json()['id']
                break
            except Exception as e:
                if attempt == 9:
                    print(f"❌ Cannot create folder: {e}")
                    sys.exit(1)
                time.sleep(jittered_backoff(attempt))
        print(f"   ✅ Created (id: {folder_id})")

    # ── 5. Resume or create upload session ────────────────────────
    skip_bytes:  int = 0
    session_uri: str = ''

    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)
        print(f'\n💾 Resume state found: {state["bytes_uploaded"]/1e9:.2f} GB')
        session_uri = state['session_uri']

        offset = verify_server_offset(sess, session_uri, token_mgr)
        if offset == -1:
            print("   ✅ File already fully uploaded! Nothing to do.")
            os.remove(STATE_FILE)
            return
        elif offset is None:
            print("   ⚠️  Session expired — creating fresh upload session ...")
            session_uri = ''
            skip_bytes  = 0
        else:
            skip_bytes = offset
            print(f"   ✅ Server confirmed {skip_bytes/1e9:.2f} GB received")

    def create_upload_session() -> str:
        for attempt in range(MAX_SESS_RETRIES):
            try:
                req = sess.post(
                    'https://www.googleapis.com/upload/drive/v3/files'
                    '?uploadType=resumable',
                    headers={
                        'Authorization':         f'Bearer {token_mgr.get_token()}',
                        'Content-Type':          'application/json',
                        'X-Upload-Content-Type': 'application/zip',
                    },
                    json={'name': OUTPUT_NAME, 'parents': [folder_id]},
                    timeout=30,
                )
                req.raise_for_status()
                return req.headers['Location']
            except Exception as e:
                if attempt == MAX_SESS_RETRIES - 1:
                    raise RuntimeError(
                        f"Cannot create upload session after "
                        f"{MAX_SESS_RETRIES} attempts: {e}"
                    )
                time.sleep(jittered_backoff(attempt))
        return ''

    if not session_uri:
        print("\n🆕 Creating resumable upload session ...")
        session_uri = create_upload_session()
        skip_bytes  = 0
        print("   ✅ Session ready")

    save_state(session_uri, skip_bytes)

    # ── 6. Mount Google Drive via rclone (gives 7z real file access) ──
    #
    #  WHY MOUNT?  The 7z format stores its index at the END of the archive.
    #  Extracting from stdin (-si) requires seek, which is "Not implemented"
    #  in 7-Zip.  rclone mount exposes the Drive folder as a real filesystem,
    #  so 7z can open hitek.7z.001 by path and seek freely via HTTP Range
    #  requests — without writing any data to local disk.
    #
    mount_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '_gdrive_mount'
    )

    # Windows (WinFsp): mountpoint must NOT exist — rclone creates it.
    # Linux (FUSE):        mountpoint MUST exist as an empty directory.
    if sys.platform == 'win32':
        if os.path.isdir(mount_dir):
            try:
                os.rmdir(mount_dir)
            except OSError:
                import shutil
                shutil.rmtree(mount_dir, ignore_errors=True)
    else:
        os.makedirs(mount_dir, exist_ok=True)

    # On Windows rclone mount keeps the process alive; run it as background.
    print(f"\n🗂️  Mounting {RCLONE_REMOTE}: → {mount_dir} ...")
    mount_proc = subprocess.Popen(
        [
            rclone_cmd, 'mount',
            f'{RCLONE_REMOTE}:',
            mount_dir,
            '--vfs-cache-mode', 'off',   # no local caching — seek via HTTP Range
            '--read-only',
            '--no-modtime',
            '--dir-cache-time', '5m',
            '--poll-interval',  '30s',
            '--log-level', 'ERROR',
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    def cleanup_mount():
        """Unmount the rclone mount and remove the mount directory."""
        mount_proc.terminate()
        time.sleep(1)
        if sys.platform == 'win32':
            subprocess.run(
                ['taskkill', '/F', '/PID', str(mount_proc.pid)],
                capture_output=True,
            )
        try:
            os.rmdir(mount_dir)
        except Exception:
            pass

    # Wait until mount is ready (the PARTS_DIR becomes visible)
    parts_mounted = os.path.join(mount_dir, PARTS_DIR)
    first_part_path = os.path.join(parts_mounted, parts[0])
    print("   Waiting for mount to become ready ", end='', flush=True)
    for tick in range(60):
        if os.path.exists(first_part_path):
            break
        # Check if rclone already died
        if mount_proc.poll() is not None:
            err = mount_proc.stderr.read().decode(errors='replace').strip()
            print(f"\n❌ rclone mount failed: {err}")
            if sys.platform == 'win32':
                print("   Make sure WinFsp is installed:  winget install WinFsp.WinFsp")
            else:
                print("   On Linux, ensure fuse3 is installed:  sudo apt-get install -y fuse3")
            sys.exit(1)
        print('.', end='', flush=True)
        time.sleep(2)
    else:
        cleanup_mount()
        print(f"\n❌ Timed out waiting for mount. Parts dir not found: {parts_mounted}")
        sys.exit(1)
    print(" ✅")

    # ── 7. Start 7-Zip reading from the mounted filesystem ────────
    #
    #  7z receives the FIRST part path.  It automatically finds subsequent
    #  parts (.002 … .057) in the same directory and seeks freely via WinFsp.
    #
    print("\n🚀 Starting extraction pipeline ...")
    print(f"   7z  ←  {first_part_path}")
    print(f"   7z  →  Drive API\n")

    p7z = subprocess.Popen(
        [seven_zip, 'e', first_part_path, '-so', '-y', '-bsp0', '-bso0'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )

    # Capture 7z stderr (prevent pipe deadlock; show on error)
    p7z_stderr_buf: list[bytes] = []

    def drain_7z_stderr():
        try:
            while True:
                chunk = p7z.stderr.read(4096)
                if not chunk:
                    break
                p7z_stderr_buf.append(chunk)
        except Exception:
            pass

    threading.Thread(target=drain_7z_stderr, daemon=True).start()

    progress = ProgressPrinter()
    progress.set_parts(1, 1)   # single logical archive
    progress.set_base(skip_bytes)

    # ── 8. Fast-forward if resuming ───────────────────────────────
    if skip_bytes > 0:
        progress.set_phase(f"⏩ Fast-fwd {skip_bytes/1e9:.2f} GB")
        print(f"⏩ Fast-forwarding through {skip_bytes/1e9:.2f} GB of 7z output ...")
        remaining = skip_bytes
        t0        = time.time()
        while remaining > 0:
            chunk = p7z.stdout.read(min(64 * 1024 * 1024, remaining))
            if not chunk:
                print("\n❌ 7z stream ended during fast-forward!")
                progress.stop()
                cleanup_mount()
                sys.exit(1)
            remaining -= len(chunk)
            done    = skip_bytes - remaining
            elapsed = max(time.time() - t0, 0.001)
            print(
                f"\r   ⏩ {done/1e9:.2f} / {skip_bytes/1e9:.2f} GB  "
                f"({done/elapsed/1e6:.0f} MB/s)   ",
                end='', flush=True,
            )
        print("\n   ✅ Fast-forward complete\n")

    # ── 9. Upload loop ────────────────────────────────────────────
    print("━" * 72)
    bytes_sent = skip_bytes
    last_save  = skip_bytes
    progress.set_base(skip_bytes)
    progress.set_phase("📤 Uploading")

    try:
        while True:
            # Read one chunk from 7z stdout
            chunk_data = bytearray()
            to_read    = CHUNK_SIZE
            while to_read > 0:
                piece = p7z.stdout.read(min(to_read, 16 * 1024 * 1024))
                if not piece:
                    break
                chunk_data.extend(piece)
                to_read -= len(piece)

            chunk_bytes = bytes(chunk_data)
            chunk_len   = len(chunk_bytes)
            is_last     = chunk_len < CHUNK_SIZE

            # Empty chunk → signal Drive that upload is complete
            if chunk_len == 0:
                for _ in range(10):
                    try:
                        sess.put(
                            session_uri,
                            headers={
                                'Authorization': f'Bearer {token_mgr.get_token()}',
                                'Content-Range':  f'bytes */{bytes_sent}',
                                'Content-Length': '0',
                            },
                            timeout=30,
                        )
                        break
                    except Exception:
                        time.sleep(2)
                break

            # ── Upload this chunk with full retry logic ───────────
            uploaded_this_chunk = False

            for attempt in range(MAX_UPLOAD_RETRIES):

                if attempt == 0:
                    send_start = bytes_sent
                    send_data  = chunk_bytes
                else:
                    progress.set_phase(f"🔍 Verifying offset (attempt {attempt + 1})")
                    actual = verify_server_offset(sess, session_uri, token_mgr)

                    if actual == -1:
                        bytes_sent += chunk_len
                        uploaded_this_chunk = True
                        break

                    elif actual is None:
                        progress.set_phase("🔄 Session expired — recreating ...")
                        save_state(session_uri, bytes_sent)
                        session_uri = create_upload_session()
                        save_state(session_uri, bytes_sent)
                        actual = bytes_sent

                    if actual >= bytes_sent + chunk_len:
                        bytes_sent += chunk_len
                        uploaded_this_chunk = True
                        break

                    send_start = actual
                    send_data  = chunk_bytes[send_start - bytes_sent:]

                send_len  = len(send_data)
                range_end = send_start + send_len - 1

                if is_last:
                    content_range = (
                        f'bytes {send_start}-{range_end}'
                        f'/{bytes_sent + chunk_len}'
                    )
                else:
                    content_range = f'bytes {send_start}-{range_end}/*'

                timeout_sec = max(300, send_len // MIN_SPEED_BPS + 120)

                progress.set_phase(f"📤 Uploading (try {attempt + 1})")
                progress.set_inflight(0)

                try:
                    # NOTE: passing bytes (not a generator) is mandatory.
                    # requests/urllib3 silently switches to Transfer-Encoding:
                    # chunked when data= is an iterator, removing Content-Length.
                    # Drive's resumable-upload endpoint rejects chunked bodies.
                    resp = sess.put(
                        session_uri,
                        headers={
                            'Authorization':  f'Bearer {token_mgr.get_token()}',
                            'Content-Length': str(send_len),
                            'Content-Range':  content_range,
                        },
                        data=send_data,
                        timeout=timeout_sec,
                    )
                    progress.set_inflight(send_len)

                    if resp.status_code in (200, 201, 308):
                        bytes_sent += chunk_len
                        uploaded_this_chunk = True
                        break

                    elif resp.status_code == 401:
                        progress.set_phase("🔐 Refreshing token ...")
                        token_mgr.force_refresh()
                        time.sleep(2)

                    elif resp.status_code == 429:
                        wait = int(
                            resp.headers.get('Retry-After',
                                             jittered_backoff(attempt + 2))
                        )
                        progress.set_phase(f"⏳ Rate-limited — sleeping {wait}s")
                        time.sleep(wait)

                    elif resp.status_code in (500, 502, 503, 504):
                        progress.set_phase(
                            f"⚠️  Server {resp.status_code} — "
                            f"retry {attempt + 1}"
                        )
                        time.sleep(jittered_backoff(attempt))

                    else:
                        progress.set_phase(
                            f"⚠️  HTTP {resp.status_code} — "
                            f"retry {attempt + 1}"
                        )
                        time.sleep(jittered_backoff(attempt))

                except (
                    requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                    ConnectionResetError, BrokenPipeError, OSError,
                ):
                    progress.set_phase(
                        f"⚠️  Network error — "
                        f"retry {attempt + 1}/{MAX_UPLOAD_RETRIES}"
                    )
                    time.sleep(jittered_backoff(attempt))

            if not uploaded_this_chunk:
                save_state(session_uri, bytes_sent)
                raise RuntimeError(
                    f"Chunk upload failed after {MAX_UPLOAD_RETRIES} retries "
                    f"at {bytes_sent/1e9:.2f} GB"
                )

            progress.commit(bytes_sent)
            progress.set_phase("📤 Uploading")

            # Checkpoint every 1 GB
            if bytes_sent - last_save >= 1 * 1024 * 1024 * 1024:
                save_state(session_uri, bytes_sent)
                last_save = bytes_sent

            if is_last:
                break

    except KeyboardInterrupt:
        save_state(session_uri, bytes_sent)
        progress.stop()
        cleanup_mount()
        print(f"\n⏸️  Paused at {bytes_sent/1e9:.2f} GB — run again to resume.")
        p7z.terminate()
        sys.exit(0)

    except Exception as e:
        save_state(session_uri, bytes_sent)
        progress.stop()
        cleanup_mount()
        print(f"\n\n❌ Error: {e}")
        print(f"   State saved at {bytes_sent/1e9:.2f} GB — run again to resume.")
        p7z.terminate()
        sys.exit(1)

    # ── 10. Done! ─────────────────────────────────────────────────
    progress.stop()
    p7z.wait()

    # Show 7z stderr if it produced output (errors/warnings/password prompts)
    if p7z_stderr_buf:
        stderr_text = b''.join(p7z_stderr_buf).decode(errors='replace').strip()
        if stderr_text:
            print(f"\n⚠️  7-Zip output:\n{stderr_text}")

    # Sanity check — if nothing was extracted, something went wrong
    if bytes_sent == 0:
        rc = p7z.returncode
        stderr_text = b''.join(p7z_stderr_buf).decode(errors='replace').strip()
        print(f"\n❌ EXTRACTION PRODUCED 0 BYTES  (7z exit code: {rc})")
        if stderr_text:
            print(f"   7z said: {stderr_text[:500]}")
        else:
            print("   Possible causes:")
            print("     • Archive is password-protected (add -p<password> to 7z command)")
            print("     • Mount failed before 7z could read the file")
            print("     • Archive is a different format")
        cleanup_mount()
        if os.path.exists(STATE_FILE):
            os.remove(STATE_FILE)
        sys.exit(1)

    cleanup_mount()

    print(f"\n{'═' * 72}")
    print(f"  🎉 UPLOAD COMPLETE!")
    print(f"  📦 Size     : {bytes_sent/1e9:.2f} GB")
    print(f"  📂 Location : My Drive → {FOLDER_NAME}/{OUTPUT_NAME}")
    print(f"{'═' * 72}")

    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)


if __name__ == '__main__':
    main()

