#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════
  ZIP EXTRACTOR — hardened edition
  Runs on Windows / macOS / Linux / GitHub Actions

  Pipeline:  rclone mount → zipfile stream → RAM → Drive API upload
  Disk usage: ~0 (reads zip via FUSE HTTP Range requests, never caches)

  What it does:
  ─────────────────────────────────────────────────────────
  • Mounts HiTek_Extracted folder from Drive via rclone
  • Opens users_data.zip using Python's zipfile (seeks via FUSE)
  • Streams each file entry (decompressed) directly to Drive
  • Uploads each file as a separate file under HiTek_Files/ on Drive
  • Skips files already marked done in state file (resume support)
  • Per-file resumable upload sessions with 20 retries + jitter
  • CI-aware logging (1 line/min in GitHub Actions, live \r in terminal)
  • Checkpoint after every file; Ctrl-C saves state cleanly

  SETUP:
  ─────────────────────────────────────────────────────────
  1. rclone configured with remote "Gdrive" (same as extract_hitek.py)
  2. Linux: sudo apt-get install -y fuse3
     Windows: WinFsp installed
  3. python3 extract_zip.py   (or add to GitHub Actions workflow)
═══════════════════════════════════════════════════════════════════════
"""

# /// script
# requires-python = ">=3.10"
# dependencies = ["requests>=2.31"]
# ///

import subprocess, os, json, time, sys, threading, configparser, random, zipfile
from datetime import datetime

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("📦 Installing 'requests' ...")
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'requests', '-q'])
    import requests
    from requests.adapters import HTTPAdapter


# ════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════
RCLONE_REMOTE   = 'Gdrive'
SOURCE_FOLDER   = 'HiTek_Extracted'       # Drive folder containing users_data.zip
ZIP_NAME        = 'users_data.zip'        # The zip file inside SOURCE_FOLDER
OUTPUT_FOLDER   = 'users_data_extracted'  # Drive folder to upload extracted files into
CHUNK_SIZE      = 512 * 1024 * 1024       # 512 MB upload chunks (fewer round trips)
MIN_SPEED_BPS   = 32 * 1024              # 32 KB/s — sets generous timeouts
MAX_UPLOAD_RETRIES = 20
MAX_SESS_RETRIES   = 15
STATE_FILE      = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'zip_state.json')

# ════════════════════════════════════════════════════════════════════
# CI DETECTION
# ════════════════════════════════════════════════════════════════════
_IS_TTY = sys.stdout.isatty() and not os.environ.get('CI')


# ════════════════════════════════════════════════════════════════════
# TOOL DETECTION
# ════════════════════════════════════════════════════════════════════
def find_rclone() -> str:
    try:
        r = subprocess.run(['rclone', 'version'], capture_output=True, text=True, timeout=15)
        if r.returncode == 0:
            return 'rclone'
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    print("❌ rclone not found!")
    print("   Linux:   curl https://rclone.org/install.sh | sudo bash")
    print("   Windows: winget install Rclone.Rclone")
    sys.exit(1)


# ════════════════════════════════════════════════════════════════════
# HTTP SESSION
# ════════════════════════════════════════════════════════════════════
def make_session() -> requests.Session:
    s = requests.Session()
    a = HTTPAdapter(pool_connections=4, pool_maxsize=4, max_retries=0)
    s.mount('https://', a)
    s.mount('http://',  a)
    return s


# ════════════════════════════════════════════════════════════════════
# TOKEN MANAGER — thread-safe, reads from rclone config
# ════════════════════════════════════════════════════════════════════
class TokenManager:
    def __init__(self, remote: str, rclone_cmd: str):
        self.remote  = remote
        self.rclone  = rclone_cmd
        self.token   = ''
        self.expiry  = 0.0
        self._lock   = threading.Lock()

        if sys.platform == 'win32':
            self.config_path = os.path.join(
                os.environ.get('APPDATA', os.path.expanduser('~')),
                'rclone', 'rclone.conf',
            )
        else:
            self.config_path = os.path.expanduser('~/.config/rclone/rclone.conf')

        if not os.path.exists(self.config_path):
            print(f"❌ rclone config not found: {self.config_path}")
            sys.exit(1)
        self._do_refresh()

    def _do_refresh(self):
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

        td = json.loads(cfg[self.remote].get('token', '{}'))
        self.token  = td.get('access_token', '')
        exp_str     = td.get('expiry', '')
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
            return self.token

    def force_refresh(self):
        with self._lock:
            self._do_refresh()


# ════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════
def fmt_time(sec: float) -> str:
    try:
        if not sec or sec != sec or sec <= 0:
            return "--:--:--"
        sec = min(sec, 999 * 3600)
        h, r = divmod(int(sec), 3600)
        m, s = divmod(r, 60)
        return f"{h}h {m:02d}m {s:02d}s"
    except Exception:
        return "--:--:--"


def fmt_bytes(b: float) -> str:
    if b >= 1e9:  return f"{b/1e9:.2f} GB"
    if b >= 1e6:  return f"{b/1e6:.1f} MB"
    return f"{b/1e3:.0f} KB"


def jittered_backoff(attempt: int, cap: float = 120.0) -> float:
    d = min(2.0 ** attempt, cap) * (1 + random.uniform(-0.3, 0.3))
    return max(d, 1.0)


def save_state(data: dict):
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"\n⚠️  Could not save state: {e}")


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


# ════════════════════════════════════════════════════════════════════
# SERVER OFFSET VERIFICATION
# Returns: int≥0 (bytes received), -1 (complete), None (session dead)
# ════════════════════════════════════════════════════════════════════
def verify_server_offset(
    sess, uri: str, token_mgr: TokenManager,
    expected_total: int = 0,
):
    # 10 attempts is plenty; 30 × 120 s = 56-minute freeze is too long.
    for attempt in range(10):
        try:
            r = sess.put(
                uri,
                headers={
                    'Authorization':  f'Bearer {token_mgr.get_token()}',
                    'Content-Length': '0',
                    'Content-Range':  'bytes */*',
                },
                timeout=30,
            )
            if r.status_code == 308:
                rang = r.headers.get('Range', '')
                if rang:
                    offset = int(rang.split('-')[1]) + 1
                    # Drive confirmed it has every byte — treat as complete.
                    if expected_total and offset >= expected_total:
                        return -1
                    return offset
                return 0
            elif r.status_code in (200, 201):
                return -1
            elif r.status_code == 401:
                token_mgr.force_refresh()
            elif r.status_code in (404, 410):
                return None
            else:
                time.sleep(jittered_backoff(attempt))
        except Exception:
            time.sleep(jittered_backoff(attempt))
    return None


# ════════════════════════════════════════════════════════════════════
# PROGRESS PRINTER — CI-aware background thread
# ════════════════════════════════════════════════════════════════════
class ProgressPrinter:
    def __init__(self, total_bytes: int):
        self._lock          = threading.Lock()
        self._total_bytes   = total_bytes     # grand total uncompressed
        self._done_bytes    = 0               # bytes fully uploaded (all prev files)
        self._file_size     = 0               # uncompressed size of current file
        self._file_done     = 0               # bytes uploaded in current file
        self._t_start       = time.time()
        self._base          = 0               # bytes at start of this run (for speed)
        self._phase         = '⏳ Preparing'
        self._filename      = ''
        self._file_idx      = 0
        self._file_total    = 0
        self._stop          = False
        self._thread        = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def set_phase(self, p: str):
        with self._lock: self._phase = p

    def set_file(self, idx: int, total: int, name: str, size: int, done_so_far: int):
        with self._lock:
            self._file_idx   = idx
            self._file_total = total
            self._filename   = os.path.basename(name)[:30]
            self._file_size  = size
            self._file_done  = 0
            self._done_bytes = done_so_far

    def set_base(self, b: int):
        with self._lock:
            self._base    = b
            self._t_start = time.time()

    def set_file_progress(self, b: int):
        with self._lock: self._file_done = b

    def commit_file(self):
        with self._lock:
            self._done_bytes += self._file_size
            self._file_done   = 0

    def stop(self):
        self._stop = True
        self._thread.join(timeout=2)
        if _IS_TTY:
            print()

    def _run(self):
        interval     = 1.0 if _IS_TTY else 60.0
        last_printed = 0.0
        while not self._stop:
            now = time.time()
            if now - last_printed >= interval:
                with self._lock:
                    visible  = self._done_bytes + self._file_done
                    elapsed  = max(now - self._t_start, 0.001)
                    speed    = max(visible - self._base, 0) / elapsed
                    pct      = visible / self._total_bytes * 100 if self._total_bytes else 0
                    rem      = max(self._total_bytes - visible, 0)
                    eta      = rem / speed if speed > 1 else 0
                    phase    = self._phase
                    fname    = self._filename
                    fidx     = self._file_idx
                    ftot     = self._file_total
                    spd_mb   = speed / 1e6

                if _IS_TTY:
                    line = (
                        f"\r📦 {fmt_bytes(visible):>10} ({pct:5.1f}%) │ "
                        f"{spd_mb:6.2f} MB/s │ ETA {fmt_time(eta)} │ "
                        f"[{fidx}/{ftot}] {fname:<30} │ {phase}   "
                    )
                    print(line, end='', flush=True)
                else:
                    ts = time.strftime('%H:%M:%S')
                    line = (
                        f"[{ts}] 📦 {fmt_bytes(visible):>10} ({pct:5.1f}%) │ "
                        f"{spd_mb:6.2f} MB/s │ ETA {fmt_time(eta)} │ "
                        f"[{fidx}/{ftot}] {fname} │ {phase}"
                    )
                    print(line, flush=True)
                last_printed = now
            time.sleep(1.0)


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════
def main():
    print("═" * 72)
    print("  ZIP EXTRACTOR  —  hardened edition")
    print("  rclone mount → zipfile stream → RAM → Drive API upload")
    print("═" * 72)

    # ── 1. Tools ──────────────────────────────────────────────────
    print("\n🔍 Checking tools...")
    rclone_cmd = find_rclone()
    print("   rclone : ✅")

    sess = make_session()

    # ── 2. Auth ───────────────────────────────────────────────────
    print(f"\n🔐 Authenticating with rclone remote '{RCLONE_REMOTE}' ...")
    token_mgr = TokenManager(RCLONE_REMOTE, rclone_cmd)
    print("   ✅ Token acquired")

    # ── 3. Find / create output folder ───────────────────────────
    print(f'\n📁 Locating Drive folder "{OUTPUT_FOLDER}" ...')
    folder_id = ''
    for attempt in range(10):
        try:
            r = sess.get(
                'https://www.googleapis.com/drive/v3/files',
                headers={'Authorization': f'Bearer {token_mgr.get_token()}'},
                params={
                    'q': (
                        f"name='{OUTPUT_FOLDER}' and "
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
                        'name': OUTPUT_FOLDER,
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

    # ── 4. Mount the SOURCE_FOLDER so zipfile can seek freely ─────
    mount_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '_zip_mount'
    )

    if sys.platform == 'win32':
        if os.path.isdir(mount_dir):
            try:
                os.rmdir(mount_dir)
            except OSError:
                import shutil
                shutil.rmtree(mount_dir, ignore_errors=True)
    else:
        os.makedirs(mount_dir, exist_ok=True)

    print(f"\n🗂️  Mounting {RCLONE_REMOTE}:{SOURCE_FOLDER} → {mount_dir} ...")
    mount_proc = subprocess.Popen(
        [
            rclone_cmd, 'mount',
            f'{RCLONE_REMOTE}:{SOURCE_FOLDER}',
            mount_dir,
            '--vfs-cache-mode', 'off',
            '--read-only',
            '--no-modtime',
            '--dir-cache-time', '5m',            '--buffer-size',    '256M',   # read-ahead buffer for sequential reads
            '--vfs-read-ahead', '512M',   # prefetch window for FUSE reads            '--log-level', 'ERROR',
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    def cleanup_mount():
        mount_proc.terminate()
        time.sleep(1)
        if sys.platform == 'win32':
            subprocess.run(
                ['taskkill', '/F', '/PID', str(mount_proc.pid)],
                capture_output=True,
            )
        else:
            # FUSE3 uses fusermount3; fall back to fusermount for older systems.
            for cmd in ['fusermount3', 'fusermount']:
                try:
                    r = subprocess.run([cmd, '-uz', mount_dir],
                                       capture_output=True, timeout=10)
                    if r.returncode == 0:
                        break
                except Exception:
                    continue
        try:
            os.rmdir(mount_dir)
        except Exception:
            pass

    zip_path = os.path.join(mount_dir, ZIP_NAME)
    print("   Waiting for mount to become ready ", end='', flush=True)
    for _ in range(60):
        if os.path.exists(zip_path):
            break
        if mount_proc.poll() is not None:
            err = mount_proc.stderr.read().decode(errors='replace').strip()
            print(f"\n❌ rclone mount failed: {err}")
            if sys.platform != 'win32':
                print("   Ensure fuse3 is installed: sudo apt-get install -y fuse3")
            sys.exit(1)
        print('.', end='', flush=True)
        time.sleep(2)
    else:
        cleanup_mount()
        print(f"\n❌ Timed out — zip not visible at: {zip_path}")
        sys.exit(1)
    print(" ✅")

    # ── 5. Open zip and inventory entries ─────────────────────────
    print(f"\n📂 Opening {ZIP_NAME} ...")
    try:
        zf = zipfile.ZipFile(zip_path, 'r')
    except Exception as e:
        cleanup_mount()
        print(f"❌ Cannot open zip: {e}")
        sys.exit(1)

    all_entries = [e for e in zf.infolist() if not e.filename.endswith('/')]
    total_uncompressed = sum(e.file_size for e in all_entries)
    print(f"   ✅ {len(all_entries)} files found")
    print(f"   📦 Total uncompressed: {total_uncompressed/1e9:.2f} GB")

    # ── 6. Load resume state ──────────────────────────────────────
    state = load_state()
    completed: set = set(state.get('completed_files', []))
    current_uri: str  = state.get('current_session_uri', '')
    current_file: str = state.get('current_file', '')
    current_bytes: int = state.get('current_bytes_uploaded', 0)

    remaining = [e for e in all_entries if e.filename not in completed]
    done_bytes = sum(e.file_size for e in all_entries if e.filename in completed)

    print(f"\n📊 Progress: {len(completed)}/{len(all_entries)} files done "
          f"({done_bytes/1e9:.2f} GB / {total_uncompressed/1e9:.2f} GB)")

    if not remaining:
        print("✅ All files already uploaded!")
        zf.close()
        cleanup_mount()
        return

    # ── 7. Upload loop ────────────────────────────────────────────
    progress = ProgressPrinter(total_uncompressed)
    progress.set_base(done_bytes)

    def create_session(entry: zipfile.ZipInfo, mime: str = 'application/octet-stream') -> str:
        """Create a Drive resumable upload session for one file."""
        file_name = os.path.basename(entry.filename) or entry.filename.replace('/', '_')
        for attempt in range(MAX_SESS_RETRIES):
            try:
                r = sess.post(
                    'https://www.googleapis.com/upload/drive/v3/files'
                    '?uploadType=resumable',
                    headers={
                        'Authorization':         f'Bearer {token_mgr.get_token()}',
                        'Content-Type':          'application/json',
                        'X-Upload-Content-Type': mime,
                        'X-Upload-Content-Length': str(entry.file_size),
                    },
                    json={'name': file_name, 'parents': [folder_id]},
                    timeout=30,
                )
                r.raise_for_status()
                return r.headers['Location']
            except Exception as e:
                if attempt == MAX_SESS_RETRIES - 1:
                    raise RuntimeError(f"Cannot create session: {e}")
                time.sleep(jittered_backoff(attempt))
        return ''

    def file_exists_on_drive(name: str) -> bool:
        """Return True if a file with this name already exists in the output folder."""
        file_name = os.path.basename(name) or name.replace('/', '_')
        # Escape single quotes to avoid breaking the Drive API query syntax.
        safe_name = file_name.replace("'", "\\'")
        for attempt in range(5):
            try:
                r = sess.get(
                    'https://www.googleapis.com/drive/v3/files',
                    headers={'Authorization': f'Bearer {token_mgr.get_token()}'},
                    params={
                        'q': (
                            f"name='{safe_name}' and "
                            f"'{folder_id}' in parents and "
                            f"trashed=false"
                        ),
                        'fields': 'files(id,size)',
                    },
                    timeout=30,
                )
                r.raise_for_status()
                files = r.json().get('files', [])
                return len(files) > 0
            except Exception:
                time.sleep(jittered_backoff(attempt))
        return False

    print("\n" + "━" * 72)

    try:
        for file_idx, entry in enumerate(remaining, start=len(completed) + 1):
            fname       = entry.filename
            file_size   = entry.file_size
            short_name  = os.path.basename(fname) or fname

            progress.set_file(file_idx, len(all_entries), fname, file_size, done_bytes)
            progress.set_phase("📤 Uploading")

            if not _IS_TTY:
                ts = time.strftime('%H:%M:%S')
                print(f"[{ts}] 📄 [{file_idx}/{len(all_entries)}] {short_name}  "
                      f"({file_size/1e6:.1f} MB)", flush=True)

            # Check if already on Drive (handles case where session expired
            # before the script could mark the file as complete).
            if file_exists_on_drive(fname):
                if not _IS_TTY:
                    ts = time.strftime('%H:%M:%S')
                    print(f"[{ts}] ⏭️  Skipping (already on Drive): {short_name}", flush=True)
                completed.add(fname)
                done_bytes += file_size
                save_state({
                    'completed_files':        list(completed),
                    'current_file':           '',
                    'current_session_uri':    '',
                    'current_bytes_uploaded': 0,
                })
                progress.commit_file()
                continue

            # Resume within this file if state matches
            session_uri  = ''
            skip_bytes   = 0
            if fname == current_file and current_uri:
                offset = verify_server_offset(sess, current_uri, token_mgr)
                if offset == -1:
                    # Already complete — mark done
                    completed.add(fname)
                    done_bytes += file_size
                    save_state({
                        'completed_files':       list(completed),
                        'current_file':          '',
                        'current_session_uri':   '',
                        'current_bytes_uploaded': 0,
                    })
                    progress.commit_file()
                    continue
                elif offset is not None and offset > 0:
                    # Session still alive — resume from Drive's confirmed offset
                    session_uri = current_uri
                    skip_bytes  = offset
                    progress.set_phase(f"⏩ Resuming from {fmt_bytes(skip_bytes)}")
                elif offset is None:
                    # Session expired — Drive sessions cannot be transferred to a
                    # new session URI.  A new session always starts at offset 0.
                    # We must re-upload the whole file from the beginning.
                    # (file_exists_on_drive above handles the case where it already
                    # completed before the script could mark it done.)
                    if not _IS_TTY:
                        ts = time.strftime('%H:%M:%S')
                        print(f"[{ts}] ⚠️  Session expired — restarting file from 0",
                              flush=True)
                    session_uri = ''   # fall through to create fresh session below
                    skip_bytes  = 0

            if not session_uri:
                # Fresh upload — no prior state for this file
                session_uri = create_session(entry)
                skip_bytes  = 0

            save_state({
                'completed_files':        list(completed),
                'current_file':           fname,
                'current_session_uri':    session_uri,
                'current_bytes_uploaded': skip_bytes,
            })

            # Stream the file from the zip in CHUNK_SIZE pieces
            bytes_sent = skip_bytes
            with zf.open(entry) as zstream:
                # Fast-forward to resume point
                if skip_bytes > 0:
                    remaining_ff = skip_bytes
                    while remaining_ff > 0:
                        piece = zstream.read(min(16 * 1024 * 1024, remaining_ff))
                        if not piece:
                            break
                        remaining_ff -= len(piece)

                while True:
                    # Read one chunk
                    chunk_data = bytearray()
                    to_read    = CHUNK_SIZE
                    while to_read > 0:
                        piece = zstream.read(min(to_read, 16 * 1024 * 1024))
                        if not piece:
                            break
                        chunk_data.extend(piece)
                        to_read -= len(piece)

                    chunk_bytes = bytes(chunk_data)
                    chunk_len   = len(chunk_bytes)
                    if chunk_len == 0:
                        break

                    # Detect end-of-stream by actual read size, not metadata.
                    # entry.file_size can differ from actual decompressed bytes
                    # (zip64 rounding, streaming zip, etc.).  If we use
                    # file_size as the Content-Range total and it's wrong,
                    # Drive rejects the request silently.
                    is_last = chunk_len < CHUNK_SIZE

                    # Upload this chunk with retries
                    uploaded = False
                    for attempt in range(MAX_UPLOAD_RETRIES):
                        if attempt == 0:
                            send_start = bytes_sent
                            send_data  = chunk_bytes
                        else:
                            progress.set_phase(f"🔍 Verifying offset (try {attempt+1})")
                            actual_total = bytes_sent + chunk_len if is_last else 0
                            actual = verify_server_offset(
                                sess, session_uri, token_mgr, actual_total
                            )
                            if actual == -1:
                                bytes_sent += chunk_len
                                uploaded = True
                                break
                            elif actual is None:
                                progress.set_phase("🔄 Session expired — recreating ...")
                                save_state({
                                    'completed_files':        list(completed),
                                    'current_file':           fname,
                                    'current_session_uri':    session_uri,
                                    'current_bytes_uploaded': bytes_sent,
                                })
                                session_uri = create_session(entry)
                                save_state({
                                    'completed_files':        list(completed),
                                    'current_file':           fname,
                                    'current_session_uri':    session_uri,
                                    'current_bytes_uploaded': bytes_sent,
                                })
                                actual = bytes_sent
                            if actual >= bytes_sent + chunk_len:
                                bytes_sent += chunk_len
                                uploaded = True
                                break
                            # Guard against server returning an offset behind our
                            # chunk start (should not happen, but prevents a
                            # Python negative-index slice bug if it ever does).
                            slice_from = max(0, actual - bytes_sent)
                            send_start = bytes_sent + slice_from
                            send_data  = chunk_bytes[slice_from:]

                        send_len  = len(send_data)
                        range_end = send_start + send_len - 1
                        if is_last:
                            # Use actual byte total, not zip metadata file_size.
                            actual_total = bytes_sent + chunk_len
                            content_range = f'bytes {send_start}-{range_end}/{actual_total}'
                        else:
                            content_range = f'bytes {send_start}-{range_end}/*'

                        timeout_sec = max(300, send_len // MIN_SPEED_BPS + 120)
                        progress.set_phase(f"📤 Uploading (try {attempt+1})")
                        progress.set_file_progress(bytes_sent)

                        try:
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
                            progress.set_file_progress(bytes_sent + send_len)

                            if resp.status_code in (200, 201, 308):
                                bytes_sent += chunk_len
                                uploaded = True
                                break
                            elif resp.status_code == 401:
                                token_mgr.force_refresh()
                                time.sleep(2)
                            elif resp.status_code == 429:
                                wait = int(resp.headers.get(
                                    'Retry-After', jittered_backoff(attempt + 2)))
                                progress.set_phase(f"⏳ Rate-limited {wait}s")
                                time.sleep(wait)
                            elif resp.status_code in (500, 502, 503, 504):
                                progress.set_phase(f"⚠️  Server {resp.status_code} retry {attempt+1}")
                                time.sleep(jittered_backoff(attempt))
                            else:
                                progress.set_phase(f"⚠️  HTTP {resp.status_code} retry {attempt+1}")
                                time.sleep(jittered_backoff(attempt))

                        except (
                            requests.exceptions.Timeout,
                            requests.exceptions.ConnectionError,
                            requests.exceptions.ChunkedEncodingError,
                            ConnectionResetError, BrokenPipeError, OSError,
                        ):
                            progress.set_phase(
                                f"⚠️  Network error retry {attempt+1}/{MAX_UPLOAD_RETRIES}"
                            )
                            time.sleep(jittered_backoff(attempt))

                    if not uploaded:
                        save_state({
                            'completed_files':        list(completed),
                            'current_file':           fname,
                            'current_session_uri':    session_uri,
                            'current_bytes_uploaded': bytes_sent,
                        })
                        raise RuntimeError(
                            f"Chunk failed after {MAX_UPLOAD_RETRIES} retries "
                            f"in {fname} at {bytes_sent/1e6:.1f} MB"
                        )

                    save_state({
                        'completed_files':        list(completed),
                        'current_file':           fname,
                        'current_session_uri':    session_uri,
                        'current_bytes_uploaded': bytes_sent,
                    })
                    progress.set_phase("📤 Uploading")

            # File complete
            completed.add(fname)
            done_bytes += file_size
            save_state({
                'completed_files':        list(completed),
                'current_file':           '',
                'current_session_uri':    '',
                'current_bytes_uploaded': 0,
            })
            progress.commit_file()

            if not _IS_TTY:
                ts = time.strftime('%H:%M:%S')
                print(f"[{ts}] ✅ Done: {short_name}", flush=True)

    except KeyboardInterrupt:
        interrupted = True
        progress.stop()
        zf.close()
        cleanup_mount()
        print(f"\n⏸️  Paused — run again to resume.")
        sys.exit(0)

    except Exception as e:
        progress.stop()
        zf.close()
        cleanup_mount()
        print(f"\n\n❌ Error: {e}")
        print("   Run again to resume from where it stopped.")
        sys.exit(1)

    # ── 8. Done ───────────────────────────────────────────────────
    progress.stop()
    zf.close()
    cleanup_mount()

    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)

    print(f"\n{'═' * 72}")
    print(f"  🎉 EXTRACTION COMPLETE!")
    print(f"  📦 {len(all_entries)} files  ({total_uncompressed/1e9:.2f} GB uncompressed)")
    print(f"  📂 Location : My Drive → {OUTPUT_FOLDER}/")
    print(f"{'═' * 72}")


if __name__ == '__main__':
    main()
