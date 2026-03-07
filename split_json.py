#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════
  JSON SPLITTER  —  streaming split of a huge JSON array into ~1 GB parts
  Runs on GitHub Actions (Ubuntu) or any machine with rclone

  Pipeline
  ────────────────────────────────────────────────────────────────────
  rclone cat  →  ijson (yajl2_c)  →  buffer records  →  write part_NNNN.json
                                                      →  rclone copy to Drive
                                                      →  delete local file

  Each output file is a valid JSON array: [ {record}, {record}, … ]

  Why streaming?
  ────────────────────────────────────────────────────────────────────
  The source file is ~446 GB.  GitHub Actions runners have ~14 GB of
  free disk.  We can only keep ONE part on disk at a time.  As soon as
  a part reaches the target size, it is flushed, uploaded, and deleted
  before starting the next one.

  Configuration (edit the constants below):
    PART_SIZE_BYTES  — target uncompressed size per part (default 1 GB)
    RCLONE_REMOTE    — rclone remote name
    SOURCE_FOLDER    — folder on Drive containing the source file
    FILE_NAME        — source JSON filename
    DEST_FOLDER      — Drive folder to upload parts into
═══════════════════════════════════════════════════════════════════════
"""

# /// script
# requires-python = ">=3.10"
# dependencies = ["ijson>=3.2"]
# ///

import os, sys, json, time, signal, subprocess, threading, io
from pathlib import Path

# ── auto-install dependencies ─────────────────────────────────────────────────
def _pip(*pkgs):
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', *pkgs])

try:
    import ijson
except ImportError:
    print("📦 Installing ijson…"); _pip('ijson')
    import ijson

_ijson = ijson   # replaced in main() with yajl2_c backend if available


# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════
RCLONE_REMOTE   = 'Gdrive'
SOURCE_FOLDER   = 'users_data_extracted'
FILE_NAME       = 'users_data.json'
DEST_FOLDER     = 'users_data_parts'       # Drive folder for output parts

PART_SIZE_BYTES = 1 * 1024 * 1024 * 1024   # 1 GB per part
WORK_DIR        = Path(os.path.dirname(os.path.abspath(__file__)))
LOG_INTERVAL    = 60                        # seconds between progress lines


# ════════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════════
def fmt_bytes(n: float) -> str:
    for u in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(n) < 1024:
            return f'{n:.2f} {u}'
        n /= 1024
    return f'{n:.2f} PB'

def fmt_num(n: int) -> str:
    return f'{n:,}'

def fmt_dur(secs: float) -> str:
    secs = int(secs)
    h, m, s = secs // 3600, (secs % 3600) // 60, secs % 60
    return f'{h}h {m:02d}m {s:02d}s' if h else (f'{m}m {s:02d}s' if m else f'{s}s')

def ts() -> str:
    return time.strftime('%H:%M:%S')

_IS_TTY = sys.stdout.isatty() and not os.environ.get('CI')

def log(msg: str):
    prefix = f'[{ts()}] ' if not _IS_TTY else ''
    print(f'{prefix}{msg}', flush=True)

def find_rclone() -> str:
    for candidate in ['rclone']:
        try:
            if subprocess.run([candidate, 'version'], capture_output=True, timeout=15).returncode == 0:
                return candidate
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
    print('❌ rclone not found. Install: https://rclone.org/install/')
    sys.exit(1)


def _read_up_to(stream, n: int) -> bytes:
    buf = b''
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            break
        buf += chunk
    return buf


def get_file_size(rclone_cmd: str) -> int:
    try:
        r = subprocess.run(
            [rclone_cmd, 'size', '--json',
             f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}'],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode == 0:
            return json.loads(r.stdout).get('bytes', 0)
    except Exception:
        pass
    return 0


# ════════════════════════════════════════════════════════════════════════════════
# COUNTING STREAM  —  tracks bytes read for progress display
# ════════════════════════════════════════════════════════════════════════════════
class _CountingStream(io.RawIOBase):
    __slots__ = ('_src', 'bytes_read')

    def __init__(self, src):
        super().__init__()
        self._src = src
        self.bytes_read = 0

    def readable(self) -> bool:
        return True

    def read(self, n: int = -1) -> bytes:
        chunk = self._src.read(n)
        if chunk:
            self.bytes_read += len(chunk)
        return chunk or b''

    def readinto(self, b) -> int:
        data = self._src.read(len(b))
        n = len(data)
        if n:
            b[:n] = data
            self.bytes_read += n
        return n


class _PrependStream:
    __slots__ = ('_buf', '_tail')

    def __init__(self, head: bytes, tail):
        self._buf = head
        self._tail = tail

    def read(self, n: int = -1) -> bytes:
        if self._buf:
            if n < 0 or n >= len(self._buf):
                out = self._buf
                self._buf = b''
                rest = self._tail.read() if n < 0 else self._tail.read(n - len(out))
                return out + rest
            out = self._buf[:n]
            self._buf = self._buf[n:]
            return out
        return self._tail.read(n)


# ════════════════════════════════════════════════════════════════════════════════
# PART WRITER  —  buffers records and writes a valid JSON array per part
# ════════════════════════════════════════════════════════════════════════════════
class PartWriter:
    """
    Accumulates JSON records and writes them to numbered part files.

    Strategy: write each record as a compact JSON line immediately to the
    output file to minimise memory usage.  The file is opened with the
    opening '[' and closed with ']' when the part is finalised.

    Byte tracking uses the file's tell() position so the size check is
    exact (not estimated).
    """

    def __init__(self, rclone_cmd: str):
        self.rclone_cmd    = rclone_cmd
        self.part_number   = 0
        self.total_records = 0
        self.parts_written = 0
        self._fp           = None
        self._records_in_part = 0
        self._part_path    = None

    def _open_new_part(self):
        """Open a fresh part file and write the opening bracket."""
        self.part_number += 1
        fname = f'part_{self.part_number:04d}.json'
        self._part_path = WORK_DIR / fname
        self._fp = open(self._part_path, 'w', encoding='utf-8', buffering=1024 * 1024)
        self._fp.write('[\n')
        self._records_in_part = 0

    def _current_size(self) -> int:
        """Return current byte position of the open file."""
        if self._fp is None:
            return 0
        self._fp.flush()
        return self._fp.tell()

    def add_record(self, record) -> None:
        """Add one parsed record.  Automatically rolls to a new part if needed."""
        if self._fp is None:
            self._open_new_part()

        # Write separator + compact JSON line
        if self._records_in_part > 0:
            self._fp.write(',\n')
        json.dump(record, self._fp, ensure_ascii=False, separators=(',', ':'))
        self._records_in_part += 1
        self.total_records += 1

        # Check if part is full
        if self._current_size() >= PART_SIZE_BYTES:
            self._finalise_and_upload()

    def _finalise_and_upload(self):
        """Close the current part file, upload it, and delete the local copy."""
        if self._fp is None:
            return

        self._fp.write('\n]\n')
        self._fp.close()
        self._fp = None

        part_path = self._part_path
        part_size = part_path.stat().st_size
        log(f'   📦 {part_path.name}  '
            f'{fmt_num(self._records_in_part)} records  '
            f'{fmt_bytes(part_size)}')

        # Upload to Google Drive
        dest = f'{RCLONE_REMOTE}:{DEST_FOLDER}/{part_path.name}'
        log(f'   ☁️  Uploading → {dest} …')
        t0 = time.time()
        try:
            r = subprocess.run(
                [self.rclone_cmd, 'copyto', str(part_path), dest,
                 '--retries', '5',
                 '--low-level-retries', '10',
                 '--retries-sleep', '5s'],
                capture_output=True, text=True, timeout=1800,  # 30 min max per upload
            )
            elapsed = time.time() - t0
            if r.returncode == 0:
                speed = part_size / elapsed if elapsed else 0
                log(f'   ✅ Uploaded in {fmt_dur(elapsed)}  ({fmt_bytes(speed)}/s)')
            else:
                log(f'   ⚠️  Upload failed (exit {r.returncode}): {r.stderr[:300]}')
                log(f'   ⚠️  Keeping local file: {part_path}')
                self.parts_written += 1
                return
        except subprocess.TimeoutExpired:
            log(f'   ⚠️  Upload timed out after 30 min — keeping local file')
            self.parts_written += 1
            return

        # Delete local file to free disk space
        try:
            part_path.unlink()
        except OSError:
            pass

        self.parts_written += 1

    def flush(self):
        """Finalise + upload the last (potentially partial) part."""
        if self._fp is not None and self._records_in_part > 0:
            self._finalise_and_upload()
        elif self._fp is not None:
            # Empty part — just close and delete
            self._fp.close()
            self._fp = None
            try:
                self._part_path.unlink()
            except OSError:
                pass


# ════════════════════════════════════════════════════════════════════════════════
# MAIN STREAMING SPLIT
# ════════════════════════════════════════════════════════════════════════════════
def stream_and_split(rclone_cmd: str):
    remote_path = f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}'

    log(f'\n📐 Querying file size for {remote_path} …')
    file_size = get_file_size(rclone_cmd)
    if file_size:
        log(f'   {fmt_bytes(file_size)}')
        estimated_parts = max(1, file_size // PART_SIZE_BYTES)
        log(f'   Estimated parts: ~{estimated_parts} × {fmt_bytes(PART_SIZE_BYTES)}')
    else:
        log('   (size unavailable)')

    log(f'\n▶️  Streaming via rclone cat …')

    _stderr_chunks: list[bytes] = []
    proc = subprocess.Popen(
        [rclone_cmd, 'cat', remote_path, '--buffer-size', '256M'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )

    def _drain_stderr():
        try:
            while True:
                chunk = proc.stderr.read(4096)
                if not chunk:
                    break
                _stderr_chunks.append(chunk)
        except Exception:
            pass

    stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    stderr_thread.start()

    writer = PartWriter(rclone_cmd)
    start_time = time.time()
    last_log_time = start_time
    record_count = 0

    try:
        # Peek at header to detect structure
        header = _read_up_to(proc.stdout, 65536)
        stripped = header.lstrip()
        if not stripped or stripped[0:1] != b'[':
            log(f'⚠️  Expected top-level JSON array, got: {stripped[:64]!r}')
            log('   This splitter only handles top-level arrays.')
            proc.kill()
            sys.exit(1)

        counter = _CountingStream(_PrependStream(header, proc.stdout))
        log('🔍 Top-level structure: array')

        # Parse records one at a time
        try:
            items_iter = _ijson.items(counter, 'item', use_float=True)
        except TypeError:
            items_iter = _ijson.items(counter, 'item')

        for record in items_iter:
            writer.add_record(record)
            record_count += 1

            # Progress logging
            now = time.time()
            if now - last_log_time >= LOG_INTERVAL:
                elapsed = now - start_time
                pos = counter.bytes_read
                pct = pos / file_size * 100 if file_size else 0
                speed = pos / elapsed if elapsed else 0
                eta = (file_size - pos) / speed if speed else 0
                last_log_time = now
                log(f'  ⏳ {fmt_num(record_count)} records  '
                    f'part {writer.part_number}  '
                    f'{pct:.1f}%  '
                    f'{fmt_bytes(pos)} / {fmt_bytes(file_size)}  '
                    f'@ {fmt_bytes(speed)}/s  '
                    f'ETA {fmt_dur(eta)}')

    except (KeyboardInterrupt, SystemExit):
        log(f'\n⚠️  Interrupted after {fmt_num(record_count)} records — flushing current part…')
    except Exception as exc:
        log(f'⚠️  Error after {fmt_num(record_count)} records: {exc}')
        log('   Flushing current part…')
    finally:
        # Always flush the last partial part
        writer.flush()

        try:
            proc.stdout.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except Exception:
                pass
        except Exception:
            pass
        stderr_thread.join(timeout=5)

    # Check rclone exit code
    _sigpipe_code = getattr(signal, 'SIGPIPE', None)
    _ok = {0}
    if _sigpipe_code is not None:
        _ok.add(-_sigpipe_code)
    if proc.returncode not in _ok:
        err = b''.join(_stderr_chunks).decode(errors='replace').strip()
        if err:
            log(f'⚠️  rclone cat exited {proc.returncode}: {err[:500]}')

    elapsed = time.time() - start_time
    log('')
    log(f'✅ Split complete!')
    log(f'   Total records : {fmt_num(record_count)}')
    log(f'   Parts created : {writer.parts_written}')
    log(f'   Destination   : {RCLONE_REMOTE}:{DEST_FOLDER}/')
    log(f'   Duration      : {fmt_dur(elapsed)}')
    if elapsed:
        log(f'   Throughput    : {fmt_bytes(counter.bytes_read / elapsed)}/s (read)')


def main():
    log('═' * 60)
    log('  JSON SPLITTER  —  users_data.json → 1 GB parts')
    log(f'  Source : {RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}')
    log(f'  Dest   : {RCLONE_REMOTE}:{DEST_FOLDER}/')
    log(f'  Part   : {fmt_bytes(PART_SIZE_BYTES)} each')
    log('═' * 60)

    rclone_cmd = find_rclone()

    # SIGTERM → SystemExit (for GHA timeout)
    def _on_sigterm(signum, _frame):
        log('\n⚠️  SIGTERM received — flushing current part…')
        raise SystemExit(128 + signum)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, _on_sigterm)

    # Enforce fastest ijson backend
    global _ijson
    try:
        import ijson.backends.yajl2_c as _backend
        _ijson = _backend
        log(f'⚡ ijson backend: yajl2_c (fastest C extension)')
    except ImportError:
        try:
            import ijson.backends.yajl2_cffi as _backend
            _ijson = _backend
            log(f'⚡ ijson backend: yajl2_cffi (CFFI wrapper)')
        except ImportError:
            log(f'⚠️  ijson pure-Python backend — install libyajl2 for 3× speedup')

    # Create destination folder on Drive
    log(f'\n📁 Ensuring {RCLONE_REMOTE}:{DEST_FOLDER}/ exists …')
    subprocess.run(
        [rclone_cmd, 'mkdir', f'{RCLONE_REMOTE}:{DEST_FOLDER}'],
        capture_output=True, timeout=30,
    )

    stream_and_split(rclone_cmd)


if __name__ == '__main__':
    main()
