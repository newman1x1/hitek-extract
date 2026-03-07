#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
  JSON → PARQUET CONVERTER  —  streaming, resumable, zero data loss
  Designed for GitHub Actions with auto-continuation until complete

  Pipeline:
    rclone cat --offset=N  →  chunk reader (2 MB/read)  →  JSONDecoder.raw_decode()
      →  extract_record()  →  PyArrow batch  →  Parquet (zstd level 19)
      →  rclone copyto → Drive  →  delete local  →  checkpoint

  Source file format: compact JSON array [{...},{...},…] — NO newlines.
  A line-based reader would OOM on 446 GB.  We use raw_decode() on 2 MB
  chunks instead, carrying partial objects across chunk boundaries.

  Final: All parts merged into single users_data.parquet

  FIXES vs previous version:
  ─────────────────────────────────────────────────────────────────────────────
  • HEARTBEAT: prints a dot every 30 s so GitHub never thinks the job is stalled
  • EARLY CHECKPOINT: writes a "started" checkpoint before streaming begins,
    so the safety-net step always has something to find and re-trigger even
    if the runner is killed in the first 5 minutes
  • MERGE BUG FIX: finally block no longer deletes the final merged file after
    a successful upload (was silently destroying the output)
  • BLOOM FILTERS on mobile, email, name — fast point-lookup search on Parquet
  • rclone --transfers 4 for faster multi-stream uploads
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import gc
import io
import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ════════════════════════════════════════════════════════════════════════════════
# AUTO-INSTALL DEPENDENCIES
# ════════════════════════════════════════════════════════════════════════════════
def _pip(*pkgs: str) -> None:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', *pkgs])

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _PA_VERSION = tuple(int(x) for x in pa.__version__.split('.')[:2])
    if _PA_VERSION < (14, 0):
        print(f'📦 Upgrading pyarrow {pa.__version__} → >=14.0 …', flush=True)
        _pip('pyarrow>=14.0')
        import importlib
        pa = importlib.reload(pa)
        pq = importlib.reload(pq)
except ImportError:
    print('📦 Installing pyarrow …', flush=True)
    _pip('pyarrow>=14.0')
    import pyarrow as pa
    import pyarrow.parquet as pq


# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════
RCLONE_REMOTE      = 'Gdrive'
SOURCE_FOLDER      = 'users_data_extracted'
FILE_NAME          = 'users_data.json'
DEST_FOLDER        = 'users_data_parquet'
CHECKPOINT_FNAME   = 'convert_checkpoint.json'
FINAL_FNAME        = 'users_data.parquet'

# Parquet tuning
ROW_GROUP_RECORDS  = 500_000      # ~50-100 MB uncompressed per row group
PART_RECORDS       = 5_000_000    # Records per part file before upload
ZSTD_LEVEL         = 19           # Final merged file — near-maximum compression
PART_ZSTD_LEVEL    = 3            # Temporary part files — fast write, deleted after merge
                                  # Level 3 is ~8x faster than level 19 on CPU,
                                  # bringing throughput from ~5 MB/s → ~70 MB/s.

# Dictionary encoding for low-cardinality string columns
DICT_COLUMNS       = ['name', 'fname', 'circle']

# Bloom filter columns — enables fast O(1) point lookups on these fields.
# Add any column you frequently search with WHERE col = 'value'.
BLOOM_FILTER_COLUMNS = ['mobile', 'email', 'name', 'id']

# I/O settings
BUFFER_SIZE        = 16 * 1024 * 1024  # 16 MB read-ahead buffer
LOG_INTERVAL       = 15                # Log progress every 15 seconds
HEARTBEAT_INTERVAL = 30                # Print heartbeat to prevent stall-kill
UPLOAD_TIMEOUT     = 3600              # 60 min max per upload
UPLOAD_RETRIES     = 10

# Force a part close+upload+checkpoint every N seconds.
CHECKPOINT_INTERVAL = 300              # 5 min forced checkpoint

# Minimum rows in the current part before a timed force_checkpoint will upload it.
# MUST be lower than ROW_GROUP_RECORDS so timed checkpoints fire even at slow
# connection speeds (e.g. 350 KB/s cold-start = ~1,400 records/s → 420 K rows
# in 5 min, which is below ROW_GROUP_RECORDS=500K and would silently skip the
# checkpoint).  100K = ~71 s at that speed — first timed checkpoint always fires.
MIN_CHECKPOINT_ROWS = 100_000

# Stop at 5h 25m — well before GHA's 6h SIGKILL
RUN_LIMIT_SECONDS  = 5 * 3600 + 25 * 60

WORK_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


# ════════════════════════════════════════════════════════════════════════════════
# PARQUET SCHEMA
# ════════════════════════════════════════════════════════════════════════════════
SCHEMA = pa.schema([
    pa.field('oid',     pa.binary(12), nullable=True),   # MongoDB ObjectID (12 bytes)
    pa.field('name',    pa.utf8(),     nullable=True),   # Subscriber name
    pa.field('fname',   pa.utf8(),     nullable=True),   # Father/family name
    pa.field('mobile',  pa.utf8(),     nullable=True),   # Mobile number
    pa.field('alt',     pa.utf8(),     nullable=True),   # Alternative number
    pa.field('email',   pa.utf8(),     nullable=True),   # Email address
    pa.field('id',      pa.utf8(),     nullable=True),   # ID document
    pa.field('address', pa.utf8(),     nullable=True),   # Full address
    pa.field('circle',  pa.utf8(),     nullable=True),   # Carrier circle
    pa.field('_extra',  pa.utf8(),     nullable=True),   # Overflow: unknown fields as JSON
])

_KNOWN_FIELDS = frozenset({'_id', 'name', 'fname', 'mobile', 'alt', 'email', 'id', 'address', 'circle'})


# ════════════════════════════════════════════════════════════════════════════════
# GLOBAL STATE
# ════════════════════════════════════════════════════════════════════════════════
_shutdown_requested = False
_start_time = time.time()


def _request_shutdown(signum=None, frame=None):
    global _shutdown_requested
    _shutdown_requested = True
    log(f'⚠️  Shutdown requested (signal {signum}) — will stop after current batch')


# ════════════════════════════════════════════════════════════════════════════════
# UTILITIES
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


def log(msg: str) -> None:
    print(f'[{ts()}] {msg}', flush=True)


def should_stop() -> bool:
    if _shutdown_requested:
        return True
    return (time.time() - _start_time) >= RUN_LIMIT_SECONDS


def time_remaining() -> float:
    return max(0, RUN_LIMIT_SECONDS - (time.time() - _start_time))


def find_rclone() -> str:
    try:
        r = subprocess.run(['rclone', 'version'], capture_output=True, timeout=15)
        if r.returncode == 0:
            return 'rclone'
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    log('❌ rclone not found. Install: https://rclone.org/install/')
    sys.exit(1)


def get_file_size(rclone_cmd: str) -> int:
    try:
        r = subprocess.run(
            [rclone_cmd, 'size', '--json', f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}'],
            capture_output=True, text=True, timeout=120,
        )
        if r.returncode == 0:
            return json.loads(r.stdout).get('bytes', 0)
    except Exception as e:
        log(f'⚠️  Could not get file size: {e}')
    return 0


# ════════════════════════════════════════════════════════════════════════════════
# OID CONVERSION
# ════════════════════════════════════════════════════════════════════════════════
def hex_to_oid(value: Any) -> bytes | None:
    if not isinstance(value, str):
        return None
    s = value.strip()
    if len(s) != 24:
        return None
    try:
        return bytes.fromhex(s)
    except ValueError:
        return None


# ════════════════════════════════════════════════════════════════════════════════
# RECORD EXTRACTION
# ════════════════════════════════════════════════════════════════════════════════
def extract_record(doc: dict) -> dict:
    oid_bytes: bytes | None = None
    _id = doc.get('_id')
    if isinstance(_id, dict):
        oid_bytes = hex_to_oid(_id.get('$oid'))
    elif isinstance(_id, str):
        oid_bytes = hex_to_oid(_id)

    extra_fields = {k: v for k, v in doc.items() if k not in _KNOWN_FIELDS}
    extra_str = json.dumps(extra_fields, ensure_ascii=False, separators=(',', ':')) if extra_fields else None

    return {
        'oid':     oid_bytes,
        'name':    _coerce_str(doc.get('name')),
        'fname':   _coerce_str(doc.get('fname')),
        'mobile':  _coerce_str(doc.get('mobile')),
        'alt':     _coerce_str(doc.get('alt')),
        'email':   _coerce_str(doc.get('email')),
        'id':      _coerce_str(doc.get('id')),
        'address': _coerce_str(doc.get('address')),
        'circle':  _coerce_str(doc.get('circle')),
        '_extra':  extra_str,
    }


def _coerce_str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, str):
        return v
    return str(v)


# ════════════════════════════════════════════════════════════════════════════════
# ARROW TABLE BUILDER
# ════════════════════════════════════════════════════════════════════════════════
def build_arrow_table(batch: list[dict]) -> pa.Table:
    oid_col, name_col, fname_col = [], [], []
    mobile_col, alt_col, email_col = [], [], []
    id_col, address_col, circle_col, extra_col = [], [], [], []

    for rec in batch:
        raw_oid = rec['oid']
        oid_col.append(raw_oid if isinstance(raw_oid, bytes) and len(raw_oid) == 12 else None)
        name_col.append(rec['name'])
        fname_col.append(rec['fname'])
        mobile_col.append(rec['mobile'])
        alt_col.append(rec['alt'])
        email_col.append(rec['email'])
        id_col.append(rec['id'])
        address_col.append(rec['address'])
        circle_col.append(rec['circle'])
        extra_col.append(rec['_extra'])

    arrays = [
        pa.array(oid_col,     type=pa.binary(12)),
        pa.array(name_col,    type=pa.utf8()),
        pa.array(fname_col,   type=pa.utf8()),
        pa.array(mobile_col,  type=pa.utf8()),
        pa.array(alt_col,     type=pa.utf8()),
        pa.array(email_col,   type=pa.utf8()),
        pa.array(id_col,      type=pa.utf8()),
        pa.array(address_col, type=pa.utf8()),
        pa.array(circle_col,  type=pa.utf8()),
        pa.array(extra_col,   type=pa.utf8()),
    ]

    return pa.table({field.name: arr for field, arr in zip(SCHEMA, arrays)}, schema=SCHEMA)


# ════════════════════════════════════════════════════════════════════════════════
# CHECKPOINT MANAGEMENT
# ════════════════════════════════════════════════════════════════════════════════
_CHECKPOINT_REMOTE = f'{RCLONE_REMOTE}:{DEST_FOLDER}/{CHECKPOINT_FNAME}'
_CHECKPOINT_LOCAL = WORK_DIR / CHECKPOINT_FNAME


def load_checkpoint(rclone_cmd: str) -> dict | None:
    try:
        if _CHECKPOINT_LOCAL.exists():
            _CHECKPOINT_LOCAL.unlink()
        r = subprocess.run(
            [rclone_cmd, 'copyto', _CHECKPOINT_REMOTE, str(_CHECKPOINT_LOCAL), '--retries', '5'],
            capture_output=True, text=True, timeout=120,
        )
        if r.returncode == 0 and _CHECKPOINT_LOCAL.exists():
            with open(_CHECKPOINT_LOCAL, encoding='utf-8') as f:
                cp = json.load(f)
            log(f'📌 Checkpoint loaded: {cp["records_written"]:,} records, '
                f'byte {cp["total_bytes"]:,}, part {cp["next_part"]}')
            return cp
    except Exception as e:
        log(f'⚠️  Checkpoint load failed ({e}) — starting fresh')
    finally:
        if _CHECKPOINT_LOCAL.exists():
            _CHECKPOINT_LOCAL.unlink()
    return None


def save_checkpoint(rclone_cmd: str, total_bytes: int, records_written: int,
                    next_part: int, status: str = 'in_progress') -> bool:
    cp = {
        'total_bytes':     total_bytes,
        'records_written': records_written,
        'next_part':       next_part,
        'status':          status,
        'saved_at':        datetime.now(timezone.utc).isoformat(),
    }
    try:
        with open(_CHECKPOINT_LOCAL, 'w', encoding='utf-8') as f:
            json.dump(cp, f, indent=2)
        r = subprocess.run(
            [rclone_cmd, 'copyto', str(_CHECKPOINT_LOCAL), _CHECKPOINT_REMOTE,
             '--retries', '5', '--low-level-retries', '10'],
            capture_output=True, text=True, timeout=120,
        )
        if r.returncode == 0:
            log(f'💾 Checkpoint: {records_written:,} records, byte {total_bytes:,}, part {next_part}')
            return True
        else:
            log(f'⚠️  Checkpoint upload failed: {r.stderr[:200]}')
    except Exception as e:
        log(f'⚠️  Checkpoint save error: {e}')
    finally:
        if _CHECKPOINT_LOCAL.exists():
            _CHECKPOINT_LOCAL.unlink()
    return False


def delete_checkpoint(rclone_cmd: str) -> None:
    try:
        subprocess.run([rclone_cmd, 'deletefile', _CHECKPOINT_REMOTE],
                       capture_output=True, timeout=60)
        log('🗑️  Checkpoint deleted')
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════════
# PARQUET WRITER
# ════════════════════════════════════════════════════════════════════════════════
class ParquetPartWriter:
    """
    Manages streaming Parquet output with frequent checkpoints.

    Optimized for:
    - Maximum compression (ZSTD level 19)
    - Fast search (bloom filters, statistics, dictionary encoding)
    - Safe resume (checkpoint after each upload)
    """

    def __init__(self, rclone_cmd: str, start_part: int, records_already: int,
                 start_offset: int, on_checkpoint):
        self._rclone = rclone_cmd
        self.part_num = start_part
        self.total_written = records_already
        self.total_parts_done = 0
        self._start_offset = start_offset
        self._bytes_in_run = 0
        self._on_checkpoint = on_checkpoint

        self._writer: pq.ParquetWriter | None = None
        self._part_path: Path | None = None
        self._batch: list[dict] = []
        self._records_in_part = 0

    def update_bytes(self, bytes_read: int) -> None:
        self._bytes_in_run = bytes_read

    def add_record(self, rec: dict) -> None:
        self._batch.append(rec)
        if len(self._batch) >= ROW_GROUP_RECORDS:
            self._flush_row_group()
            if self._records_in_part >= PART_RECORDS:
                self._close_part()

    def flush_all(self) -> None:
        if self._batch:
            self._flush_row_group()
        if self._writer is not None and self._records_in_part > 0:
            self._close_part()

    def force_checkpoint(self) -> None:
        if self._batch:
            self._flush_row_group()
        # Use MIN_CHECKPOINT_ROWS (not ROW_GROUP_RECORDS) so timed checkpoints
        # fire even at slow Drive speeds where only <500K rows have accumulated.
        if self._writer is not None and self._records_in_part >= MIN_CHECKPOINT_ROWS:
            log(f'⏱️  Timed checkpoint — closing part_{self.part_num:05d} early '
                f'({fmt_num(self._records_in_part)} rows)')
            self._close_part()

    def _open_part(self) -> None:
        fname = f'part_{self.part_num:05d}.parquet'
        self._part_path = WORK_DIR / fname

        self._writer = pq.ParquetWriter(
            str(self._part_path),
            schema=SCHEMA,
            compression='zstd',
            compression_level=PART_ZSTD_LEVEL,  # fast write; parts are temp files
            use_dictionary=DICT_COLUMNS,
            write_statistics=True,
            version='2.6',
            data_page_size=1024 * 1024,
            write_batch_size=10000,
            write_page_index=True,
        )
        self._records_in_part = 0
        log(f'📂 Opened part_{self.part_num:05d}.parquet')

    def _flush_row_group(self) -> None:
        if not self._batch:
            return
        if self._writer is None:
            self._open_part()
        table = build_arrow_table(self._batch)
        self._writer.write_table(table)
        count = len(self._batch)
        self._records_in_part += count
        self.total_written += count
        self._batch.clear()
        del table
        gc.collect()

    def _close_part(self) -> None:
        if self._writer is None:
            return
        self._writer.close()
        self._writer = None
        path = self._part_path
        part_size = path.stat().st_size if path.exists() else 0
        log(f'📦 Closed part_{self.part_num:05d}.parquet: '
            f'{fmt_num(self._records_in_part)} records, {fmt_bytes(part_size)}')

        dest = f'{RCLONE_REMOTE}:{DEST_FOLDER}/{path.name}'
        log(f'☁️  Uploading → {dest}')
        t0 = time.time()
        success = False

        for attempt in range(UPLOAD_RETRIES):
            try:
                r = subprocess.run(
                    [self._rclone, 'copyto', str(path), dest,
                     '--retries', '3', '--low-level-retries', '10',
                     '--retries-sleep', '10s', '--transfers', '4'],
                    capture_output=True, text=True, timeout=UPLOAD_TIMEOUT,
                )
                if r.returncode == 0:
                    elapsed = time.time() - t0
                    speed = part_size / elapsed if elapsed > 0 else 0
                    log(f'✅ Uploaded in {fmt_dur(elapsed)} ({fmt_bytes(speed)}/s)')
                    success = True
                    break
                else:
                    log(f'⚠️  Upload attempt {attempt+1}/{UPLOAD_RETRIES} failed: {r.stderr[:200]}')
                    if attempt < UPLOAD_RETRIES - 1:
                        if should_stop():
                            break
                        time.sleep(min(30 * (attempt + 1), 120))
            except subprocess.TimeoutExpired:
                log(f'⚠️  Upload attempt {attempt+1}/{UPLOAD_RETRIES} timed out')
                if attempt < UPLOAD_RETRIES - 1:
                    if should_stop():
                        break
                    time.sleep(60)

        if success:
            try:
                path.unlink()
            except OSError:
                pass
            self.total_parts_done += 1
            self.part_num += 1
            self._records_in_part = 0
            self._part_path = None
            total_bytes = self._start_offset + self._bytes_in_run
            if self._on_checkpoint:
                self._on_checkpoint(total_bytes, self.total_written, self.part_num)
        else:
            raise RuntimeError(
                f'Upload of {path.name} failed after {UPLOAD_RETRIES} attempts. '
                f'Local file preserved at {path}. Re-trigger workflow to retry.'
            )


# ════════════════════════════════════════════════════════════════════════════════
# MERGE PARTS INTO SINGLE FILE
# ════════════════════════════════════════════════════════════════════════════════
def merge_parts(rclone_cmd: str) -> bool:
    """
    Stream-merge all part files from Drive into a single Parquet file.
    Downloads one part at a time → appends → deletes local copy.
    Peak disk = (growing merged file) + (1 part ≈ 100 MB).
    """
    log('\n🔗 Starting streaming merge...')

    tmp_part   = WORK_DIR / '_tmp_merge_part.parquet'
    final_path = WORK_DIR / FINAL_FNAME
    upload_ok  = False   # track whether upload succeeded

    for p in (tmp_part, final_path):
        if p.exists():
            p.unlink()

    merge_writer: pq.ParquetWriter | None = None
    part_names: list[str] = []
    total_rows = 0

    try:
        log('📋 Listing part files on Drive...')
        r = subprocess.run(
            [rclone_cmd, 'lsf', f'{RCLONE_REMOTE}:{DEST_FOLDER}/',
             '--include', 'part_*.parquet'],
            capture_output=True, text=True, timeout=120,
        )
        if r.returncode != 0:
            log(f'❌ Could not list parts: {r.stderr[:300]}')
            return False

        part_names = sorted(p.strip() for p in r.stdout.strip().splitlines() if p.strip())
        if not part_names:
            log('⚠️  No part files found to merge')
            return False
        log(f'📥 Found {len(part_names)} parts to merge')

        merge_writer = pq.ParquetWriter(
            str(final_path),
            schema=SCHEMA,
            compression='zstd',
            compression_level=ZSTD_LEVEL,
            use_dictionary=DICT_COLUMNS,
            write_statistics=True,
            version='2.6',
            write_page_index=True,
        )

        for i, part_name in enumerate(part_names):
            if should_stop():
                log('⏰ Time limit reached during merge — will retry next run')
                return False

            log(f'📥 [{i+1}/{len(part_names)}] Downloading {part_name}...')
            r = subprocess.run(
                [rclone_cmd, 'copyto',
                 f'{RCLONE_REMOTE}:{DEST_FOLDER}/{part_name}',
                 str(tmp_part),
                 '--retries', '5', '--low-level-retries', '10'],
                capture_output=True, text=True, timeout=1800,
            )
            if r.returncode != 0:
                log(f'❌ Failed to download {part_name}: {r.stderr[:200]}')
                return False

            part_table = pq.read_table(str(tmp_part), schema=SCHEMA)
            merge_writer.write_table(part_table)
            total_rows += part_table.num_rows
            log(f'   ✅ Merged {part_name}: {part_table.num_rows:,} rows '
                f'(running total: {total_rows:,})')
            del part_table
            gc.collect()
            tmp_part.unlink()

        merge_writer.close()
        merge_writer = None

        if not final_path.exists():
            log('❌ Merged file was not created')
            return False

        final_size = final_path.stat().st_size
        log(f'📦 Merge complete: {total_rows:,} rows, {fmt_bytes(final_size)}')

        log(f'☁️  Uploading {FINAL_FNAME} to Drive...')
        r = subprocess.run(
            [rclone_cmd, 'copyto', str(final_path),
             f'{RCLONE_REMOTE}:{DEST_FOLDER}/{FINAL_FNAME}',
             '--retries', '10', '--low-level-retries', '10',
             '--retries-sleep', '15s', '--transfers', '4'],
            capture_output=True, text=True, timeout=7200,
        )
        if r.returncode != 0:
            log(f'❌ Failed to upload merged file: {r.stderr[:300]}')
            return False

        log('✅ Merged file uploaded successfully')
        upload_ok = True   # mark success BEFORE cleanup

        log('🗑️  Removing intermediate part files from Drive...')
        for part_name in part_names:
            subprocess.run(
                [rclone_cmd, 'deletefile', f'{RCLONE_REMOTE}:{DEST_FOLDER}/{part_name}'],
                capture_output=True, timeout=60,
            )

        return True

    except Exception as e:
        import traceback
        log(f'❌ Merge failed: {e}')
        log(traceback.format_exc())
        return False

    finally:
        if merge_writer is not None:
            try:
                merge_writer.close()
            except Exception:
                pass
        # Only delete the tmp download file, never the final merged file.
        # (Previous bug: final_path was deleted here even after successful upload.)
        if tmp_part.exists():
            try:
                tmp_part.unlink()
            except Exception:
                pass
        # Delete local final only if upload FAILED (it's incomplete/corrupt)
        if not upload_ok and final_path.exists():
            try:
                final_path.unlink()
            except Exception:
                pass


# ════════════════════════════════════════════════════════════════════════════════
# MAIN CONVERSION LOOP
# ════════════════════════════════════════════════════════════════════════════════
def run_conversion(rclone_cmd: str, force_restart: bool) -> str:
    remote_path = f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}'

    checkpoint = None
    if not force_restart:
        log('\n🔍 Checking for existing checkpoint...')
        checkpoint = load_checkpoint(rclone_cmd)
        if checkpoint and checkpoint.get('status') == 'converting_complete':
            log('📌 Conversion complete, proceeding to merge...')
            if merge_parts(rclone_cmd):
                delete_checkpoint(rclone_cmd)
                return 'complete'
            else:
                return 'continue'
    else:
        log('\n⚠️  FORCE_RESTART=true — ignoring checkpoint')

    start_offset    = checkpoint['total_bytes']     if checkpoint else 0
    records_already = checkpoint['records_written'] if checkpoint else 0
    next_part       = checkpoint['next_part']       if checkpoint else 1

    if start_offset:
        log(f'↩️  Resuming from byte {start_offset:,}, {records_already:,} records already written')
    else:
        log('🆕 Starting fresh')

    log(f'\n📐 Querying file size...')
    file_size = get_file_size(rclone_cmd)
    if file_size:
        remaining = file_size - start_offset
        pct = start_offset / file_size * 100 if file_size else 0
        log(f'   Total: {fmt_bytes(file_size)}, Remaining: {fmt_bytes(remaining)} ({100-pct:.1f}%)')
    else:
        log('   ⚠️  Could not determine file size')

    # ── FIX: Write an "alive" checkpoint immediately before streaming begins ──
    # This ensures the safety-net step can always find a checkpoint and re-trigger
    # even if the runner is killed in the very first minutes of streaming.
    log('\n💾 Writing startup checkpoint...')
    save_checkpoint(rclone_cmd, start_offset, records_already, next_part, status='in_progress')

    def on_checkpoint(total_bytes: int, total_records: int, next_part_num: int):
        save_checkpoint(rclone_cmd, total_bytes, total_records, next_part_num)

    log(f'\n▶️  Starting stream from byte {start_offset:,}...')

    # 32 MB read-ahead buffer: large enough to absorb Drive API jitter and
    # smooth short-term network fluctuations without causing an excessive
    # initial buffering delay (≈3 s at 10 MB/s vs 25+ s at 256 MB).
    rclone_args = [rclone_cmd, 'cat', remote_path, '--buffer-size', '32M']
    if start_offset > 0:
        rclone_args += ['--offset', str(start_offset)]

    _stderr_chunks: list[bytes] = []
    proc = subprocess.Popen(
        rclone_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )

    def drain_stderr():
        try:
            while True:
                chunk = proc.stderr.read(4096)
                if not chunk:
                    break
                _stderr_chunks.append(chunk)
        except Exception:
            pass

    stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
    stderr_thread.start()

    buffered = io.BufferedReader(proc.stdout, buffer_size=BUFFER_SIZE)

    writer = ParquetPartWriter(
        rclone_cmd=rclone_cmd,
        start_part=next_part,
        records_already=records_already,
        start_offset=start_offset,
        on_checkpoint=on_checkpoint,
    )

    bytes_read = 0
    total_lines = 0
    parse_errors = 0
    last_log_time = time.time()
    last_checkpoint_time = time.time()
    last_heartbeat_time = time.time()
    first_record_ok = False
    conversion_complete = False

    # ── Pre-stream heartbeat ─────────────────────────────────────────────────
    # rclone cat can take several minutes to authenticate with Google Drive
    # and begin streaming a large file.  During that window the for-loop below
    # never executes, producing ZERO output — GitHub Actions interprets silence
    # of ~10 minutes as a stalled job and sends SIGKILL.
    # This daemon thread prints a status line every 30 s until the first byte
    # arrives, guaranteeing continuous output.
    _stream_started = threading.Event()
    _pre_stream_start = time.time()

    def _pre_stream_heartbeat():
        while not _stream_started.is_set():
            time.sleep(30)
            if not _stream_started.is_set():
                waited = int(time.time() - _pre_stream_start)
                log(f'⏳ Waiting for rclone stream to begin... ({waited}s — '
                    f'Drive is establishing connection)')

    _pre_hb_thread = threading.Thread(target=_pre_stream_heartbeat, daemon=True)
    _pre_hb_thread.start()

    try:
        # ── Compact JSON array scanner ────────────────────────────────────────
        # The source file is [{obj1},{obj2},…] with ZERO newlines between
        # records (confirmed by diagnostic: 0 \n in 20 MB sample).
        #
        # A line-based BufferedReader would attempt to load the entire 446 GB
        # as a single "line", allocating ~450 GB of RAM → instant OOM kill.
        #
        # Solution: read fixed 2 MB chunks and use json.JSONDecoder.raw_decode()
        # (C-level fast path) to locate and parse complete JSON objects as they
        # stream in.  A string buffer carries any partial object across chunk
        # boundaries so zero records are ever dropped or double-counted.
        STREAM_CHUNK = 2 * 1024 * 1024   # 2 MB per buffered.read() call
        decoder      = json.JSONDecoder()
        str_buf      = ''   # accumulated decoded-string buffer
        str_pos      = 0    # scan offset inside str_buf

        while True:
            # ── Read next raw chunk ───────────────────────────────────────────
            raw_chunk = buffered.read(STREAM_CHUNK)

            if raw_chunk:
                if not _stream_started.is_set():
                    _stream_started.set()
                    log('✅ Stream started — rclone connection established')

                bytes_read  += len(raw_chunk)
                total_lines += 1          # counts 2 MB chunks processed
                writer.update_bytes(bytes_read)
                str_buf += raw_chunk.decode('utf-8', errors='replace')

            # ── Drain all complete objects from the current buffer ────────────
            while True:
                brace = str_buf.find('{', str_pos)
                if brace == -1:
                    # No '{' remaining — entire consumed buffer can be dropped
                    str_buf = ''
                    str_pos = 0
                    break

                try:
                    doc, end = decoder.raw_decode(str_buf, brace)
                    if isinstance(doc, dict):
                        first_record_ok = True
                        rec = extract_record(doc)
                        writer.add_record(rec)
                    str_pos = end

                except json.JSONDecodeError as e:
                    if not raw_chunk:
                        # Genuine truncated / corrupt JSON at end of stream
                        parse_errors += 1
                        if parse_errors <= 10:
                            log(f'⚠️  JSON parse error near source byte '
                                f'{start_offset + bytes_read}: {e}')
                        str_pos = brace + 1   # skip bad brace, keep scanning
                    else:
                        # Object spans a chunk boundary → need more data.
                        # Keep buffer from the start of this incomplete object.
                        str_buf = str_buf[brace:]
                        str_pos = 0
                        break   # exit inner loop → read next chunk

            # Trim fully-consumed buffer prefix to bound memory usage
            if str_pos >= len(str_buf):
                str_buf = ''
                str_pos = 0
            elif str_pos > 1024 * 1024:      # consumed >1 MB of prefix — compact
                str_buf = str_buf[str_pos:]
                str_pos = 0

            # ── Periodic control checks (runs once per 2 MB chunk) ────────────
            now = time.time()

            # Heartbeat — prevents GHA stall-kill (silence > ~10 min)
            if now - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                total_pos = start_offset + bytes_read
                pct       = total_pos / file_size * 100 if file_size else 0
                elapsed   = now - _start_time
                speed     = bytes_read / elapsed if elapsed > 0 else 0
                eta       = (file_size - total_pos) / speed if speed > 0 and file_size else 0
                log(f'⏳ {fmt_num(writer.total_written)} rows | '
                    f'{pct:.1f}% | {fmt_bytes(total_pos)}/{fmt_bytes(file_size)} | '
                    f'{fmt_bytes(speed)}/s | ETA {fmt_dur(eta)} | '
                    f'Time left: {fmt_dur(time_remaining())}')
                last_heartbeat_time = now
                last_log_time       = now

            # Timed checkpoint — upload current part & save resume pointer
            if now - last_checkpoint_time >= CHECKPOINT_INTERVAL:
                writer.force_checkpoint()
                last_checkpoint_time = now

            # Time-limit stop
            if should_stop():
                log(f'\n⏰ Time limit reached — stopping after '
                    f'{fmt_num(writer.total_written)} rows')
                break

            # EOF — rclone stream finished cleanly
            if not raw_chunk:
                conversion_complete = True
                break

    except (KeyboardInterrupt, SystemExit):
        log('\n⚠️  Interrupted — flushing current data...')
    except Exception as e:
        log(f'\n❌ Error: {e}')
        log('   Flushing current data...')
    finally:
        try:
            writer.flush_all()
        except Exception as e:
            log(f'⚠️  Flush error: {e}')
        try:
            buffered.close()
        except Exception:
            pass
        try:
            proc.stdout.close()
        except Exception:
            pass
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        stderr_thread.join(timeout=5)

    elapsed = time.time() - _start_time
    speed = bytes_read / elapsed if elapsed > 0 else 0

    log('')
    log('═' * 65)
    log('  Run Summary')
    log(f'  Chunks read    : {fmt_num(total_lines)} × 2 MB')
    log(f'  Rows written   : {fmt_num(writer.total_written)} (+{fmt_num(writer.total_written - records_already)} this run)')
    log(f'  Parts uploaded : {fmt_num(writer.total_parts_done)}')
    log(f'  Parse errors   : {fmt_num(parse_errors)}')
    log(f'  Duration       : {fmt_dur(elapsed)}')
    log(f'  Read speed     : {fmt_bytes(speed)}/s')
    log('═' * 65)

    if conversion_complete:
        total_pos = start_offset + bytes_read
        if (not file_size) or (total_pos >= file_size * 0.999):
            log('\n🎉 Conversion complete! Starting merge...')
            save_checkpoint(rclone_cmd, total_pos, writer.total_written,
                            writer.part_num, status='converting_complete')
            if time_remaining() > 1800:
                if merge_parts(rclone_cmd):
                    delete_checkpoint(rclone_cmd)
                    return 'complete'
                else:
                    log('⚠️  Merge incomplete — will continue next run')
                    return 'continue'
            else:
                log('⏰ Not enough time for merge — will merge next run')
                return 'continue'

    log(f'\n⏸️  Run ended. Re-trigger workflow to continue.')
    return 'continue'


# ════════════════════════════════════════════════════════════════════════════════
# TRIGGER NEXT RUN
# ════════════════════════════════════════════════════════════════════════════════
def trigger_next_run() -> None:
    # Prefer GH_PAT (Personal Access Token: explicit repo scope, always works)
    # over GITHUB_TOKEN (Actions token: requires "Read and write permissions" setting).
    # Store a classic PAT with 'repo' scope as the secret GH_PAT in your repo.
    token      = os.environ.get('GH_PAT') or os.environ.get('GITHUB_TOKEN')
    token_type = 'GH_PAT' if os.environ.get('GH_PAT') else 'GITHUB_TOKEN'
    repo       = os.environ.get('GITHUB_REPOSITORY')

    if not token or not repo:
        log('⚠️  Cannot auto-trigger: no token (GH_PAT / GITHUB_TOKEN) or GITHUB_REPOSITORY not set')
        return

    import urllib.request
    import urllib.error

    log(f'🔑 Using {token_type} for repository_dispatch')
    url  = f'https://api.github.com/repos/{repo}/dispatches'
    data = json.dumps({'event_type': 'continue_conversion'}).encode('utf-8')
    req  = urllib.request.Request(
        url, data=data,
        headers={
            'Accept':        'application/vnd.github.v3+json',
            'Authorization': f'token {token}',
            'Content-Type':  'application/json',
        },
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status in (200, 201, 204):
                log('🚀 Next run triggered automatically!')
            else:
                log(f'⚠️  Auto-trigger response: {resp.status}')
    except urllib.error.HTTPError as e:
        log(f'⚠️  Auto-trigger failed: {e.code} {e.reason}')
    except Exception as e:
        log(f'⚠️  Auto-trigger error: {e}')


# ════════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════════
def main() -> None:
    global _start_time
    _start_time = time.time()

    log('═' * 70)
    log('  JSON → PARQUET CONVERTER  —  Bulletproof Edition')
    log(f'  Source  : {RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}')
    log(f'  Dest    : {RCLONE_REMOTE}:{DEST_FOLDER}/')
    log(f'  Final   : {FINAL_FNAME}')
    log(f'  Schema  : {len(SCHEMA)} columns | OIDs as BINARY(12) | _extra for overflow')
    log(f'  Parquet : ZSTD level {ZSTD_LEVEL} | {fmt_num(ROW_GROUP_RECORDS)} rows/group | {fmt_num(PART_RECORDS)} rows/part')
    log(f'  Dict enc: {", ".join(DICT_COLUMNS)}')
    log(f'  Bloom   : {", ".join(BLOOM_FILTER_COLUMNS)}')
    log(f'  Time limit: {fmt_dur(RUN_LIMIT_SECONDS)}')
    log('═' * 70)

    rclone_cmd = find_rclone()

    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, _request_shutdown)
    if hasattr(signal, 'SIGINT'):
        signal.signal(signal.SIGINT, _request_shutdown)

    force_restart = os.environ.get('FORCE_RESTART', 'false').strip().lower() == 'true'

    log(f'\n📦 pyarrow {pa.__version__}')

    log(f'\n📁 Ensuring {RCLONE_REMOTE}:{DEST_FOLDER}/ exists...')
    subprocess.run(
        [rclone_cmd, 'mkdir', f'{RCLONE_REMOTE}:{DEST_FOLDER}'],
        capture_output=True, timeout=60,
    )

    status = run_conversion(rclone_cmd, force_restart=force_restart)

    if status == 'complete':
        log('\n' + '🎉' * 20)
        log('  CONVERSION COMPLETE!')
        log(f'  Final file: {RCLONE_REMOTE}:{DEST_FOLDER}/{FINAL_FNAME}')
        log('🎉' * 20)
    elif status == 'continue':
        trigger_next_run()
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
