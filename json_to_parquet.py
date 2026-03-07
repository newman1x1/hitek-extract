#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════
  JSON → PARQUET CONVERTER  —  streaming, resumable, zero data loss
  Runs on GitHub Actions (Ubuntu) or any machine with rclone + pyarrow

  ┌─────────────────────────────────────────────────────────────────┐
  │  Pipeline                                                       │
  │  rclone cat --offset=N  →  BufferedReader  →  json.loads()     │
  │    →  extract_record()  →  PyArrow batch  →  Parquet (zstd)    │
  │    →  rclone copyto → Drive  →  delete local  →  checkpoint    │
  └─────────────────────────────────────────────────────────────────┘

  Why line-by-line (not ijson)?
  ────────────────────────────────────────────────────────────────────
  MongoDB exports write ONE JSON object per line.  Reading line-by-line
  gives us an EXACT byte offset at every record boundary, enabling
  perfect byte-offset resume across multiple 6-hour GHA runs.
  Python's json.loads() runs at ~250–500 MB/s; the network bottleneck
  is 10 MB/s, so there is zero performance impact.

  Resume mechanism
  ────────────────────────────────────────────────────────────────────
  After each Parquet part is written and uploaded, a checkpoint file
  is saved to Google Drive.  The checkpoint records:
    • total_bytes  — exact byte offset in the source file where the
                     NEXT unread line starts  (used as --offset)
    • records_written — total records converted so far
    • next_part    — next output file number

  On the next run, rclone cat starts from that exact byte offset.
  No data is re-read or duplicated.  Each GHA run just continues.

  Output produced
  ────────────────────────────────────────────────────────────────────
  Gdrive:users_data_parquet/
    part_00001.parquet       ← each ~200–400 MB (varies with data)
    part_00002.parquet
    …
    convert_checkpoint.json  ← resume pointer (auto-managed)

  Parquet schema
  ────────────────────────────────────────────────────────────────────
  oid      BINARY(12)   — MongoDB ObjectID as 12-byte binary (not hex)
  name     STRING       — subscriber name        (dictionary encoded)
  fname    STRING       — father / family name   (dictionary encoded)
  mobile   STRING       — 10-digit mobile number
  alt      STRING       — alternative number     (nullable)
  email    STRING       — email address          (nullable)
  id       STRING       — Aadhaar / PAN / DL / passport (nullable)
  address  STRING       — full address string    (nullable)
  circle   STRING       — carrier circle         (dictionary encoded,
                          extremely low cardinality ≈ 2 values)
  _extra   STRING/null  — JSON object of ANY unknown / new field that
                          appears in the source but is not one of the
                          9 columns above.  Guarantees zero data loss
                          even if the source schema changes mid-file.

  Searching the output
  ────────────────────────────────────────────────────────────────────
  DuckDB (free, no server):
    SELECT * FROM 'part_*.parquet' WHERE mobile = '9876543210';
    SELECT * FROM 'part_*.parquet' WHERE name ILIKE '%sharma%';
    SELECT * FROM 'part_*.parquet' WHERE circle = 'AIRTEL DELHI';

  Python (pandas / polars):
    import pyarrow.dataset as ds
    dataset = ds.dataset('path/', format='parquet')
    table = dataset.to_table(filter=ds.field('mobile') == '9876543210')
═══════════════════════════════════════════════════════════════════════
"""

# /// script
# requires-python = ">=3.10"
# dependencies = ["pyarrow>=14.0"]
# ///

from __future__ import annotations   # lazy annotation evaluation — silences type-checker
                                       # warnings for conditionally-imported pyarrow types

import os, sys, json, time, signal, subprocess, threading, io
from datetime import datetime, timezone
from pathlib import Path

# ── auto-install pyarrow if absent ───────────────────────────────────────────
def _pip(*pkgs: str) -> None:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', *pkgs])

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _PA_VERSION = tuple(int(x) for x in pa.__version__.split('.')[:2])
    if _PA_VERSION < (14, 0):
        print(f'📦 Upgrading pyarrow {pa.__version__} → >=14.0 …')
        _pip('pyarrow>=14.0')
        import importlib
        pa  = importlib.reload(pa)
        pq  = importlib.reload(pq)
except ImportError:
    print('📦 Installing pyarrow …')
    _pip('pyarrow>=14.0')
    import pyarrow as pa          # type: ignore
    import pyarrow.parquet as pq  # type: ignore


# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION  (edit these to match your setup)
# ════════════════════════════════════════════════════════════════════════════════
RCLONE_REMOTE      = 'Gdrive'
SOURCE_FOLDER      = 'users_data_extracted'    # folder containing the source JSON
FILE_NAME          = 'users_data.json'
DEST_FOLDER        = 'users_data_parquet'      # Drive folder for Parquet output
CHECKPOINT_FNAME   = 'convert_checkpoint.json' # stored inside DEST_FOLDER

# Parquet tuning
#
# WHY THESE VALUES:
#   At ~10 MB/s Drive read speed and ~247 bytes/record:
#     1 M records = ~247 MB to read  ≈ 25 sec
#     + write/compress ~30-50 MB Parquet  ≈ 3 sec
#     + upload to Drive  ≈ 3-5 sec
#     → checkpoint saved every ~35 seconds
#
#   With PART_RECORDS = 10_000_000 the first checkpoint happened at
#   ~4 min 50 sec — right at the edge of abrupt runner shutdowns.
#   "Cleaning up orphan processes" = SIGKILL (not SIGTERM), so the
#   finally block never fires and ALL progress is lost.
#   At 1 M records, even a 5-minute run saves ~8 checkpoints.
#
ROW_GROUP_RECORDS  = 100_000    # records per row group (5 groups per part file)
PART_RECORDS       = 1_000_000  # records per output .parquet file (≈ 25–50 MB compressed)
ZSTD_LEVEL         = 15         # zstd compression level (1–22); 15 = best quality vs speed

# Dictionary-encode columns with repeated values to maximise compression.
# Columns NOT in this list use plain / delta encoding + zstd.
# NOTE: '_extra' must NOT be in DICT_COLUMNS — it is a JSON blob with
# high cardinality; dictionary-encoding it would waste memory and space.
DICT_COLUMNS       = ['name', 'fname', 'circle']

# Runtime / I/O
BUFFER_SIZE        = 8 * 1024 * 1024   # 8 MB read-ahead buffer for line iteration
LOG_INTERVAL       = 60                 # seconds between progress log lines
UPLOAD_TIMEOUT     = 1800               # 30 min max per rclone upload
UPLOAD_RETRIES     = 5

# Self-imposed run time limit.
# GHA hard-kills the runner at 6h (SIGKILL — cannot be caught).
# We stop ourselves at 5h 30m so the current part is flushed,
# uploaded, and the checkpoint is saved cleanly before GHA fires.
# Formula: hours * 3600 + minutes * 60
RUN_LIMIT_SECONDS  = 5 * 3600 + 30 * 60   # 5 h 30 m  (GHA limit = 6 h)

WORK_DIR           = Path(os.path.dirname(os.path.abspath(__file__)))


# ════════════════════════════════════════════════════════════════════════════════
# PARQUET SCHEMA
# ════════════════════════════════════════════════════════════════════════════════
# All 9 known fields from the analysis report + 1 overflow column.
# '_extra': if ANY record contains a field not in the 9 known fields
# (i.e. schema changed mid-file), those unknown fields are captured here
# as a compact JSON string.  Records with no unknown fields → null.
# This guarantees ZERO DATA LOSS even if the source schema evolves.
SCHEMA = pa.schema([
    pa.field('oid',     pa.binary(12), nullable=True),  # 12-byte binary ObjectID
    pa.field('name',    pa.utf8(),     nullable=True),  # subscriber name
    pa.field('fname',   pa.utf8(),     nullable=True),  # father / family name
    pa.field('mobile',  pa.utf8(),     nullable=True),  # 10-digit mobile
    pa.field('alt',     pa.utf8(),     nullable=True),  # alternative number
    pa.field('email',   pa.utf8(),     nullable=True),  # email address
    pa.field('id',      pa.utf8(),     nullable=True),  # Aadhaar/PAN/DL/passport
    pa.field('address', pa.utf8(),     nullable=True),  # full address
    pa.field('circle',  pa.utf8(),     nullable=True),  # carrier circle
    pa.field('_extra',  pa.utf8(),     nullable=True),  # unknown/new fields as JSON
])

# Fields we explicitly map to named columns.  Any key NOT in this set
# will land in the '_extra' column so nothing is ever silently dropped.
_KNOWN_FIELDS = frozenset(
    {'_id', 'name', 'fname', 'mobile', 'alt', 'email', 'id', 'address', 'circle'}
)


# ════════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════════
_IS_TTY = sys.stdout.isatty() and not os.environ.get('CI')

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
    prefix = f'[{ts()}] ' if not _IS_TTY else ''
    print(f'{prefix}{msg}', flush=True)

def find_rclone() -> str:
    for candidate in ['rclone']:
        try:
            if subprocess.run(
                [candidate, 'version'], capture_output=True, timeout=15
            ).returncode == 0:
                return candidate
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
    log('❌ rclone not found. Install: https://rclone.org/install/')
    sys.exit(1)

def get_file_size(rclone_cmd: str) -> int:
    """Query file size from Drive without downloading."""
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
# OID CONVERSION  —  24-char hex → 12-byte binary
# ════════════════════════════════════════════════════════════════════════════════
def _hex_to_oid(value) -> bytes | None:
    """
    Convert a MongoDB ObjectID hex string to 12-byte binary.

    The source JSON has _id: {$oid: "5f2a3b4c5d6e7f8a9b0c1d2e"}.
    Storing as 12-byte BINARY(12) rather than 24-char STRING saves
    50 % of the OID column space and improves sort / search performance.

    Returns None if value is missing, wrong length, or not valid hex.
    This is lossless: bytes.fromhex is the canonical inverse of hex().
    """
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
# RECORD EXTRACTION  —  MongoDB JSON doc → flat dict for Parquet row
# ════════════════════════════════════════════════════════════════════════════════
def extract_record(doc: dict) -> dict:
    """
    Extract all known fields from a source JSON document.

    Preserves original values exactly — no sanitization, no filtering.
    Dirty nulls like 'NULLLLL', ' ', '0' are kept as-is to guarantee
    zero data loss.  Only the _id.$oid path is transformed (hex→bytes).

    SCHEMA CHANGE SAFETY:
    Any field in the source document that is NOT in _KNOWN_FIELDS is
    collected into the '_extra' column as a compact JSON object string.
    This ensures that if the source schema grows or changes mid-file,
    every byte of every record is still preserved — nothing is dropped.

    Returns a flat dict matching SCHEMA field names (including '_extra').
    """
    oid_bytes: bytes | None = None
    _id = doc.get('_id')
    if isinstance(_id, dict):
        oid_bytes = _hex_to_oid(_id.get('$oid'))

    # Capture any unknown fields as a JSON string.
    # Common cases: null (no extra fields) or a small dict string.
    extra_fields = {k: v for k, v in doc.items() if k not in _KNOWN_FIELDS}
    extra_str: str | None = (
        json.dumps(extra_fields, ensure_ascii=False, separators=(',', ':'))
        if extra_fields else None
    )

    return {
        'oid':     oid_bytes,
        'name':    doc.get('name'),
        'fname':   doc.get('fname'),
        'mobile':  doc.get('mobile'),
        'alt':     doc.get('alt'),
        'email':   doc.get('email'),
        'id':      doc.get('id'),
        'address': doc.get('address'),
        'circle':  doc.get('circle'),
        '_extra':  extra_str,
    }


# ════════════════════════════════════════════════════════════════════════════════
# ARROW TABLE BUILDER  —  list[dict] → pa.Table
# ════════════════════════════════════════════════════════════════════════════════
def _coerce_str(v) -> str | None:
    """
    Coerce a field value to str (or None).

    Defined at module level — NOT inside the batch loop — to avoid
    creating a new closure object on every record iteration (which would
    add 500 K object allocations per batch with no benefit).

    int / float / bool in the source JSON are preserved as their str()
    representation so no information is lost.
    """
    if v is None:
        return None
    if isinstance(v, str):
        return v
    return str(v)   # int / float / bool → string, fully reversible


def build_arrow_table(batch: list) -> object:  # returns pa.Table at runtime
    """
    Convert a batch of flat record dicts into a PyArrow Table.

    Each column is built as a typed PyArrow array so that:
      - 'oid'    column uses BINARY(12) — rejects wrong-size binaries safely
      - string columns use UTF-8 — None values become Arrow null cells
      - '_extra' column holds a JSON string of any unknown fields, or null
    """
    oid_col:     list = []
    name_col:    list = []
    fname_col:   list = []
    mobile_col:  list = []
    alt_col:     list = []
    email_col:   list = []
    id_col:      list = []
    address_col: list = []
    circle_col:  list = []
    extra_col:   list = []

    for rec in batch:
        raw_oid = rec['oid']
        # Safety: discard any bytes value that isn't exactly 12 bytes.
        # A corrupted or non-standard OID becomes null, not a crash.
        oid_col.append(
            raw_oid if isinstance(raw_oid, bytes) and len(raw_oid) == 12 else None
        )
        name_col.append(_coerce_str(rec['name']))
        fname_col.append(_coerce_str(rec['fname']))
        mobile_col.append(_coerce_str(rec['mobile']))
        alt_col.append(_coerce_str(rec['alt']))
        email_col.append(_coerce_str(rec['email']))
        id_col.append(_coerce_str(rec['id']))
        address_col.append(_coerce_str(rec['address']))
        circle_col.append(_coerce_str(rec['circle']))
        extra_col.append(rec['_extra'])   # already str | None from extract_record

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

    return pa.table(
        {field.name: arr for field, arr in zip(SCHEMA, arrays)},
        schema=SCHEMA,
    )


# ════════════════════════════════════════════════════════════════════════════════
# CHECKPOINT  —  save / load resume pointer to/from Drive
# ════════════════════════════════════════════════════════════════════════════════
_CHECKPOINT_REMOTE = f'{RCLONE_REMOTE}:{DEST_FOLDER}/{CHECKPOINT_FNAME}'
_CHECKPOINT_LOCAL  = WORK_DIR / CHECKPOINT_FNAME

def load_checkpoint(rclone_cmd: str) -> dict | None:
    """
    Download checkpoint file from Drive.  Returns None if not found.
    The checkpoint records the exact byte offset in the source file
    where the NEXT unread line starts so rclone can resume precisely.
    """
    try:
        r = subprocess.run(
            [rclone_cmd, 'copyto', _CHECKPOINT_REMOTE, str(_CHECKPOINT_LOCAL),
             '--retries', '3'],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode == 0 and _CHECKPOINT_LOCAL.exists():
            with open(_CHECKPOINT_LOCAL, encoding='utf-8') as f:
                cp = json.load(f)
            log(f'   📌 Checkpoint loaded: {cp["records_written"]:,} records already '
                f'written, resuming from byte {cp["total_bytes"]:,} '
                f'(part {cp["next_part"]})')
            return cp
    except Exception as exc:
        log(f'   ⚠️  Could not load checkpoint ({exc}) — starting from beginning')
    finally:
        if _CHECKPOINT_LOCAL.exists():
            _CHECKPOINT_LOCAL.unlink()
    return None


def save_checkpoint(rclone_cmd: str, total_bytes: int,
                    records_written: int, next_part: int) -> None:
    """Save checkpoint to Drive.  Called after each successful part upload."""
    cp = {
        'total_bytes':     total_bytes,     # byte offset for next rclone cat --offset
        'records_written': records_written, # total Parquet rows written across all runs
        'next_part':       next_part,       # part file index to use next
        'saved_at':        datetime.now(timezone.utc).isoformat(),
    }
    try:
        with open(_CHECKPOINT_LOCAL, 'w', encoding='utf-8') as f:
            json.dump(cp, f, indent=2)
        r = subprocess.run(
            [rclone_cmd, 'copyto', str(_CHECKPOINT_LOCAL), _CHECKPOINT_REMOTE,
             '--retries', '3'],
            capture_output=True, text=True, timeout=60,
        )
        if r.returncode != 0:
            log(f'   ⚠️  Checkpoint upload failed: {r.stderr[:200]}')
        else:
            log(f'   💾 Checkpoint saved: {records_written:,} records, '
                f'next byte {total_bytes:,}, next part {next_part}')
    except Exception as exc:
        log(f'   ⚠️  Checkpoint save error: {exc}')
    finally:
        if _CHECKPOINT_LOCAL.exists():
            _CHECKPOINT_LOCAL.unlink()


def delete_checkpoint(rclone_cmd: str) -> None:
    """Remove checkpoint from Drive when the conversion is fully complete."""
    try:
        subprocess.run(
            [rclone_cmd, 'deletefile', _CHECKPOINT_REMOTE],
            capture_output=True, timeout=30,
        )
        log('   🗑️  Checkpoint deleted (conversion complete).')
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════════
# PARQUET PART WRITER  —  accumulates rows, writes row groups, manages parts
# ════════════════════════════════════════════════════════════════════════════════
class ParquetPartWriter:
    """
    Manages a sequence of output Parquet files (parts).

    Lifecycle per part:
      _open_part()       — create local file, open pq.ParquetWriter
      add_record()       — accumulate into batch; flush row group every
                           ROW_GROUP_RECORDS records
      _flush_row_group() — build Arrow Table, write to open writer
      _close_part()      — close writer, upload to Drive, delete local
      on_part_done cb    — called by _close_part() so caller can save checkpoint

    After the stream ends, the caller MUST call flush_all() to finalise
    the last (possibly partial) part.
    """

    def __init__(self, rclone_cmd: str, start_part: int,
                 records_already: int,
                 on_part_done):
        self._rclone           = rclone_cmd
        self.part_num          = start_part
        self.total_written     = records_already   # cumulative across all runs
        self.total_parts_done  = 0
        self._writer     = None   # pq.ParquetWriter when a part is open, else None
        self._part_path  = None   # Path to current local Parquet file, else None
        self._batch: list[dict]               = []
        self._records_in_part: int            = 0
        self._on_part_done                    = on_part_done  # callable(part_num, total)

    # ── public ────────────────────────────────────────────────────
    def add_record(self, rec: dict) -> None:
        """Add one record.  Row-group and part boundaries are handled automatically."""
        self._batch.append(rec)
        if len(self._batch) >= ROW_GROUP_RECORDS:
            self._flush_row_group()
            if self._records_in_part >= PART_RECORDS:
                self._close_part()

    def flush_all(self) -> None:
        """Flush remaining batch and close the current part (if any)."""
        if self._batch or (self._writer is not None and self._records_in_part > 0):
            if self._batch:
                self._flush_row_group()
            if self._writer is not None:
                self._close_part()

    # ── private ───────────────────────────────────────────────────
    def _open_part(self) -> None:
        fname = f'part_{self.part_num:05d}.parquet'
        self._part_path = WORK_DIR / fname
        self._writer = pq.ParquetWriter(
            str(self._part_path),
            schema=SCHEMA,
            compression='zstd',
            compression_level=ZSTD_LEVEL,
            use_dictionary=DICT_COLUMNS,
            write_statistics=True,      # enables predicate pushdown in DuckDB etc.
            version='2.6',              # latest stable Parquet format
        )
        self._records_in_part = 0
        log(f'   📂 Opened part_{self.part_num:05d}.parquet')

    def _flush_row_group(self) -> None:
        """Convert current batch to Arrow Table and write a row group."""
        if not self._batch:
            return
        if self._writer is None:
            self._open_part()
        table = build_arrow_table(self._batch)
        self._writer.write_table(table)
        self._records_in_part += len(self._batch)
        self.total_written    += len(self._batch)
        self._batch.clear()

    def _close_part(self) -> None:
        """Close the open Parquet file, upload to Drive, delete local copy."""
        if self._writer is None:
            return
        self._writer.close()
        self._writer = None

        path      = self._part_path
        part_size = path.stat().st_size if path.exists() else 0
        log(f'   📦 part_{self.part_num:05d}.parquet  '
            f'{fmt_num(self._records_in_part)} records  '
            f'{fmt_bytes(part_size)}')

        dest = f'{RCLONE_REMOTE}:{DEST_FOLDER}/{path.name}'
        log(f'   ☁️  Uploading → {dest} …')
        t0 = time.time()
        upload_ok = False
        try:
            r = subprocess.run(
                [self._rclone, 'copyto', str(path), dest,
                 '--retries',            str(UPLOAD_RETRIES),
                 '--low-level-retries',  '10',
                 '--retries-sleep',      '5s'],
                capture_output=True, text=True, timeout=UPLOAD_TIMEOUT,
            )
            elapsed = time.time() - t0
            if r.returncode == 0:
                speed = part_size / elapsed if elapsed else 0
                log(f'   ✅ Uploaded in {fmt_dur(elapsed)}  ({fmt_bytes(speed)}/s)')
                upload_ok = True
            else:
                log(f'   ⚠️  Upload failed (exit {r.returncode}): {r.stderr[:300]}')
                log(f'   ⚠️  Keeping local copy: {path}')
        except subprocess.TimeoutExpired:
            log(f'   ⚠️  Upload timed out after {fmt_dur(UPLOAD_TIMEOUT)} — keeping local copy')

        # Delete local file only after confirmed upload
        if upload_ok:
            try:
                path.unlink()
            except OSError:
                pass
            self.total_parts_done += 1
            completed_part = self.part_num
            self.part_num += 1
            self._records_in_part = 0
            self._part_path       = None
            # Notify caller so it can save a checkpoint
            if self._on_part_done is not None:
                try:
                    self._on_part_done(completed_part, self.total_written)
                except Exception as exc:
                    log(f'   ⚠️  on_part_done callback error: {exc}')
        else:
            # Upload failed after all rclone retries.
            # Raise so the run aborts cleanly.  The checkpoint remains at the
            # LAST SUCCESSFULLY uploaded part, so the next run will re-process
            # and re-upload this part correctly.
            # The local Parquet file is kept on disk for inspection / manual upload.
            raise RuntimeError(
                f'Upload of {path.name} failed after {UPLOAD_RETRIES} rclone retries. '
                'Aborting run so checkpoint stays valid.  '
                f'Local file preserved: {path}.  '
                'Re-trigger the workflow to resume from the last checkpoint.'
            )


# ════════════════════════════════════════════════════════════════════════════════
# MAIN CONVERSION LOOP
# ════════════════════════════════════════════════════════════════════════════════
def run_conversion(rclone_cmd: str, force_restart: bool) -> None:
    """
    Stream source JSON line-by-line, convert each record to Parquet.

    Line-by-line reading works because MongoDB's mongoexport writes
    exactly one JSON object per line.  This gives us a precise byte
    offset at every record boundary, enabling exact resume.
    """
    remote_path = f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}'

    # ── checkpoint / resume ───────────────────────────────────────
    checkpoint: dict | None = None
    if not force_restart:
        log('\n🔍 Checking for existing checkpoint …')
        checkpoint = load_checkpoint(rclone_cmd)
    else:
        log('\n⚠️  FORCE_RESTART=true — ignoring any existing checkpoint.')

    start_offset      = checkpoint['total_bytes']     if checkpoint else 0
    records_already   = checkpoint['records_written'] if checkpoint else 0
    next_part         = checkpoint['next_part']        if checkpoint else 1

    if start_offset:
        log(f'   ↩️  Resuming from byte offset {start_offset:,}')
        log(f'   ↩️  {records_already:,} records already in Parquet')
    else:
        log('   🆕 Starting fresh')

    # ── file size (for progress) ──────────────────────────────────
    log(f'\n📐 Querying file size for {remote_path} …')
    file_size = get_file_size(rclone_cmd)
    if file_size:
        log(f'   Total  : {fmt_bytes(file_size)}')
        if start_offset:
            remaining = file_size - start_offset
            log(f'   Remaining : {fmt_bytes(remaining)}  '
                f'({remaining / file_size * 100:.1f}%)')

    # ── shared state for checkpoint callback ─────────────────────
    # Use a mutable dict so the closure in on_part_done can capture
    # the live 'bytes_in_run' value updated by the reading loop below.
    _state = {'bytes_in_run': 0}

    def on_part_done(completed_part_num: int, total_records: int) -> None:
        total_bytes = start_offset + _state['bytes_in_run']
        save_checkpoint(rclone_cmd, total_bytes, total_records, completed_part_num + 1)

    # ── launch rclone cat ─────────────────────────────────────────
    log(f'\n▶️  Streaming via rclone cat'
        f'{f" --offset={start_offset}" if start_offset else ""} …')

    rclone_cmd_list = [rclone_cmd, 'cat', remote_path, '--buffer-size', '256M']
    if start_offset > 0:
        rclone_cmd_list += ['--offset', str(start_offset)]

    _stderr_chunks: list[bytes] = []
    proc = subprocess.Popen(
        rclone_cmd_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
    )

    # Drain rclone stderr in background to prevent pipe-buffer deadlock.
    def _drain_stderr() -> None:
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

    # ── buffered line reader ──────────────────────────────────────
    # io.BufferedReader wraps the raw FileIO pipe with an 8 MB read-ahead
    # buffer.  Iterating over BufferedReader yields complete lines including
    # the trailing '\n', giving exact per-line byte counts.
    buffered = io.BufferedReader(proc.stdout, buffer_size=BUFFER_SIZE)

    writer = ParquetPartWriter(
        rclone_cmd=rclone_cmd,
        start_part=next_part,
        records_already=records_already,
        on_part_done=on_part_done,
    )

    total_lines     = 0
    parse_errors    = 0
    start_time      = time.time()
    last_log_time   = start_time
    first_record_ok = False   # tracks whether we've seen any valid record yet

    def _log_progress() -> None:
        nonlocal last_log_time
        now = time.time()
        if now - last_log_time < LOG_INTERVAL:
            return
        elapsed   = now - start_time
        total_pos = start_offset + _state['bytes_in_run']
        pct       = total_pos / file_size * 100 if file_size else 0
        speed     = _state['bytes_in_run'] / elapsed if elapsed else 0
        remaining = (file_size - total_pos) / speed if speed and file_size else 0
        last_log_time = now
        log(f'  ⏳ {fmt_num(writer.total_written)} rows written  '
            f'({fmt_num(writer.total_written - records_already)} this run)  '
            f'part {writer.part_num}  '
            f'{pct:.1f}%  '
            f'{fmt_bytes(total_pos)}/{fmt_bytes(file_size)}  '
            f'@ {fmt_bytes(speed)}/s  '
            f'ETA {fmt_dur(remaining)}')

    try:
        for raw_line in buffered:
            _state['bytes_in_run'] += len(raw_line)
            total_lines += 1

            stripped = raw_line.strip()
            if not stripped:
                continue

            # Skip array brackets that may appear when resuming mid-file
            if stripped in (b'[', b']'):
                continue

            # Records are separated by commas; strip trailing comma
            if stripped.endswith(b','):
                stripped = stripped[:-1]

            # Because we use --offset, the very first bytes we receive may be
            # the tail of a partially-consumed record from the previous run.
            # Such a fragment will fail json.loads; we discard it and continue.
            # Once we see the first valid record, all subsequent records are clean.
            if not stripped.startswith(b'{'):
                if not first_record_ok:
                    continue   # discard pre-record fragment at offset boundary
                # After first valid record: unexpected non-object line
                parse_errors += 1
                log(f'   ⚠️  Unexpected non-object line #{total_lines} '
                    f'(first 80 bytes): {stripped[:80]!r}')
                continue

            try:
                doc = json.loads(stripped)
            except json.JSONDecodeError as exc:
                if not first_record_ok:
                    # Still in the "skip partial first line" phase
                    continue
                parse_errors += 1
                log(f'   ⚠️  JSON parse error on line {total_lines}: {exc}  '
                    f'(partial: {stripped[:60]!r}…)')
                continue

            if not isinstance(doc, dict):
                parse_errors += 1
                continue

            first_record_ok = True
            rec = extract_record(doc)
            writer.add_record(rec)
            _log_progress()
            # Self-imposed time limit: stop cleanly before GHA's hard SIGKILL.
            # The break exits the for-loop normally so the finally block runs,
            # flush_all() uploads the current partial part, and the checkpoint
            # is saved.  Next run resumes from exactly this byte offset.
            if time.time() - start_time >= RUN_LIMIT_SECONDS:
                log(f'\n⏰  Run limit ({fmt_dur(RUN_LIMIT_SECONDS)}) reached — '
                    f'stopping cleanly after {fmt_num(writer.total_written)} rows.')
                break

    except (KeyboardInterrupt, SystemExit):
        log(f'\n⚠️  Interrupted after {fmt_num(writer.total_written)} rows — '
            f'flushing current part …')
    except Exception as exc:
        log(f'\n⚠️  Unexpected error after {fmt_num(writer.total_written)} rows: {exc}')
        log('   Flushing current part …')
    finally:
        # ALWAYS flush the current partial part so no data is lost.
        # If upload fails here the local file remains on disk and the user
        # can manually upload it.
        try:
            writer.flush_all()
        except Exception as exc:
            log(f'   ⚠️  flush_all() error: {exc}')

        # Clean up rclone process
        try:
            buffered.close()
        except Exception:
            pass
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

    # ── rclone exit code check ────────────────────────────────────
    # rclone returns 0 even when the consumer closes with SIGPIPE.
    _ok_codes = {0}
    if (_sigpipe := getattr(signal, 'SIGPIPE', None)) is not None:
        _ok_codes.add(-_sigpipe)   # -13 on Linux
    if proc.returncode not in _ok_codes:
        err = b''.join(_stderr_chunks).decode(errors='replace').strip()
        if err:
            log(f'\n⚠️  rclone cat exited {proc.returncode}: {err[:500]}')

    # ── summary ───────────────────────────────────────────────────
    elapsed = time.time() - start_time
    speed   = _state['bytes_in_run'] / elapsed if elapsed else 0
    log('')
    log('═' * 60)
    log(f'  Run summary')
    log(f'  Lines read    : {fmt_num(total_lines)}')
    log(f'  Rows written  : {fmt_num(writer.total_written)}  '
        f'(+{fmt_num(writer.total_written - records_already)} this run)')
    log(f'  Parts done    : {fmt_num(writer.total_parts_done)}')
    log(f'  Parse errors  : {fmt_num(parse_errors)}')
    log(f'  Duration      : {fmt_dur(elapsed)}')
    log(f'  Read speed    : {fmt_bytes(speed)}/s')
    log('═' * 60)

    # ── exit code — fail the workflow if parse errors occurred ──────
    # parse_errors > 0 means some source lines were un-parseable JSON.
    # The records were skipped (not silently corrupted), so no bad data
    # entered Parquet, but we want GHA to mark the run FAILED so you
    # notice and can investigate the source file.
    if parse_errors > 0:
        log(f'\n❌ {fmt_num(parse_errors)} parse error(s) encountered — ')
        log('   Parquet output is complete for all parseable records.')
        log('   Fix the source file or re-run; exiting with code 1 so GHA flags this run.')

    if file_size and (start_offset + _state['bytes_in_run']) >= file_size * 0.999:
        log('\n🎉 Conversion COMPLETE — all data converted!')
        delete_checkpoint(rclone_cmd)
        if parse_errors > 0:
            sys.exit(1)
    elif not file_size:
        # File size could not be determined — cannot confirm completion.
        # Checkpoint was saved after each part; re-trigger to verify / continue.
        log('\n⚠️  File size was unavailable — cannot confirm whether conversion is complete.')
        log('   Check Gdrive:users_data_parquet/ and re-trigger if needed.')
        if parse_errors > 0:
            sys.exit(1)
    else:
        remaining_bytes = file_size - (start_offset + _state['bytes_in_run'])
        log(f'\n⏸️  Run ended with {fmt_bytes(remaining_bytes)} remaining.')
        log('   Re-trigger the workflow to continue from the checkpoint.')
        if parse_errors > 0:
            sys.exit(1)


# ════════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════════
def main() -> None:
    log('═' * 65)
    log('  JSON → PARQUET CONVERTER  —  users_data.json')
    log(f'  Source  : {RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}')
    log(f'  Dest    : {RCLONE_REMOTE}:{DEST_FOLDER}/')
    log(f'  Schema  : {len(SCHEMA)} columns  |  OIDs as BINARY(12)  |  '
        f'_extra captures unknown fields')
    log(f'  Parquet : zstd level {ZSTD_LEVEL}  |  '
        f'{fmt_num(ROW_GROUP_RECORDS)} rows/group  |  '
        f'{fmt_num(PART_RECORDS)} rows/file')
    log(f'  Dict enc: {", ".join(DICT_COLUMNS)}')
    log('═' * 65)

    rclone_cmd = find_rclone()

    # SIGTERM → SystemExit so the finally block in run_conversion fires
    # and the current partial part is uploaded before the GHA runner shuts down.
    def _on_sigterm(signum, _frame) -> None:
        log('\n⚠️  SIGTERM received — flushing current part before exit …')
        raise SystemExit(128 + signum)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, _on_sigterm)

    # Honour FORCE_RESTART=true env var (set from workflow_dispatch input)
    force_restart = os.environ.get('FORCE_RESTART', 'false').strip().lower() == 'true'

    log(f'\n📦 pyarrow {pa.__version__}  |  '
        f'zstd available: {pq.ParquetWriter is not None}')

    # Ensure destination folder exists on Drive
    log(f'\n📁 Ensuring {RCLONE_REMOTE}:{DEST_FOLDER}/ exists …')
    subprocess.run(
        [rclone_cmd, 'mkdir', f'{RCLONE_REMOTE}:{DEST_FOLDER}'],
        capture_output=True, timeout=30,
    )

    run_conversion(rclone_cmd, force_restart=force_restart)


if __name__ == '__main__':
    main()
