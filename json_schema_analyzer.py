#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
  JSON SCHEMA ANALYZER  —  streaming, resumable, zero data loss
  Designed for GitHub Actions with auto-continuation until complete

  Pipeline:
    rclone cat --offset=N  →  2 MB chunk reader  →  JSONDecoder.raw_decode()
      →  analyze_value() (recursive)  →  in-memory field stats accumulator
      →  checkpoint every 5 min (Drive)  →  final reports (JSON + TXT + CSV)

  Source file format: compact JSON array [{...},{...},…] — no newlines.

  What the schema captures per field path (dot-notation for nesting):
    • presence rate       — how often the field appears across all records
    • null rate           — how often value is null/None when present
    • type distribution   — string/integer/float/boolean/object/array/null
    • distinct count      — HyperLogLog estimate (128 buckets, ~9% error)
    • exact distinct      — precise count if ≤ UNIQUE_CAP per field
    • sample values       — up to 20 representative unique values
    • string stats        — min/max/avg length, detected patterns (email,
                            phone, date, URL, MongoDB OID, UUID, IPv4, numeric ID)
    • number stats        — min, max, mean, integer-only flag
    • boolean stats       — true/false counts and percentages
    • array stats         — min/max/avg element count, element types
    • nested object paths — recursed up to MAX_DEPTH levels via dot-notation
    • array element paths — tracked as path[].field for object-typed elements

  Output files uploaded to Google Drive:
    • schema_report.json  — full machine-readable report
    • schema_report.txt   — human-readable summary
    • schema_report.csv   — one row per field, spreadsheet-friendly

  Resumable: exact byte-offset checkpoint saved to Drive every 5 minutes.
  Auto-continues via GitHub repository_dispatch until the whole file is done.
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import csv
import gc
import hashlib
import io
import json
import math
import os
import re
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════

RCLONE_REMOTE      = 'Gdrive'
SOURCE_FOLDER      = 'users_data_extracted'
FILE_NAME          = 'users_data.json'
DEST_FOLDER        = 'users_data_parquet'          # reuse same Drive folder
CHECKPOINT_FNAME   = 'schema_checkpoint.json'
REPORT_JSON_FNAME  = 'schema_report.json'
REPORT_TXT_FNAME   = 'schema_report.txt'
REPORT_CSV_FNAME   = 'schema_report.csv'

# Streaming
STREAM_CHUNK       = 2 * 1024 * 1024               # 2 MB per raw read
BUFFER_SIZE        = 16 * 1024 * 1024              # 16 MB BufferedReader buffer

# Timing
RUN_LIMIT_SECONDS  = 5 * 3600 + 25 * 60           # stop at 5h 25m (35 min before GHA kill)
CHECKPOINT_INTERVAL = 300                           # save state every 5 minutes
HEARTBEAT_INTERVAL  = 30                            # print progress every 30 s
UPLOAD_TIMEOUT      = 600                           # 10 min max per checkpoint upload

# Schema analysis
MAX_DEPTH           = 8       # max nesting depth for recursive analysis
MAX_FIELDS          = 50_000  # safety cap: stop adding new paths beyond this
UNIQUE_CAP          = 500     # track exact unique values up to this per top-level field
UNIQUE_CAP_NESTED   = 100     # lower cap for fields at depth > 1
PATTERN_SAMPLE      = 50_000  # detect string patterns only on first N values per field
HLL_BUCKETS         = 128     # HyperLogLog bucket count (~9.2% error, ~512 B / field)

# Safety guard: maximum uncommitted str_buf tail before we declare an object
# malformed or pathologically large and skip it.  Without this, a single
# unclosed '{' anywhere in the 446 GB file would grow str_buf by 2 MB per
# chunk until the runner is OOM-killed.  64 MB covers any legitimate record.
MAX_BUF_SIZE        = 64 * 1024 * 1024   # 64 MB

WORK_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

# ════════════════════════════════════════════════════════════════════════════════
# COMPILED PATTERNS  (compiled once at import time)
# ════════════════════════════════════════════════════════════════════════════════

_RE_EMAIL    = re.compile(r'^[^@\s]{1,64}@[^@\s]{1,255}\.[a-zA-Z]{2,}$')
_RE_PHONE    = re.compile(r'^\+?[\d\s\-(). ]{7,20}$')
_RE_DATE_ISO = re.compile(r'^\d{4}[-/]\d{2}[-/]\d{2}')
_RE_DATE_DMY = re.compile(r'^\d{2}[-/]\d{2}[-/]\d{4}')
_RE_URL      = re.compile(r'^https?://')
_RE_OID      = re.compile(r'^[0-9a-f]{24}$', re.I)
_RE_UUID     = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
_RE_IPV4     = re.compile(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$')
_RE_NUM_ONLY = re.compile(r'^\d{6,}$')               # ≥6 digit numeric string (IDs, etc.)
_RE_HEX_STR  = re.compile(r'^[0-9a-f]{8,}$', re.I)  # hex-only string (≥8 chars)

# ════════════════════════════════════════════════════════════════════════════════
# GLOBAL STATE
# ════════════════════════════════════════════════════════════════════════════════

_shutdown_requested = False
_start_time         = time.time()


def _request_shutdown(signum=None, frame=None):
    global _shutdown_requested
    _shutdown_requested = True
    log(f'⚠️  Shutdown requested (signal {signum}) — will stop after current chunk')


def should_stop() -> bool:
    return _shutdown_requested or (time.time() - _start_time) >= RUN_LIMIT_SECONDS


def time_remaining() -> float:
    return max(0.0, RUN_LIMIT_SECONDS - (time.time() - _start_time))


# ════════════════════════════════════════════════════════════════════════════════
# LOGGING & FORMATTING
# ════════════════════════════════════════════════════════════════════════════════

def ts() -> str:
    return time.strftime('%H:%M:%S')


def log(msg: str) -> None:
    print(f'[{ts()}] {msg}', flush=True)


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


# ════════════════════════════════════════════════════════════════════════════════
# RCLONE HELPERS
# ════════════════════════════════════════════════════════════════════════════════

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
            [rclone_cmd, 'size', '--json',
             f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}'],
            capture_output=True, text=True, timeout=120,
        )
        if r.returncode == 0:
            return json.loads(r.stdout).get('bytes', 0)
    except Exception as e:
        log(f'⚠️  Could not get file size: {e}')
    return 0


def rclone_upload(rclone_cmd: str, local: Path, remote_name: str,
                  timeout: int = UPLOAD_TIMEOUT) -> bool:
    for attempt in range(5):
        r = subprocess.run(
            [rclone_cmd, 'copyto', str(local),
             f'{RCLONE_REMOTE}:{DEST_FOLDER}/{remote_name}',
             '--retries', '3', '--low-level-retries', '10',
             '--retries-sleep', '10s', '--drive-chunk-size', '128M'],
            capture_output=True, text=True, timeout=timeout,
        )
        if r.returncode == 0:
            return True
        log(f'⚠️  Upload attempt {attempt + 1}/5: {r.stderr[:200]}')
        if attempt < 4:
            time.sleep(min(20 * (attempt + 1), 90))
    return False


def rclone_download(rclone_cmd: str, remote_name: str, local: Path,
                    timeout: int = 300) -> bool:
    r = subprocess.run(
        [rclone_cmd, 'copyto',
         f'{RCLONE_REMOTE}:{DEST_FOLDER}/{remote_name}', str(local),
         '--retries', '5'],
        capture_output=True, text=True, timeout=timeout,
    )
    return r.returncode == 0 and local.exists()


def rclone_delete(rclone_cmd: str, remote_name: str) -> None:
    try:
        subprocess.run(
            [rclone_cmd, 'deletefile',
             f'{RCLONE_REMOTE}:{DEST_FOLDER}/{remote_name}'],
            capture_output=True, timeout=60,
        )
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════════
# HYPERLOGLOG  (128 buckets, no external deps, serializable as plain list)
# ════════════════════════════════════════════════════════════════════════════════

def _hll_new() -> list[int]:
    return [0] * HLL_BUCKETS


def _hll_add(hll: list[int], value: str) -> None:
    """Add a string value to a HyperLogLog sketch."""
    # Use MD5 as a cheap 128-bit hash (not security-critical here)
    h = int(hashlib.md5(value.encode('utf-8', errors='replace')).hexdigest(), 16)
    bucket = h & (HLL_BUCKETS - 1)    # lower bits → bucket index 0..127
    rest   = h >> 7                    # remaining bits for leading-zero count
    # rho = position of the first 1-bit (1-indexed)
    rho = 1
    while rho < 121 and not (rest & 1):
        rest >>= 1
        rho += 1
    if rho > hll[bucket]:
        hll[bucket] = rho


def _hll_estimate(hll: list[int]) -> int:
    """Estimate cardinality from HyperLogLog registers."""
    # All-zero registers == nothing was ever added (e.g. field only ever seen
    # as an object container, never as a primitive value).  Return 0 exactly
    # instead of the ~92 the formula would produce for an empty sketch.
    if all(v == 0 for v in hll):
        return 0
    m = len(hll)
    # Bias correction constant for m=128
    alpha = 0.7213 / (1.0 + 1.079 / m)
    Z = sum(2.0 ** (-v) for v in hll)
    raw = alpha * m * m / Z
    # Small-range correction (linear counting)
    V = hll.count(0)
    if raw <= 2.5 * m and V > 0:
        raw = m * math.log(m / V)
    # Large-range correction
    two_32 = 2 ** 32
    if raw > two_32 / 30:
        raw = -two_32 * math.log(1.0 - raw / two_32)
    return max(int(raw), 0)


# ════════════════════════════════════════════════════════════════════════════════
# FIELD STATS STRUCTURE
# In-memory: 'uniq_set' is a Python set for O(1) lookup.
# JSON form:  'uniq_set' is serialized as sorted list 'uniq'.
# ════════════════════════════════════════════════════════════════════════════════

def _new_field() -> dict:
    return {
        # Core
        'total':        0,          # records where this path appeared
        'null':         0,          # times value was null/None
        'types':        {},         # type_name → count

        # String stats
        's_min':        None,       # min string length seen
        's_max':        None,       # max string length seen
        's_sum_len':    0,          # sum of all string lengths (for avg)
        's_count':      0,          # count of string occurrences
        'patterns':     {},         # pattern_name → count (sampled)
        'pat_sample_done': False,   # True once PATTERN_SAMPLE threshold hit

        # Number stats
        'n_min':        None,
        'n_max':        None,
        'n_sum':        0.0,
        'n_count':      0,
        'n_int_count':  0,          # how many numeric values were whole numbers

        # Boolean stats
        'b_true':       0,
        'b_false':      0,

        # Array stats
        'a_min_len':    None,
        'a_max_len':    None,
        'a_sum_len':    0,
        'a_count':      0,
        'a_elem_types': {},         # element type_name → count

        # Cardinality tracking
        'uniq_set':     set(),      # exact unique values (in-memory only, not in JSON)
        'uniq_cap_hit': False,      # True once we stopped tracking exactly
        'hll':          _hll_new(), # HyperLogLog registers for approx. distinct count
    }


# ════════════════════════════════════════════════════════════════════════════════
# PATTERN DETECTION
# ════════════════════════════════════════════════════════════════════════════════

def _detect_patterns(s: str) -> list[str]:
    """Return list of pattern labels that match the string s."""
    found = []
    if _RE_EMAIL.match(s):      found.append('email')
    if _RE_PHONE.match(s):      found.append('phone')
    if _RE_DATE_ISO.match(s):   found.append('date_iso')
    if _RE_DATE_DMY.match(s):   found.append('date_dmy')
    if _RE_URL.match(s):        found.append('url')
    if _RE_OID.match(s):        found.append('mongodb_oid')
    if _RE_UUID.match(s):       found.append('uuid')
    if _RE_IPV4.match(s):       found.append('ipv4')
    if _RE_NUM_ONLY.match(s):   found.append('numeric_id')
    if _RE_HEX_STR.match(s):    found.append('hex_string')
    return found


# ════════════════════════════════════════════════════════════════════════════════
# VALUE ANALYSIS  (recursive)
# ════════════════════════════════════════════════════════════════════════════════

def _type_name(v: Any) -> str:
    if v is None:              return 'null'
    if isinstance(v, bool):    return 'boolean'
    if isinstance(v, int):     return 'integer'
    if isinstance(v, float):   return 'float'
    if isinstance(v, str):     return 'string'
    if isinstance(v, dict):    return 'object'
    if isinstance(v, list):    return 'array'
    return type(v).__name__


def _track_unique(f: dict, sv: str, depth: int) -> None:
    """Track unique string representation for cardinality and sampling."""
    # Always update HLL
    _hll_add(f['hll'], sv)
    # Exact set (capped)
    if not f['uniq_cap_hit']:
        cap = UNIQUE_CAP if depth <= 1 else UNIQUE_CAP_NESTED
        us = f['uniq_set']
        if sv not in us:
            if len(us) < cap:
                us.add(sv)
            else:
                f['uniq_cap_hit'] = True


def analyze_value(path: str, value: Any, fields: dict, depth: int = 0) -> None:
    """
    Recursively analyze `value` at `path` and update `fields`.

    Handles: None, bool, int, float, str, dict, list, and unknown types.
    Nested dicts create child paths via dot-notation: 'parent.child'.
    Array object elements create paths via: 'parent[].child'.
    """
    if depth > MAX_DEPTH:
        return

    # Enforce field cap to prevent memory explosion from adversarial/corrupted data
    if path not in fields:
        if len(fields) >= MAX_FIELDS:
            return
        fields[path] = _new_field()

    f = fields[path]
    f['total'] += 1

    # ── None / null ──────────────────────────────────────────────────────────
    if value is None:
        f['null'] += 1
        f['types']['null'] = f['types'].get('null', 0) + 1
        return

    # ── Boolean  (must precede int — bool is subclass of int in Python) ──────
    if isinstance(value, bool):
        f['types']['boolean'] = f['types'].get('boolean', 0) + 1
        if value:
            f['b_true'] += 1
        else:
            f['b_false'] += 1
        _track_unique(f, str(value), depth)
        return

    # ── Integer ───────────────────────────────────────────────────────────────
    if isinstance(value, int):
        f['types']['integer'] = f['types'].get('integer', 0) + 1
        fv = float(value)
        if f['n_min'] is None or fv < f['n_min']:  f['n_min'] = fv
        if f['n_max'] is None or fv > f['n_max']:  f['n_max'] = fv
        f['n_sum']       += fv
        f['n_count']     += 1
        f['n_int_count'] += 1
        _track_unique(f, str(value), depth)
        return

    # ── Float ─────────────────────────────────────────────────────────────────
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            f['types']['float_special'] = f['types'].get('float_special', 0) + 1
        else:
            f['types']['float'] = f['types'].get('float', 0) + 1
            if f['n_min'] is None or value < f['n_min']:  f['n_min'] = value
            if f['n_max'] is None or value > f['n_max']:  f['n_max'] = value
            f['n_sum']   += value
            f['n_count'] += 1
            if value == int(value):
                f['n_int_count'] += 1
            _track_unique(f, repr(value), depth)
        return

    # ── String ────────────────────────────────────────────────────────────────
    if isinstance(value, str):
        f['types']['string'] = f['types'].get('string', 0) + 1
        n = len(value)
        if f['s_min'] is None or n < f['s_min']:  f['s_min'] = n
        if f['s_max'] is None or n > f['s_max']:  f['s_max'] = n
        f['s_sum_len'] += n
        f['s_count']   += 1
        _track_unique(f, value, depth)
        # Pattern detection — only on the first PATTERN_SAMPLE values (perf guard)
        if not f['pat_sample_done'] and value:
            for pat in _detect_patterns(value):
                f['patterns'][pat] = f['patterns'].get(pat, 0) + 1
            if f['s_count'] >= PATTERN_SAMPLE:
                f['pat_sample_done'] = True
        return

    # ── Dict / object ─────────────────────────────────────────────────────────
    if isinstance(value, dict):
        f['types']['object'] = f['types'].get('object', 0) + 1
        for k, v in value.items():
            # Sanitize key: strip dots/brackets to avoid path confusion
            safe_k = str(k).replace('.', '_').replace('[', '(').replace(']', ')')
            analyze_value(f'{path}.{safe_k}', v, fields, depth + 1)
        return

    # ── List / array ──────────────────────────────────────────────────────────
    if isinstance(value, list):
        f['types']['array'] = f['types'].get('array', 0) + 1
        n = len(value)
        if f['a_min_len'] is None or n < f['a_min_len']:  f['a_min_len'] = n
        if f['a_max_len'] is None or n > f['a_max_len']:  f['a_max_len'] = n
        f['a_sum_len'] += n
        f['a_count']   += 1
        # Track element types and recurse into nested objects
        # Limit: analyse at most 50 elements per array occurrence (perf guard)
        for elem in value[:50]:
            etype = _type_name(elem)
            f['a_elem_types'][etype] = f['a_elem_types'].get(etype, 0) + 1
            if isinstance(elem, dict):
                for k2, v2 in elem.items():
                    safe_k2 = str(k2).replace('.', '_').replace('[', '(').replace(']', ')')
                    analyze_value(f'{path}[].{safe_k2}', v2, fields, depth + 1)
        return

    # ── Unknown type ─────────────────────────────────────────────────────────
    tn = type(value).__name__
    f['types'][f'unknown_{tn}'] = f['types'].get(f'unknown_{tn}', 0) + 1


# ════════════════════════════════════════════════════════════════════════════════
# CHECKPOINT SERIALIZATION / DESERIALIZATION
# ════════════════════════════════════════════════════════════════════════════════

def _fields_to_json(fields: dict) -> dict:
    """Convert in-memory fields dict to JSON-serializable form."""
    out = {}
    for path, f in fields.items():
        out[path] = {
            'total':           f['total'],
            'null':            f['null'],
            'types':           f['types'],
            's_min':           f['s_min'],
            's_max':           f['s_max'],
            's_sum_len':       f['s_sum_len'],
            's_count':         f['s_count'],
            'patterns':        f['patterns'],
            'pat_sample_done': f['pat_sample_done'],
            'n_min':           f['n_min'],
            'n_max':           f['n_max'],
            'n_sum':           f['n_sum'],
            'n_count':         f['n_count'],
            'n_int_count':     f['n_int_count'],
            'b_true':          f['b_true'],
            'b_false':         f['b_false'],
            'a_min_len':       f['a_min_len'],
            'a_max_len':       f['a_max_len'],
            'a_sum_len':       f['a_sum_len'],
            'a_count':         f['a_count'],
            'a_elem_types':    f['a_elem_types'],
            'uniq':            sorted(f['uniq_set']),   # set → sorted list
            'uniq_cap_hit':    f['uniq_cap_hit'],
            'hll':             f['hll'],
        }
    return out


def _fields_from_json(data: dict) -> dict:
    """Restore in-memory fields dict from JSON checkpoint data."""
    fields = {}
    for path, d in data.items():
        fields[path] = {
            'total':           d['total'],
            'null':            d['null'],
            'types':           d['types'],
            's_min':           d['s_min'],
            's_max':           d['s_max'],
            's_sum_len':       d['s_sum_len'],
            's_count':         d['s_count'],
            'patterns':        d['patterns'],
            'pat_sample_done': d.get('pat_sample_done', d['s_count'] >= PATTERN_SAMPLE),
            'n_min':           d['n_min'],
            'n_max':           d['n_max'],
            'n_sum':           d['n_sum'],
            'n_count':         d['n_count'],
            'n_int_count':     d['n_int_count'],
            'b_true':          d['b_true'],
            'b_false':         d['b_false'],
            'a_min_len':       d['a_min_len'],
            'a_max_len':       d['a_max_len'],
            'a_sum_len':       d['a_sum_len'],
            'a_count':         d['a_count'],
            'a_elem_types':    d.get('a_elem_types', {}),
            'uniq_set':        set(d.get('uniq', [])),   # list → set
            'uniq_cap_hit':    d['uniq_cap_hit'],
            'hll':             d['hll'],
        }
    return fields


# ════════════════════════════════════════════════════════════════════════════════
# CHECKPOINT MANAGEMENT
# ════════════════════════════════════════════════════════════════════════════════

_CP_LOCAL  = WORK_DIR / CHECKPOINT_FNAME
_CP_REMOTE = CHECKPOINT_FNAME


def save_checkpoint(rclone_cmd: str, total_bytes: int, total_records: int,
                    parse_errors: int, fields: dict,
                    status: str = 'in_progress') -> bool:
    cp = {
        'total_bytes':   total_bytes,
        'total_records': total_records,
        'parse_errors':  parse_errors,
        'status':        status,
        'saved_at':      datetime.now(timezone.utc).isoformat(),
        'fields':        _fields_to_json(fields),
    }
    try:
        with open(_CP_LOCAL, 'w', encoding='utf-8') as fh:
            # Compact separators — minimise checkpoint file size
            json.dump(cp, fh, separators=(',', ':'), ensure_ascii=False)
        sz = _CP_LOCAL.stat().st_size
        log(f'💾 Checkpoint: {fmt_num(total_records)} records | '
            f'{len(fields)} fields | {fmt_bytes(sz)}')
        ok = rclone_upload(rclone_cmd, _CP_LOCAL, _CP_REMOTE)
        if not ok:
            log('⚠️  Checkpoint upload failed — will retry next interval')
        return ok
    except Exception as e:
        log(f'⚠️  Checkpoint save error: {e}')
        return False
    finally:
        if _CP_LOCAL.exists():
            try: _CP_LOCAL.unlink()
            except OSError: pass


def load_checkpoint(rclone_cmd: str) -> dict | None:
    try:
        if _CP_LOCAL.exists():
            _CP_LOCAL.unlink()
        if rclone_download(rclone_cmd, _CP_REMOTE, _CP_LOCAL):
            with open(_CP_LOCAL, encoding='utf-8') as fh:
                cp = json.load(fh)
            log(f'📌 Checkpoint loaded: {fmt_num(cp["total_records"])} records | '
                f'{len(cp["fields"])} fields | byte {cp["total_bytes"]:,}')
            return cp
    except Exception as e:
        log(f'⚠️  Checkpoint load failed ({e}) — starting fresh')
    finally:
        if _CP_LOCAL.exists():
            try: _CP_LOCAL.unlink()
            except OSError: pass
    return None


def delete_checkpoint(rclone_cmd: str) -> None:
    rclone_delete(rclone_cmd, _CP_REMOTE)
    log('🗑️  Checkpoint deleted')


# ════════════════════════════════════════════════════════════════════════════════
# REPORT GENERATION
# ════════════════════════════════════════════════════════════════════════════════

def _build_field_report(path: str, f: dict, total_records: int) -> dict:
    """Build the report dict for a single field path."""
    total  = f['total']
    null   = f['null']
    types  = f['types']

    presence_pct = round(total / total_records * 100, 4) if total_records > 0 else 0.0
    null_pct     = round(null  / total * 100, 4)         if total > 0 else 0.0

    dominant_type = max(types, key=types.get) if types else 'unknown'

    type_dist = {
        k: {'count': v, 'pct': round(v / total * 100, 2) if total > 0 else 0.0}
        for k, v in sorted(types.items(), key=lambda x: -x[1])
    }

    est_distinct   = _hll_estimate(f['hll'])
    exact_distinct = len(f['uniq_set']) if not f['uniq_cap_hit'] else None

    rep: dict = {
        'path':           path,
        # FIX: path.count('.') + path.count('[].') double-counts the '.' inside
        # '[].', e.g. 'a[].b' gives 2 instead of the correct 1.
        # Replacing '[]. ' with a plain '.' normalises array separators first.
        'depth':          path.replace('[].', '.').count('.'),
        'total':          total,
        'missing':        total_records - total,
        'null':           null,
        'presence_pct':   presence_pct,
        'null_pct':       null_pct,
        'dominant_type':  dominant_type,
        'type_dist':      type_dist,
        'distinct_est':   est_distinct,
        'distinct_exact': exact_distinct,
    }

    # String stats
    if f['s_count'] > 0:
        rep['string'] = {
            'count':   f['s_count'],
            'min_len': f['s_min'],
            'max_len': f['s_max'],
            'avg_len': round(f['s_sum_len'] / f['s_count'], 2),
            'patterns': {
                k: {
                    'count': v,
                    'sample_pct': round(v / min(f['s_count'], PATTERN_SAMPLE) * 100, 2),
                    'note': 'sampled' if f['s_count'] > PATTERN_SAMPLE else 'exact',
                }
                for k, v in sorted(f['patterns'].items(), key=lambda x: -x[1])
            },
            'pattern_sampled': f['pat_sample_done'],
            # Up to 20 representative sample values, sorted for readability
            'samples': sorted(list(f['uniq_set']))[:20],
            'samples_truncated': f['uniq_cap_hit'],
        }

    # Number stats
    if f['n_count'] > 0:
        rep['number'] = {
            'count':      f['n_count'],
            'min':        f['n_min'],
            'max':        f['n_max'],
            'mean':       round(f['n_sum'] / f['n_count'], 6),
            'always_int': f['n_int_count'] == f['n_count'],
            'int_pct':    round(f['n_int_count'] / f['n_count'] * 100, 2),
        }

    # Boolean stats
    bool_total = f['b_true'] + f['b_false']
    if bool_total > 0:
        rep['boolean'] = {
            'true_count':  f['b_true'],
            'false_count': f['b_false'],
            'true_pct':    round(f['b_true'] / bool_total * 100, 2),
        }

    # Array stats
    if f['a_count'] > 0:
        rep['array'] = {
            'count':       f['a_count'],
            'min_len':     f['a_min_len'],
            'max_len':     f['a_max_len'],
            'avg_len':     round(f['a_sum_len'] / f['a_count'], 2),
            'elem_types':  dict(
                sorted(f['a_elem_types'].items(), key=lambda x: -x[1])
            ),
        }

    return rep


def build_report(fields: dict, total_records: int, total_bytes: int,
                 parse_errors: int, elapsed: float) -> dict:
    """Assemble the full JSON schema report."""
    field_reports = {
        path: _build_field_report(path, f, total_records)
        for path, f in sorted(fields.items())
    }
    return {
        'meta': {
            'source':          f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}',
            'total_records':   total_records,
            'total_bytes':     total_bytes,
            'total_bytes_fmt': fmt_bytes(total_bytes),
            'parse_errors':    parse_errors,
            'duration_secs':   round(elapsed, 1),
            'duration_fmt':    fmt_dur(elapsed),
            'total_fields':    len(field_reports),
            'generated_at':    datetime.now(timezone.utc).isoformat(),
            'analyzer_config': {
                'max_depth':      MAX_DEPTH,
                'unique_cap':     UNIQUE_CAP,
                'pattern_sample': PATTERN_SAMPLE,
                'hll_buckets':    HLL_BUCKETS,
            },
        },
        'fields': field_reports,
    }


# ════════════════════════════════════════════════════════════════════════════════
# TEXT REPORT
# ════════════════════════════════════════════════════════════════════════════════

def write_text_report(report: dict) -> str:
    lines = []
    m = report['meta']

    lines += [
        '═' * 80,
        '  JSON SCHEMA ANALYSIS REPORT',
        f'  Source    : {m["source"]}',
        f'  Records   : {m["total_records"]:,}',
        f'  File size : {m["total_bytes_fmt"]}',
        f'  Fields    : {m["total_fields"]}',
        f'  Errors    : {m["parse_errors"]:,}',
        f'  Duration  : {m["duration_fmt"]}',
        f'  Generated : {m["generated_at"]}',
        f'  Config    : max_depth={m["analyzer_config"]["max_depth"]} | '
            f'unique_cap={m["analyzer_config"]["unique_cap"]} | '
            f'pattern_sample={m["analyzer_config"]["pattern_sample"]:,} | '
            f'hll_buckets={m["analyzer_config"]["hll_buckets"]}',
        '═' * 80,
        '',
        f'{"FIELD PATH":<55} {"PRESENCE%":>9} {"NULL%":>7} {"DOMINANT TYPE":<15} {"~DISTINCT":>10}',
        '─' * 100,
    ]

    # Summary table: all fields sorted by presence descending
    for f in sorted(report['fields'].values(), key=lambda x: (-x['presence_pct'], x['path'])):
        dist_str = (str(f['distinct_exact']) if f.get('distinct_exact') is not None
                    else f'~{f["distinct_est"]:,}')
        lines.append(
            f'{f["path"]:<55} {f["presence_pct"]:>8.2f}% {f["null_pct"]:>6.2f}% '
            f'{f["dominant_type"]:<15} {dist_str:>10}'
        )

    lines += ['', '═' * 80, '  DETAILED FIELD ANALYSIS', '═' * 80, '']

    for path, f in sorted(report['fields'].items()):
        lines.append(f'┌── {path}')
        lines.append(f'│   presence  : {f["presence_pct"]}%  '
                     f'({f["total"]:,} of {m["total_records"]:,} records | '
                     f'missing={f["missing"]:,})')
        lines.append(f'│   null      : {f["null_pct"]}%  ({f["null"]:,})')
        lines.append(f'│   types     :')
        for tname, tinfo in f['type_dist'].items():
            lines.append(f'│      {tname:<20} {tinfo["count"]:>12,}  ({tinfo["pct"]}%)')

        dist_exact = f.get('distinct_exact')
        if dist_exact is not None:
            lines.append(f'│   distinct  : {dist_exact:,} (exact)')
        else:
            lines.append(f'│   distinct  : ~{f["distinct_est"]:,} (HyperLogLog estimate)')

        if 'string' in f:
            s = f['string']
            lines.append(f'│   ── string ──')
            lines.append(f'│   count     : {s["count"]:,}')
            lines.append(f'│   length    : {s["min_len"]}..{s["max_len"]}  avg={s["avg_len"]}')
            if s['patterns']:
                lines.append(f'│   patterns  :')
                for pat, pinfo in s['patterns'].items():
                    note = f' [{pinfo["note"]}]' if s.get('pattern_sampled') else ''
                    lines.append(f'│      {pat:<22} {pinfo["count"]:>10,}  '
                                 f'({pinfo["sample_pct"]}% of sample{note})')
            if s['samples']:
                sample_str = ',  '.join(repr(x) for x in s['samples'][:15])
                lines.append(f'│   samples   : {sample_str}')
                if s.get('samples_truncated'):
                    lines.append(f'│               ... (>{UNIQUE_CAP} distinct values, truncated)')

        if 'number' in f:
            n = f['number']
            lines.append(f'│   ── number ──')
            lines.append(f'│   count     : {n["count"]:,}')
            lines.append(f'│   range     : {n["min"]} → {n["max"]}  mean={n["mean"]}')
            lines.append(f'│   always_int: {n["always_int"]}  ({n["int_pct"]}% are integers)')

        if 'boolean' in f:
            b = f['boolean']
            lines.append(f'│   ── boolean ──')
            lines.append(f'│   true      : {b["true_count"]:,}  ({b["true_pct"]}%)')
            lines.append(f'│   false     : {b["false_count"]:,}')

        if 'array' in f:
            a = f['array']
            lines.append(f'│   ── array ──')
            lines.append(f'│   count     : {a["count"]:,}')
            lines.append(f'│   length    : {a["min_len"]}..{a["max_len"]}  avg={a["avg_len"]}')
            elem_str = ',  '.join(f'{k}={v:,}' for k, v in a['elem_types'].items())
            lines.append(f'│   elem types: {elem_str}')

        lines.append('│')

    lines += ['═' * 80, '  END OF REPORT', '═' * 80]
    return '\n'.join(lines)


# ════════════════════════════════════════════════════════════════════════════════
# CSV REPORT
# ════════════════════════════════════════════════════════════════════════════════

def write_csv_report(report: dict) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        'path', 'depth', 'total', 'missing', 'null',
        'presence_pct', 'null_pct',
        'dominant_type',
        'type_string', 'type_integer', 'type_float', 'type_boolean',
        'type_object', 'type_array', 'type_null',
        'distinct_est', 'distinct_exact',
        'str_count', 'str_min_len', 'str_max_len', 'str_avg_len',
        'pattern_email', 'pattern_phone', 'pattern_date_iso', 'pattern_date_dmy',
        'pattern_url', 'pattern_mongodb_oid', 'pattern_uuid',
        'pattern_ipv4', 'pattern_numeric_id', 'pattern_hex_string',
        'pattern_sampled',
        'num_count', 'num_min', 'num_max', 'num_mean', 'num_always_int', 'num_int_pct',
        'bool_true_count', 'bool_false_count', 'bool_true_pct',
        'arr_count', 'arr_min_len', 'arr_max_len', 'arr_avg_len',
        'arr_elem_types',
        'sample_values',
    ])

    for path, f in sorted(report['fields'].items()):
        td = f['type_dist']
        s  = f.get('string', {})
        n  = f.get('number', {})
        b  = f.get('boolean', {})
        a  = f.get('array', {})
        pat = s.get('patterns', {})

        def tc(tname):  # type count helper
            return td.get(tname, {}).get('count', '')

        arr_elem_str = '; '.join(
            f'{k}={v}' for k, v in a.get('elem_types', {}).items()
        )
        samples_str = ' | '.join(
            (str(x) for x in s.get('samples', [])[:10])
        )

        writer.writerow([
            path,
            f['depth'],
            f['total'],
            f['missing'],
            f['null'],
            f['presence_pct'],
            f['null_pct'],
            f['dominant_type'],
            tc('string'), tc('integer'), tc('float'), tc('boolean'),
            tc('object'), tc('array'), tc('null'),
            f['distinct_est'],
            f.get('distinct_exact', ''),
            s.get('count', ''), s.get('min_len', ''), s.get('max_len', ''), s.get('avg_len', ''),
            pat.get('email', {}).get('count', ''),
            pat.get('phone', {}).get('count', ''),
            pat.get('date_iso', {}).get('count', ''),
            pat.get('date_dmy', {}).get('count', ''),
            pat.get('url', {}).get('count', ''),
            pat.get('mongodb_oid', {}).get('count', ''),
            pat.get('uuid', {}).get('count', ''),
            pat.get('ipv4', {}).get('count', ''),
            pat.get('numeric_id', {}).get('count', ''),
            pat.get('hex_string', {}).get('count', ''),
            s.get('pattern_sampled', ''),
            n.get('count', ''), n.get('min', ''), n.get('max', ''),
            n.get('mean', ''), n.get('always_int', ''), n.get('int_pct', ''),
            b.get('true_count', ''), b.get('false_count', ''), b.get('true_pct', ''),
            a.get('count', ''), a.get('min_len', ''), a.get('max_len', ''),
            a.get('avg_len', ''), arr_elem_str,
            samples_str,
        ])

    return buf.getvalue()


# ════════════════════════════════════════════════════════════════════════════════
# UPLOAD REPORTS
# ════════════════════════════════════════════════════════════════════════════════

def upload_reports(rclone_cmd: str, report: dict) -> bool:
    """
    Generate JSON, TXT, CSV reports and upload all three to Drive.
    Returns True only when ALL three uploads succeed.
    Callers MUST check the return value — if False, save status='reports_pending'
    so the next run retries without re-streaming the entire source file.
    """
    log('\n📊 Generating and uploading reports...')
    all_ok = True

    # ── JSON ──
    json_path = WORK_DIR / REPORT_JSON_FNAME
    try:
        with open(json_path, 'w', encoding='utf-8') as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
        sz = json_path.stat().st_size
        log(f'   JSON  : {fmt_bytes(sz)}')
        if rclone_upload(rclone_cmd, json_path, REPORT_JSON_FNAME, timeout=1200):
            log(f'   ✅ Uploaded {REPORT_JSON_FNAME}')
        else:
            log(f'   ❌ Failed to upload {REPORT_JSON_FNAME}')
            all_ok = False
    except Exception as e:
        log(f'   ❌ JSON report error: {e}')
        all_ok = False
    finally:
        json_path.unlink(missing_ok=True)

    # ── TXT ──
    txt_path = WORK_DIR / REPORT_TXT_FNAME
    try:
        txt = write_text_report(report)
        with open(txt_path, 'w', encoding='utf-8') as fh:
            fh.write(txt)
        sz = txt_path.stat().st_size
        log(f'   TXT   : {fmt_bytes(sz)}')
        if rclone_upload(rclone_cmd, txt_path, REPORT_TXT_FNAME):
            log(f'   ✅ Uploaded {REPORT_TXT_FNAME}')
        else:
            log(f'   ❌ Failed to upload {REPORT_TXT_FNAME}')
            all_ok = False
    except Exception as e:
        log(f'   ❌ TXT report error: {e}')
        all_ok = False
    finally:
        txt_path.unlink(missing_ok=True)

    # ── CSV ──
    csv_path = WORK_DIR / REPORT_CSV_FNAME
    try:
        csv_data = write_csv_report(report)
        with open(csv_path, 'w', encoding='utf-8', newline='') as fh:
            fh.write(csv_data)
        sz = csv_path.stat().st_size
        log(f'   CSV   : {fmt_bytes(sz)}')
        if rclone_upload(rclone_cmd, csv_path, REPORT_CSV_FNAME):
            log(f'   ✅ Uploaded {REPORT_CSV_FNAME}')
        else:
            log(f'   ❌ Failed to upload {REPORT_CSV_FNAME}')
            all_ok = False
    except Exception as e:
        log(f'   ❌ CSV report error: {e}')
        all_ok = False
    finally:
        csv_path.unlink(missing_ok=True)

    return all_ok


# ════════════════════════════════════════════════════════════════════════════════
# MAIN ANALYSIS LOOP
# ════════════════════════════════════════════════════════════════════════════════

def run_analysis(rclone_cmd: str, force_restart: bool) -> str:
    """
    Stream the source JSON file, analyze every field, checkpoint every 5 min.
    Returns 'complete' or 'continue'.
    """
    # ── Load or initialize state ─────────────────────────────────────────────
    fields: dict  = {}
    start_offset  = 0
    total_records = 0
    parse_errors  = 0

    if not force_restart:
        log('\n🔍 Checking for existing checkpoint on Drive...')
        cp = load_checkpoint(rclone_cmd)
        if cp:
            start_offset  = cp['total_bytes']
            total_records = cp['total_records']
            parse_errors  = cp.get('parse_errors', 0)
            fields        = _fields_from_json(cp['fields'])

            if cp.get('status') == 'complete':
                log('✅ Analysis was already completed per checkpoint — done!')
                return 'complete'

            # ── FIX: Handle reports_pending status ───────────────────────────
            # Previous run finished streaming the entire file and accumulated
            # all field stats, but one or more Drive uploads failed (quota,
            # timeout, etc.).  Rebuild the reports and re-upload them without
            # re-streaming the 446 GB source file — the full schema state is
            # already in the checkpoint and restored above.
            if cp.get('status') == 'reports_pending':
                log('📊 Previous run completed analysis but reports were not')
                log('   fully uploaded — rebuilding and re-uploading now...')
                _report = build_report(fields, total_records, start_offset,
                                       parse_errors, 0.0)
                _ok = upload_reports(rclone_cmd, _report)
                if _ok:
                    log('✅ All reports uploaded — marking complete')
                    save_checkpoint(rclone_cmd, start_offset, total_records,
                                    parse_errors, fields, status='complete')
                    delete_checkpoint(rclone_cmd)
                    return 'complete'
                else:
                    log('⚠️  Some reports still failed — will retry next run')
                    save_checkpoint(rclone_cmd, start_offset, total_records,
                                    parse_errors, fields, status='reports_pending')
                    return 'continue'

            log(f'↩️  Resuming: {fmt_num(total_records)} records | '
                f'{len(fields)} fields | byte {start_offset:,}')
        else:
            log('🆕 No checkpoint — starting fresh')
    else:
        log('⚠️  FORCE_RESTART=true — ignoring existing checkpoint')

    # ── File size ─────────────────────────────────────────────────────────────
    log('\n📐 Querying file size...')
    file_size = get_file_size(rclone_cmd)
    if file_size:
        pct = start_offset / file_size * 100
        log(f'   Total: {fmt_bytes(file_size)} | '
            f'Done: {fmt_bytes(start_offset)} ({pct:.1f}% complete)')
    else:
        log('   ⚠️  Could not determine file size — continuing without ETA')

    # ── Startup checkpoint (written before any streaming begins) ─────────────
    # Ensures the safety-net step in the workflow can always find a checkpoint
    # and re-trigger even if the runner is SIGKILL'd in the first few minutes.
    log('\n💾 Writing startup checkpoint...')
    save_checkpoint(rclone_cmd, start_offset, total_records, parse_errors, fields)

    # ── Launch rclone stream ──────────────────────────────────────────────────
    remote_path = f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}'
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

    buffered = io.BufferedReader(proc.stdout, buffer_size=BUFFER_SIZE)

    # ── Pre-stream heartbeat ──────────────────────────────────────────────────
    # rclone can take several minutes to auth with Google Drive before the first
    # byte arrives. This thread prevents GHA stall-detection killing the job
    # during that silent window.
    _stream_started = threading.Event()
    _pre_start      = time.time()

    def _pre_heartbeat():
        while not _stream_started.is_set():
            time.sleep(HEARTBEAT_INTERVAL)
            if not _stream_started.is_set():
                waited = int(time.time() - _pre_start)
                log(f'⏳ Waiting for rclone stream... ({waited}s — '
                    f'Drive is establishing connection)')

    threading.Thread(target=_pre_heartbeat, daemon=True).start()

    # ── Streaming state ───────────────────────────────────────────────────────
    bytes_read           = 0
    analysis_complete    = False
    last_checkpoint_time = time.time()
    last_heartbeat_time  = time.time()

    decoder  = json.JSONDecoder()
    str_buf  = ''       # accumulated decoded-text buffer
    str_pos  = 0        # scan position within str_buf

    log(f'\n▶️  Starting stream from byte {start_offset:,}...')

    try:
        while True:
            # ── Read next raw chunk ───────────────────────────────────────────
            raw = buffered.read(STREAM_CHUNK)

            if raw:
                if not _stream_started.is_set():
                    _stream_started.set()
                    log('✅ Stream started — connection established')
                bytes_read += len(raw)
                str_buf    += raw.decode('utf-8', errors='replace')

            # ── Drain all complete JSON objects from the buffer ───────────────
            while True:
                brace = str_buf.find('{', str_pos)
                if brace == -1:
                    str_buf = ''
                    str_pos = 0
                    break

                try:
                    doc, end = decoder.raw_decode(str_buf, brace)
                    if isinstance(doc, dict):
                        total_records += 1
                        for key, val in doc.items():
                            analyze_value(key, val, fields, depth=0)
                    str_pos = end

                except json.JSONDecodeError:
                    if not raw:
                        # Genuine corrupt / truncated JSON at end of stream
                        parse_errors += 1
                        str_pos = brace + 1     # skip bad '{', keep scanning
                    else:
                        # Object spans chunk boundary — need more data
                        str_buf = str_buf[brace:]
                        str_pos = 0
                        break   # get next raw chunk

            # Trim fully-consumed buffer prefix
            if str_pos >= len(str_buf):
                str_buf = ''
                str_pos = 0
            elif str_pos > 1024 * 1024:     # >1 MB consumed — compact
                str_buf = str_buf[str_pos:]
                str_pos = 0

            # ── Buffer overflow guard (BUG FIX) ───────────────────────────────
            # If the uncommitted tail of str_buf exceeds MAX_BUF_SIZE we have
            # been failing to close one object across many chunks.  This means
            # either a malformed unclosed '{' or a single pathologically large
            # record.  Without this guard str_buf grows by 2 MB every chunk
            # until the 15 GB runner RAM is exhausted and the job is OOM-killed.
            # Fix: log the overflow, count it as a parse error, then seek
            # forward to the next '{' so processing continues normally.
            uncommitted = len(str_buf) - str_pos
            if uncommitted > MAX_BUF_SIZE:
                parse_errors += 1
                log(f'⚠️  Buffer overflow: {fmt_bytes(uncommitted)} uncommitted '
                    f'at source byte ~{start_offset + bytes_read:,} — '
                    f'skipping malformed/oversized object')
                next_brace = str_buf.find('{', str_pos + 1)
                if next_brace != -1:
                    str_buf = str_buf[next_brace:]
                else:
                    str_buf = ''
                str_pos = 0
                gc.collect()

            # ── Periodic control ──────────────────────────────────────────────
            now = time.time()

            # Heartbeat (keeps GHA alive and shows progress)
            if now - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                total_pos = start_offset + bytes_read
                pct       = total_pos / file_size * 100 if file_size else 0.0
                elapsed   = now - _start_time
                speed     = bytes_read / elapsed if elapsed > 0 else 0
                eta       = (file_size - total_pos) / speed if (speed > 0 and file_size) else 0
                log(f'⏳ {fmt_num(total_records)} records | {len(fields)} fields | '
                    f'{pct:.2f}% | {fmt_bytes(speed)}/s | ETA {fmt_dur(eta)} | '
                    f'time left: {fmt_dur(time_remaining())}')
                last_heartbeat_time = now

            # Checkpoint
            if now - last_checkpoint_time >= CHECKPOINT_INTERVAL:
                save_checkpoint(
                    rclone_cmd,
                    start_offset + bytes_read,
                    total_records, parse_errors, fields,
                )
                last_checkpoint_time = now

            # Time-limit stop
            if should_stop():
                log(f'\n⏰ Time limit reached — stopping at {fmt_num(total_records)} records')
                break

            # EOF — stream finished cleanly
            if not raw:
                analysis_complete = True
                log('✅ Stream finished — EOF received')
                break

    except (KeyboardInterrupt, SystemExit):
        log('\n⚠️  Interrupted — saving progress...')
    except Exception as e:
        import traceback
        log(f'\n❌ Unexpected error: {e}')
        log(traceback.format_exc())
    finally:
        try: buffered.close()
        except Exception: pass
        try: proc.stdout.close()
        except Exception: pass
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try: proc.kill()
            except Exception: pass
        stderr_thread.join(timeout=5)

    elapsed         = time.time() - _start_time
    speed_avg       = bytes_read / elapsed if elapsed > 0 else 0
    total_bytes_done = start_offset + bytes_read

    log('')
    log('═' * 65)
    log('  Run Summary')
    log(f'  Records analyzed : {fmt_num(total_records)}')
    log(f'  Fields discovered: {len(fields)}')
    log(f'  Parse errors     : {fmt_num(parse_errors)}')
    log(f'  Bytes this run   : {fmt_bytes(bytes_read)} at {fmt_bytes(speed_avg)}/s')
    log(f'  Duration         : {fmt_dur(elapsed)}')
    log('═' * 65)

    # ── Check if we finished the whole file ───────────────────────────────────
    if analysis_complete and (not file_size or total_bytes_done >= file_size * 0.999):
        log('\n🎉 Analysis complete! Building and uploading reports...')
        report     = build_report(fields, total_records, total_bytes_done, parse_errors, elapsed)
        reports_ok = upload_reports(rclone_cmd, report)

        if reports_ok:
            # All three report files confirmed on Drive.
            # Save a 'complete' marker and then delete it so any accidental
            # re-trigger exits immediately rather than re-streaming.
            save_checkpoint(rclone_cmd, total_bytes_done, total_records,
                            parse_errors, fields, status='complete')
            delete_checkpoint(rclone_cmd)
            return 'complete'
        else:
            # FIX: do NOT delete the checkpoint when reports fail.
            # Saving status='reports_pending' lets the next run re-upload
            # reports without re-streaming the entire 446 GB source file.
            # The full field accumulator is preserved in the checkpoint.
            log('⚠️  Some reports failed to upload — saving reports_pending')
            save_checkpoint(rclone_cmd, total_bytes_done, total_records,
                            parse_errors, fields, status='reports_pending')
            log('⏸️  Will re-upload reports next run (no re-streaming needed)')
            return 'continue'

    # ── Partial run — save state and request continuation ─────────────────────
    save_checkpoint(rclone_cmd, total_bytes_done, total_records, parse_errors, fields)
    log('\n⏸️  Run ended. Auto-triggering next run...')
    return 'continue'


# ════════════════════════════════════════════════════════════════════════════════
# TRIGGER NEXT RUN
# ════════════════════════════════════════════════════════════════════════════════

def trigger_next_run() -> None:
    """Fire repository_dispatch to auto-chain the next GitHub Actions run."""
    token      = os.environ.get('GH_PAT') or os.environ.get('GITHUB_TOKEN')
    token_type = 'GH_PAT' if os.environ.get('GH_PAT') else 'GITHUB_TOKEN'
    repo       = os.environ.get('GITHUB_REPOSITORY')

    if not token or not repo:
        log('⚠️  Cannot auto-trigger: missing GH_PAT/GITHUB_TOKEN or GITHUB_REPOSITORY')
        return

    import urllib.request
    import urllib.error

    log(f'🔑 Triggering next run via {token_type}...')
    url  = f'https://api.github.com/repos/{repo}/dispatches'
    data = json.dumps({'event_type': 'continue_schema_analysis'}).encode('utf-8')
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
                log('🚀 Next run triggered successfully!')
            else:
                log(f'⚠️  Auto-trigger response: {resp.status}')
    except urllib.error.HTTPError as e:
        log(f'⚠️  Auto-trigger HTTP error: {e.code} {e.reason}')
    except Exception as e:
        log(f'⚠️  Auto-trigger error: {e}')


# ════════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════════

def main() -> None:
    global _start_time
    _start_time = time.time()

    log('═' * 70)
    log('  JSON SCHEMA ANALYZER  —  Streaming, Resumable, Full Coverage')
    log(f'  Source     : {RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}')
    log(f'  Dest       : {RCLONE_REMOTE}:{DEST_FOLDER}/')
    log(f'  Max depth  : {MAX_DEPTH}  (dot-notation nesting)')
    log(f'  Unique cap : {UNIQUE_CAP} (top-level) / {UNIQUE_CAP_NESTED} (nested)')
    log(f'  Pat sample : first {fmt_num(PATTERN_SAMPLE)} strings per field')
    log(f'  HLL bucket : {HLL_BUCKETS}  (~{round(1.04 / math.sqrt(HLL_BUCKETS) * 100, 1)}% cardinality error)')
    log(f'  Time limit : {fmt_dur(RUN_LIMIT_SECONDS)}')
    log('═' * 70)

    # Signal handlers for graceful shutdown
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, _request_shutdown)
    if hasattr(signal, 'SIGINT'):
        signal.signal(signal.SIGINT, _request_shutdown)

    rclone_cmd    = find_rclone()
    force_restart = os.environ.get('FORCE_RESTART', 'false').strip().lower() == 'true'

    # Ensure destination folder exists on Drive
    log(f'\n📁 Ensuring {RCLONE_REMOTE}:{DEST_FOLDER}/ exists...')
    subprocess.run(
        [rclone_cmd, 'mkdir', f'{RCLONE_REMOTE}:{DEST_FOLDER}'],
        capture_output=True, timeout=60,
    )

    status = run_analysis(rclone_cmd, force_restart=force_restart)

    if status == 'complete':
        log('')
        log('🎉' * 25)
        log('  SCHEMA ANALYSIS COMPLETE!')
        log(f'  Reports on Drive: {RCLONE_REMOTE}:{DEST_FOLDER}/')
        log(f'    • {REPORT_JSON_FNAME}  — full machine-readable report')
        log(f'    • {REPORT_TXT_FNAME}  — human-readable summary')
        log(f'    • {REPORT_CSV_FNAME}  — spreadsheet-friendly one-row-per-field')
        log('🎉' * 25)
        sys.exit(0)
    elif status == 'continue':
        trigger_next_run()
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
