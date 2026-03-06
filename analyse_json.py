#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════
  JSON ANALYSER  —  zero-download, fully-streaming edition
  Runs on GitHub Actions (Ubuntu) or any machine with rclone

  Pipeline:  rclone cat (single HTTP stream) → ijson pipe → stats tree → report

  NO FUSE MOUNT.  rclone cat downloads straight into this process via a pipe
  at 50–100 MB/s.  FUSE VFS was causing ~9 MB/s due to per-read HTTP requests.

  What it produces
  ────────────────────────────────────────────────────────────────────
  json_analysis_report.json   — full machine-readable data
  json_analysis_report.txt    — human-readable detailed summary

  Analysis covers
  ────────────────────────────────────────────────────────────────────
  • File size, top-level structure (array / object), record count
  • Every field at every nesting depth (up to MAX_DEPTH)
  • Per-field: type distribution, null/missing rate, sample values,
    string-length stats, numeric min/max/mean, array-length stats,
    top-N most frequent values, estimated cardinality
  • Schema tree with field presence rates
  • Runtime estimate and throughput

  Runtime estimate on GitHub Actions
  ────────────────────────────────────────────────────────────────────
  479 GB file, rclone cat ~60–100 MB/s → ~1.5–2.5 h, well within 6 h.
═══════════════════════════════════════════════════════════════════════
"""

# /// script
# requires-python = ">=3.10"
# dependencies = ["ijson>=3.2"]
# ///

import os, sys, json, time, re, signal, subprocess, threading, collections, io, math
from datetime import datetime, timezone

# ── auto-install dependencies ─────────────────────────────────────────────────
def _pip(*pkgs):
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', *pkgs])

try:
    import ijson
except ImportError:
    print("📦 Installing ijson…"); _pip('ijson')
    import ijson

# Replaced in main() with the specific C backend module when available.
# run_analysis() uses _ijson so the backend choice is actually enforced.
_ijson = ijson


# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════
RCLONE_REMOTE  = 'Gdrive'
SOURCE_FOLDER  = 'users_data_extracted'   # Drive folder that holds users_data.json
FILE_NAME      = 'users_data.json'

REPORT_JSON    = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'json_analysis_report.json')
REPORT_TXT     = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'json_analysis_report.txt')

MAX_DEPTH             = 6     # max nesting depth to recurse into
MAX_SAMPLES           = 25    # unique sample values kept per field
MAX_TOP_VALUES        = 200   # top-N frequent values tracked per field (Counter cap)
MAX_CHILDREN_PER_DEPTH = 500  # max unique keys tracked per object node (memory guard)
LOG_INTERVAL          = 60    # seconds between progress lines in CI

# ════════════════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════════════════
_IS_TTY = sys.stdout.isatty() and not os.environ.get('CI')

def fmt_bytes(n: float) -> str:
    for u in ('B','KB','MB','GB','TB'):
        if abs(n) < 1024: return f'{n:.2f} {u}'
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
    """
    Read exactly n bytes from a pipe stream, retrying until n bytes or EOF.

    A single pipe_stream.read(n) on a raw OS pipe (bufsize=0 FileIO) returns
    only what is currently in the kernel pipe buffer — often much fewer than
    n bytes when the producer (rclone) has only just started writing.  This
    retry loop guarantees enough bytes are available to reliably detect the
    JSON top-level structure before handing control to ijson.
    """
    buf = b''
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:   # EOF or closed pipe
            break
        buf += chunk
    return buf


def get_file_size(rclone_cmd: str) -> int:
    """Query the remote for file size without downloading."""
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
# SCHEMA TREE  — accumulates per-field statistics
# ════════════════════════════════════════════════════════════════════════════════
def new_field() -> dict:
    """Create a fresh statistics node for one field."""
    return {
        'seen':        0,     # times this field was present in a record
        'null_count':  0,     # times value was JSON null
        'type_counts': {},    # {type_name: count}
        # string stats
        'str_len': None,      # (min, max, total, count)  — None until first string
        # number stats
        'num': None,          # (min, max, total, count)  — None until first number
        # array-length stats
        'arr_len': None,      # (min, max, total, count)
        # bool breakdown
        'bool_true':  0,
        'bool_false': 0,
        # value sampling
        'samples':    [],     # first MAX_SAMPLES unique str representations
        '_sset':      set(),  # backing set — stripped before serialisation
        # top frequent values  (capped at MAX_TOP_VALUES entries)
        'top_vals':   collections.Counter(),
        # child fields (nested objects / array items)
        'children':   {},
    }


def _update_minmax(slot, value: float, count_delta: int = 1):
    """Update a (min, max, total, count) tuple in place."""
    if slot is None:
        return (value, value, value, 1)
    vmin, vmax, vtotal, vcount = slot
    return (
        min(vmin, value),
        max(vmax, value),
        vtotal + value,
        vcount + count_delta,
    )


def analyse_value(node: dict, value, depth: int = 0) -> None:
    """Recursively analyse one field value and update node statistics."""
    node['seen'] += 1

    # ── null ──────────────────────────────────────────────────────
    if value is None:
        node['null_count'] += 1
        node['type_counts']['null'] = node['type_counts'].get('null', 0) + 1
        return

    # ── bool (must come before int because bool is subclass of int) ──
    if isinstance(value, bool):
        tc = node['type_counts']
        tc['bool'] = tc.get('bool', 0) + 1
        if value:
            node['bool_true'] += 1
        else:
            node['bool_false'] += 1
        _add_sample(node, value)
        _add_top(node, str(value))
        return

    # ── int / float ───────────────────────────────────────────────
    if isinstance(value, (int, float)):
        kind = 'int' if isinstance(value, int) else 'float'
        tc = node['type_counts']
        tc[kind] = tc.get(kind, 0) + 1
        try:
            node['num'] = _update_minmax(node['num'], float(value))
        except (ValueError, OverflowError):
            pass
        _add_sample(node, value)
        _add_top(node, str(value))
        return

    # ── string ────────────────────────────────────────────────────
    if isinstance(value, str):
        tc = node['type_counts']
        tc['string'] = tc.get('string', 0) + 1
        node['str_len'] = _update_minmax(node['str_len'], len(value))
        _add_sample(node, value)
        # only track top-values for reasonably short strings (not 10 KB blobs)
        if len(value) <= 200:
            _add_top(node, value)
        return

    # ── list ──────────────────────────────────────────────────────
    if isinstance(value, list):
        tc = node['type_counts']
        tc['array'] = tc.get('array', 0) + 1
        node['arr_len'] = _update_minmax(node['arr_len'], len(value))
        if depth < MAX_DEPTH and value:
            child = node['children'].setdefault('[item]', new_field())
            for item in value:
                analyse_value(child, item, depth + 1)
        return

    # ── object ────────────────────────────────────────────────────
    if isinstance(value, dict):
        tc = node['type_counts']
        tc['object'] = tc.get('object', 0) + 1
        if depth < MAX_DEPTH:
            for k, v in value.items():
                key = str(k)
                # Once the cap is reached, skip NEW keys but continue updating
                # already-tracked ones.  Using break here would give different
                # records different key subsets, producing artifically low
                # presence_rate values for keys beyond position 500.
                if key not in node['children']:
                    if len(node['children']) >= MAX_CHILDREN_PER_DEPTH:
                        continue
                    node['children'][key] = new_field()
                analyse_value(node['children'][key], v, depth + 1)
        return

    # ── fallback (bytes, etc.) ────────────────────────────────────
    tc = node['type_counts']
    tc[type(value).__name__] = tc.get(type(value).__name__, 0) + 1


def _add_sample(node: dict, value) -> None:
    sv = str(value)
    ss = node['_sset']
    if len(node['samples']) < MAX_SAMPLES and sv not in ss:
        node['samples'].append(sv)
        ss.add(sv)


def _add_top(node: dict, sv: str) -> None:
    tv = node['top_vals']
    if sv in tv or len(tv) < MAX_TOP_VALUES:
        tv[sv] += 1


# ════════════════════════════════════════════════════════════════════════════════
# SERIALISE schema tree to plain dicts (remove non-JSON helpers)
# ════════════════════════════════════════════════════════════════════════════════
def serialise_node(node: dict, total_records: int) -> dict:
    presence = node['seen'] / total_records if total_records else 0
    out: dict = {
        'presence_rate': round(presence, 6),
        'seen':          node['seen'],
        'null_count':    node['null_count'],
        'null_rate':     round(node['null_count'] / node['seen'], 6) if node['seen'] else 0,
        'type_counts':   dict(node['type_counts']),
        'dominant_type': max(node['type_counts'], key=node['type_counts'].get)
                         if node['type_counts'] else 'unknown',
    }

    # string length stats
    if node['str_len']:
        vmin, vmax, vtot, vcnt = node['str_len']
        out['string_length'] = {
            'min': vmin, 'max': vmax,
            'mean': round(vtot / vcnt, 2),
            'total_chars': vtot,
        }

    # numeric stats
    if node['num']:
        vmin, vmax, vtot, vcnt = node['num']
        out['numeric'] = {
            'min': vmin, 'max': vmax,
            'mean': round(vtot / vcnt, 6),
            'count': vcnt,
        }

    # array-length stats
    if node['arr_len']:
        vmin, vmax, vtot, vcnt = node['arr_len']
        out['array_length'] = {
            'min': vmin, 'max': vmax,
            'mean': round(vtot / vcnt, 2),
        }

    # bool breakdown
    if node['bool_true'] or node['bool_false']:
        btotal = node['bool_true'] + node['bool_false']
        out['bool'] = {
            'true':  node['bool_true'],
            'false': node['bool_false'],
            'true_rate': round(node['bool_true'] / btotal, 4) if btotal else 0,
        }

    # sample values
    out['samples'] = node['samples']

    # top values (top-10 by frequency)
    top = node['top_vals']
    if top:
        most_common = top.most_common(10)
        out['top_values']         = {k: v for k, v in most_common}
        out['estimated_unique']   = len(top)
        out['counter_saturated']  = len(top) >= MAX_TOP_VALUES

    # child fields
    if node['children']:
        out['children'] = {
            k: serialise_node(v, node['seen'])
            for k, v in sorted(node['children'].items())
        }

    return out


# ════════════════════════════════════════════════════════════════════════════════
# STREAMING ANALYSIS  —  reads from a raw byte stream (rclone cat pipe)
# ════════════════════════════════════════════════════════════════════════════════

class _CountingStream(io.RawIOBase):
    """
    Wraps a raw binary pipe and counts bytes consumed.

    Inherits from io.RawIOBase so that C-extension ijson backends (yajl2_c,
    yajl2_cffi) accept it without silently falling back to a Python-side read
    loop.  Both read() and readinto() are implemented: some C backends call
    read(), others call readinto() for zero-copy efficiency; either way
    bytes_read is updated accurately so progress percentages are correct.

    NOTE: __slots__ still works with io.RawIOBase (a C type) — Python will
    create slots for _src and bytes_read and suppress __dict__ on instances.
    """
    __slots__ = ('_src', 'bytes_read')

    def __init__(self, src):
        super().__init__()
        self._src       = src
        self.bytes_read = 0

    def readable(self) -> bool:
        return True

    def read(self, n: int = -1) -> bytes:
        chunk = self._src.read(n)
        if chunk:
            self.bytes_read += len(chunk)
        return chunk or b''

    def readinto(self, b) -> int:
        """Called by some C backends instead of read() for zero-copy efficiency."""
        data = self._src.read(len(b))
        n = len(data)
        if n:
            b[:n] = data
            self.bytes_read += n
        return n


class _PrependStream:
    """
    Concatenates a bytes header with a tail stream so ijson sees one stream.
    Necessary because we peeked at the first 65536 bytes to detect structure.
    """
    __slots__ = ('_buf', '_tail')

    def __init__(self, head: bytes, tail):
        self._buf  = head
        self._tail = tail

    def read(self, n: int = -1) -> bytes:
        if self._buf:
            if n < 0 or n >= len(self._buf):
                out        = self._buf
                self._buf  = b''
                rest       = self._tail.read() if n < 0 else self._tail.read(n - len(out))
                return out + rest
            out       = self._buf[:n]
            self._buf = self._buf[n:]
            return out
        return self._tail.read(n)


def detect_top_level_from_bytes(header: bytes) -> tuple[str, str]:
    """
    Inspect the first bytes of the stream to determine JSON structure.
    Returns ('array', 'item'), ('object', '<key>.item'), or ('object', '').
    """
    stripped = header.lstrip()
    if not stripped:
        return ('unknown', '')

    if stripped[0:1] == b'[':
        return ('array', 'item')

    if stripped[0:1] == b'{':
        try:
            partial = stripped.decode('utf-8', errors='replace')
            # Prefer a key whose array is non-empty (has content after '[').
            # This avoids picking a metadata/empty array like "tags": [] when
            # the real records array "users": [{...}] appears later.
            m = re.search(r'"([^"]+)"\s*:\s*\[\s*\S', partial)
            if m:
                return ('object', m.group(1) + '.item')
            # Fall back: first array-valued key regardless of content.
            m = re.search(r'"([^"]+)"\s*:\s*\[', partial)
            if m:
                return ('object', m.group(1) + '.item')
        except Exception:
            pass
        return ('object', '')

    return ('unknown', '')


def run_analysis(pipe_stream, file_size: int) -> dict:
    """
    Stream-parse bytes from pipe_stream (rclone cat stdout) with ijson.
    file_size is used only for progress percentage / ETA display.
    """
    # Peek at first 64 KB to detect structure, then prepend back.
    # Use _read_up_to instead of a single read(): a raw OS pipe returns only
    # what is currently buffered -- often far fewer than 65536 bytes when
    # rclone has just started.  The retry loop guarantees enough context for
    # detect_top_level_from_bytes to work correctly.
    header    = _read_up_to(pipe_stream, 65536)
    top_level, prefix = detect_top_level_from_bytes(header)
    if top_level == 'unknown':
        log(f'\u26a0\ufe0f  Unrecognised top-level JSON structure.')
        log(f'   First bytes: {header[:64]!r}')
        log('   Expected "[" (array) or "{" (object). Check RCLONE_REMOTE config.')
    log(f'\U0001f50d Top-level structure: {top_level}  |  ijson prefix: "{prefix}"')

    counter = _CountingStream(_PrependStream(header, pipe_stream))
    # bytes_read is already 0 from __init__.  The header is replayed through
    # _PrependStream so the counter correctly tracks total bytes consumed from
    # the very start of the JSON file (header bytes + remainder).

    root          = new_field()
    record_count  = 0
    start_time    = time.time()
    last_log_time = start_time

    def _items(pfx):
        try:
            return _ijson.items(counter, pfx, use_float=True)
        except TypeError:
            return _ijson.items(counter, pfx)

    def _kvitems():
        try:
            return _ijson.kvitems(counter, '', use_float=True)
        except TypeError:
            return _ijson.kvitems(counter, '')

    def _log_progress():
        nonlocal last_log_time
        now = time.time()
        if now - last_log_time < LOG_INTERVAL:
            return
        elapsed = now - start_time
        pos     = counter.bytes_read
        pct     = pos / file_size * 100 if file_size else 0
        speed   = pos / elapsed if elapsed else 0
        eta     = (file_size - pos) / speed if speed else 0
        last_log_time = now
        log(f'  ⏳ {fmt_num(record_count)} records  '
            f'{pct:.1f}%  '
            f'{fmt_bytes(pos)} / {fmt_bytes(file_size)}  '
            f'@ {fmt_bytes(speed)}/s  '
            f'ETA {fmt_dur(eta)}')

    parse_error: str | None = None
    interrupted: bool       = False
    try:
        if top_level == 'array' or (top_level == 'object' and '.' in prefix):
            for record in _items(prefix):
                record_count += 1
                if isinstance(record, dict):
                    for k, v in record.items():
                        analyse_value(root['children'].setdefault(str(k), new_field()), v, 1)
                    root['seen'] += 1
                else:
                    analyse_value(root, record, 0)
                _log_progress()
        else:
            for key, value in _kvitems():
                record_count += 1
                analyse_value(root['children'].setdefault(str(key), new_field()), value, 1)
                root['seen'] = record_count
                _log_progress()
    except Exception as exc:
        # Catch all parse/backend errors, not just ijson.JSONError: when _ijson
        # is set to a specific backend module (yajl2_cffi, yajl2_c), it may raise
        # a backend-local JSONError subclass that isn't reachable via ijson.JSONError.
        # PARTIAL DATA: a parse error after N records must not discard everything.
        parse_error = str(exc)
        log(f'⚠️  JSON parse error after {fmt_num(record_count)} records: {exc}')
        log('   Saving partial report with data collected so far…')
    except (KeyboardInterrupt, SystemExit):
        # Ctrl-C or SIGTERM (converted to SystemExit by the signal handler in
        # main).  Do NOT re-raise: fall through so run_analysis returns partial
        # data and the caller can save a report before exiting.
        interrupted = True
        log(f'\n⚠️  Interrupted after {fmt_num(record_count)} records'
            f' — saving partial report…')

    elapsed    = time.time() - start_time
    total_read = counter.bytes_read
    throughput = total_read / elapsed if elapsed else 0

    result = {
        '_root':           root,
        'record_count':    record_count,
        'top_level_type':  top_level,
        'ijson_prefix':    prefix,
        'file_size_bytes': file_size,
        'bytes_consumed':  total_read,
        'elapsed_seconds': round(elapsed, 1),
        'throughput_bps':  round(throughput, 1),
    }
    if parse_error is not None:
        result['parse_error'] = parse_error
        result['partial']     = True
    if interrupted:
        result['interrupted'] = True
        result['partial']     = True
    return result


# ════════════════════════════════════════════════════════════════════════════════
# REPORT GENERATION
# ════════════════════════════════════════════════════════════════════════════════
def _sanitise_for_json(obj):
    """
    Recursively replace float('inf') / float('nan') / float('-inf') with None.

    json.dump() raises ValueError for these even when default=str is set,
    because default= only handles *unknown types*, not built-in floats.
    These values can enter the schema tree through _update_minmax() when the
    source JSON contains non-finite numbers or when use_float=True is active.
    """
    if isinstance(obj, float):
        return None if not math.isfinite(obj) else obj
    if isinstance(obj, dict):
        return {k: _sanitise_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitise_for_json(v) for v in obj]
    return obj


def build_report(raw: dict) -> dict:
    root         = raw['_root']
    total        = raw['record_count']
    file_size    = raw['file_size_bytes']
    elapsed      = raw['elapsed_seconds']
    throughput   = raw['throughput_bps']

    schema = {}
    if root['children']:
        # records were dicts — children are top-level fields
        schema = {
            k: serialise_node(v, total)
            for k, v in sorted(root['children'].items())
        }
    else:
        schema = {'[root]': serialise_node(root, total)}

    return {
        'generated_at':      datetime.now(timezone.utc).isoformat(),
        'source':            f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}',
        'file_size_bytes':   file_size,
        'file_size_human':   fmt_bytes(file_size),
        'top_level_type':    raw['top_level_type'],
        'ijson_prefix':      raw['ijson_prefix'],
        'total_records':     total,
        'elapsed_seconds':   elapsed,
        'elapsed_human':     fmt_dur(elapsed),
        'throughput_human':  fmt_bytes(throughput) + '/s',
        'field_count':       len(schema),
        'schema':            schema,
        **({'WARNING':      f'PARTIAL DATA — JSON parse error: {raw["parse_error"]}',
             'partial':     True}
           if raw.get('partial') else {}),
    }


# ════════════════════════════════════════════════════════════════════════════════
# HUMAN-READABLE REPORT
# ════════════════════════════════════════════════════════════════════════════════
def render_txt(report: dict) -> str:
    lines = []
    W = 80

    def hr(char='═'): lines.append(char * W)
    def h1(t):        hr(); lines.append(f'  {t}'); hr()
    def h2(t):        lines.append(''); lines.append(f'── {t} ' + '─' * max(0, W - 4 - len(t)))
    def row(k, v):    lines.append(f'  {k:<38} {v}')

    h1('JSON STRUCTURE ANALYSIS REPORT')
    lines.append(f'  Generated : {report["generated_at"]}')
    lines.append(f'  Source    : {report["source"]}')
    lines.append('')

    h2('FILE OVERVIEW')
    row('File size',          report['file_size_human'])
    row('Top-level type',     report['top_level_type'])
    row('Total records',      fmt_num(report['total_records']))
    row('Top-level fields',   fmt_num(report['field_count']))
    row('Analysis duration',  report['elapsed_human'])
    row('Average throughput', report['throughput_human'])

    h2('SCHEMA — ALL FIELDS')

    def render_field(name: str, fdata: dict, indent: int = 0):
        pad    = '  ' * indent
        prefix = '├─ ' if indent > 0 else ''
        tc     = fdata.get('type_counts', {})
        pres   = fdata.get('presence_rate', 0)
        null_r = fdata.get('null_rate', 0)

        type_str = ', '.join(f'{k}:{v}' for k, v in sorted(tc.items(), key=lambda x: -x[1]))
        lines.append('')
        lines.append(f'{pad}{prefix}◆ {name}')
        lines.append(f'{pad}   presence : {pres*100:.1f}%  (seen {fmt_num(fdata["seen"])} times)')
        lines.append(f'{pad}   types    : {type_str}')
        if null_r > 0:
            lines.append(f'{pad}   nulls    : {null_r*100:.2f}%  ({fmt_num(fdata["null_count"])})')

        if 'string_length' in fdata:
            sl = fdata['string_length']
            lines.append(f'{pad}   str-len  : min={sl["min"]}  max={sl["max"]}  avg={sl["mean"]}')

        if 'numeric' in fdata:
            nm = fdata['numeric']
            lines.append(f'{pad}   numeric  : min={nm["min"]}  max={nm["max"]}  mean={nm["mean"]}')

        if 'array_length' in fdata:
            al = fdata['array_length']
            lines.append(f'{pad}   arr-len  : min={al["min"]}  max={al["max"]}  avg={al["mean"]}')

        if 'bool' in fdata:
            b = fdata['bool']
            lines.append(f'{pad}   bool     : true={b["true"]} false={b["false"]} true_rate={b["true_rate"]*100:.1f}%')

        if fdata.get('samples'):
            sv = '  |  '.join(str(s)[:60] for s in fdata['samples'][:8])
            lines.append(f'{pad}   samples  : {sv}')

        if fdata.get('top_values'):
            top = fdata['top_values']
            saturated = fdata.get('counter_saturated', False)
            satd = ' [counter full — more values exist]' if saturated else ''
            top_str = '  |  '.join(f'"{k}"={v}' for k, v in list(top.items())[:5])
            lines.append(f'{pad}   top vals : {top_str}{satd}')
            uniq = fdata.get('estimated_unique', 0)
            uniq_str = f'≥{fmt_num(uniq)}' if saturated else fmt_num(uniq)
            lines.append(f'{pad}   est. uniq: {uniq_str}')

        # recurse into children
        for child_name, child_data in fdata.get('children', {}).items():
            render_field(child_name, child_data, indent + 1)

    for field_name, field_data in report['schema'].items():
        render_field(field_name, field_data, indent=0)

    hr()
    lines.append(f'  End of report  ·  {report["source"]}')
    hr()

    return '\n'.join(lines)


# ════════════════════════════════════════════════════════════════════════════════
# RCLONE CAT  —  single HTTP stream, no FUSE, ~60-100 MB/s
# ════════════════════════════════════════════════════════════════════════════════
def stream_and_analyse(rclone_cmd: str) -> dict:
    """
    Launch  rclone cat  and pipe its stdout directly into run_analysis.
    One continuous HTTP stream — no FUSE, no kernel round trips.
    """
    remote_path = f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}'

    log(f'\n📐 Querying file size for {remote_path} …')
    file_size = get_file_size(rclone_cmd)
    if file_size:
        log(f'   {fmt_bytes(file_size)}')
    else:
        log('   (size unavailable — % and ETA will show 0)')

    log(f'\n▶️  Streaming via rclone cat …')

    # Drain rclone stderr continuously in a background thread.
    # Without this the 64 KB OS pipe buffer fills, rclone blocks inside write(),
    # nothing arrives on stdout, and Python blocks in ijson — deadlock.
    # The thread is daemon so it never prevents the process from exiting.
    _stderr_chunks: list[bytes] = []

    proc = subprocess.Popen(
        [
            rclone_cmd, 'cat',
            remote_path,
            '--buffer-size', '256M',
            # NOTE: --drive-chunk-size controls multipart UPLOAD chunk size and
            # is silently ignored (or may error on newer rclone) for cat/reads.
        ],
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

    _stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    _stderr_thread.start()

    try:
        raw = run_analysis(proc.stdout, file_size)
    except Exception:
        proc.kill()
        raise
    finally:
        try:
            proc.stdout.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=5)   # reap zombie after SIGKILL
            except Exception:
                pass
        except Exception:
            pass
        _stderr_thread.join(timeout=5)

    # rclone returns 0 even when the consumer closes the pipe (SIGPIPE/exit 141).
    # Suppress -SIGPIPE so an interrupted-but-successful run looks clean.
    _sigpipe_code = getattr(signal, 'SIGPIPE', None)
    _ok_returncodes = {0}
    if _sigpipe_code is not None:
        _ok_returncodes.add(-_sigpipe_code)   # typically -13 on Linux
    if proc.returncode not in _ok_returncodes:
        # Genuine rclone-level error — log captured stderr for diagnosis.
        err = b''.join(_stderr_chunks).decode(errors='replace').strip()
        if err:
            log(f'⚠️  rclone cat exited {proc.returncode}: {err[:500]}')

    return raw


# ════════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════════
def main():
    log('═' * 60)
    log('  JSON ANALYSER  —  users_data.json')
    log(f'  Source  : {RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}')
    log(f'  Reports : {REPORT_JSON}')
    log(f'            {REPORT_TXT}')
    log('═' * 60)

    rclone_cmd = find_rclone()

    # Convert SIGTERM to SystemExit so the finally blocks in stream_and_analyse
    # and run_analysis fire and the partial report is saved before exit.
    # GitHub Actions sends SIGTERM when the 6-hour job timeout is reached.
    def _on_sigterm(signum, _frame):
        log('\n\u26a0\ufe0f  SIGTERM received \u2014 saving partial report before exit\u2026')
        raise SystemExit(128 + signum)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, _on_sigterm)

    # Enforce the fastest available C backend rather than just logging it.
    # Importing the backend module and calling _ijson.items() directly is the
    # only reliable way to guarantee which backend is used.  The module-level
    # ijson.items() auto-selects at import time and the log message below could
    # say “yajl2_cffi active” while ijson.items() silently uses something else.
    global _ijson
    try:
        import ijson.backends.yajl2_cffi as _ijson_backend
        _ijson = _ijson_backend
        log(f'⚡ ijson C backend (yajl2_cffi) enforced  '
            f'[ijson.backend={getattr(ijson, "backend", "?")!r}]')
    except ImportError:
        try:
            import ijson.backends.yajl2_c as _ijson_backend
            _ijson = _ijson_backend
            log(f'⚡ ijson C backend (yajl2_c) enforced  '
                f'[ijson.backend={getattr(ijson, "backend", "?")!r}]')
        except ImportError:
            log(f'⚠️  ijson pure-Python backend active  '
                f'[ijson.backend={getattr(ijson, "backend", "?")!r}]  '
                f'— install yajl2 for ~3× speedup')

    raw_stats = stream_and_analyse(rclone_cmd)

    if not raw_stats or (raw_stats['record_count'] == 0
                          and not raw_stats.get('partial')
                          and not raw_stats.get('interrupted')):
        log('No data collected \u2014 exiting.')
        sys.exit(1)

    if raw_stats.get('interrupted'):
        log(f'\u26a0\ufe0f  Run was interrupted after {fmt_num(raw_stats["record_count"])} records.')
        log('   Partial report will be saved.')
    elif raw_stats.get('partial'):
        log(f'\u26a0\ufe0f  PARTIAL DATA — JSON parse error interrupted the stream.')
        log(f'   Records collected before error: {fmt_num(raw_stats["record_count"])}')
        log(f'   Error: {raw_stats["parse_error"]}')

    record_count = raw_stats['record_count']
    elapsed      = raw_stats['elapsed_seconds']
    throughput   = raw_stats['throughput_bps']
    file_size    = raw_stats['file_size_bytes']

    log('')
    log(f'✅ Stream complete:  {fmt_num(record_count)} records  '
        f'in {fmt_dur(elapsed)}  '
        f'({fmt_bytes(throughput)}/s avg)')

    log('\n📊 Building report…')
    report = build_report(raw_stats)

    # ── save JSON report ──────────────────────────────────────────
    with open(REPORT_JSON, 'w', encoding='utf-8') as f:
        json.dump(_sanitise_for_json(report), f, indent=2, default=str, ensure_ascii=False)
    log(f'   💾 {REPORT_JSON}  ({fmt_bytes(os.path.getsize(REPORT_JSON))})')

    # ── save text report ─────────────────────────────────────────
    txt = render_txt(report)
    with open(REPORT_TXT, 'w', encoding='utf-8') as f:
        f.write(txt)
    log(f'   📄 {REPORT_TXT}  ({fmt_bytes(os.path.getsize(REPORT_TXT))})')

    # ── print text summary to stdout ──────────────────────────────
    print('\n')
    print(txt)

    log('')
    if raw_stats.get('interrupted'):
        log('⚠️  Analysis was interrupted — partial report saved.')
        sys.exit(130)   # conventional: 128 + SIGINT(2)
    log('🎉 Analysis complete!')


if __name__ == '__main__':
    main()
