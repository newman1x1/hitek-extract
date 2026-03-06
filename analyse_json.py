#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════
  JSON ANALYSER  —  zero-download, fully-streaming edition
  Runs on GitHub Actions (Ubuntu) or any machine with rclone + fuse3

  Pipeline:  rclone mount (read-only) → ijson stream → stats tree → report

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
  479 GB file, FUSE read ~40–60 MB/s → ~2.0–3.3 h, well within 6 h.
═══════════════════════════════════════════════════════════════════════
"""

# /// script
# requires-python = ">=3.10"
# dependencies = ["ijson>=3.2"]
# ///

import os, sys, json, time, re, subprocess, collections
from datetime import datetime, timezone

# ── auto-install dependencies ─────────────────────────────────────────────────
def _pip(*pkgs):
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', *pkgs])

try:
    import ijson
except ImportError:
    print("📦 Installing ijson…"); _pip('ijson')
    import ijson


# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════
RCLONE_REMOTE  = 'Gdrive'
SOURCE_FOLDER  = 'users_data_extracted'   # Drive folder that holds users_data.json
FILE_NAME      = 'users_data.json'

REPORT_JSON    = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'json_analysis_report.json')
REPORT_TXT     = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'json_analysis_report.txt')

MAX_DEPTH      = 6      # max nesting depth to recurse into
MAX_SAMPLES    = 25     # unique sample values kept per field
MAX_TOP_VALUES = 200    # top-N frequent values tracked per field (Counter cap)
LOG_INTERVAL   = 60     # seconds between progress lines in CI
SAVE_EVERY     = 0      # save partial report every N records (0 = only at end)

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
                child = node['children'].setdefault(str(k), new_field())
                analyse_value(child, v, depth + 1)
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
# STREAMING ANALYSIS
# ════════════════════════════════════════════════════════════════════════════════
def detect_top_level(path: str) -> tuple[str, str]:
    """
    Peek at the first non-whitespace character to determine structure.
    Returns ('array', 'item') or ('object', '<key>') for ijson prefix.
    """
    with open(path, 'rb') as f:
        chunk = f.read(65536)

    stripped = chunk.lstrip()
    if not stripped:
        return ('unknown', '')

    if stripped[0:1] == b'[':
        return ('array', 'item')

    if stripped[0:1] == b'{':
        # Try to find the first array value inside the object
        try:
            partial = stripped.decode('utf-8', errors='replace')
            # Look for pattern like  "somekey": [
            m = re.search(r'"([^"]+)"\s*:\s*\[', partial)
            if m:
                return ('object', m.group(1) + '.item')
        except Exception:
            pass
        return ('object', '')

    return ('unknown', '')


def run_analysis(file_path: str) -> dict:
    """
    Stream-parse file_path with ijson and return a raw schema tree + metadata.
    """
    file_size = os.path.getsize(file_path)
    log(f'📄 File size on disk: {fmt_bytes(file_size)}')

    top_level, prefix = detect_top_level(file_path)
    log(f'🔍 Top-level structure: {top_level}  |  ijson prefix: "{prefix}"')

    root = new_field()         # schema root (top-level record fields go here)
    record_count   = 0
    start_time     = time.time()
    last_log_time  = start_time

    with open(file_path, 'rb') as fh:
        # ── array of records (most common for user data) ──────────
        if top_level == 'array' or (top_level == 'object' and '.' in prefix):
            try:
                parser = ijson.items(fh, prefix, use_float=True)
            except TypeError:
                # older ijson without use_float
                parser = ijson.items(fh, prefix)

            for record in parser:
                record_count += 1

                if isinstance(record, dict):
                    for k, v in record.items():
                        child = root['children'].setdefault(str(k), new_field())
                        analyse_value(child, v, depth=1)
                    root['seen'] += 1
                else:
                    # array of scalars / mixed
                    analyse_value(root, record, depth=0)

                # ── progress logging ──────────────────────────────
                now = time.time()
                if now - last_log_time >= LOG_INTERVAL:
                    elapsed  = now - start_time
                    pos      = fh.tell()
                    pct      = pos / file_size * 100 if file_size else 0
                    speed    = pos / elapsed if elapsed else 0
                    eta      = (file_size - pos) / speed if speed else 0
                    last_log_time = now
                    log(f'  ⏳ {fmt_num(record_count)} records  '
                        f'{pct:.1f}%  '
                        f'{fmt_bytes(pos)} / {fmt_bytes(file_size)}  '
                        f'@ {fmt_bytes(speed)}/s  '
                        f'ETA {fmt_dur(eta)}')

        # ── single top-level object (not an array) ────────────────
        else:
            try:
                parser = ijson.kvitems(fh, '', use_float=True)
            except TypeError:
                parser = ijson.kvitems(fh, '')

            for key, value in parser:
                record_count += 1
                child = root['children'].setdefault(str(key), new_field())
                analyse_value(child, value, depth=1)
                root['seen'] = record_count

                now = time.time()
                if now - last_log_time >= LOG_INTERVAL:
                    elapsed = now - start_time
                    pos     = fh.tell()
                    pct     = pos / file_size * 100 if file_size else 0
                    speed   = pos / elapsed if elapsed else 0
                    eta     = (file_size - pos) / speed if speed else 0
                    last_log_time = now
                    log(f'  ⏳ {fmt_num(record_count)} top-level keys  '
                        f'{pct:.1f}%  {fmt_bytes(pos)} / {fmt_bytes(file_size)}  '
                        f'@ {fmt_bytes(speed)}/s  ETA {fmt_dur(eta)}')

    elapsed    = time.time() - start_time
    throughput = file_size / elapsed if elapsed else 0

    return {
        '_root':          root,
        'record_count':   record_count,
        'top_level_type': top_level,
        'ijson_prefix':   prefix,
        'file_size_bytes': file_size,
        'elapsed_seconds': round(elapsed, 1),
        'throughput_bps':  round(throughput, 1),
    }


# ════════════════════════════════════════════════════════════════════════════════
# REPORT GENERATION
# ════════════════════════════════════════════════════════════════════════════════
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
        lines.append(f'')
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
            satd = ' [counter full, values may be incomplete]' if fdata.get('counter_saturated') else ''
            top_str = '  |  '.join(f'"{k}"={v}' for k, v in list(top.items())[:5])
            lines.append(f'{pad}   top vals : {top_str}{satd}')
            lines.append(f'{pad}   est. uniq: ≥{fmt_num(fdata.get("estimated_unique", 0))}')

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
# RCLONE MOUNT
# ════════════════════════════════════════════════════════════════════════════════
def mount_and_analyse(rclone_cmd: str) -> dict:
    mount_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), '_analyse_mount'
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

    log(f'\n🗂️  Mounting {RCLONE_REMOTE}:{SOURCE_FOLDER} → {mount_dir} …')
    mount_proc = subprocess.Popen(
        [
            rclone_cmd, 'mount',
            f'{RCLONE_REMOTE}:{SOURCE_FOLDER}',
            mount_dir,
            '--vfs-cache-mode', 'off',
            '--read-only',
            '--no-modtime',
            '--dir-cache-time', '5m',
            '--buffer-size',    '256M',   # FUSE read-ahead buffer
            '--vfs-read-ahead', '512M',   # sequential prefetch window
            '--log-level', 'ERROR',
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )

    def cleanup():
        log('🧹 Unmounting…')
        mount_proc.terminate()
        time.sleep(1)
        if sys.platform == 'win32':
            subprocess.run(['taskkill', '/F', '/PID', str(mount_proc.pid)],
                           capture_output=True)
        else:
            for fm in ['fusermount3', 'fusermount']:
                try:
                    r = subprocess.run([fm, '-uz', mount_dir],
                                       capture_output=True, timeout=10)
                    if r.returncode == 0:
                        break
                except Exception:
                    continue
        try:
            os.rmdir(mount_dir)
        except Exception:
            pass

    json_path = os.path.join(mount_dir, FILE_NAME)
    log('   Waiting for mount…')
    for _ in range(60):
        if os.path.exists(json_path):
            break
        if mount_proc.poll() is not None:
            err = mount_proc.stderr.read().decode(errors='replace').strip()
            log(f'❌ rclone mount failed: {err}')
            if sys.platform != 'win32':
                log('   Ensure fuse3 is installed:  sudo apt-get install -y fuse3')
            sys.exit(1)
        print('.', end='', flush=True)
        time.sleep(2)
    else:
        cleanup()
        log(f'\n❌ Timed out — {FILE_NAME} not visible at {json_path}')
        sys.exit(1)
    log('  ✅ Mounted')

    try:
        raw = run_analysis(json_path)
    finally:
        cleanup()

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

    raw_stats = mount_and_analyse(rclone_cmd)

    if not raw_stats or raw_stats['record_count'] == 0:
        log('No data collected — exiting.')
        sys.exit(1)

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
        json.dump(report, f, indent=2, default=str, ensure_ascii=False)
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
    log('🎉 Analysis complete!')


if __name__ == '__main__':
    main()
