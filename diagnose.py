#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
  DIAGNOSTICS SCRIPT — JSON → Parquet Pipeline
  Run this via the diagnose.yml workflow BEFORE the main conversion to verify
  every component is set up correctly.

  Checks:
    1.  System info (OS, CPU, RAM, disk space)
    2.  Python & pyarrow version
    3.  rclone installation & version
    4.  rclone config (remotes listed)
    5.  Google Drive connectivity
    6.  Source file (existence + size)
    7.  Destination folder (existence / creatable)
    8.  Existing checkpoint on Drive (resume state)
    9.  Existing part files on Drive (partial progress)
   10.  GITHUB_TOKEN permissions (read vs write)
   11.  repository_dispatch capability (dry-run)
   12.  rclone stream test (first 512 KB, measure speed)
   13.  PyArrow Parquet write+read round-trip (sanity check)
   14.  Time & timezone on the runner
   15.  Estimated total job count for 446 GB file

  Exit code 0 = all checks passed or non-fatal warnings only.
  Exit code 1 = at least one CRITICAL check failed.
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import gc
import io
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── Configuration (must match json_to_parquet / V2_json_to_parquet) ──────────
RCLONE_REMOTE   = 'Gdrive'
SOURCE_FOLDER   = 'users_data_extracted'
FILE_NAME       = 'users_data.json'
DEST_FOLDER     = 'users_data_parquet'
CHECKPOINT_FNAME = 'convert_checkpoint.json'
FINAL_FNAME     = 'users_data.parquet'

WORK_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

# ── Counters ─────────────────────────────────────────────────────────────────
_critical_failures: list[str] = []
_warnings:          list[str] = []
_passed:            list[str] = []

# ── Helpers ───────────────────────────────────────────────────────────────────

def section(title: str) -> None:
    print(f'\n{"═" * 68}', flush=True)
    print(f'  {title}', flush=True)
    print(f'{"═" * 68}', flush=True)


def ok(msg: str) -> None:
    print(f'  ✅  {msg}', flush=True)
    _passed.append(msg)


def warn(msg: str) -> None:
    print(f'  ⚠️   {msg}', flush=True)
    _warnings.append(msg)


def fail(msg: str) -> None:
    print(f'  ❌  {msg}', flush=True)
    _critical_failures.append(msg)


def info(msg: str) -> None:
    print(f'       {msg}', flush=True)


def fmt_bytes(n: float) -> str:
    for u in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(n) < 1024:
            return f'{n:.2f} {u}'
        n /= 1024
    return f'{n:.2f} PB'


def run(cmd: list[str], timeout: int = 60, env: dict | None = None) -> tuple[int, str, str]:
    """Run a subprocess, return (returncode, stdout, stderr)."""
    try:
        r = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
            env={**os.environ, **(env or {})},
        )
        return r.returncode, r.stdout.strip(), r.stderr.strip()
    except subprocess.TimeoutExpired:
        return -1, '', f'TIMEOUT after {timeout}s'
    except FileNotFoundError:
        return -2, '', f'Command not found: {cmd[0]}'
    except Exception as e:
        return -3, '', str(e)


# ── Check 1: System info ──────────────────────────────────────────────────────

def check_system() -> None:
    section('1. SYSTEM INFO')

    print(f'  Platform   : {platform.platform()}', flush=True)
    print(f'  Python     : {sys.version}', flush=True)
    print(f'  UTC time   : {datetime.now(timezone.utc).isoformat()}', flush=True)
    print(f'  Working dir: {WORK_DIR}', flush=True)

    # RAM
    try:
        with open('/proc/meminfo') as f:
            lines = f.readlines()
        mem = {k.strip(): int(v.split()[0]) for k, v in
               (l.split(':', 1) for l in lines if ':' in l)}
        total_mb  = mem.get('MemTotal', 0) // 1024
        avail_mb  = mem.get('MemAvailable', 0) // 1024
        print(f'  RAM total  : {total_mb:,} MB', flush=True)
        print(f'  RAM avail  : {avail_mb:,} MB', flush=True)
        if avail_mb < 1024:
            warn(f'Low available RAM ({avail_mb} MB) — may cause OOM during merge')
        else:
            ok(f'RAM available: {avail_mb:,} MB')
    except Exception:
        print('  RAM        : (could not read /proc/meminfo)', flush=True)

    # Disk
    try:
        usage = shutil.disk_usage(str(WORK_DIR))
        free_gb = usage.free / 1024**3
        total_gb = usage.total / 1024**3
        print(f'  Disk total : {total_gb:.1f} GB', flush=True)
        print(f'  Disk free  : {free_gb:.1f} GB', flush=True)
        if free_gb < 5:
            fail(f'CRITICAL: Only {free_gb:.1f} GB disk free — need ≥5 GB for temp part files')
        elif free_gb < 20:
            warn(f'Disk free {free_gb:.1f} GB — merge phase downloads parts one-at-a-time; may be tight')
        else:
            ok(f'Disk free: {free_gb:.1f} GB')
    except Exception as e:
        warn(f'Could not check disk space: {e}')

    # CPU count
    try:
        cpus = os.cpu_count() or 1
        print(f'  CPUs       : {cpus}', flush=True)
        ok(f'{cpus} CPUs available')
    except Exception:
        pass


# ── Check 2: Python pyarrow ───────────────────────────────────────────────────

def check_pyarrow() -> None:
    section('2. PYTHON & PYARROW')

    # Python version
    major, minor = sys.version_info[:2]
    if (major, minor) >= (3, 9):
        ok(f'Python {major}.{minor} — compatible')
    else:
        fail(f'Python {major}.{minor} is too old — need ≥3.9')

    # pyarrow
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        ver = tuple(int(x) for x in pa.__version__.split('.')[:2])
        if ver >= (14, 0):
            ok(f'pyarrow {pa.__version__} — compatible')
        else:
            warn(f'pyarrow {pa.__version__} is old — need ≥14.0; will be auto-upgraded at runtime')

        # Quick write/read round-trip
        import pyarrow as pa
        import pyarrow.parquet as pq
        test_file = WORK_DIR / '_diag_test.parquet'
        try:
            schema = pa.schema([
                pa.field('id',  pa.utf8()),
                pa.field('val', pa.int64()),
            ])
            tbl = pa.table({'id': ['a', 'b', 'c'], 'val': [1, 2, 3]}, schema=schema)
            pq.write_table(tbl, str(test_file), compression='zstd', compression_level=19)
            tbl2 = pq.read_table(str(test_file), schema=schema)
            assert tbl2.num_rows == 3
            ok('PyArrow Parquet round-trip (ZSTD level 19): OK')
        except Exception as e:
            fail(f'PyArrow Parquet round-trip FAILED: {e}')
        finally:
            if test_file.exists():
                test_file.unlink()

    except ImportError:
        warn('pyarrow not installed — will be auto-installed at runtime')


# ── Check 3: rclone ───────────────────────────────────────────────────────────

def check_rclone() -> tuple[str | None, str | None]:
    """Returns (rclone_cmd, version_string) or (None, None) on failure."""
    section('3. RCLONE')

    rc, out, err = run(['rclone', 'version'])
    if rc == 0:
        first_line = out.splitlines()[0] if out else '(no output)'
        ok(f'rclone found: {first_line}')
        return 'rclone', first_line
    else:
        fail(f'rclone not found or failed: {err}')
        return None, None


# ── Check 4: rclone config ────────────────────────────────────────────────────

def check_rclone_config(rclone_cmd: str) -> None:
    section('4. RCLONE CONFIG')

    rc, out, err = run([rclone_cmd, 'listremotes'])
    if rc != 0:
        fail(f'rclone listremotes failed: {err}')
        return

    remotes = [r.strip() for r in out.splitlines() if r.strip()]
    if not remotes:
        fail('No remotes configured in rclone — check RCLONE_CONF secret')
        return

    print(f'  Configured remotes: {remotes}', flush=True)

    gdrive_remote = f'{RCLONE_REMOTE}:'
    if gdrive_remote in remotes:
        ok(f'Remote "{RCLONE_REMOTE}" is configured')
    else:
        fail(f'Remote "{RCLONE_REMOTE}" NOT found in rclone config. '
             f'Available remotes: {remotes}')


# ── Check 5: Drive connectivity ───────────────────────────────────────────────

def check_drive_connectivity(rclone_cmd: str) -> None:
    section('5. GOOGLE DRIVE CONNECTIVITY')

    # List root
    t0 = time.time()
    rc, out, err = run([rclone_cmd, 'lsf', f'{RCLONE_REMOTE}:'], timeout=60)
    latency = time.time() - t0

    if rc == 0:
        ok(f'Drive accessible (latency: {latency:.1f}s)')
        lines = [l for l in out.splitlines() if l.strip()][:10]
        if lines:
            info(f'Root contents (first 10): {lines}')
    else:
        fail(f'Cannot access Drive root ({RCLONE_REMOTE}:): {err}')


# ── Check 6: Source file ──────────────────────────────────────────────────────

def check_source_file(rclone_cmd: str) -> int:
    """Returns file size in bytes (0 if unknown)."""
    section('6. SOURCE FILE')

    src = f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}'

    # lsf check
    rc, out, err = run([rclone_cmd, 'lsf', src], timeout=60)
    if rc != 0:
        fail(f'Source file not found: {src}\n       Error: {err}')
        return 0

    ok(f'Source file exists: {src}')

    # size
    rc2, out2, err2 = run([rclone_cmd, 'size', '--json', src], timeout=120)
    if rc2 == 0:
        try:
            sz = json.loads(out2).get('bytes', 0)
            info(f'Size: {fmt_bytes(sz)} ({sz:,} bytes)')
            ok(f'Source file size: {fmt_bytes(sz)}')
            return sz
        except Exception:
            pass
    warn(f'Could not determine source file size: {err2}')
    return 0


# ── Check 7: Destination folder ───────────────────────────────────────────────

def check_dest_folder(rclone_cmd: str) -> None:
    section('7. DESTINATION FOLDER')

    dest = f'{RCLONE_REMOTE}:{DEST_FOLDER}'

    # Try mkdir (idempotent)
    rc, _, err = run([rclone_cmd, 'mkdir', dest], timeout=60)
    if rc == 0:
        ok(f'Destination folder ready: {dest}/')
    else:
        fail(f'Cannot create/access destination folder {dest}/: {err}')
        return

    # List contents
    rc2, out2, _ = run([rclone_cmd, 'lsf', f'{dest}/'], timeout=60)
    if rc2 == 0:
        items = [l for l in out2.splitlines() if l.strip()]
        if items:
            info(f'Existing contents ({len(items)} items):')
            for item in items[:20]:
                info(f'  {item}')
            if len(items) > 20:
                info(f'  ... and {len(items)-20} more')
        else:
            info('Destination folder is empty')


# ── Check 8: Existing checkpoint ─────────────────────────────────────────────

def check_checkpoint(rclone_cmd: str) -> None:
    section('8. EXISTING CHECKPOINT')

    cp_remote = f'{RCLONE_REMOTE}:{DEST_FOLDER}/{CHECKPOINT_FNAME}'
    cp_local  = WORK_DIR / f'_diag_{CHECKPOINT_FNAME}'

    try:
        rc, _, _ = run([rclone_cmd, 'copyto', cp_remote, str(cp_local), '--retries', '3'], timeout=60)
        if rc == 0 and cp_local.exists():
            with open(cp_local, encoding='utf-8') as f:
                cp = json.load(f)
            ok(f'Checkpoint found on Drive')
            info(f'Status          : {cp.get("status", "unknown")}')
            info(f'Records written : {cp.get("records_written", 0):,}')
            info(f'Bytes processed : {fmt_bytes(cp.get("total_bytes", 0))}')
            info(f'Next part       : {cp.get("next_part", "?")}')
            info(f'Saved at        : {cp.get("saved_at", "?")}')
        else:
            ok('No checkpoint found — conversion will start fresh')
    except Exception as e:
        warn(f'Checkpoint check error: {e}')
    finally:
        if cp_local.exists():
            cp_local.unlink()


# ── Check 9: Existing part files ─────────────────────────────────────────────

def check_parts(rclone_cmd: str) -> None:
    section('9. EXISTING PART FILES ON DRIVE')

    rc, out, err = run(
        [rclone_cmd, 'lsf', f'{RCLONE_REMOTE}:{DEST_FOLDER}/', '--include', 'part_*.parquet'],
        timeout=120,
    )
    if rc != 0:
        warn(f'Could not list part files: {err}')
        return

    parts = sorted(p.strip() for p in out.splitlines() if p.strip())
    if parts:
        ok(f'{len(parts)} part file(s) already on Drive — conversion was partially done')
        for p in parts[:10]:
            info(f'  {p}')
        if len(parts) > 10:
            info(f'  ... and {len(parts)-10} more')
    else:
        ok('No part files yet — clean slate')

    # Check if final merged file exists
    rc2, out2, _ = run(
        [rclone_cmd, 'lsf', f'{RCLONE_REMOTE}:{DEST_FOLDER}/', '--include', FINAL_FNAME],
        timeout=60,
    )
    if rc2 == 0 and FINAL_FNAME in out2:
        ok(f'⭐ FINAL merged file already exists: {RCLONE_REMOTE}:{DEST_FOLDER}/{FINAL_FNAME}')
        info('   -> Conversion may already be COMPLETE!')
    else:
        info(f'Final file ({FINAL_FNAME}) not yet present — conversion not complete')


# ── Check 10: GITHUB_TOKEN permissions ────────────────────────────────────────

def check_github_token() -> None:
    section('10. GITHUB_TOKEN PERMISSIONS')

    # Prefer GH_PAT (explicit repo scope) over GITHUB_TOKEN (needs settings change)
    pat   = os.environ.get('GH_PAT', '')
    token = pat or os.environ.get('GITHUB_TOKEN', '')
    repo  = os.environ.get('GITHUB_REPOSITORY', '')

    if not token:
        warn('Neither GH_PAT nor GITHUB_TOKEN is set — auto-continuation will not work')
        info('Add secret GH_PAT (classic PAT with repo scope) for most reliable auto-continuation')
        return

    if not repo:
        warn('GITHUB_REPOSITORY not set — cannot check token scope against this repo')
        return

    if pat:
        ok(f'GH_PAT is present (length: {len(pat)}) — using Personal Access Token ✓')
        info('GH_PAT has explicit repo scope; immune to Workflow permissions setting')
    else:
        ok(f'GITHUB_TOKEN is present (length: {len(token)}) — using Actions token')
        info('GITHUB_TOKEN requires Settings → Actions → Workflow permissions = Read and write')
    ok(f'GITHUB_REPOSITORY: {repo}')

    # Check token permissions via the /rate_limit endpoint (always accessible)
    req = urllib.request.Request(
        'https://api.github.com/rate_limit',
        headers={
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json',
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            headers = dict(resp.headers)
            # GitHub sets X-OAuth-Scopes for personal access tokens
            scopes = headers.get('X-OAuth-Scopes', headers.get('x-oauth-scopes', ''))
            if scopes:
                info(f'Token scopes (PAT): {scopes}')
                if 'repo' in scopes or 'workflow' in scopes:
                    ok(f'Token has repo/workflow scope — repository_dispatch will work')
                else:
                    warn(f'Token scopes ({scopes!r}) may not include repo write access')
            else:
                # GITHUB_TOKEN (Actions token) doesn't expose scopes via this header
                info('No X-OAuth-Scopes header — likely using GITHUB_TOKEN (Actions token)')
                info('Actions token permissions depend on repository "Workflow permissions" setting')
    except urllib.error.HTTPError as e:
        warn(f'Rate limit check returned {e.code} — token may be invalid')
    except Exception as e:
        warn(f'Could not check token: {e}')

    # ── Step 2: Test repository_dispatch with a real POST ────────────────────
    # The /dispatches endpoint is POST-ONLY.  GitHub returns 404 for GET on
    # this path when using an Actions GITHUB_TOKEN (not a PAT), because
    # GitHub intentionally hides the endpoint to prevent information leakage —
    # a 404 from GET does NOT mean POST will fail.  We must use the actual POST
    # method to get a conclusive answer.
    #
    # We send event_type='diag_dispatch_test': no workflow listens to this
    # event type so nothing is actually triggered, but the auth is fully tested.
    # Expected responses:
    #   204 — success: auto-continuation WILL work
    #   422 — bad payload (event_type format): auth OK, continuation will work
    #   403 — forbidden: token lacks write permission → needs "Read and write"
    #   404 — can't access endpoint → same fix as 403
    url  = f'https://api.github.com/repos/{repo}/dispatches'
    data = json.dumps({'event_type': 'diag_dispatch_test'}).encode('utf-8')
    req2 = urllib.request.Request(
        url,
        data=data,
        headers={
            'Authorization': f'token {token}',
            'Accept':        'application/vnd.github.v3+json',
            'Content-Type':  'application/json',
        },
        method='POST',
    )
    try:
        with urllib.request.urlopen(req2, timeout=15) as resp:
            if resp.status in (200, 201, 204):
                ok(f'repository_dispatch POST: {resp.status} — auto-continuation WILL WORK ✓')
            else:
                warn(f'repository_dispatch POST returned unexpected {resp.status}')
    except urllib.error.HTTPError as e:
        body = ''
        try:
            body = e.read().decode('utf-8', errors='replace')[:300]
        except Exception:
            pass
        if e.code == 422:
            # Unprocessable = event_type string is unusual but auth is fine
            ok(f'repository_dispatch POST: 422 (auth OK, nothing triggered) — auto-continuation WILL WORK')
        elif e.code == 403:
            fail(
                'CRITICAL: 403 Forbidden on POST /dispatches — GITHUB_TOKEN lacks write permissions.\n'
                '       Fix: GitHub repo → Settings → Actions → General → Workflow permissions\n'
                '            Set to "Read and write permissions"'
            )
        elif e.code == 404:
            fail(
                f'404 on POST /dispatches for repo "{repo}".\n'
                f'       This means GITHUB_TOKEN cannot use repository_dispatch.\n'
                f'       Fix: Settings → Actions → General → Workflow permissions\n'
                f'            Set to "Read and write permissions"\n'
                f'       Body: {body}'
            )
        else:
            warn(f'Dispatches POST returned {e.code}: {e.reason} — {body}')
    except Exception as e:
        warn(f'Could not reach dispatches endpoint: {e}')


# ── Check 11: rclone stream test — sustained speed measurement ───────────────

def check_stream(rclone_cmd: str, file_size: int) -> float:
    """
    Measure SUSTAINED Drive → GHA download speed.
    Returns measured speed in bytes/sec (0 if test failed).

    Strategy:
      • Read up to 20 MB total (30-second timeout).
      • First 1 MB is discarded as "connection warm-up" (TCP slow-start,
        TLS handshake, OAuth refresh, Drive API setup).
      • Speed is calculated only on the remaining bytes to avoid the
        misleading cold-start dip (e.g. 347 KB/s measured on first 512 KB
        became 373-hour estimate when actual sustained speed is 10+ MB/s).
    """
    section('11. RCLONE STREAM TEST (sustained speed — 20 MB)')

    src = f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}'
    WARMUP_BYTES  = 1 * 1024 * 1024   # 1 MB   warm-up (discarded for speed calc)
    TARGET_BYTES  = 20 * 1024 * 1024  # 20 MB  total read
    TIMEOUT_SEC   = 45                 # abort if it takes longer than this

    proc = subprocess.Popen(
        [rclone_cmd, 'cat', src, '--buffer-size', '4M'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    data        = bytearray()
    warmup_done = False
    t_warmup_end: float = 0.0
    t_start     = time.time()
    error_msg   = ''

    try:
        while len(data) < TARGET_BYTES:
            if time.time() - t_start > TIMEOUT_SEC:
                error_msg = f'timeout after {TIMEOUT_SEC}s'
                break
            chunk = proc.stdout.read(65536)
            if not chunk:
                break
            data += chunk
            if not warmup_done and len(data) >= WARMUP_BYTES:
                warmup_done = True
                t_warmup_end = time.time()
    except Exception as e:
        error_msg = str(e)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()

    total_elapsed  = time.time() - t_start
    total_bytes    = len(data)
    cold_speed     = total_bytes / total_elapsed if total_elapsed > 0 else 0

    if total_bytes < 1024:
        fail(f'No data received from stream ({error_msg}) — rclone cat failed')
        return 0.0

    # Show cold-start speed (total including warmup) for reference
    info(f'Total read      : {fmt_bytes(total_bytes)} in {total_elapsed:.1f}s')
    info(f'Cold-start speed: {fmt_bytes(cold_speed)}/s  '
         f'(includes ~{WARMUP_BYTES//1024}KB TCP/TLS/OAuth setup — unreliable)')

    # First 200 chars of real data for schema preview
    preview = bytes(data[:200]).decode('utf-8', errors='replace').strip()
    info(f'First 200 chars : {preview!r}')

    # Sustained speed: bytes AFTER warm-up
    if warmup_done and t_warmup_end > 0:
        sustained_bytes   = total_bytes - WARMUP_BYTES
        sustained_elapsed = total_elapsed - (t_warmup_end - t_start)
        if sustained_elapsed > 0.1 and sustained_bytes > 0:
            sustained_speed = sustained_bytes / sustained_elapsed
            ok(f'Sustained speed : {fmt_bytes(sustained_speed)}/s  '
               f'({fmt_bytes(sustained_bytes)} after warm-up in {sustained_elapsed:.1f}s)')

            if file_size and sustained_speed > 0:
                total_seconds = file_size / sustained_speed
                hours         = total_seconds / 3600
                runs_needed   = hours / 5.4167
                info(f'Projected stream time  : ~{hours:.1f}h at {fmt_bytes(sustained_speed)}/s')
                info(f'Projected runs needed  : ~{runs_needed:.1f} (5h 25m each)')
                if hours > 6:
                    ok(f'File too large for one run — auto-continuation REQUIRED (expected)')
                else:
                    ok(f'File may fit in a single run')
            return sustained_speed
        else:
            warn('Not enough post-warm-up data to measure sustained speed')
    else:
        # Didn't reach 1 MB — just report what we have
        ok(f'Partial stream test: {fmt_bytes(total_bytes)} read ({cold_speed:.0f} B/s — cold start only)')

    return cold_speed


# ── Check 12: Estimate conversion jobs ────────────────────────────────────────

def check_estimates(file_size: int, measured_speed_bps: float = 0) -> None:
    section('12. CONVERSION ESTIMATES')

    if not file_size:
        warn('File size unknown — cannot estimate')
        return

    # Use measured speed if available, otherwise fall back to 10 MB/s assumption
    if measured_speed_bps > 0:
        stream_speed     = measured_speed_bps
        speed_source     = f'measured: {fmt_bytes(stream_speed)}/s'
    else:
        stream_speed     = 10 * 1024 * 1024   # 10 MB/s fallback
        speed_source     = '10 MB/s assumed (run stream test to measure actual)'

    record_size_bytes  = 250  # ~250 bytes per JSON record (typical)
    compress_ratio     = 10   # ZSTD level 19 typical compression ratio

    total_seconds_stream = file_size / stream_speed if stream_speed > 0 else 0
    total_records        = file_size // record_size_bytes
    estimated_output_gb  = (file_size / compress_ratio) / 1024**3
    runs_needed          = total_seconds_stream / (5.4167 * 3600)  # 5h 25m per run

    info(f'Source file        : {fmt_bytes(file_size)}')
    info(f'Stream speed used  : {speed_source}')
    info(f'Est. records       : ~{total_records:,} (at ~{record_size_bytes}B/record)')
    info(f'Est. output size   : ~{estimated_output_gb:.1f} GB (at {compress_ratio}x compression)')
    info(f'Est. stream time   : ~{total_seconds_stream/3600:.1f}h')
    info(f'Est. runs needed   : ~{runs_needed:.1f} (5h 25m per run)')
    info(f'Part files expected: ~{total_records // 5_000_000 + 1} (at 5M records/part)')
    ok('Estimates generated')


# ── Check 13: JSON schema validation ─────────────────────────────────────────

def check_json_schema(rclone_cmd: str) -> None:
    """
    Stream the first 5,000 records from the source file, parse them as JSON,
    and validate the field distribution against the expected Parquet schema.

    This catches schema mismatches BEFORE running a 69-hour conversion and
    ensures that the extraction logic (extract_record) will work correctly.
    """
    section('13. JSON SCHEMA VALIDATION (first 5,000 records)')

    src        = f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}'
    MAX_RECS   = 5_000
    READ_LIMIT = 5 * 1024 * 1024   # stop reading after 5 MB regardless

    EXPECTED_FIELDS = {'_id', 'name', 'fname', 'mobile', 'alt', 'email',
                       'id', 'address', 'circle'}

    proc = subprocess.Popen(
        [rclone_cmd, 'cat', src, '--buffer-size', '4M'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    field_counts: dict[str, int] = {}
    id_formats:   dict[str, int] = {}   # oid format distribution
    parse_errors  = 0
    records_ok    = 0
    extra_fields: set[str] = set()
    bytes_read    = 0

    # ── Read raw chunks with hard timeout — avoids the hang that killed check 13
    # Do NOT use io.BufferedReader + for-line-in-buf here: that blocks waiting
    # for data indefinitely when rclone takes time to reconnect to Drive after
    # the previous stream test.  We read raw chunks with a wall-clock timeout
    # then split lines from the in-memory buffer.
    TIMEOUT_SEC = 90
    t_read_start = time.time()
    raw_buf = bytearray()

    try:
        while len(raw_buf) < READ_LIMIT:
            if time.time() - t_read_start > TIMEOUT_SEC:
                info(f'Read timeout after {TIMEOUT_SEC}s — processing {fmt_bytes(len(raw_buf))} collected so far')
                break
            chunk = proc.stdout.read(65536)
            if not chunk:
                break
            raw_buf += chunk
    except Exception as e:
        warn(f'Schema read error: {e}')
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()

    bytes_read = len(raw_buf)

    # ── Process lines from the in-memory buffer ──────────────────────────────
    for raw_line in raw_buf.split(b'\n'):
        if records_ok >= MAX_RECS:
            break

        stripped = raw_line.strip()
        if not stripped or stripped in (b'[', b']'):
            continue
        if stripped.endswith(b','):
            stripped = stripped[:-1]
        if not stripped.startswith(b'{'):
            continue

        try:
            doc = json.loads(stripped)
        except json.JSONDecodeError:
            parse_errors += 1
            continue

        if not isinstance(doc, dict):
            continue

        records_ok += 1
        for k in doc:
            field_counts[k] = field_counts.get(k, 0) + 1

        # _id format
        _id = doc.get('_id')
        if _id is None:
            id_formats['null'] = id_formats.get('null', 0) + 1
        elif isinstance(_id, dict) and '$oid' in _id:
            id_formats['extended_json_{$oid}'] = id_formats.get('extended_json_{$oid}', 0) + 1
        elif isinstance(_id, str):
            if len(_id) == 24:
                id_formats['plain_hex_string'] = id_formats.get('plain_hex_string', 0) + 1
            else:
                id_formats['other_string'] = id_formats.get('other_string', 0) + 1
        else:
            id_formats[f'type:{type(_id).__name__}'] = id_formats.get(f'type:{type(_id).__name__}', 0) + 1

        # collect unknown fields
        for k in doc:
            if k not in EXPECTED_FIELDS:
                extra_fields.add(k)

    if records_ok == 0:
        fail('Could not parse any JSON records for schema validation')
        return

    info(f'Records parsed      : {records_ok:,} ({parse_errors} parse errors)')
    info(f'Data read           : {fmt_bytes(bytes_read)}')

    # Field coverage
    info(f'')
    info(f'Field coverage against expected schema:')
    all_good = True
    for field in sorted(EXPECTED_FIELDS):
        count    = field_counts.get(field, 0)
        coverage = count / records_ok * 100
        marker   = '✅' if coverage >= 50 else ('⚠️ ' if coverage > 0 else '❌')
        info(f'  {marker}  {field:<12} {count:>6,} / {records_ok:,}  ({coverage:.1f}%)')
        if coverage == 0 and field not in ('_id',):   # _id may map to oid column
            warn(f'Field "{field}" is 0% — it will always be NULL in the Parquet output')
            all_good = False

    # Unknown fields (will go to _extra column)
    if extra_fields:
        info(f'')
        info(f'Unknown fields → captured in _extra column: {sorted(extra_fields)}')
    else:
        info(f'No unknown fields — _extra column will always be NULL (clean data)')

    # _id format distribution
    info(f'')
    info(f'_id format distribution:')
    for fmt, cnt in sorted(id_formats.items(), key=lambda x: -x[1]):
        info(f'  {fmt}: {cnt:,} ({cnt/records_ok*100:.1f}%)')

    if all_good:
        ok(f'Schema validation passed for {records_ok:,} records')
    else:
        ok(f'Schema validated ({records_ok:,} records) — review warnings above')


# ── Summary ───────────────────────────────────────────────────────────────────

def print_summary() -> None:
    section('DIAGNOSTIC SUMMARY')

    print(f'\n  ✅  Passed  : {len(_passed)}', flush=True)
    print(f'  ⚠️   Warnings: {len(_warnings)}', flush=True)
    print(f'  ❌  Failed  : {len(_critical_failures)}', flush=True)

    if _warnings:
        print('\n  Warnings:', flush=True)
        for w in _warnings:
            print(f'    ⚠️   {w}', flush=True)

    if _critical_failures:
        print('\n  CRITICAL FAILURES — FIX BEFORE RUNNING CONVERSION:', flush=True)
        for i, f in enumerate(_critical_failures, 1):
            print(f'    ❌  [{i}] {f}', flush=True)

    if _critical_failures:
        print('\n  🔴 ACTION REQUIRED: Fix the failures listed above.', flush=True)
    elif _warnings:
        print('\n  🟡 REVIEW WARNINGS: Conversion may run but check warnings.', flush=True)
    else:
        print('\n  🟢 ALL CHECKS PASSED — Safe to run the conversion workflow!', flush=True)

    print('', flush=True)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    print('═' * 68, flush=True)
    print('  DIAGNOSTIC REPORT — JSON → Parquet Conversion Pipeline', flush=True)
    print(f'  Generated: {datetime.now(timezone.utc).isoformat()}', flush=True)
    print('═' * 68, flush=True)

    check_system()
    check_pyarrow()

    rclone_cmd, _ = check_rclone()

    if rclone_cmd:
        check_rclone_config(rclone_cmd)
        check_drive_connectivity(rclone_cmd)
        file_size = check_source_file(rclone_cmd)
        check_dest_folder(rclone_cmd)
        check_checkpoint(rclone_cmd)
        check_parts(rclone_cmd)
        measured_speed = check_stream(rclone_cmd, file_size)
        check_estimates(file_size, measured_speed)
        check_json_schema(rclone_cmd)
    else:
        warn('rclone not available — skipping all Drive checks')
        file_size = 0

    check_github_token()
    print_summary()

    if _critical_failures:
        sys.exit(1)
    sys.exit(0)


if __name__ == '__main__':
    main()
