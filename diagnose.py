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

    token = os.environ.get('GITHUB_TOKEN', '')
    repo  = os.environ.get('GITHUB_REPOSITORY', '')

    if not token:
        warn('GITHUB_TOKEN not set — auto-continuation via repository_dispatch will not work')
        info('This is expected when running locally; it will be set in GitHub Actions')
        return

    if not repo:
        warn('GITHUB_REPOSITORY not set — cannot check token scope against this repo')
        return

    ok(f'GITHUB_TOKEN is present (length: {len(token)})')
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

    # Dry-run: check if we can GET the dispatches endpoint (it only supports POST)
    # A 405 Method Not Allowed means auth is fine; a 403 means insufficient permissions
    url = f'https://api.github.com/repos/{repo}/dispatches'
    req2 = urllib.request.Request(
        url,
        headers={
            'Authorization': f'token {token}',
            'Accept':        'application/vnd.github.v3+json',
        },
        method='GET',
    )
    try:
        with urllib.request.urlopen(req2, timeout=15) as _:
            pass
    except urllib.error.HTTPError as e:
        if e.code == 405:
            ok(f'repository_dispatch endpoint reachable (405 = Method Not Allowed, auth OK)')
        elif e.code == 403:
            fail(
                'CRITICAL: 403 Forbidden on /dispatches — GITHUB_TOKEN lacks write permissions.\n'
                '       Fix: In GitHub repo → Settings → Actions → General → Workflow permissions\n'
                '            Set to "Read and write permissions"'
            )
        elif e.code == 404:
            fail(f'404 on /dispatches for repo "{repo}" — check GITHUB_REPOSITORY is correct')
        else:
            warn(f'Dispatches endpoint returned {e.code}: {e.reason}')
    except Exception as e:
        warn(f'Could not reach dispatches endpoint: {e}')


# ── Check 11: rclone stream test ──────────────────────────────────────────────

def check_stream(rclone_cmd: str, file_size: int) -> None:
    section('11. RCLONE STREAM TEST (first 512 KB)')

    src = f'{RCLONE_REMOTE}:{SOURCE_FOLDER}/{FILE_NAME}'
    test_bytes = 512 * 1024  # 512 KB

    t0 = time.time()
    proc = subprocess.Popen(
        [rclone_cmd, 'cat', src, '--buffer-size', '1M'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    data = b''
    try:
        # Read exactly test_bytes
        while len(data) < test_bytes:
            chunk = proc.stdout.read(min(4096, test_bytes - len(data)))
            if not chunk:
                break
            data += chunk
    except Exception as e:
        fail(f'Stream read error: {e}')
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()

    elapsed = time.time() - t0
    if len(data) >= 1024:
        speed = len(data) / elapsed if elapsed > 0 else 0
        ok(f'Stream test OK: read {fmt_bytes(len(data))} in {elapsed:.1f}s '
           f'({fmt_bytes(speed)}/s)')

        # Peek at first few bytes
        preview = data[:200].decode('utf-8', errors='replace').strip()
        info(f'First 200 chars: {preview!r}')

        # Estimate total time
        if file_size and speed > 0:
            total_seconds = file_size / speed
            hours = total_seconds / 3600
            runs_needed = hours / 5.4167  # 5h 25m per run
            info(f'Estimated download speed: {fmt_bytes(speed)}/s')
            info(f'Estimated total stream time: {hours:.1f}h')
            info(f'Estimated runs needed (~5h25m each): {runs_needed:.1f}')
            if hours > 6:
                ok(f'File too large for one run — auto-continuation is REQUIRED (expected)')
            else:
                ok(f'File may fit in a single run')
    else:
        if len(data) > 0:
            warn(f'Only read {len(data)} bytes — stream may be slow or rate-limited')
        else:
            fail(f'No data received from stream — rclone cat failed')


# ── Check 12: Estimate conversion jobs ────────────────────────────────────────

def check_estimates(file_size: int) -> None:
    section('12. CONVERSION ESTIMATES')

    if not file_size:
        warn('File size unknown — cannot estimate')
        return

    # Assumptions
    stream_speed_mbps  = 10   # ~10 MB/s typical for Drive on GHA runner
    record_size_bytes  = 250  # ~250 bytes per JSON record (typical)
    compress_ratio     = 10   # ZSTD level 19 typical compression ratio

    total_seconds_stream = file_size / (stream_speed_mbps * 1024**2)
    total_records        = file_size // record_size_bytes
    estimated_output_gb  = (file_size / compress_ratio) / 1024**3
    runs_needed          = total_seconds_stream / (5.4167 * 3600)  # 5h 25m per run

    info(f'Source file        : {fmt_bytes(file_size)}')
    info(f'Est. records       : ~{total_records:,} (at ~{record_size_bytes}B/record)')
    info(f'Est. output size   : ~{estimated_output_gb:.1f} GB (at {compress_ratio}x compression)')
    info(f'Est. stream time   : ~{total_seconds_stream/3600:.1f}h at {stream_speed_mbps} MB/s')
    info(f'Est. runs needed   : ~{runs_needed:.1f} (5h 25m per run)')
    info(f'Part files expected: ~{total_records // 5_000_000 + 1} (at 5M records/part)')
    ok('Estimates generated (actual speed may vary)')


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
        check_stream(rclone_cmd, file_size)
        check_estimates(file_size)
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
