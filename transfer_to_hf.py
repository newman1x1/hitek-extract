"""
transfer_to_hf.py
─────────────────
Transfers 19 Parquet files from Google Drive to a HuggingFace dataset repo.
Downloads one file at a time (stays within the 14 GB GitHub Actions disk limit).
Resumes automatically — skips files that are already on HuggingFace.

Required environment variables:
  HF_TOKEN  — HuggingFace token with write access to the dataset repo
  HF_REPO   — HuggingFace dataset repo, e.g. "yourname/telecom-data"

Optional:
  GDRIVE_FOLDER  — rclone remote path, default "Gdrive:users_data_parquet"
  FILE_PATTERN   — rclone include pattern, default "merge_l1_*.parquet"
  LOCAL_TMP      — local temp dir, default "/tmp/parquet_transfer"
"""

import os
import subprocess
import sys
import time
from pathlib import Path

from huggingface_hub import HfApi, list_repo_files

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

HF_TOKEN      = os.environ.get("HF_TOKEN", "")
HF_REPO       = os.environ.get("HF_REPO", "")
GDRIVE_FOLDER = os.environ.get("GDRIVE_FOLDER", "Gdrive:users_data_parquet")
FILE_PATTERN  = os.environ.get("FILE_PATTERN", "merge_l1_*.parquet")
LOCAL_TMP     = Path(os.environ.get("LOCAL_TMP", "/tmp/parquet_transfer"))

# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

def abort(msg: str) -> None:
    print(f"\n[ERROR] {msg}", flush=True)
    sys.exit(1)

if not HF_TOKEN:
    abort("HF_TOKEN environment variable is not set.")
if not HF_REPO:
    abort("HF_REPO environment variable is not set.")

LOCAL_TMP.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def fmt_size(n_bytes: int) -> str:
    if n_bytes >= 1_073_741_824:
        return f"{n_bytes / 1_073_741_824:.2f} GB"
    if n_bytes >= 1_048_576:
        return f"{n_bytes / 1_048_576:.0f} MB"
    return f"{n_bytes / 1024:.0f} KB"


def fmt_dur(seconds: float) -> str:
    if seconds >= 60:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    return f"{seconds:.1f}s"


def rclone_list_files(folder: str, pattern: str) -> list[str]:
    """List files matching pattern in a rclone remote folder."""
    result = subprocess.run(
        ["rclone", "lsf", f"{folder}/", "--include", pattern, "--max-depth", "1"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        abort(f"rclone lsf failed:\n{result.stderr.strip()}")
    files = sorted(line.strip() for line in result.stdout.splitlines() if line.strip())
    return files


def rclone_download(remote_path: str, local_path: Path) -> bool:
    """Download a single file via rclone. Returns True on success."""
    print(f"  Downloading from Drive...", flush=True)
    t0 = time.time()
    result = subprocess.run(
        [
            "rclone", "copyto", remote_path, str(local_path),
            "--retries", "5",
            "--retries-sleep", "10s",
            "--buffer-size", "32M",
            "--stats", "30s",
            "--stats-one-line",
        ],
        text=True
    )
    elapsed = time.time() - t0
    if result.returncode != 0:
        print(f"  [WARN] rclone download failed after {fmt_dur(elapsed)}", flush=True)
        return False
    size = local_path.stat().st_size if local_path.exists() else 0
    print(f"  Downloaded {fmt_size(size)} in {fmt_dur(elapsed)}", flush=True)
    return True


def hf_upload(api: HfApi, local_path: Path, fname: str, repo: str) -> bool:
    """Upload a single file to HuggingFace dataset repo. Returns True on success."""
    print(f"  Uploading to HuggingFace...", flush=True)
    t0 = time.time()
    try:
        api.upload_file(
            path_or_fileobj=str(local_path),
            path_in_repo=fname,
            repo_id=repo,
            repo_type="dataset",
            commit_message=f"Add {fname}",
        )
        elapsed = time.time() - t0
        size = local_path.stat().st_size if local_path.exists() else 0
        speed = (size / elapsed / 1_048_576) if elapsed > 0 else 0
        print(f"  Uploaded {fmt_size(size)} in {fmt_dur(elapsed)} ({speed:.1f} MB/s)", flush=True)
        return True
    except Exception as e:
        elapsed = time.time() - t0
        print(f"  [WARN] Upload failed after {fmt_dur(elapsed)}: {e}", flush=True)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("  Google Drive → HuggingFace Transfer")
    print(f"  Repo   : {HF_REPO}")
    print(f"  Source : {GDRIVE_FOLDER}/{FILE_PATTERN}")
    print(f"  Tmp    : {LOCAL_TMP}")
    print("=" * 60)

    api = HfApi(token=HF_TOKEN)

    # Ensure dataset repo exists (create if missing)
    from huggingface_hub import create_repo
    try:
        create_repo(HF_REPO, repo_type="dataset", private=True, exist_ok=True, token=HF_TOKEN)
        print(f"Dataset repo ready: {HF_REPO}\n")
    except Exception as e:
        print(f"[WARN] Could not verify/create repo: {e}\nContinuing anyway...\n", flush=True)

    # Get files already on HuggingFace
    try:
        existing = set(list_repo_files(HF_REPO, repo_type="dataset", token=HF_TOKEN))
        print(f"Files already on HuggingFace : {len(existing)}")
    except Exception as e:
        print(f"[WARN] Could not list HF repo files: {e}\nAssuming none exist.\n", flush=True)
        existing = set()

    # List source files on Drive
    drive_files = rclone_list_files(GDRIVE_FOLDER, FILE_PATTERN)
    if not drive_files:
        abort(f"No files found matching '{FILE_PATTERN}' in {GDRIVE_FOLDER}")

    print(f"Files found on Drive         : {len(drive_files)}")

    pending = [f for f in drive_files if f not in existing]
    print(f"Files pending transfer       : {len(pending)}")
    print()

    if not pending:
        print("Nothing to do — all files are already on HuggingFace.")
        return

    # Transfer each pending file
    session_start = time.time()
    success_count = 0
    fail_count = 0

    for i, fname in enumerate(pending, 1):
        local_path = LOCAL_TMP / fname
        print(f"[{i}/{len(pending)}] {fname}")

        # Download
        remote_src = f"{GDRIVE_FOLDER}/{fname}"
        ok = rclone_download(remote_src, local_path)
        if not ok:
            fail_count += 1
            print(f"  SKIP — download failed\n", flush=True)
            local_path.unlink(missing_ok=True)
            continue

        # Upload
        ok = hf_upload(api, local_path, fname, HF_REPO)
        local_path.unlink(missing_ok=True)   # always delete local copy
        if not ok:
            fail_count += 1
            print(f"  SKIP — upload failed\n", flush=True)
            continue

        success_count += 1
        elapsed_total = time.time() - session_start
        remaining = len(pending) - i
        if success_count > 0:
            avg = elapsed_total / success_count
            eta = avg * remaining
            print(f"  Done. Session: {fmt_dur(elapsed_total)} elapsed, "
                  f"~{fmt_dur(eta)} remaining\n", flush=True)

    # Final summary
    print("=" * 60)
    print(f"  Transfer complete")
    print(f"  Succeeded : {success_count}")
    print(f"  Failed    : {fail_count}")
    print(f"  Total time: {fmt_dur(time.time() - session_start)}")
    print("=" * 60)

    if fail_count > 0:
        print(f"\n[WARN] {fail_count} file(s) failed. Re-run the workflow to retry.", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
