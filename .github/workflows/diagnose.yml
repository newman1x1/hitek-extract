name: 🔬 Diagnose pipeline

# ─────────────────────────────────────────────────────────────────────────────
# Run this workflow BEFORE the main conversion to pinpoint any configuration
# issues: rclone connectivity, Drive access, GITHUB_TOKEN permissions, stream
# speed, existing checkpoints/parts, and whether auto-continuation will work.
#
# Required secrets:
#   RCLONE_CONF — base64-encoded rclone config
# ─────────────────────────────────────────────────────────────────────────────

on:
  workflow_dispatch: {}

jobs:
  diagnose:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    permissions:
      contents: write     # required to test repository_dispatch endpoint

    steps:
      # ── 1. Checkout ─────────────────────────────────────────────────────────
      - name: Checkout repository
        uses: actions/checkout@v4

      # ── 2. Python ───────────────────────────────────────────────────────────
      - name: Set up Python 3.12
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      # ── 3. Dependencies ─────────────────────────────────────────────────────
      - name: Install Python packages
        run: pip install "pyarrow>=14.0"

      # ── 4. rclone ───────────────────────────────────────────────────────────
      - name: Install rclone
        run: |
          curl -fsSL https://rclone.org/install.sh | sudo bash
          rclone version

      # ── 5. rclone config ────────────────────────────────────────────────────
      - name: Configure rclone
        env:
          RCLONE_CONF: ${{ secrets.RCLONE_CONF }}
        run: |
          mkdir -p ~/.config/rclone
          echo "$RCLONE_CONF" | base64 -d > ~/.config/rclone/rclone.conf
          echo "✅ rclone config written"
          rclone listremotes

      # ── 6. Run diagnostics ──────────────────────────────────────────────────
      - name: Run diagnostics
        env:
          GH_PAT: ${{ secrets.GH_PAT }}
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          GITHUB_REPOSITORY: ${{ github.repository }}
        run: python3 diagnose.py
