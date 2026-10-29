#!/usr/bin/env bash
# Um processo por terminal. Argumentos adicionais seguem para peer.py.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
if [[ ! -x .venv/bin/python ]]; then
    echo 'Prepare o ambiente primeiro: bash preparar.sh' >&2
    exit 1
fi
exec .venv/bin/python -u peer.py "$@"
