#!/usr/bin/env bash
# Cria um ambiente local ao trabalho e instala as dependências usadas na validação.
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
