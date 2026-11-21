#!/usr/bin/env bash

set -euo pipefail


.venv/bin/ruff format src tests
.venv/bin/ruff check --fix src tests
