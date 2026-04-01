#!/usr/bin/env bash
# Start the Firestore emulator for integration tests.
# Usage: ./scripts/start_emulator.sh
#
# Requires: Firebase CLI (`npm install -g firebase-tools`)
# The emulator listens on port 8686 (see firebase.json).
# Tests expect FIRESTORE_EMULATOR_HOST=127.0.0.1:8686 in the environment.
set -euo pipefail

cd "$(dirname "$0")/.."
exec firebase -P firedantic-extras-test emulators:start --only firestore
