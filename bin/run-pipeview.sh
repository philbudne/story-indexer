#!/bin/sh

. bin/func.sh

alembic --config indexer/workers/pipeview/alembic.ini upgrade head

run_python indexer.workers.pipeview.collector "$@"
