#!/bin/sh

. bin/func.sh

run_python indexer.workers.fetcher.queue-rss "$@"
