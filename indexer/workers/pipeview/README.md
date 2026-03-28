This worker processes messages containing "breadcrumbs" and collects
counts in a Postgres database to be able to track where stories end
up, especially if they're being dropped or ending up in an unexpected
domain!!

This processing is COMPLETELY optional/extra and does not interfere
with the processing of stories!

Currently just a demo/test, should probably export an API in real life!
