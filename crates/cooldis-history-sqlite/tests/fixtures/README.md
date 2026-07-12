# History stream compatibility fixtures

`rusqlite-history-stream-v0.sqlite3` was generated once with
`rusqlite = 0.32.1` (`bundled`) by a standalone seed program. It contains the
pre-stream-envelope `event_records` schema and two deterministic legacy events.
It is committed as a binary fixture so decode-compat tests never add or execute
rusqlite at test time.
