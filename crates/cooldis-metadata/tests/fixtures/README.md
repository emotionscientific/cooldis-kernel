# Metadata compatibility fixtures

`rusqlite-metadata-v1.sqlite3` was generated once with `rusqlite = 0.32.1`
(`bundled`) by a standalone seed program. It is committed as a binary fixture
so compatibility tests never add or execute rusqlite at test time.
