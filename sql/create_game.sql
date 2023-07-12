CREATE TABLE IF NOT EXISTS game (
  "id" INTEGER PRIMARY KEY,
  "name" text,
  category INTEGER,
  cover INTEGER,
  created_at INTEGER,
  first_release_date INTEGER,
  slug TEXT,
  summary TEXT,
  updated_at INTEGER,
  url TEXT,
  checksum TEXT
)
