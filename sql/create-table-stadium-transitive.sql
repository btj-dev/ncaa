CREATE TABLE stadium_transitive (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stadium INTEGER NOT NULL,
  name TEXT,
  capacity INTEGER,
  surface TEXT,
  FOREIGN KEY (stadium) REFERENCES stadium(id)
)
