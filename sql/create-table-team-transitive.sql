CREATE TABLE team_transitive (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  team_id INTEGER,
  home_stadium INTEGER,
  year INTEGER NOT NULL,
  FOREIGN KEY ( team_id ) REFERENCES team(id),
  FOREIGN KEY ( home_stadium ) REFERENCES stadium(id)
)
