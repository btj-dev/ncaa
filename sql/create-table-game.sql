CREATE TABLE game (
  id INTEGER PRIMARY KEY,
  date VARCHAR(16) NOT NULL,
  home_team INTEGER NOT NULL,
  away_team INTEGER NOT NULL,
  stadium VARCHAR(2),
  site VARCHAR(6),
  FOREIGN KEY ( home_team ) REFERENCES team(id),
  FOREIGN KEY ( away_team ) REFERENCES team(id)
)

