CREATE TABLE stadium (
  id INTEGER PRIMARY KEY,
  city INTEGER NOT NULL,
  year_opened INTEGER,
  FOREIGN KEY(city) REFERENCES city(id)
)
