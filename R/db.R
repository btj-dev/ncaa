library(RSQLite)

dbname <- '../data/sports-stats.sqlite3'

## Add a set of cities to the database (assumes they do not already exist)
db.add.cities <- function(cities) {
    con <- db.connect()
    if(is.null(cities$name))
        stop('City names must be provided')
    if(is.null(cities$state))
        stop('City states must be provided')
    cities$key <- sapply(seq(nrow(cities)), function(i)
                         return(db.add.city(con, cities[i,])))
    db.disconnect(con)
    return(cities)
}

## Adds a single city to the database
db.add.city <- function(con, city) {
    if(is.null(city$latitude))
        city$latitude <- 'NULL'
    if(is.null(city$longitude))
        city$longitude <- 'NULL'
    stmt <- paste("INSERT INTO city (name, state, latitude, longitude)",
                  "VALUES ('", city$name, "', '", city$state, "',",
                  city$latitude, ", ", city$longitude, ")",
                  sep='')
    return(db.add.record(con, stmt))
}

## Adds a record to the database and returns the row id of the new record
db.add.record <- function(con, sql) {
    ## Enter the data
    dbSendQuery(con, sql)
    ## Get the id of the last entered record 
    stmt <- "SELECT last_insert_rowid()"
    res <- dbSendQuery(con, stmt)
    key <- as.integer(fetch(res, 1))
    dbClearResult(res)
    return(key)
}

## Adds a single stadium to the database.
db.add.stadium <- function(con, stadium) {
    if(is.null(stadium$city))
        stadium$city <- 'NULL'
    if(is.null(stadium$yr.opened))
        stadium$yr.opened <- 'NULL'
    stmt <- paste("INSERT INTO stadium (id, city, year_opened) VALUES (",
                  stadium$key, ", ", stadium$city, ", ", stadium$yr.opened,
                  ')', sep="")
    id <- db.add.record(con, stmt)
    return(dbGetQuery(con,
                      sprintf('SELECT id FROM stadium WHERE OID = %d', id)))
}

## Adds a set of stadiums to the database
db.add.stadiums <- function(stadiums) {
    con <- db.connect()
    stadiums$key <- sapply(seq(nrow(stadiums)), function(i)
                           return(db.add.stadium(con, stadiums[i,])))
    db.disconnect(con)
    return(stadiums)
}

## Adds the transitive properties of a single stadium to the database
db.add.stadium.transitive <- function(con, stadium) {
    if(is.null(stadium$key))
        stop('Stadium keys must be provided')
    if(is.null(stadium$name)) {
        stadium$name <- 'NULL'
    } else { ## Escape quotes in stadium names
        stadium$name <- gsub("'", "''", stadium$name)
    }
    if(is.null(stadium$capacity))
        stadium$capacity <- 'NULL'
    if(is.null(stadium$surface))
        stadium$surface <- 'NULL'
    stmt <- paste("INSERT INTO stadium_transitive ",
                  "(stadium, name, capacity, surface)",
                  "VALUES (", stadium$key, ", '", stadium$name, "', ",
                  stadium$capacity, ", '", stadium$surface, "')",
                  sep="")
    return(db.add.record(con, stmt))
}

## Adds the transitive properties of a set of stadiums to the database
## TODO This function is redundant with other db.add.plural
db.add.stadiums.transitive <- function(stadiums) {
    con <- db.connect()
    stadiums$key <- sapply(seq(nrow(stadiums)), function(i)
                           return(db.add.stadium.transitive(con,
                                                            stadiums[i,])))
    db.disconnect(con)
    return(stadiums)
}

## Adds the permenant team data for a single team
db.add.team <- function(con, team) {
    if(is.null(team$university)) {
        stop("The university name must be provided")
    } else {
        team$university <- gsub("'", "''", team$university)
    }
    stmt <- paste("INSERT INTO team ",
                  "(university) ",
                  "VALUES ( '", team$university, "' )",
                  sep="")
    return(db.add.record(con, stmt))
}

## Adds the permanent team data for a set of teams
db.add.teams <- function(teams) {
    con <- db.connect()
    teams$key <- sapply(seq(nrow(teams)), function(i)
                            return(db.add.team(con, teams[i,])))
    db.disconnect(con)
    return(teams)
}

## Connects to the database
db.connect <- function()
    return(dbConnect(dbDriver('SQLite'), dbname=dbname))

## Create the database schema
db.create <- function() {
    con <- db.connect()
    db.create.table.city(con)
    db.create.table.stadium(con)
    db.create.table.stadium.transitive(con)
    db.create.table.team(con)
    db.create.table.team.transitive(con)
    db.create.table.game(con)
    db.create.table.play(con)
    db.disconnect(con)
}

## Create the table for storing cities
db.create.table.city <- function(con) {
    dbSendQuery(con, paste(
        "CREATE TABLE city (",
        "id INTEGER PRIMARY KEY AUTOINCREMENT,",
        "name TEXT NOT NULL,",
        "state VARCHAR(2) NOT NULL,",
        "latitude REAL,",
        "longitude REAL",
        ")"
        ))
}

## Create te table for storing games
db.create.table.game <- function(con) {
    dbSendQuery(con, paste(
        "CREATE TABLE game (",
        "id INTEGER PRIMARY KEY,",
        "date VARCHAR(16) NOT NULL,",
        "home_team INTEGER NOT NULL,",
        "away_team INTEGER NOT NULL,",
        "stadium VARCHAR(2) NOT NULL,",
        "FOREIGN KEY ( home_team ) REFERENCES team(id),",
        "FOREIGN KEY ( away_team ) REFERENCES team(id)",
        ")"
        ))
}

## Create a table for storing play information
## TODO Add triggers
db.create.table.play <- function(con) {
    dbSendQuery(con, paste(
        "CREATE TABLE play (",
        "id INTEGER PRIMARY KEY AUTOINCREMENT,",
        "game_code INTEGER NOT NULL,",
        "play_number INTEGER,",
        "quarter INTEGER NOT NULL,",
        "clock INTEGER,",
        ## TODO Store home/away are reference game for team names
        "offense INTEGER NOT NULL,", 
        "defense INTEGER NOT NULL,",
        "offense_points INTEGER,",
        "defense_points INTEGER,",
        "down INTEGER NOT NULL,",
        "distance INTEGER NOT NULL,",
        "spot INTEGER NOT NULL,",
        "play_type TEXT,",
        "drive_number INTEGER NOT NULL,",
        "drive_play INTEGER NOT NULL,",
        "FOREIGN KEY ( game_code ) REFERENCES game(id)",
        "FOREIGN KEY ( offense ) REFERENCES team(id)",
        "FOREIGN KEY ( defense ) REFERENCES team(id)",
        ")"
        ))
}

## Create the table for storing stadium permanent properties
db.create.table.stadium <- function(con) {
    dbSendQuery(con, paste(
        "CREATE TABLE stadium (",
        "id INTEGER PRIMARY KEY,",
        "city INTEGER NOT NULL,",
        "year_opened INTEGER,",
        "FOREIGN KEY(city) REFERENCES city(id)",
        ")"
        ))
}

## Create the table for storing stadium transitive properties
db.create.table.stadium.transitive <- function(con) {
    dbSendQuery(con, paste(
        "CREATE TABLE stadium_transitive (",
        "id INTEGER PRIMARY KEY AUTOINCREMENT,",
        "stadium INTEGER NOT NULL,",
        "name TEXT,",
        "capacity INTEGER,",
        "surface TEXT,",
        "FOREIGN KEY (stadium) REFERENCES stadium(id)",
        ")"
        ))
}

## Create the table for storing teams
db.create.table.team <- function(con) {
    dbSendQuery(con, paste(
        "CREATE TABLE team (",
        "id INTEGER PRIMARY KEY,",
        "university TEXT NOT NULL UNIQUE",
        ")"
        ))
}

## Create the table for storing teams transitive properties
db.create.table.team.transitive <- function(con) {
    dbSendQuery(con, paste(
        "CREATE TABLE team_transitive (",
        "id INTEGER PRIMARY KEY AUTOINCREMENT,",
        "team_id INTEGER,",
        "home_stadium INTEGER NOT NULL,",
        "year INTEGER NOT NULL,",
        "FOREIGN KEY ( team_id ) REFERENCES team(id)",
        "FOREIGN KEY ( home_stadium ) REFERENCES stadium(id)",
        ")"
        ))
}

## Flushes and transactions and disconnects from database con
db.disconnect <- function(con) {
    dbCommit(con)
    dbDisconnect(con)
}

## Retrieves all cities from the database
db.fetch.cities <- function() {
    con <- db.connect()
    cities <- dbGetQuery(con, 'SELECT * from city')
    db.disconnect(con)
    return(cities)
}

## ## Executes an arbitrary sql statement and returns the result
db.get.query <- function(sql) {
    con <- db.connect()
    res <- dbGetQuery(con, sql)
    db.disconnect(con)
    return(res)
}

## Updates multiple city records in the database
db.update.cities <- function(cities) {
    con <- db.connect()
    sapply(seq(nrow(cities)), function(i)
           db.update.city(con, cities[i,]))
    db.disconnect(con)
}

## Updates a city record in the database
db.update.city <- function(con, city) {
    if(is.null(city$latitude) | is.na(city$latitude))
        city$latitude <- 'NULL'
    if(is.null(city$longitude)| is.na(city$longitude))
        city$longitude <- 'NULL'
    stmt <- paste("UPDATE city SET ",
                  "name = '", city$name, "', ",
                  "state = '", city$state, "', ",
                  "latitude=", city$latitude, ", ",
                  "longitude=", city$longitude, " ",
                  "WHERE id=", city$id,
                  sep="")
    dbSendQuery(con, stmt)
}
