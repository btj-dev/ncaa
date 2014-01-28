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
    stmt <- paste("INSERT INTO cities (name, state, latitude, longitude)",
                  "VALUES ('", city$name, "', '", city$state, "',",
                  city$latitude, ", ", city$longitude, ")",
                  sep='')
    print(stmt)
    dbSendQuery(con, stmt)
    stmt <- sprintf("SELECT id FROM cities WHERE name = '%s' and state = '%s'",
                    city$name, city$state)
    print(stmt)
    res <- dbSendQuery(con, stmt)
    
    key <- as.integer(fetch(res, 1))
    dbClearResult(res)
    return(key)
}

## Create the database schema
db.create <- function() {
    con <- db.connect()
    db.create.table.cities(con)
    db.create.table.stadiums(con)
    db.create.table.stadiums.transitive(con)
    db.disconnect(con)
}

## Connects to the database
db.connect <- function()
    return(dbConnect(dbDriver('SQLite'), dbname=dbname))

## Create the table for storing cities
db.create.table.cities <- function(con) {
    dbSendQuery(con, paste(
        "CREATE TABLE cities (",
        "id INTEGER PRIMARY KEY AUTOINCREMENT,",
        "name TEXT NOT NULL,",
        "state VARCHAR(2) NOT NULL,",
        "latitude REAL,",
        "longitude REAL",
        ")"
        ))
}

## Create the table for storing stadium permanent properties
db.create.table.stadiums <- function(con) {
    dbSendQuery(con, paste(
        "CREATE TABLE stadiums (",
        "id INTEGER PRIMARY KEY AUTOINCREMENT,",
        "city INTEGER,",
        "year_opened INTEGER,",
        "FOREIGN KEY(city) REFERENCES cities(id)",
        ")"
        ))
}

## Create the table for storing stadium transitive properties
db.create.table.stadiums.transitive <- function(con) {
    dbSendQuery(con, paste(
        "CREATE TABLE stadiums_transitive (",
        "id INTEGER PRIMARY KEY AUTOINCREMENT,",
        "stadium INTEGER NOT NULL,",
        "name TEXT,",
        "capacity INTEGER,",
        "surface TEXT,",
        "FOREIGN KEY (stadium) REFERENCES stadium(id)",
        ")"
        ))
}

## Flushes and transactions and disconnects from database con
db.disconnect <- function(con) {
    dbCommit(con)
    dbDisconnect(con)
}
