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

## Adds a single stadium to the database. If neither 
db.add.stadium <- function(con, stadium) {
    if(is.null(stadium$city))
        stadium$city <- 'NULL'
    if(is.null(stadium$yr.opened))
        stadium$yr.opened <- 'NULL'
    stmt <- paste("INSERT INTO stadiums (city, year_opened) VALUES (",
                  stadium$city, ", ", stadium$yr.opened, ')',
                  sep="")
    return(db.add.record(con, stmt))
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
    stmt <- paste("INSERT INTO stadiums_transitive ",
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

## Connects to the database
db.connect <- function()
    return(dbConnect(dbDriver('SQLite'), dbname=dbname))

## Create the database schema
db.create <- function() {
    con <- db.connect()
    db.create.table.cities(con)
    db.create.table.stadiums(con)
    db.create.table.stadiums.transitive(con)
    db.disconnect(con)
}

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
        "city INTEGER NOT NULL,",
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

## Retrieves all cities from the database
db.fetch.cities <- function() {
    con <- db.connect()
    cities <- dbGetQuery(con, 'SELECT * from cities')
    db.disconnect(con)
    return(cities)
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
    if(is.null(city$latitude))
        city$latitude <- 'NULL'
    if(is.null(city$longitude))
        city$longitude <- 'NULL'
    stmt <- paste("UPDATE cities SET ",
                  "name = '", city$name, "', ",
                  "state = '", city$state, "', ",
                  "latitude=", city$latitude, ", ",
                  "longitude=", city$longitude, " ",
                  "WHERE id=", city$id,
                  sep="")
    dbSendQuery(con, stmt)
}
