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
compile(db.add.cities)

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
compile(db.add.city)

## Adds a single game to the database
db.add.game <- function(con, game) {
    if(is.null(game$id))
        stop("The game id number must be provided")
    if(is.null(game$date))
        stop("The game date must be provided")
    if(is.null(game$home.team))
        stop("The home team must be specified")
    if(is.null(game$away.team))
        stop("The away team must be specified")
    if(is.null(game$stadium))
        game$stadium <- 'NULL'
    if(is.null(game$site))
        game$site <- 'NULL'
    stmt <- with(game,
                 paste("INSERT INTO game ",
                       "(id, date, home_team, away_team, stadium, site) ",
                       "VALUES ( ",
                       id, ", '", date, "', ", home.team, ", ", away.team,
                       ", ", stadium, ", '", site, "')",
                       sep=""))
    return(db.add.record(con, stmt))
}
compile(db.add.game)

## Adds a set of games to the database
db.add.games <- function(games) {
    con <- db.connect()
    games$key <- sapply(seq(nrow(games)), function(i)
                           return(db.add.game(con, games[i,])))
    db.disconnect(con)
    return(games)
}
compile(db.add.games)

## Adds a single play to the database
## There is so far no need to know the ROWID, so nothing is returned
## is.null || is.na -> is.missing (function)
db.add.play <- function(con, play) {
    if(is.null(play$game.code) || is.na(play$game.code))
        stop('A game code must be provided')
    if(is.null(play$play.number) || is.na(play$play.number))
        stop('Play number must be provided')
    if(is.null(play$quarter) || is.na(play$distance))
        play$quarter <- 'NULL'
    if(is.null(play$clock) || is.na(play$clock))
        play$clock <- 'NULL'
    if(is.null(play$offense))
        stop('Offense team id must be provided')
    if(is.null(play$defense))
        stop('Defense team id must be provided')
    if(is.null(play$offense.points) || is.na(play$distance))
        play$offense.points <- 'NULL'
    if(is.null(play$defense.points) || is.na(play$distance))
        play$defense.points <- 'NULL'
    if(is.null(play$down) || is.na(play$down))
        play$down <- 'NULL'
    if(is.null(play$distance) || is.na(play$distance))
        play$distance <- 'NULL'
    if(is.null(play$spot) || is.na(play$distance))
        play$spot <- 'NULL'
    if(is.null(play$play.type) || is.na(play$distance))
        play$play.type <- 'NULL'
    if(is.null(play$drive.number) || is.na(play$drive.number))
        play$drive.number <- 'NULL'
    if(is.null(play$drive.play) || is.na(play$drive.play))
        play$drive.play <- 'NULL'
    stmt <- with(play,
                 paste("INSERT INTO play ( game_code, play_number, quarter, ",
                       "clock, offense, defense, offense_points, ",
                       "defense_points, down, distance, spot, play_type, ",
                       "drive_number, drive_play ) VALUES ( ",
                       game.code, ", ", play.number, ", ",
                       quarter, ", ", clock, ", ", offense, ", ", defense,
                       ", ", offense.points, ", ", defense.points, ", ",
                       down, ", ", distance, ", ", spot, ", '", play.type,
                       "', ", drive.number, ", ", drive.play, " )",
                       sep=""))
    dbSendQuery(con, stmt)
}
compile(db.add.play)

## Adds a set of plays to the database and returns plays unchanged.
## TODO redundant functionality
db.add.plays <- function(plays) {
    cat(sprintf('Adding %d plays...\n', nrow(plays)))
    con <- db.connect()
    ## The entire dataframe is added in a single transaction for efficiency.
    ## Using a single transaction achieves a rate of 15,000 records/min vs.
    ## 13,000 records/min without. This is still an estimated 90 minutes/yr,
    ## which is too slow.
    dbBeginTransaction(con)
    sapply(seq(nrow(plays)), function(i) {
        if(!(i %% 10000))
            print(sprintf('%d %s', i, Sys.time()))
        return(db.add.play(con, plays[i,]))
    }
           )
    dbCommit(con)
    db.disconnect(con)
    return(plays)
}
compile(db.add.plays)

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

## Add the transitive data for a single team in a single year
db.add.team.transitive <- function(con, team) {
    if(is.null(team$team.id))
        stop("Team ID must be provided")
    if(is.null(team$year))
        stop("Year must be provided")
    if(is.null(team$home.stadium))
        team$home_stadium = 'NULL'
    stmt <- paste("INSERT INTO team_transitive ",
                  "(team_id, year, home_stadium) VALUES ( ",
                  team$team.id, ", ", team$year, ", '", team$home.stadium,
                  "' )",
                  sep="")
    return(db.add.record(con, stmt))
}

## Add the transitive data for a set of teams
db.add.teams.transitive <- function(teams) {
    con <- db.connect()
    teams$key <- sapply(seq(nrow(teams)), function(i)
                            return(db.add.team.transitive(con, teams[i,])))
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

## Create the table for storing games
db.create.table.game <- function(con) {
    dbSendQuery(con, paste(
        "CREATE TABLE game (",
        "id INTEGER PRIMARY KEY,",
        "date VARCHAR(16) NOT NULL,",
        "home_team INTEGER NOT NULL,",
        "away_team INTEGER NOT NULL,",
        "stadium VARCHAR(2),",
        "site VARCHAR(6),",
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
        "play_number INTEGER NOT NULL,",
        "quarter INTEGER,",
        "clock INTEGER,",
        ## TODO Store home/away are reference game for team names
        "offense INTEGER NOT NULL,", 
        "defense INTEGER NOT NULL,",
        "offense_points INTEGER,",
        "defense_points INTEGER,",
        "down INTEGER,",
        "distance INTEGER,",
        "spot INTEGER,",
        "play_type TEXT,",
        "drive_number INTEGER,",
        "drive_play INTEGER,",
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
        "home_stadium INTEGER,",
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
