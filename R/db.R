library(compiler)
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
    if(is.null(play$offense.points) || is.na(play$offense.points))
        play$offense.points <- 'NULL'
    if(is.null(play$defense.points) || is.na(play$defense.points))
        play$defense.points <- 'NULL'
    if(is.null(play$down) || is.na(play$down))
        play$down <- 'NULL'
    if(is.null(play$distance) || is.na(play$distance))
        play$distance <- 'NULL'
    if(is.null(play$spot) || is.na(play$spot))
        play$spot <- 'NULL'
    if(is.null(play$play.type) || is.na(play$play.type))
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

## Returns TRUE if NULL or NA, and FALSE otherwise
is.missing <- function(x)
    return(is.null(x) || is.na(x))

## Adds a set of plays to the database and returns plays unchanged.
## TODO Bulk inserts are much faster (few seconds vs. hours), modify all
## inserts to use them
## TODO redundant functionality
## TODO Play types are not inserted correctly
db.add.plays <- function(plays) {
    ## Check the plays for validity
    nplays <- nrow(plays)
    plays <- plays[!is.missing(plays$game.code),]
    plays <- plays[!is.missing(plays$play.number),]
    plays <- plays[!is.missing(plays$offense),]
    plays <- plays[!is.missing(plays$defense),]
    plays$quarter[is.missing(plays$quarter)] <- 'NULL'
    plays$clock[is.missing(plays$clock)] <- 'NULL'
    plays$offense.points[is.missing(plays$offense.points)] <- 'NULL'
    plays$defense.points[is.missing(plays$defense.points)] <- 'NULL'
    plays$down[is.missing(plays$down)] <- 'NULL'
    plays$distance[is.missing(plays$distance)] <- 'NULL'
    plays$spot[is.missing(plays$spot)] <- 'NULL'
    plays$play.type <- as.character(plays$play.type)
    plays$play.type[is.missing(plays$play.type)] <- 'NULL'
    plays$drive.number[is.missing(plays$drive.number)] <- 'NULL'
    plays$drive.play[is.missing(plays$drive.play)] <- 'NULL'
    if(nplays != nrow(plays))
        warning(sprintf('%d plays were ignored due to NULL values.',
                        nplays - nrow(plays)))
    ## Rename variables in plays to use _ instead of . (for SQL)
    names(plays) <- gsub("\\.", "_", names(plays))
    ## Bulk insert the plays
    sql <- read.sql('../sql/insert-into-play.sql')
    db.send.prepared.query(sql, plays)
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
db.create.table.city <- function(con)
    dbSendQuery(con, read.sql('../sql/create-table-city.sql'))

## Create the table for storing games
db.create.table.game <- function(con)
    dbSendQuery(con, read.sql('../sql/create-table-game.sql'))

## Create a table for storing play information
## TODO Add triggers
db.create.table.play <- function(con)
    dbSendQuery(con, read.sql('../sql/create-table-play.sql'))

## Create the table for storing stadium permanent properties
db.create.table.stadium <- function(con)
    dbSendQuery(con, read.sql('../sql/create-table-stadium.sql'))

## Create the table for storing stadium transitive properties
db.create.table.stadium.transitive <- function(con)
    dbSendQuery(con, read.sql('../sql/create-table-stadium-transitive.sql'))

## Create the table for storing teams
db.create.table.team <- function(con)
    dbSendQuery(con, read.sql('../sql/create-table-team.sql'))

## Create the table for storing teams transitive properties
db.create.table.team.transitive <- function(con)
    dbSendQuery(con, read.sql('../sql/create-table-team-transitive.sql'))

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

## Sends a set of prepared queries to the database
db.send.prepared.query <- function(sql, bind.data) {
    con <- db.connect()
    dbBeginTransaction(con)
    dbSendPreparedQuery(con, sql, bind.data=bind.data)
    dbCommit(con)
    db.disconnect(con)
}
compile(db.send.prepared.query)

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

## Reads and returns a sql statement from file stripping any whitespace that
## consists of 2 spaces.
read.sql <- function(file)
    return(gsub('  ', '', paste(readLines(file), collapse='')))

