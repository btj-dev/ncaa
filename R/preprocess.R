#!/usr/bin/Rscript

library(RSQLite)

years <- seq(2005, 2013)

load.multi.files <- function(format, itr) {
    ## Read the data from each file into a list
    lst <- list()
    for(i in seq(length(itr)))
        lst[[i]] <- read.csv(sprintf(format, itr[i]))
    
    ## Aggregate the data, this will be slow for large datasets due to
    ## reallocating the dataframe
    df <- lst[[1]]
    for(i in seq(2, length(itr)))
        df <- rbind(df, lst[[i]])

    return(df)
}

## Read the data from the csv files
confs <- load.multi.files('../data/raw/%d/conference.csv', years)
games <- load.multi.files('../data/raw/%d/game.csv', years)
game.stats <- load.multi.files('../data/raw/%d/game-statistics.csv', years)
kickoffs <- load.multi.files('../data/raw/%d/kickoff.csv', years)
kickoff.returns <-
    load.multi.files('../data/raw/%d/kickoff-return.csv', years)
passes <- load.multi.files('../data/raw/%d/pass.csv', years)
player.game.stats <-
    load.multi.files('../data/raw/%d/player-game-statistics.csv', years)
players <- load.multi.files('../data/raw/%d/player.csv', years)
punt.returns <- load.multi.files('../data/raw/%d/punt-return.csv', years)
punts <- load.multi.files('../data/raw/%d/punt.csv', years)
receptions <- load.multi.files('../data/raw/%d/reception.csv', years)
rushes <- load.multi.files('../data/raw/%d/rush.csv', years)
stadiums <- load.multi.files('../data/raw/%d/stadium.csv', years)
team.game.stats <-
    load.multi.files('../data/raw/%d/team-game-statistics.csv', years)

## Write the data to the database
con <- dbConnect(dbDriver('SQLite'), dbname='../data/ncaa-fb.sqlite3')
dbWriteTable(con, 'confs', confs)
dbWriteTable(con, 'games', games)
dbWriteTable(con, 'game_stats', game.stats)
dbWriteTable(con, 'kickoffs', kickoffs)
dbWriteTable(con, 'kickoff_returns', kickoff.returns)
dbWriteTable(con, 'passes', passes)
dbWriteTable(con, 'player_game_stats', player.game.stats)
dbWriteTable(con, 'players', players)
dbWriteTable(con, 'punt_returns', punt.returns)
dbWriteTable(con, 'punts', punts)
dbWriteTable(con, 'receptions', receptions)
dbWriteTable(con, 'rushes', rushes)
dbWriteTable(con, 'stadiums', stadiums)
dbWriteTable(con, 'team_game_stats', team.game.stats)
dbDisconnect(con)
