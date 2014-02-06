#!/usr/local/bin/Rscript

## This script downloads the data and populates the database.

library(compiler)

source('cfbstats.R') ## Functions to download from cfbstats.com
source('config.R') ## Configuration
source('db.R') ## Functions for interacting with the database
source('extra-data.R') ## Functions for download data from various sources

## TODO Enable JIT compilation
## Some of the functions use global variables. This makes JIT compiliation
## impossible. Since there are a large number of play, it would be very
## helpful to enable JIT compilation.
enableJIT(0)

## Create the database
db.create()

## Download stats from cfbstats.com and add to db as needed
## TODO There needs to be error catching in db.R. It should be possible to
## call these functions again if they fail and have them cleanly pick up where
## they left off.
download.cfbstats(cfg$rawdatadir)
load.stadiums(cfg$rawdatadir)
load.teams(cfg$rawdatadir)
load.games(cfg$rawdatadir)
load.plays(cfg$rawdatadir)

library(profr)
summaryRprof()

## Download supplementary info from other sites
download.city.locations(cfg$rawdatadir)
load.city.locations(cfg$rawdatadir)
