#!/usr/local/bin/Rscript

## This script downloads the data and populates the database.

source('cfbstats.R') ## Functions to download from cfbstats.com
source('config.R') ## Configuration
source('db.R') ## Functions for interacting with the database

## Create the database
db.create()

## Download stats from cfbstats.com and add to db as needed
download.cfbstats(cfg$rawdatadir)
load.stadiums(cfg$rawdatadir)

## Download supplementary info from other sites
