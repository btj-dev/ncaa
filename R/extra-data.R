## This file contains functions for downloading data from additional sources.

## Downloads a csv file with a list of US to dir
download.city.locations <- function(dir) {
    download.file('http://notebook.gaslampmedia.com/wp-content/uploads/2013/08/zip_codes_states.csv', sprintf('%s/zip-codes.csv', dir))
}

## Finds the row index for city.rcd in zip.code. Only 1 value will be returned## regardless of the number of matches.
get.city.index <- function(city.rcd, zip.codes)
    return(subset(subset(zip.codes, as.character(state) == city.rcd$state),
                  city == city.rcd$name)$id[1])

## Adds the lat/lon for any any cities in the database that do not have them.
## dir specifies the directory where the zip code data is stored.
load.city.locations <- function(dir) {
    zip.codes <- read.zip.codes(dir)
    zip.codes$id <- seq(nrow(zip.codes))
    cities <- db.fetch.cities()
    cities.idxs <- sapply(seq(nrow(cities)), function(i)
                          get.city.index(cities[i,], zip.codes))
    cities$latitude <- zip.codes$latitude[cities.idxs]
    cities$longitude <- zip.codes$longitude[cities.idxs]
    db.update.cities(cities)
}

## Loads the zip codes from the csv file and fixes some discrepencies between
## datasets
read.zip.codes <- function(dir) {
    zip.codes <- read.csv(sprintf('%s/zip-codes.csv', dir), header=T)
    zip.codes$city <- as.character(zip.codes$city)
    zip.codes$city[which(zip.codes$city == 'Foxboro' &
          as.character(zip.codes$state) == 'MA')] <- 'Foxborough'
    ## Not entirely true, but Landover doesn't have it's own zip code
    zip.codes$city[which(zip.codes$city == 'Hyattsville' &
          as.character(zip.codes$state) == 'MD')] <- 'Landover'
    zip.codes$city <- as.factor(zip.codes$city)
    return(zip.codes)
}
