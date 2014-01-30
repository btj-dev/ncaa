## Plotting functions for the stadiums

library(maps)

source('db.R')

## Get the stadiums from the database
stadiums <- db.get.query(paste("SELECT stadiums.id, latitude, longitude ",
                               "FROM cities JOIN stadiums ",
                               "ON stadiums.city = cities.id ",
                               "WHERE cities.name != 'Honolulu'",
                               sep=''))

## Plot the US with stadiums as targets
map('state', col='grey', lwd=0.5)
map('usa', add=T)
points(stadiums$longitude, stadiums$latitude, pch=13)
