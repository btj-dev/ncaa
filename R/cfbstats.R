source('db.R')

## This function downloads the college football data from cfbstats.com and
## saves it to dir. The compressed archives are saved to dir/zipped, then
## upzipped to dir/[YEAR] where [YEAR] is the 4 digit year. This function
## will create dir and any subdirectories as necessary, but only if the parent
## of dir exists.
download.cfbstats <- function(dir='../data/raw', years=seq(2005, 2013)) {
    if(!file.exists(dir))
        dir.create(dir)
    for(year in years)
        if(!download.year(dir, year))
            print(sprintf('Could not download %d.', year))
}

## This function downloads a single year of data to dir/zipped and expands the
## archive to dir/[YEAR]. If the year is unavailable, it does nothing and
## returns FALSE, otherwise it returns TRUE. The year should be expressed
## with 4 digits.
download.year <- function(dir, year) {
    ## Set the url
    if((year < 2005) || (year > 2013)) { # Unavailable
        return(FALSE)
    } else if(year == 2013) { # Version 1.5.20
        url <- 'http://www.cfbstats.com/data/cfbstats.com-2013-1.5.20.zip'
    } else if(year == 2012) {# Version 1.5.4
        url <- 'http://www.cfbstats.com/data/cfbstats.com-2012-1.5.4.zip'
    } else { # Version 1.5.0 URL name
        url <-
            sprintf('http://www.cfbstats.com/data/cfbstats.com-%d-1.5.0.zip',
                    year)
    }
    ## Create the zipped directory if it doesn't exist and download there
    zip.dir <- sprintf('%s/zipped', dir)
    if(!file.exists(zip.dir))
        dir.create(zip.dir)
    zipfile = sprintf('%s/cfbstats-%d.zip', zip.dir, year)
    download.file(url, zipfile)
    ## Expand the archive
    unzip(zipfile, exdir=sprintf('%s/%d', dir, year))
    return(TRUE)
}

## Fix some naming errors and discrepencies between datasets for the cities
fix.cities <- function(cities) {
    cities$City <- as.character(cities$City)
    cities$City[which((cities$City == 'De Kalb') &
                      (as.character(cities$State) == 'IL'))] <- 'Dekalb'
    cities$City[which((cities$City == 'Munice') &
                      (as.character(cities$State) == 'IN'))] <- 'Muncie'
    cities$City[which((cities$City == 'St. Louis') &
                      (as.character(cities$State) == 'MO'))] <- 'Saint Louis'
    cities$City[which((cities$City == 'St. Petersburg') &
                      (as.character(cities$State) == 'FL'))] <-
                          'Saint Petersburg'
    cities$City <- as.factor(cities$City)
    return(cities)
}

## Returns a list of all available years
get.all.years <- function() return(seq(2005, 2013))

## Identifies the primary (most common) home stadium for each team in each
## year
get.home.stadiums <- function(datadir, years) {
    ## Load the data and merge teams with the games they played
    teams.raw <- read.multi.files(sprintf('%s/%%d/team.csv', datadir),
                                  years)
    games.raw <- read.multi.files(sprintf('%s/%%d/game.csv', datadir),
                                  years)
    df <- with(subset(merge(teams.raw, games.raw,
                            by.x='Team.Code', by.y='Home.Team.Code'),
                      Site=='TEAM'),
               data.frame(team=Team.Code, year=itr.y,
                          stadium=Stadium.Code))
    ## Find the most common home stadium for each team, each year
    teams <- unique(df$team)
    return.stadiums <- expand.grid(team.id=teams, year=years) ## Return df
    return.stadiums$home.stadium <- sapply(seq(nrow(return.stadiums)),
        function(i) {
            data <- subset(subset(df, team == return.stadiums$team.id[i]),
                           year == return.stadiums$year[i])
            uni <- unique(data$stadium)
            return(uni[which.max(tabulate(match(data$stadium, uni)))])
        })
    return(return.stadiums)
}

## Loads the plays from disk into the database as table play
load.plays <- function(datadir, year=get.all.years()) {
    plays <- read.multi.files(sprintf('%s/%%d/play.csv', datadir),
                              years)
}

## Loads the stadiums from disk into the database as tables city, stadium, and
## stadium_transitive dataset must have already been downloaded.
load.stadiums <- function(datadir, years=get.all.years()) {
    stadiums.raw <- read.multi.files(sprintf('%s/%%d/stadium.csv', datadir),
                                     years)
    stadiums.raw <- fix.cities(stadiums.raw)
    ## Extract the cities that house stadiums
    cities <- with(stadiums.raw, unique(data.frame(name=City, state=State)))
    cities <- db.add.cities(cities)
    ## Extract the permanent properties of stadiums
    stadiums <- with(stadiums.raw, unique(data.frame(key=Stadium.Code,
                                                     city=City, state=State,
                                                     yr.opened=Year.Opened)))
    stadiums$city.key <- sapply(seq(nrow(stadiums)), function(i)
        return(cities$key[which(cities$name == stadiums$city[i] &
                                cities$state == stadiums$state[i])]))
    stadiums <- with(stadiums, data.frame(key=key, city=city.key,
                                          yr.opened=yr.opened))
    stadiums <- db.add.stadiums(stadiums)
    ## Extract the transitive properties of stadiums
    stadiums.transitive <- with(stadiums.raw, data.frame(key=Stadium.Code,
                                                         name=Name,
                                                         capacity=Capacity,
                                                         surface=Surface))
    ## Replace the csv keys to the database keys
    stadiums.transitive$key <- sapply(seq(nrow(stadiums.transitive)),
        function(i)
            stadiums$key[which(stadiums$key ==
                               stadiums.transitive$key[i])])
    ## Add the transitive properties to the database
    stadiums.transitive <- db.add.stadiums.transitive(stadiums.transitive)
}

## Loads the teams from disk into the database
load.teams <- function(datadir, years=get.all.years()) {
    ## Load the permanent data for each team
    teams.raw <- read.multi.files(sprintf('%s/%%d/team.csv', datadir),
                                  years)
    teams <- db.add.teams(unique(data.frame(university=teams.raw$Name,
                                            old.key=teams.raw$Team.Code)))
    ## Load the home stadium for each team each year
    home.stadiums <- get.home.stadiums(datadir, years)
    home.stadiums <- db.add.teams.transitive(home.stadiums)
}

## Loads multiple years of data into a single data frame. Format is a string
## to be passed to sprintf along with each element of itr (e.g.
## sprintf(format, itr[1])).
read.multi.files <- function(format, itr) {
    ## Read the data from each file into a list
    lst <- list()
    for(i in seq(length(itr)))
        lst[[i]] <- read.csv(sprintf(format, itr[i]))

    ## Add the value of itr as an additional attribute
    for(i in seq(length(itr)))
        lst[[i]]$itr <- itr[i]
    
    ## Aggregate the data, this will be slow for large datasets due to
    ## reallocating the dataframe
    df <- lst[[1]]
    for(i in seq(2, length(itr)))
        df <- rbind(df, lst[[i]])

    return(df)
}
