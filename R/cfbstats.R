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

