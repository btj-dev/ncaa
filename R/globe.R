## This file contains functions for obtaining and manipulation the GLOBE
## topographic data available from
## http://www.ngdc.noaa.gov/mgg/topo/gltiles.html.

## Download the GLOBE data, unzip it, and add the associated header files.
## The output is stored in /dir/.
## If save.tarball=FALSE (default), then the compressed tar archive is deleted
## after being expanded.
download.globe <- function(dir, save.tarball=FALSE) {
    ## Create the output directory
    if(substr(dir, nchar(dir), nchar(dir)) == '/')
        dir <- substr(dir, 1, nchar(dir) - 1) # Strip trailing slash
    dir.create(dir)
    ## Download and unzip all tiles, then delete the tarball
    tarball <- sprintf('%s/all10g.tgz', dir)
    download.file(
        'http://www.ngdc.noaa.gov/mgg/topo/DATATILES/elev/all10g.tgz',
        tarball)
    untar(tarball, exdir=normalizePath(dir), compressed='gzip')
    ## Move the files from dir/all10/* to dir/*
    files <- dir(sprintf('%s/all10', dir))
    sapply(files,
           function(file)
           file.rename(sprintf('%s/all10/%s', dir, file),
                       sprintf('%s/%s', dir, file)))
    unlink(sprintf('%s/all10', dir), r=TRUE)
    if(!save.tarball)
        unlink(tarball)
    ## Dowload the header files to the same directory as the data
    hdr.urls <- sapply(letters[1:16],
        function(let) {
            file <- sprintf(
                'http://www.ngdc.noaa.gov/mgg/topo/elev/grass/cellhd/%s10g',
                let)
            download.file(file, sprintf('%s/%s10g.grass', dir, let))
        })
}

# Returns string without leading whitespace
trim.leading <- function (x)  sub("^\\s+", "", x)

# Returns string without trailing whitespace
trim.trailing <- function (x) sub("\\s+$", "", x)

# Returns string without leading or trailing whitespace
trim <- function (x) gsub("^\\s+|\\s+$", "", x)

read.grass <- function(tile.hdr) {
    dat <- read.table(tile.hdr, header=F, sep=':')
    opts <- list()
    get.opt <- function(dat, opt)
        return(trim(as.character(dat[which(dat[,1] == opt), 2])))
    opts$dim <- c(as.integer(get.opt(dat, 'rows')),
                  as.integer(get.opt(dat, 'cols')))
    opts$proj <- as.integer(get.opt(dat, 'proj'))
    opts$zone <- as.integer(get.opt(dat, 'zone'))
    get.lat.lon <- function(dat, direction) {
        val <- get.opt(dat, direction)
        int <- as.integer(substr(val, 1, 2))
        if((substr(val, nchar(val), nchar(val)) == 'S') |
           (substr(val, nchar(val), nchar(val)) == 'W'))
            int <- -int
        return(int)
    }
    opts$xlim <- c(get.lat.lon(dat, 'east'),
                    get.lat.lon(dat, 'west'))
    opts$ylim <- c(get.lat.lon(dat, 'north'),
                   get.lat.lon(dat, 'south'))
    get.res <- function(dat, direction) {
        val <- get.opt(dat, direction)
        flt <- as.numeric(substr(val, 1, 1) +
                          substr(val, 3, 4) / 60 +
                          substr(val, 6, 7) / 3600)
        return(flt)
    }
    opts$res <- c(30 / 3600,
                  30 / 3600) ## TODO Read these from file
    return(opts)
}

read.tile <- function(tile.name, tile.hdr.name) {
    ## Read the header specification
    opts <- read.grass(tile.hdr.name)
    ## Load the tile data
    con <- file(tile.name, 'rb')
    tile <- matrix(
        readBin(con, 'numeric', size=4, n=opts$dim[1] * opts$dim[2],
                   endian='little'),
        opts$dim[1], opts$dim[2])
    close(con)
    ## Return the tile data and header info
    return(list(header=opts, tile=tile))
}

load.globe <- function(dir) {
    
}
