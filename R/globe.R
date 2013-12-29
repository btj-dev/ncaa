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
                'http://www.ngdc.noaa.gov/mgg/topo/elev/esri/hdr/%s10g.hdr',
                let)
            download.file(file, sprintf('%s/%s10g.hdr', dir, let))
        })
}
