## Loads all tables frm the database into the global environment.
##
library(RSQLite)

dbname <- '../data/ncaa-fb.sqlite3'

## This function loads the entire contents of the SQLite database specified
## into the global environment. Each table is a separate data.frame with
## the smae name as that from the database except that underscores are
## replaced with periods.
load.data <- function(dbname) {
    con <- dbConnect(dbDriver('SQLite'), dbname)
    tables <- dbListTables(con)
    for(table in tables)
        assign(gsub('_', '.', table, ),
               dbGetQuery(con, sprintf("select * from %s", table)),
               pos=globalenv())
    dbDisconnect(con)
}
