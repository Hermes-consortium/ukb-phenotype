# script to put data into sqlite database

pacman::p_load(vroom, tidyverse, purrr, DBI, RSQLite)

list_file <- snakemake@input
db <- snakemake@output[[1]]
params <- snakemake@params

# for testing=============
# list_file <-
#   paste0("data/app9922/",
#          c("hesin", "hesin_diag", "hesin_oper", "death", "death_cause"),
#          ".txt") %>%
#   as.list
#
# db <- "data/app9922/ehr.sqlite"
# params <- list(overwrite_db = F,
#                overwrite_table = F,
#                append_table = F)
# ========================
dataset <- map_chr(list_file, ~basename(.) %>% str_remove_all("\\..*"))

new_eid <- paste0("eid", snakemake@wildcards[['app_id']])

list_df <- map(list_file,
                ~vroom(.) %>%
                  rename(!!rlang::quo_name(new_eid) := eid)) %>%
  set_names(dataset)

# Store into SQLite database
if (params$overwrite_db) system(paste("rm -f", db))

db_conn <- dbConnect(SQLite(), db)

into_sqlite <- function(dataset, conn = db_conn,
                        overwrite_table = params$overwrite_table,
                        append_table = params$append_table){
  dbWriteTable(conn, dataset, list_df[[dataset]],
               overwrite = overwrite_table,
               append = append_table)
}

walk(dataset, into_sqlite)

# TEST =============
# df <- dbGetQuery(db_conn, 'SELECT * FROM int WHERE value IS NOT NULL')
#
# df_str <- tbl(db_conn, "str")
#
# df_str %>% distinct(eid) %>% tally
#
# main_int_db %>% filter(!is.na(value))
#
# main_int_db %>% show_query()

dbDisconnect(db_conn)
