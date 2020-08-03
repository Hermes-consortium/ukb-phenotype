# script to put data into sqlite database

pacman::p_load(vroom, tidyverse, purrr, DBI, RSQLite)

list_file <- snakemake@input
db <- snakemake@output[[1]]
params <- snakemake@params
app_id <- snakemake@wildcards[['app_id']]

# for testing=============
# list_file <-
#   paste0("data/app15422/by_type/ukb42306_",
#          c("str", "int", "float", "multiCat", "singleCat"),
#          "_long.csv") %>%
#   as.list
# 
# db <- "data/app15422/ukb42306.sqlite"
# params <- list(overwrite_db = T,
#                overwrite_table = T)
# app_id <- 15422
# ========================

types <- map_chr(list_file, basename) %>% str_split("_") %>% map_chr(`[`, 2)

r_types <- case_when(types %in% c("int", "singleCat", "multiCat", "eid") ~ "i",
                    types %in% c("str") ~ "c",
                    types == "float" ~ "d") %>% set_names(types)

new_eid <- paste0("eid", app_id)

list_df <- map2(list_file, r_types,
                ~vroom(.x, col_types=c(.default = "i", udi = "c", value = .y)) %>%
                  rename(!!rlang::quo_name(new_eid) := eid)) %>%
  set_names(types)

# Store into SQLite database
if (params$overwrite_db) system(paste("rm -f", db))

db_conn <- dbConnect(SQLite(), db)

into_sqlite <- function(type, conn = db_conn,
                        overwrite_table = params$overwrite_table,
                        append_table = !params$overwrite_table){
  dbWriteTable(conn, type, list_df[[type]],
               overwrite = overwrite_table,
               append = append_table)
}

walk(types, into_sqlite)

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
