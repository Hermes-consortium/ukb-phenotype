# make database

# scripts to prepare UKB data (converted to fst format for speed)

pacman::p_load(vroom, tidyverse, purrr, DBI, RSQLite)

app_id <- snakemake@wildcards[["app_id"]]
data_id <- snakemake@wildcards[["data_id"]]
input <- snakemake@input
output <- snakemake@output

# For testing
# app_id <- "app15422"
# data_id <- "ukb42306"
#
# input <- list(
#   data="test.csv",
#   profile="data/app15422/ukb42306.profile.csv"
# )
#
# output <- list(
#   db="data/ukb.sqlite"
# )

# MAIN DATASET ===========
df <- vroom(input$data)

df_profile <- vroom(input$profile) %>%
  mutate(r_type = case_when(db %in% c("int", "singleCat", "multiCat", "eid") ~ "i",
                            db %in% c("str", "other") ~ "c",
                            db == "float" ~ "d")) %>%
  nest_by(db)

# Separate database based on datatype, preserving eid

read <- function(file, cols, type){
  cols <- c("eid", cols)
  vroom(file, col_select = cols,
        col_types = c("i", type) %>% set_names(cols))
}


list_df_type <- map(df_profile$data[-1],
                     ~read(input$data, .$UDI, .$r_type)
                    ) %>% set_names(df_profile$db[-1])

# create list of DT based on data types
wide2long <- function(df){
  df %>% pivot_longer(cols = -eid,
                      names_to = c("field", "instance", "array"),
                      names_pattern = "(.*)-(.*)\\.(.*)")
}

# Transform into long format
list_df_type_long <- map(list_df_type, wide2long)

# Store into SQLite database
db <- dbConnect(SQLite(), output$db)

# write data into database
for (type in names(list_df_type_long)) {
  dbWriteTable(db, paste(app_id, data_id, type, sep = "_"),
               list_df_type_long[[type]])
}



# TEST =============
# df <- dbGetQuery(db, 'SELECT * FROM main_int WHERE value IS NOT NULL')
#
# main_int_db <- tbl(db, "main_int")
#
# main_int_db %>% filter(!is.na(value))
#
# main_int_db %>% show_query()

dbDisconnect(db)
