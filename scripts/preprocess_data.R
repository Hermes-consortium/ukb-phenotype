# scripts to prepare UKB data (converted to fst format for speed)

pacman::p_load(tidyverse, data.table, rvest, RSQLite, DBI)

app_id <- "app15422"
dataset_id <- "ukb42306"

data_path <- paste0("data/", app_id, "/", dataset_id)

# MAIN DATASET ===========
DT <- fread(paste0(data_path, ".csv"))

# create an fst format
# library(fst)
# write_fst(DT, "data/2020-08-06_ukb42306_app15422.fst")

# test
# test_DT <- read_fst("data/2020-08-06_ukb42306_app15422.fst",
#                     columns = c("eid", "31-0.0", "34-0.0"),
#                     from = 1, to = 100000, as.data.table = T)

# Separate database based on datatype, preserving eid
col_types <- sapply(DT, class) %>% .[names(.)!="eid"]

types <- list(int = "integer",
              chr = "character",
              real = "numeric",
              bool = "logical")

# create list of DT based on data types
list.DT <- map(types, function(type){
  DT[,.SD, .SDcols = c("eid", names(col_types[col_types == type]))]
  })

# Transform into long format
list.DT <- map(list.DT, pivot_longer, cols = -eid,
               names_to = c("field", "instance", "array"),
               names_pattern = "(.*)-(.*)\\.(.*)")

# Store into SQLite database
db <- dbConnect(SQLite(), paste0(data_path, ".sqlite"))

# write data into database
for (type in names(types)) {
  dbWriteTable(db, paste0("main_", type), list.DT[[type]])
}

# DATA DICTIONARY ==============
UKB_data_dict <- read_html(paste0(data_path, ".html"))

# create lookup tables
list_data_dict <- html_nodes(UKB_data_dict, "table") %>% .[3:length(.)] %>%
  map(function(node){
    DT <- as.data.table(html_table(node))
    coding_id <- html_attr(node, "summary") %>% str_remove("Coding ") %>% as.numeric
    DT[, coding_id := coding_id]
    select(DT, coding_id, code = Code, meaning = Meaning)
  })

DT_data_dict <- rbindlist(list_data_dict)
setnames(DT_data_dict, "coding_id", "field")

dbWriteTable(db, "main_dict", DT_data_dict)


# TEST =============
# df <- dbGetQuery(db, 'SELECT * FROM main_int WHERE value IS NOT NULL')
# 
# main_int_db <- tbl(db, "main_int")
# 
# main_int_db %>% filter(!is.na(value))
# 
# main_int_db %>% show_query()

dbDisconnect(db)