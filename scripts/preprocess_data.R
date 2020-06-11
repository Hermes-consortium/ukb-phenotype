# scripts to prepare UKB data (converted to fst format for speed)

pacman::p_load(tidyverse, data.table, fst, XML,rvest)

# ukb_data_dictionary <- read_html("data/ukb42306.html") %>% 
#   html_nodes("table") %>% .[[2]] %>% 
#   html_table()
# 
# DT <- as.data.table(ukb_data_dictionary)
# 
# # check data type
# DT[,.N, by=Type]

# create column_name - data_type pair for creating SQL table

DT <- fread("data/ukb42306.csv")

# create an fst format
write_fst(DT, "data/2020-08-06_ukb42306_app15422.fst")

# test
test_DT <- read_fst("data/2020-08-06_ukb42306_app15422.fst",
                    columns = c("eid", "31-0.0", "34-0.0"),
                    from = 1, to = 100000, as.data.table = T)