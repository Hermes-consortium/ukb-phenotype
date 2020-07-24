# script to put data into sqlite database

pacman::p_load(vroom, tidyverse, purrr, DBI, RSQLite)

list_file <- snakemake@input

# for testing
list_file <-
  paste0("data/app15422/by_type/ukb42306_",
         c("str", "int", "float", "multiCat", "singleCat"),
         "_long.csv") %>%
  as.list

types <- map_chr(list_file, basename) %>% str_split("_") %>% map_chr(`[`, 2)

r_types <- case_when(types %in% c("int", "singleCat", "multiCat", "eid") ~ "i",
                    types %in% c("str") ~ "c",
                    types == "float" ~ "d") %>% set_names(types)

list_df <- map2(list_file, types, ~vroom(.x, col_types=c(.default = r_type, eid = "i")))

into_sqlite <- function(type){

}
