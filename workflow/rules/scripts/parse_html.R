# parse ukbiobank data dictionary

pacman::p_load(tidyverse, rvest)

ukb_html <- snakemake@input[[1]]
# ukb_html <- "data/app15422/ukb42306.html"

UKB_data_dict <- read_html(ukb_html)

# list of target db by type
# load data

df <- html_nodes(UKB_data_dict, "table") %>% .[2] %>%
  html_table() %>%
  .[[1]] %>%
  as_tibble() %>%
  mutate(db = case_when(UDI == "eid" ~ "eid",
                        Type == "Integer" ~ "int",
                        Type == "Categorical (single)" ~ "singleCat",
                        Type == "Categorical (multiple)" ~ "multiCat",
                        Type == "Continuous" ~ "float",
                        TRUE ~ "str")) %>%
  separate(UDI, into = c("field", "instance", "array"),
           sep = "[-\\.]", remove=F, fill = "right") %>% 
  separate(Description, into = c("Description", "Coding"),
           sep = "(Uses data-coding )", fill = "right")

# write table
write_tsv(df, snakemake@output[[1]])
