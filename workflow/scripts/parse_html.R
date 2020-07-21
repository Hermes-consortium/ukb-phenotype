# parse ukbiobank data dictionary

pacman::p_load(dplyr, readr, rvest)

ukb_html <- snakemake@input[[1]]
# ukb_html <- "~/mount/UCL_myriad/Projects/ukb-phenotype/data/app15422/ukb42306.html"

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
                        Type %in% c("Text", "Date", "Time") ~ "str",
                        TRUE ~ "other"))

# write table
write_csv(df, snakemake@output[[1]])
