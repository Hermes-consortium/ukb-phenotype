# R script to transform from wide to long format

pacman::p_load(tidyverse,vroom)

type <- snakemake@wildcards[["type"]]
format <- snakemake@wildcards[["format"]]

r_type <- case_when(type == "str" ~ "c",
                    type == "float" ~ "d",
                    TRUE ~ "i")

delim <- ifelse(format == "csv", ",", "\t")

vroom(snakemake@input[[1]], col_types = c(.default = r_type, eid ="i")) %>%
  pivot_longer(cols = -eid,
               names_to = c("field", "instance", "array"),
               names_pattern = "(.*)-(.*)\\.(.*)") %>%
  filter(!is.na(value)) %>%
  vroom_write(snakemake@output[[1]], delim=delim)
