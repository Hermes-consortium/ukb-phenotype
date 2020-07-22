# R script to transform from wide to long format

pacman::p_load(tidyverse,vroom)

type <- snakemake@wildcards[["type"]]

r_type <- case_when(type %in% c("int", "singleCat", "multiCat", "eid") ~ "i",
                    type %in% c("str", "other") ~ "c",
                    type == "float" ~ "d")

vroom(snakemake@input[[1]], col_types = r_type) %>%
  pivot_longer(cols = -eid,
               names_to = c("field", "instance", "array"),
               names_pattern = "(.*)-(.*)\\.(.*)") %>%
  vroom_write(snakemake@output[[1]], delim=",")
