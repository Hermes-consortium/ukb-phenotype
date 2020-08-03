# R script to transform from wide to long format

pacman::p_load(tidyverse,vroom)

type <- snakemake@wildcards[["type"]]
format <- snakemake@wildcards[["format"]]
input <- snakemake@input[[1]]
output <- snakemake@output[[1]]

# Interactive run ------------------------------------
# type <- "int"
# format <- "csv"
# input <- "data/app15422/by_type/ukb42306_int.csv"
# output <- "data/app15422/by_type/ukb42306_int_long.csv"
# ------------------------------------------------------

r_type <- case_when(type == "str" ~ "c",
                    type == "float" ~ "d",
                    TRUE ~ "i")

delim <- ifelse(format == "csv", ",", "\t")

df <- vroom(input, col_types = c(.default = r_type, eid ="i")) %>%
  pivot_longer(cols = -eid,
               names_to = "udi") %>% 
               # names_to =  c("field", "instance", "array"),
               # names_pattern = "(.*)-(.*)\\.(.*)") %>%
  filter(!is.na(value)) %>%
  separate(udi, into = c("field", "instance", "array"),
           sep = "[-\\.]", remove=F, fill = "right") %>% 
  mutate(across(c(field, instance, array), as.integer))

vroom_write(df, output, delim=delim)
