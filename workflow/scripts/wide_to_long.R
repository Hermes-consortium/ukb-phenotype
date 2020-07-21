# R script to transform from wide to long format

library(tidyverse)

df <- read_csv(snakemake@input[[1]])

df %>%
  pivot_longer(cols = -eid,
               names_to = c("field", "instance", "array"),
               names_pattern = "(.*)-(.*)\\.(.*)")
