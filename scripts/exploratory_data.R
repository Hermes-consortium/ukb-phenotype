# 

pacman::p_load(fst, data.table, tidyverse)

source("scripts/explore_helper.R")

fields_id <- c(
  sex = "31-0.0",
  yob = "34-0.0",
  "41202-0.0"
)

DT <- get_data(fields_id)
db