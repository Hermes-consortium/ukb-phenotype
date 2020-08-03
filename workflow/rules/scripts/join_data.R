pacman::p_load(tidyverse, vroom, purrr)

input <- snakemake@input
output <- snakemake@output[[1]]

# for interactive / testing
# input <- list(
#   sample = list("data/app9922/ukb9922_imp_chr21_v3_s487296.sample",
#                 "data/app15422/ukb15422_imp_chr21_v3_s487296.sample"),
#   data = list("results/hermes2/1.tsv",
#               "results/hermes2/2.tsv")
# )

list_df <- map(input$data, vroom)

eid_cols <- map_chr(list_df, ~colnames(.x) %>% .[str_detect(., "^eid")]) %>% 
  unique()

if (length(eid_cols) == 1) {
  df_merged <- reduce(list_df, full_join, by = eid_cols)
} else {
  read_sample <- function(sample_file){
    eid_id <- basename(sample_file) %>% str_match("ukb(.*?)_") %>% .[,2] %>% 
      paste0("eid", .)
    vroom(sample_file, skip = 2, col_select = 1, col_names = eid_id)
  }
  
  df_sample <- map_dfc(input$sample, read_sample)
  
  get_key <- function(df, df_sample){
    key <- colnames(df) %>% .[which(. %in% colnames(df_sample))]
    inner_join(df, df_sample, by = key)
  }
  
  join_df <- function(df1, df2){
    df1 <- get_key(df1, df_sample)
    df2 <- get_key(df2, df_sample)
    
    full_join(df1, df2, by = colnames(df_sample)) %>% 
      select(starts_with("eid"), everything())
  }
  
  df_merged <- reduce(list_df, join_df)
}



vroom_write(df_merged, output)
