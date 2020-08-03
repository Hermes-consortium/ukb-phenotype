# Querying phenotype from yaml file

pacman::p_load(tidyverse, yaml, purrr, glue)

input <- snakemake@input
output <- snakemake@output[[1]]
params <- snakemake@params

# For testing
# input <- list(
#   yaml = "results/hermes2/1.yaml",
#   case_list = list('data/app9922/ehr_case/HF/hesin_diag.txt',
#                     'data/app9922/ehr_case/AF/all.txt',
#                     'data/app9922/ehr_case/CKD/all.txt',
#                     'data/app9922/ehr_case/LVSD/all.txt',
#                     'data/app9922/ehr_case/MI/all.txt',
#                     'data/app9922/ehr_case/HTN/all.txt',
#                     'data/app9922/ehr_case/DM/all.txt',
#                     'data/app9922/ehr_case/congHD/all.txt',
#                     'data/app9922/ehr_case/valveHD/all.txt')
# )
# output <- "results/query/hermes2/ehr.1.yaml"
# params <- list(case_code = 1, noncase_code = 0)


# parsing yaml file
yaml <- read_yaml(input$yaml)
eid_id <- glue("eid{yaml$app}")

make_df <- function(file){
  pheno <- dirname(file) %>% basename
  tibble(name = pheno, !!eid_id := read_lines(file))
}

# pheno <- map(input$case_list, ~dirname(.x) %>% basename)

df_pheno <- map_df(input$case_list, make_df)

df_composite <- tibble(pheno = yaml$composite_pheno) %>% 
  unnest_longer(pheno, indices_to = "composite_pheno") %>% 
  mutate(data := map2(pheno, composite_pheno,
                      ~filter(df_pheno, name %in% .x) %>% 
                              mutate(name = .y)))

phenos <- yaml$pheno %>% map_if(is.list,names) %>% as.character

df_all <- df_pheno %>% 
  filter(name %in% phenos) %>% 
  bind_rows(c(list(.), df_composite$data)) %>% 
  group_by(name) %>% 
  distinct(across()) %>% 
  mutate(value = params$case_code) %>% 
  pivot_wider()

vroom::vroom_write(df_all, snakemake@output[[1]])
