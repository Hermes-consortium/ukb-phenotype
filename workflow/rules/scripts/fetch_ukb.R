# Querying phenotype from yaml file

pacman::p_load(tidyverse, yaml, purrr, DBI, RSQLite, vroom, glue)

input <- snakemake@input
output <- snakemake@output[[1]]

# For testing
input <- list(
  yaml = "results/hermes2/2.yaml",
  db = "data/app15422/ukb42306.db",
  profile = "data/app15422/ukb42306.profile.tsv"
)
# output <- "results/query/hermes2/ukb.1.yaml"

# parsing yaml file
yaml <- read_yaml(input$yaml)

df_profile <- vroom(input$profile)
con <- dbConnect(SQLite(), input$db)

# ---------------------------
# QUERY main UKB data
# ---------------------------

df_query <- yaml$pheno %>%
  tibble(value = map_chr(., pluck, 1),
         name = map_if(., is.list, names) %>% as.character) %>% 
  separate(value, into = c("field", "instance", "array"),
           sep = "[-\\.]", remove=F, fill = "right") %>% 
  mutate(across(c(instance,array), as.numeric))

df1 <- df_query %>% 
  filter(is.na(instance) & is.na(array)) %>% 
  inner_join(df_profile %>% select(field, UDI, db),
             by = "field") %>% 
  mutate(name_udi = str_replace(UDI, glue("{field}"), name))

df2 <- df_query %>% 
  filter(!is.na(instance) & is.na(array)) %>% 
  inner_join(df_profile %>% select(field, instance, UDI, db),
             by = c("field", "instance")) %>% 
  mutate(name_udi = str_replace(UDI, glue("{field}-.?"), name))

df3 <- df_query %>% 
  inner_join(df_profile %>% select(field, instance, array, UDI, db),
             by = c("field", "instance", "array")) %>% 
  mutate(name_udi = name)


mapping <- bind_rows(df1, df2, df3) %>%
  distinct(UDI, name_udi, db) %>% 
  group_by(db) %>% 
  nest() %>% 
  mutate(result = map2(db, data,
                       ~tbl(con, .x) %>% 
                         filter(udi %in% local(.y[["UDI"]])) %>% 
                         collect))

eid_col <- rlang::quo_name(glue("eid{yaml$app}"))

map_name <- bind_rows(mapping$data) %>%
  select(UDI, name_udi) %>% 
  deframe

df_all <- mapping$result %>% 
  map(pivot_wider,
      id_cols = !!eid_col,
      names_from = udi) %>% 
  reduce(full_join, by = eid_col) %>% 
  rename_with(~recode(.x, !!!map_name))

vroom_write(df_all, output)

dbDisconnect(con)