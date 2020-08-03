# Querying phenotype from yaml file

pacman::p_load(tidyverse, yaml, purrr, DBI, RSQLite, glue, vroom, fs, rlang)

input <- snakemake@input

# For testing
# input <- list(
#   yaml = "data/query/hermes2.yaml"
# )

# parsing yaml file
yaml <- read_yaml(input$yaml) %>% 
  as_tibble %>% 
  unnest_wider(data)

# ---------------------------
# QUERY main UKB data
# ---------------------------
get_udi <- function(df_query, df_profile){
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
  
  return(bind_rows(df1, df2, df3) %>% distinct(UDI, name_udi, db))
}

fetch_ukb <- function(app_id, dataset, df_query){
  db <- path("data", glue("app{app_id}"), glue("{dataset}.db"))
  con <- dbConnect(SQLite(), db)
  
  eid_col <- quo_name(glue("eid{app_id}"))
  
  list_df <- df_query %>% 
    group_by(db) %>%
    nest() %>% 
    mutate(query_results =
             map2(db, data, {~tbl(con, .x) %>% 
                 filter(udi %in% local(.y[["UDI"]])) %>% 
                 collect}))
  
  map_name <- df_query$name_udi %>% 
    set_names(df_query$UDI)
  
  df_all <- map(list_df$query_results,
                pivot_wider,
                id_cols = !!eid_col,
                names_from = udi) %>% 
    reduce(full_join, by = eid_col) %>% 
    rename_with(~recode(.x, !!!map_name))
  
  return(df_all)  
}



data_ukb <- yaml %>% 
  filter(str_detect(dataset, "^ukb")) %>%
  unnest_longer(pheno) %>% 
  mutate(value = map_chr(pheno, pluck, 1),
         name = map_if(pheno, is.list, names) %>% as.character) %>% 
  separate(value, into = c("field", "instance", "array"),
           sep = "[-\\.]", remove=F, fill = "right") %>% 
  mutate(across(c(instance,array), as.numeric)) %>% 
  select(-pheno) %>% 
  nest(df_query = value:name) %>% 
  mutate(path = path("data",
                     paste0("app", app),
                     paste0(dataset, ".profile.tsv")),
         df_profile = map(path, vroom),
         df_query = map2(df_query, df_profile, get_udi),
         result = pmap(list(app, dataset, df_query), fetch_ukb))

read_sample <- function(app_id, sample_file){
  vroom(sample_file, skip =2, col_select = 1,
        col_names = glue("eid{app_id}"))
}

# ---------------------
# QUERY EHR data
# ---------------------

tables <- c("hesin_diag", "hesin_oper", "death_cause")

table_map <- tibble(
  Source = c("ICD9", "ICD10", "OPCS4"),
  data = list(
    tibble(table = c("hesin_diag"),
           col = c("diag_icd9")),
    tibble(table = c("hesin_diag", "death_cause"),
           col = c("diag_icd10", "cause_icd10")),
    tibble(table = c("hesin_oper"),
           col = c("oper4"))
  )) %>% 
  unnest(data)

query_ehr <- function(tables, condition, app_id, db){
  con_ehr <- dbConnect(SQLite(), db)
  
  eid_id <- glue("eid{app_id}")
  
  join_clause <-
    map2_chr(tables[1], tables[-1],
             ~glue("INNER JOIN {.y} ON {.x}.{eid_id}={.y}.{eid_id}")) %>% 
    glue_collapse(sep = "\n")
  
  query <- glue("
    SELECT *
    FROM {tables[1]}
    {join_clause}
    WHERE {condition}
    GROUP BY {tables[1]}.{eid_id}
  ")
  
  df_res <- dbGetQuery(con_ehr, query) %>% 
    as_tibble
  
  dbDisconnect(con_ehr)
  df_res
}


fetch_ehr <- function(app_id, pheno, tables){
  db_ehr <- path("data", glue("app{app_id}"), "ehr.db")

  data_ehr <- vroom(path("data", "code_list", glue("{pheno}.tsv"))) %>% 
    inner_join(table_map %>% filter(table %in% tables)) %>% 
    mutate(search = paste(col, "LIKE",
                          Code %>% str_remove_all("[\\.\\*]") %>%
                            glue("'{code}%'", code = .))) %>% 
    group_by(table) %>% 
    nest() %>% 
    mutate(condition = map_chr(data, ~glue_collapse(.$search, " OR ")),
           result = map2(table, condition, query_ehr, app_id, db_ehr))
  
  df_ehr <- code_list %>% 
    mutate(tbl(con_ehr, table))
  
}


data_ehr <- yaml %>% 
  filter(dataset == "ehr") %>% 
  unnest_longer(pheno) %>% 
  mutate(tables = map(pheno, ~if (is.list(.)) unlist(.) else tables),
         pheno = map_chr(pheno, ~if (is.list(.)) names(.) else .))


get_pheno_ehr <- function(app_id, pheno){
  codes <- path("data", glue("app{app_id}"), )
}



read_sample(app_id, data_ukb$sample_file)
data_ukb %>%
  mutate(sample)
  
  unnest(cols = c(df_query)) %>% 
  select(-path, -df_profile) %>% 
  group_by(app,dataset,sample_file, db) %>% 
  nest()
  mutate(sql_query = pmap(list(app, dataset, db, map(data, `[[`, "UDI")),paste))




list_db <- list(
  "data/app9922/ehr.db",
  "data/app15422/ukb42306.db"
)

names(list_db) <- map_chr(list_db, ~basename(.) %>% str_remove_all("\\..*"))

list_con <- map(list_db, ~dbConnect(SQLite(), .))




for (db in list_db[[-1]]) {
  name <- basename(db) %>% str_remove_all("\\..*")
  query <- glue_sql("ATTACH DATABASE {`db`} AS {`name`}",
                    .con = con)
  dbSendQuery(con, query)
}

dbListConnections()
