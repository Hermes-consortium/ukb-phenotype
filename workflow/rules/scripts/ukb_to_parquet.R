# script to put data into sqlite database

pacman::p_load(arrow, tidyverse, purrr, vroom, fs, glue)

list_file <- snakemake@input
dir_out <- snakemake@output[[1]]
app_id <- snakemake@wildcards[['app_id']]

# for testing=============
list_file <-
  paste0("data/app15422/by_type/ukb42306_",
         c("str", "int", "float", "multiCat", "singleCat"),
         "_long.csv") %>%
  as.list

dir_out <- "data/app=15422/set=ukb42306"
app_id <- 15422
# ========================

types <- map_chr(list_file, basename) %>% str_split("_") %>% map_chr(`[`, 2)

r_types <- case_when(types %in% c("int", "singleCat", "multiCat", "eid") ~ "i",
                    types %in% c("str") ~ "c",
                    types == "float" ~ "d") %>% set_names(types)

new_eid <- paste0("eid", app_id)

list_df <- map2(list_file, r_types,
                ~vroom(.x, col_types=c(.default = "i", udi = "c", value = .y)) %>%
                  rename(!!rlang::quo_name(new_eid) := eid)) %>%
  set_names(types)

list_df <- map(list_df, ~.x %>% group_by(field) %>% nest())

df_to_parquet <- function(field_id, data){
  subset <- select(data, !!new_eid, udi, value)
  
  arrow_type <-
    if (type %in% c("int", "singleCat", "multiCat", "eid")) {
      int16()
    } else if (type %in% c("str")) {
      string()
    } else if (type == "float") {
      float()
    }

  args <- list(int16(), string(), arrow_type) %>%
    set_names(new_eid, "udi", "value")

  df_schema <- exec(schema, !!!args)
  
  table <- Table$create(subset, schema = df_schema)
  outfile <- path(dir_out, glue("field={field_id}"), "data.parquet")
  dir_create(dirname(outfile))
  write_parquet(subset, outfile)
}

walk(list_df, ~walk2(.x[["field"]], .x[["data"]], df_to_parquet))

