# HELPER functions to explore data

pacman::p_load(fst, data.table, tidyverse, rvest)

UKB_data_fst <- "/Volumes/groupfolders/FPHS_IHI_DataLab_UKBiobank/application-15422/ukb-phenotype/data/2020-08-06_ukb42306_app15422.fst"

get_data <- function(fields_id, fst_data = UKB_data_fst,
                     as.data.table = T,
                     incl_eid = T, ...){
  cols <- if (incl_eid) c("eid", fields_id) else fields_id 
  
  DT <- read_fst(fst_data, columns = cols,
                 as.data.table = as.data.table,
                 ...)
  if (!is.null(names(fields_id))){
    index <- names(fields_id) != ""
    setnames(DT, fields_id[index], names(fields_id)[index])
  }
}

# UKB data fields
UKB_data_dict_path <- "/Volumes/groupfolders/FPHS_IHI_DataLab_UKBiobank/application-15422/ukb-phenotype/data/ukb42306.html"
UKB_data_dict <- read_html(UKB_data_dict_path)

# create lookup tables
list_data_dict <- html_nodes(UKB_data_dict, "table") %>% .[3:length(.)] %>%
  map(function(node){
    DT <- as.data.table(html_table(node))
    coding_id <- html_attr(node, "summary") %>% str_remove("Coding ") %>% as.numeric
    DT[, coding_id := coding_id]
    select(DT, coding_id, code = Code, meaning = Meaning)
  })

DT_data_dict <- rbindlist(list_data_dict)

# From Spiros' tofu
DT_lookups_coding <- fread("https://github.com/alhenry/tofu/raw/master/tofu/lookups/df_lkp_encodings.csv.gz")
DT_lookup_fields <- fread("https://github.com/alhenry/tofu/raw/master/tofu/lookups/df_lkp_fields.tsv.gz")

# from bjcairns/ukbschemas
library(ukbschemas)
db <- ukbschemas_db(path = tempdir(), overwrite = T)
sch <- load_db(db = db)

# ---------------------------
# DATA TRANSFORMATION
# ---------------------------

# DT_all <- read_fst(UKB_data_fst, as.data.table = T)




