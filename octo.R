
pacman::p_load(reticulate)
pacman::p_load(jsonlite)
pacman::p_load(tidyverse)

setwd('/Users/benwhalley/dev/asynchttp/octopus-api/')
use_python("/Users/benwhalley/Library/Caches/pypoetry/virtualenvs/octopus-api-muCaxvtf-py3.11/bin/python")
# py_config()

octopy <- import("octo")

jobstodo <- jsonlite::read_json("jobs.json")


args <- list(
  rpm=500,
  max_tries=10,
  job_list=jobstodo,
  echo=T
)

webresults <- octopy$run_requests(args)
web_df <- webresults %>% map_dfr( ~as_tibble(.x) %>% .$body %>% map_dfr(~ jsonlite::fromJSON(.x, flatten = T, simplifyDataFrame = T) %>% as_tibble()))

jsonlite::read_json("jobs.json", simplifyVector = T) %>% 
  bind_cols(web_df) %>% 
  unnest(params, names_sep = "_")

