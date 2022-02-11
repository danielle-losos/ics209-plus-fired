# LIBRARIES

library(tidyverse)

# Read in the GRIDMET summaries, tidy

vpd <- read.csv("data/tabular/mod/gridmet/gridmet_states_month_avg_vpd.csv")

fm1000 <- read.csv("data/tabular/mod/gridmet/gridmet_states_month_avg_fm1000.csv")

# Tidy, overwrite

vpd %>% 
 select(NAME, STUSPS, GEOID, contains('_vpd')) %>%
 write.csv(., "data/tabular/mod/gridmet/gridmet_states_month_avg_vpd.csv")

fm1000 %>% 
 select(NAME, STUSPS, GEOID, contains('_fm1000')) %>%
 write.csv(., "data/tabular/mod/gridmet/gridmet_states_month_avg_vpd.csv")
