# LIBRARIES

library(tidyverse)

# Read in the GRIDMET summaries, tidy
getwd()
## States
vpd.st <- read_csv("data/tabular/mod/gridmet/gridmet_west_states_may_sept_avg_vpd.csv")
vpd.st %>% select(NAME, STUSPS, contains("_vpd")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_west_states_may_sept_avg_vpd_.csv")

fm1000.st <- read_csv("data/tabular/mod/gridmet/gridmet_west_states_may_sept_avg_fm1000.csv")
fm1000.st %>% select(NAME, STUSPS, contains("_fm1000")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_west_states_may_sept_avg_fm1000_.csv")

fm1000min.st <- read_csv("data/tabular/mod/gridmet/gridmet_west_states_may_sept_min_fm1000.csv")
fm1000min.st %>% select(NAME, STUSPS, contains("_fm1000")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_west_states_may_sept_min_fm1000_.csv")


## Ecoregions (Level I)
vpd.eco1 <- read_csv("data/tabular/mod/gridmet/gridmet_west_ecol1_may_sept_avg_vpd.csv")
vpd.eco1 %>% select(NA_L1NAME, NA_L1CODE, contains("_vpd")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_west_ecol1_may_sept_avg_vpd_.csv")

fm1000.eco1 <- read_csv("data/tabular/mod/gridmet/gridmet_west_ecol1_may_sept_avg_fm1000.csv")
fm1000.eco1 %>% select(NA_L1NAME, NA_L1CODE, contains("_fm1000")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_west_ecol1_may_sept_avg_fm1000_.csv")

fm1000min.eco1 <- read_csv("data/tabular/mod/gridmet/gridmet_west_ecol1_may_sept_min_fm1000.csv")
fm1000min.eco1 %>% select(NA_L1NAME, NA_L1CODE, contains("_fm1000")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_west_ecol1_may_sept_min_fm1000_.csv")


## Ecoregions (Level II)
vpd.eco2 <- read_csv("data/tabular/mod/gridmet/gridmet_west_ecol2_may_sept_avg_vpd.csv")
vpd.eco2 %>% select(NA_L2NAME, NA_L2CODE, contains("_vpd")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_west_ecol2_may_sept_avg_vpd_.csv")

fm1000.eco2 <- read_csv("data/tabular/mod/gridmet/gridmet_west_ecol2_may_sept_avg_fm1000.csv")
fm1000.eco2 %>% select(NA_L2NAME, NA_L2CODE, contains("_fm1000")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_west_ecol2_may_sept_avg_fm1000_.csv")

fm1000min.eco2 <- read_csv("data/tabular/mod/gridmet/gridmet_west_ecol2_may_sept_min_fm1000.csv")
fm1000min.eco2 %>% select(NA_L2NAME, NA_L2CODE, contains("_fm1000")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_west_ecol2_may_sept_min_fm1000_.csv")


## Ecoregions (Level III)
vpd.eco3 <- read_csv("data/tabular/mod/gridmet/gridmet_west_ecol3_may_sept_avg_vpd.csv")
vpd.eco3 %>% select(NA_L3NAME, NA_L3CODE, contains("_vpd")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_west_ecol3_may_sept_avg_vpd_.csv")

fm1000.eco3 <- read_csv("data/tabular/mod/gridmet/gridmet_west_ecol3_may_sept_avg_fm1000.csv")
fm1000.eco3 %>% select(NA_L3NAME, NA_L3CODE, contains("_fm1000")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_west_ecol3_may_sept_avg_fm1000_.csv")

fm1000min.eco3 <- read_csv("data/tabular/mod/gridmet/gridmet_west_ecol3_may_sept_min_fm1000.csv")
fm1000min.eco3 %>% select(NA_L3NAME, NA_L3CODE, contains("_fm1000")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_west_ecol3_may_sept_min_fm1000_.csv")

## GACC
vpd.gacc <- read_csv("data/tabular/mod/gridmet/gridmet_natl_gaccs_may_sept_avg_vpd.csv")
vpd.gacc %>% select(GACCName, GACCUnitID, GACCAbbrev, contains("_vpd")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_natl_gaccs_may_sept_avg_vpd_.csv") 

fm1000.gacc <- read_csv("data/tabular/mod/gridmet/gridmet_natl_gaccs_may_sept_avg_fm1000.csv")
fm1000.gacc %>% select(GACCName, GACCUnitID, GACCAbbrev, contains("_fm1000")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_natl_gaccs_may_sept_avg_fm1000_.csv")

fm1000min.gacc <- read_csv("data/tabular/mod/gridmet/gridmet_natl_gaccs_may_sept_min_fm1000.csv")
fm1000.gacc %>% select(GACCName, GACCUnitID, GACCAbbrev, contains("_fm1000")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_natl_gaccs_may_sept_min_fm1000_.csv")


## WEST
vpd.west <- read_csv("data/tabular/mod/gridmet/gridmet_west_west_may_sept_avg_vpd.csv")
tmp <- vpd.west %>% select("system:index", contains("_vpd")) %>%
        rename(REGION = "system:index") %>%
        mutate(REGION = "WEST_WIDE") %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_natl_gaccs_may_sept_avg_vpd_.csv") 

fm1000.west <- read_csv("data/tabular/mod/gridmet/gridmet_natl_gaccs_may_sept_avg_fm1000.csv")
fm1000.west %>% select(GACCName, GACCUnitID, GACCAbbrev, contains("_fm1000")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_natl_gaccs_may_sept_avg_fm1000_.csv")

fm1000min.west <- read_csv("data/tabular/mod/gridmet/gridmet_natl_gaccs_may_sept_min_fm1000.csv")
fm1000.west %>% select(GACCName, GACCUnitID, GACCAbbrev, contains("_fm1000")) %>%
        write.csv(., "data/tabular/mod/gridmet/gridmet_natl_gaccs_may_sept_min_fm1000_.csv")
