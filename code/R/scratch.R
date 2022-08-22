library(tidyverse)
library(sf)

# Environment variables and data
setwd("C:/Users/mccoo/OneDrive/mcook/")
# Load the ICS-209-PLUS spatial points
ics <- st_read("ics209/data/spatial/ics209plus_conus_incidents_spatial_1999to2020.gpkg") %>%
  mutate(START_MONTH = as.factor(month(DISCOVERY_DATE, label=T)),
         START_WEEK = as.factor(week(DISCOVERY_DATE)),
         MTBS_ID = gsub("\\s*\\([^\\)]+\\)","", LRGST_MTBS_FIRE_INFO),
         MTBS_FIRE_NAME = stringr::str_extract(string = LRGST_MTBS_FIRE_INFO,
                                               pattern = "(?<=\\().*(?=\\))"))
# West-wide
ics.west <- read.csv("ics209/data/ics209plus_wf_incidents_west_1999to2020.csv")
# Load the joined database
ics.fired <- st_read("ics209/data/ics-fired/ics-fired_conus_2001to2020.gpkg")
# Load the FIRED database
fired <- st_read("fired/data/update0621/fired_events_conus_to2021091.gpkg") %>%
  st_transform(st_crs(ics))
# isolate joined records in the west
ics.fired.west <- ics.fired %>% filter(inci_id %in% ics.west$INCIDENT_ID)

# Get some summary statistics
sum(ics.fired.west$st_des_res)
sum(ics.west$STR_DESTROYED_RES_TOTAL)
sum(ics.fired.west$st_des_res)/sum(ics.west$STR_DESTROYED_RES_TOTAL)*100
# 81.6% of residential structures destroyed are captured in the join
# find the remaining percentage and export
no.join <- ics.west %>% filter(!INCIDENT_ID %in% ics.fired.west$inci_id)
# check
sum(no.join$STR_DESTROYED_RES_TOTAL)
sum(no.join$STR_DESTROYED_RES_TOTAL)/sum(ics.west$STR_DESTROYED_RES_TOTAL)*100
# export
hm_des <- no.join %>% filter(STR_DESTROYED_RES_TOTAL >= 1)
write.csv(hm_des, "ics209/data/no_join_west.csv")
