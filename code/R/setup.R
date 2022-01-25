# Libraries
library(tidyverse)
library(sf)
library(lubridate)
library(scales)
library(ggpubr)
library(grid)
#################################
## Environment variables and data
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd("../../")

## Bring in some boundary data
states <- st_read("../data/boundaries/political/TIGER/tl19_us_states_conus.gpkg")
west <- c("AZ", "CO", "NV", "WY", "CA", "ID", "WA", "OR", "NM", "MT", "UT")
states.west <- states %>% filter(STUSPS %in% west)
counties <- st_read("../data/boundaries/political/TIGER/tl19_us_counties_west.gpkg")
ecoregions <- st_read("../data/boundaries/ecological/ecoregion/na_cec_eco_l3_conus.gpkg")
gaccs <- st_read("../data/boundaries/political/GACC/natl_gacc_albers_conus.gpkg")

## Load the ICS-209-PLUS spatial table for CONUS (QC / edited by MC 01-2022)
ics.conus <- st_read("data/spatial/mod/wf-incidents/ics209plus_wf_incidents_spatial_conus_1999to2020_qc.gpkg")
# og.conus <- st_read("data/spatial/raw/wf-incidents/ics209plus_wf_incidents_spatial_conus_1999to2020.gpkg")

## West-Wide ICS-209-PLUS spatial (QC / edited by MC 01-2022)
ics.west <- st_read("data/spatial/mod/wf-incidents/ics209plus_wf_incidents_spatial_west_1999to2020_qc.gpkg")
# og.west <- st_read("data/spatial/raw/wf-incidents/ics209plus_wf_incidents_spatial_west_1999to2020.gpkg")

## FIRED Events and updated QC for some 2020 complex fires (MC 01-2021).
fired.conus <- st_read("../fired/data/events/mod/fired_events_conus_to2020_qc.gpkg") %>%
 dplyr::select(-fid_) %>%
 mutate_at(vars(contains("_date")), as.Date, "%Y-%m-%d") %>%
 st_transform(st_crs(ics.conus))

