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
west <- c("AZ", "CO", "NV", "WY", "CA", "ID", "WA", "OR", "NM", "MT", "UT")
west.plus <- append(west, c("TX", "OK"))
states <- st_read("../data/boundaries/political/TIGER/tl19_us_states_conus.gpkg")

## Load the ICS-209-PLUS spatial table for CONUS (QC / edited by MC 01-2022)
ics.conus <- st_read("data/spatial/mod/wf-incidents/ics209plus_wf_incidents_spatial_conus_1999to2020_qc.gpkg")
## And the table for the 11 western U.S. states
ics.west <- st_read("data/spatial/mod/wf-incidents/ics209plus_wf_incidents_spatial_west_1999to2020_qc.gpkg")

## FIRED Events and updated QC for some 2020 complex fires (MC 01-2021).
fired.conus <- st_read("../fired/data/events/mod/fired_events_conus_to2020_qc.gpkg") %>%
 dplyr::select(-fid_) %>%
 mutate_at(vars(contains("_date")), as.Date, "%Y-%m-%d") %>%
 st_transform(st_crs(ics.conus))

