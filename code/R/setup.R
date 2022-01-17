# Libraries
library(tidyverse)
library(sf)
library(lubridate)
library(scales)
library(ggpubr)
library(grid)
################################
# Environment variables and data
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd("../../../")

# Load the ICS-209-PLUS spatial points for CONUS
ics <- st_read("ics209plus/data/spatial/raw/ics209plus_wf_incidents_spatial_conus_1999to2020.gpkg") %>%
  mutate(START_MONTH = as.factor(month(DISCOVERY_DATE, label=T)),
         START_WEEK = as.factor(week(DISCOVERY_DATE)),
         MTBS_ID = gsub("\\s*\\([^\\)]+\\)","", LRGST_MTBS_FIRE_INFO),
         MTBS_FIRE_NAME = stringr::str_extract(string = LRGST_MTBS_FIRE_INFO,
                                               pattern = "(?<=\\().*(?=\\))"))

# western U.S. (edited)
ics.west <- st_read("ics209plus/data/spatial/mod/ics209plus_wf_incidents_spatial_west_1999to2020_qc.gpkg")

# Load the previous joined database
ics.fired <- st_read("ics209plus/data/spatial/mod/ics-fired_conus_2001to2020.gpkg")

# Load the FIRED database, updated manually
fired.west <- st_read("fired/data/fired_events_west_to2020.gpkg") %>%
  st_transform(st_crs(ics))
# convex hull area
hull <- read.csv("fired/data/fired_events_hull.csv") %>%
  mutate(hull_km2 = area*1e-6) %>%
  dplyr::select(id, hull_km2)
# add convex hull as attribute
fired.west <- fired.west %>%
  mutate(ig_date = as.Date(ig_date, "%Y-%m-%d")) %>% 
  filter(ig_year>=2001, ig_year<=2020) %>%
  inner_join(., hull, by="id")
rm(hull)

# MTBS perimeters
mtbs <- st_read("data/mtbs/mtbs_perims_DD/mtbs_perims_DD.gpkg") %>% 
  mutate(
    MTBS_DATE = as.Date(Ig_Date, "%Y-%m-%d"),
    MTBS_YEAR = format(Ig_Date, format = "%Y"),
    MTBS_ID = Event_ID,
    MTBS_FIRE_NAME = Incid_Name,
    MTBS_ACRES = BurnBndAc) %>% 
  dplyr::select(MTBS_DATE, MTBS_YEAR, MTBS_FIRE_NAME, MTBS_ID, MTBS_ACRES) %>%
  st_transform(st_crs(ics))

# Boundary data
states <- st_read("data/boundaries/political/TIGER/tl_2019_conus_state_albers.gpkg") %>%
  st_transform(st_crs(ics))
ics <- st_intersection(ics, states%>%dplyr::select(STUSPS))

# Subset to western states
# Western states list
west <- c("AZ", "CO", "NV", "WY", "CA", "ID", "WA", "OR", "NM", "MT", "UT")
west <- states %>% 
  filter(STUSPS %in% west) %>% 
  dplyr::select(STUSPS)
# Geographic Area Coordination Centers (GACC)
gacc <- st_read("data/boundaries/political/GACC/natl_gacc_albers_conus.gpkg") %>%
  dplyr::select("GACCAbbrev", "GACCName") %>%
  st_transform(st_crs(ics))
# ecoregion
eco <- st_read("data/boundaries/ecological/na_cec_eco_l3_conus.gpkg") %>%
  dplyr::select(NA_L3CODE, NA_L3NAME, NA_L2CODE, NA_L2NAME, NA_L1CODE, NA_L1NAME) %>%
  st_transform(st_crs(ics)) 

# join spatial records to their GACC, ecoregion, and subset to western states
ics <- st_join(ics, gacc, join=st_intersects)
ics <- st_join(ics, eco, join=st_intersects)
ics.west.og <- st_intersection(ics, west)
# Filter and merge back with updated western U.S. 
ics <- ics %>%
  mutate(LRGST_FOD_ID = as.character(LRGST_FOD_ID),
         START_WEEK = as.integer(START_WEEK),
         NA_L2CODE = as.double(NA_L2CODE),
         NA_L1CODE = as.double(NA_L1CODE)) %>%
  filter(!STUSPS %in% ics.west$STUSPS) %>%
  bind_rows(., ics.west%>%mutate(COMPLEX = as.character(COMPLEX),
                                 LL_UPDATE = as.character(LL_UPDATE),
                                 EVACUATION_REPORTED = as.character(EVACUATION_REPORTED),
                                 LRGST_FOD_ID = as.character(LRGST_FOD_ID)))
# Create subset also with TX and OK
west.plus <- c("AZ", "CO", "NV", "WY", "CA", "ID", "WA", "OR", "NM", "MT", "UT", "TX", "OK")
west.plus <- states %>% filter(STUSPS %in% west.plus) %>% 
  st_transform(., st_crs(ics)) %>%
  dplyr::select(STUSPS)
ics.west.plus <- st_intersection(ics, west.plus)
################################################################################################


################################################################################################
# Functions
# Compare attributes function
compare_attr <- function(ftr){
  ftr <- ftr %>% mutate(
    daydiff = (abs(as.numeric(difftime(DISCOVERY_DATE, ig_date, units = "days")))),
    # Create a percent difference in acres from 209s
    sizediff = (abs(tot_ar_km2 - FINAL_KM2)/
                  ((tot_ar_km2 + FINAL_KM2)/2))*100,
    hulldiff = (abs(hull_km2 - FINAL_KM2)/
                  ((hull_km2 + FINAL_KM2)/2))*100,
    lowdiff = if_else(sizediff < hulldiff, sizediff, hulldiff)) %>%
    filter(
      daydiff < 32 & lowdiff < 50
    )
  return(ftr)
}