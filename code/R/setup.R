###################
# Libraries
library(tidyverse)
library(sf)
library(lubridate)
library(scales)
library(ggpubr)
library(grid)
###################

################################
# Set up the parallel processing

library(doParallel)

# Set up the parallel compute
cl = makeCluster(parallel::detectCores()-1, type = "SOCK")
registerDoParallel(cl) 
getDoParWorkers() 

############################

## Environment variables and data
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd("../../")

# Define the MODIS projection
modis.prj = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m"
# Define Lambert projection
lambert.prj <- "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs"
# Default WGS projection
wgs.prj <- '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'

# STUSPS for 11 Western US states
west <- c("AZ", "CO", "NV", "WY", "CA", "ID", "WA", "OR", "NM", "MT", "UT")
# Bring in state boundaries (w/Alaska)
states <- st_read("../data/boundaries/political/TIGER/tl19_us_states_w_ak_lambert.gpkg") %>%
 st_transform(st_crs(wgs.prj))

# Load the latest ICS-209-PLUS raw tables
ics209 <- read_csv("data/tabular/raw/wf-incidents/ics209-plus_v2.0/ics209plus-wildfire/ics209-plus-wf_incidents_1999to2020.csv")

# Convert to spatial points
ics.pts <- ics209 %>%
 rename(GID='...1') %>%
 mutate(POO_LONGITUDE = if_else(is.na(POO_LONGITUDE), LRGST_FOD_LONGITUDE, POO_LONGITUDE),
        POO_LATITUDE = if_else(is.na(POO_LATITUDE), LRGST_FOD_LATITUDE, POO_LATITUDE)) %>%
 # remove rows with missing coordinates
 filter(!is.na(POO_LONGITUDE)) %>%
 # convert to spatial object
 st_as_sf(coords=c("POO_LONGITUDE", "POO_LATITUDE"),crs=wgs.prj) %>%
 # retain coordinates
 mutate(POO_LONGITUDE = unlist(map(.$geometry,1)),
        POO_LATITUDE = unlist(map(.$geometry,2))) %>%
 # ensure transformation to WGS
 st_set_crs(st_crs(wgs.prj)) %>%
 # perform an intersection with our states (removing invalid points)
 st_intersection(., states%>%dplyr::select(STUSPS)) %>%
 # cast to point
 st_cast("POINT") %>%
 st_transform(st_crs(lambert.prj))
# Write
st_write(ics.pts,
         "data/spatial/raw/wf-incidents/ics-209-plus-2.0/ics209plus-wf_incidents_spatial_us_ak_1999to2020.gpkg",
         driver="GPKG",delete_dsn=T)


# # Situation Reports
# sitreps <- read_csv("data/tabular/raw/wf-incidents/ics209-plus_v2.0/ics209plus-wildfire/ics209-plus-wf_sitreps_1999to2020.csv")

# Load the latest FIRED data (manually QC'd)
events <- st_read("../FIRED/data/spatial/mod/event-updates/conus-ak_to2022_events_qc.gpkg") %>%
 dplyr::select(-c(x,y)) %>%
 distinct(., id, .keep_all=TRUE)

# MTBS data, filter to time-period of FIRED (2001-2020)
# tidy data fields
mtbs <- st_read("../data/mtbs/mtbs_perimeter_data/mtbs_perims_conus_ak.gpkg") %>%
 rename(MTBS_Ig_Date = Ig_Date) %>%
 mutate(MTBS_Ig_Year = lubridate::year(MTBS_Ig_Date),
        MTBS_Ig_Month = lubridate::month(MTBS_Ig_Date)) %>%
 filter(MTBS_Ig_Year >= 2001) %>%
 select(Event_ID, Incid_Name, MTBS_Ig_Date, MTBS_Ig_Year, MTBS_Ig_Month, BurnBndAc)

# stop the cluster 
stopCluster(cl)
rm(cl)
gc() # garbage clean
