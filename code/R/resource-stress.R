library(tidyverse)
library(lubridate)
library(sf)
setwd("C:/Users/mccoo/OneDrive/mcook/")

# load the ics-209s
ics <- st_read("ics209/data/spatial/ics209plus_conus_incidents_spatial_1999to2020.gpkg") %>%
  mutate(START_MONTH = as.factor(month(DISCOVERY_DATE, label=T)),
         START_WEEK = as.factor(week(DISCOVERY_DATE)))
glimpse(ics)
# West-wide, with HISDAC-US summary for POO buffer
ics.west <- read.csv("ics209/data/table/ics209plus_wf_incidents_west_1999to2020.csv") %>%
  mutate(INCIDENT_ID = X,
         GROUP = "ICS-209-PLUS",
         STUSPS = NA_L1NAME) # need to fix this

# Load ICS-FIRED
ics.fired <- st_read("ics209/data/ics-fired/ics-fired_conus_2001to2020.gpkg") %>%
  mutate(INCIDENT_ID = inci_id,
         GROUP = "ICS-FIRED")
model <- st_read("home-loss/data/ics-fired_model_data.gpkg") %>% 
  filter(lc_name != "Croplands",
         lc_name != "Urban and Built-Up Lands")

# Plot the number of structures exposed by POO buffer and by FIRED overlay
str <- ics.west %>% 
  dplyr::select(INCIDENT_ID, STUSPS, FINAL_KM2, BUPR_SUM, GROUP) %>%
  inner_join(
    ., ics.fired %>%as_tibble() %>%
      dplyr::select(INCIDENT_ID, ig_year, tot_ar_km2, bupr_sum),
    by="INCIDENT_ID") %>%
  filter(!is.na(tot_ar_km2))
glimpse(str)
# Plot it by group
str %>% group_by(STUSPS) %>%
  summarize(BUPR_SUM_POO = sum(BUPR_SUM),
            BUPR_SUM_FIRED = sum(bupr_sum)) %>%
  mutate(BUPR_DIFF = BUPR_SUM_FIRED - BUPR_SUM_POO) %>%
  ggplot(data=.)+
  geom_bar(aes(x=reorder(STUSPS, BUPR_DIFF), y=BUPR_DIFF), stat="identity")
# t test
t.test(str$BUPR_SUM, y = str$bupr_sum, alternative = c("two.sided", "less", "greater"), mu = 0, 
       paired = FALSE, var.equal = FALSE, conf.level = 0.95)
# summary
summary(str$BUPR_SUM)
summary(str$bupr_sum)
# Western states list
west <- c("AZ", "CO", "NV", "WY", "CA", "ID", "WA", "OR", "NM", "MT", "UT")

# load some boundary data
# states
states <- st_read("data/boundaries/political/TIGER/tl_2019_conus_state_albers.gpkg")
west <- states %>% 
  filter(STUSPS %in% west) %>% 
  st_transform(., st_crs(ics)) %>%
  dplyr::select(STUSPS)
# Geographic Area Coordination Centers (GACC)
gacc <- st_read("data/boundaries/political/GACC/natl_gacc_albers_conus.gpkg") %>%
  dplyr::select("GACCAbbrev", "GACCName")
# ecoregion
eco <- st_read("data/boundaries/ecological/na_cec_eco_l3_conus.gpkg") %>%
  dplyr::select(NA_L3CODE, NA_L3NAME, NA_L2CODE, NA_L2NAME, NA_L1CODE, NA_L1NAME) %>%
  st_transform(st_crs(ics))

# join spatial records to their GACC, ecoregion, and subset to western states
ics <- st_join(ics, gacc, join=st_intersects)
ics <- st_join(ics, eco, join=st_intersects)
ics.west <- st_intersection(ics, west)

# Handle erroneous FINAL_ACRE attributes (where FINAL_ACRES = 0)
dim(ics.west %>% filter(FINAL_ACRES == 0))
zeros <- ics.west %>% 
  filter(FINAL_ACRES == 0) %>%
  mutate(FOD_FINAL_ACRES = if_else(is.na(FOD_FINAL_ACRES), FINAL_ACRES, FOD_FINAL_ACRES),
         FINAL_ACRES = if_else(FINAL_ACRES < FOD_FINAL_ACRES, FOD_FINAL_ACRES, FINAL_ACRES))
ics.west <- ics.west %>% 
  filter(FINAL_ACRES != 0) %>%
  bind_rows(., zeros)

# # write to file
# st_write(ics.west, "ics209/data/ics209plus_wf_incidents_west_1999to2020.gpkg",
#          layer = "ics209plus_wf_incidents_west_1999to2020", append = FALSE, quiet = T)
# write.csv(ics.west%>%as_tibble(), "ics209/data/ics209plus_wf_incidents_west_1999to2020.csv")

# Read in the BUPR summaries
sums <- st_read("ics209/data/wf_incidents_west_bupr_summaries/wf_incidents_west_bupr_summaries.gpkg") %>%
  as_tibble() %>%
  dplyr::select(INCIDENT_I, RADII, BUPR_SUM, BUPR_MAX, BUPR_MEDIA, pct10, pct90) %>%
  rename(INCIDENT_ID = INCIDENT_I,
         BUPR_MEDIAN = BUPR_MEDIA,
         BUPR_PCT10 = pct10,
         BUPR_PCT90 = pct90)
# Join back to the west-wide ICS-209-PLUS
ics.west <- inner_join(ics.west %>% as_tibble(), sums, by="INCIDENT_ID") %>% st_as_sf()
# write to file
st_write(ics.west, "ics209/data/ics209plus_wf_incidents_west_1999to2020.gpkg",
         layer = "ics209plus_wf_incidents_west_1999to2020", append = FALSE, quiet = T)
write.csv(ics.west%>%as_tibble(), "ics209/data/ics209plus_wf_incidents_west_1999to2020.csv")

# Create a simple map
ggplot() +
  geom_sf(data=ics.west, color="gray40", size=0.2, alpha=0.8) +
  geom_sf(data=west, fill=NA, color="gray20", size=0.8) +
  theme_minimal()

# groups
annual <- ics.west %>% group_by(START_YEAR)
month <- ics.west %>% group_by(START_MONTH)
week <- ics.west %>% group_by(START_WEEK)

# Plot stacked area chart
month <- ics.west %>% group_by(START_MONTH) %>%
  summarize(
    N = n()
  ) %>% ungroup() 
