##################
library(tidyverse)
library(sf)
##################

# Environments
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd("../../")

# Load the latest ICS-209-PLUS raw table
# Incident Summary Reports
incidents <- read_csv(
        "data/tabular/raw/wf-incidents/ics-209-plus-2.0-pre-release-v2/ics209-plus-wf_incidents_1999to2020.csv") %>%
        rename(GID = '...1') %>%
        mutate(INCIDENT_NAME = toupper(INCIDENT_NAME),
               INCIDENT_ID = toupper(INCIDENT_ID))
# Situation Reports
sitreps <- read_csv(
        "data/tabular/raw/wf-incidents/ics-209-plus-2.0-pre-release-v2/ics209-plus-wf_sitreps_1999to2020.csv") %>%
        rename(GID = '...1') %>%
        mutate(INCIDENT_NAME = toupper(INCIDENT_NAME),
               INCIDENT_ID = toupper(INCIDENT_ID))



# Create the spatial version
spatial <- incidents %>%
        # Use the FPA FOD coordinates where possible
        mutate(LATITUDE = if_else(is.na(LRGST_FOD_LATITUDE), POO_LATITUDE, LRGST_FOD_LATITUDE),
               LONGITUDE = if_else(is.na(LRGST_FOD_LONGITUDE), POO_LONGITUDE, LRGST_FOD_LONGITUDE)) %>%
        # set the sf coordinates
        st_as_sf(., coords=c("LONGITUDE", "LATITUDE"), na.fail=FALSE) %>%
        # set the default projections (WGS84)
        st_set_crs(st_crs(4326)) %>%
        # retain the lat/long information
        mutate(LONGITUDE = unlist(map(.$geometry,1)),
               LATITUDE = unlist(map(.$geometry,2))) %>%
        # transform to Albers Equal Area
        st_transform(., crs=st_crs(5070))

# Subset incidents with missing geographic information, write out as table
spatial %>% filter(is.na(LATITUDE)) %>%
        as_tibble() %>%
        write_csv("data/spatial/raw/wf-incidents/pre-release_v2/incidents_no_geo.csv")

# Remove records with no spatial information
spatial <- spatial %>% filter(!is.na(LATITUDE))

# Save the spatial layer
st_write(spatial, "data/spatial/raw/wf-incidents/pre-release_v2/ics209-plus-wf_incidents_spatial_1999to2020.gpkg",
         delete_dsn=TRUE)


## Bring in boundary data to subset incidents, add attributes
west <- c("AZ", "CO", "NV", "WY", "CA", "ID", "WA", "OR", "NM", "MT", "UT")
states <- st_read("../data/boundaries/political/TIGER/tl19_us_states_conus.gpkg") %>%
        st_transform(crs=st_crs(5070)) %>%
        select(STUSPS)
gaccs <- st_read("../data/boundaries/political/GACC/natl_gacc_albers_conus.gpkg") %>%
        st_transform(crs=st_crs(spatial)) %>%
        select(GACCAbbrev)
ecol3 <- st_read("../data/boundaries/ecological/ecoregion/conus_cec_eco.gpkg", layer="eco_l3") %>%
        st_transform(crs=st_crs(spatial)) %>%
        select(NA_L3CODE, NA_L3NAME)
neon <- st_read("../data/boundaries/bioclimatic/neon_domains.gpkg") %>%
        st_transform(crs=st_crs(spatial)) %>%
        select(DomainName) %>% rename(NEON_DOMAIN=DomainName)

## add attributes from the boundary data
spatial.conus <- spatial %>%
        st_join(states) %>%
        st_join(gaccs) %>%
        st_join(ecol3) %>%
        st_join(neon) %>%
        filter(!is.na(STUSPS))

# Save the spatial layer
st_write(spatial.conus,
         "data/spatial/raw/wf-incidents/pre-release_v2/ics209-plus-wf_incidents_spatial_conus_1999to2020.gpkg",
         delete_dsn=TRUE)



# create summary tables of key variables across spatial domains

# states
state.summary <- spatial.conus %>%
        as_tibble() %>%
        group_by(STUSPS) %>%
        summarize(
                FIRE_COUNT = n(),
                BURNED_AREA = sum(FINAL_ACRES, na.rm=T),
                FIRE_SIZE_MEAN = mean(FINAL_ACRES, na.rm=T),
                WF_FSR_MEAN = mean(WF_MAX_FSR, na.rm=T),
                WF_DURATION_MEAN = mean(WF_GROWTH_DURATION, na.rm=T),
                STR_THREAT_TOTAL = sum(STR_THREATENED_MAX, na.rm=T),
                EVAC_TOTAL = sum(PEAK_EVACUATIONS, na.rm=T),
                STR_DAMAGE_TOTAL = sum(STR_DAMAGED_TOTAL, na.rm=T),
                STR_DESTR_TOTAL = sum(STR_DESTROYED_TOTAL, na.rm=T),
                STR_DESTR_RES = sum(STR_DESTROYED_RES_TOTAL, na.rm=T),
                FATALITIES_P = sum(FATALITIES_PUBLIC, na.rm=T),
                FATALITIES_R = sum(FATALITIES_RESPONDER, na.rm=T),
                NUM_SITREPS_TOTAL = sum(INC_MGMT_NUM_SITREPS, na.rm=T),
                PROJ_IM_COSTS = sum(PROJECTED_FINAL_IM_COST, na.rm=T),
                TOTAL_PERSONNEL = sum(TOTAL_PERSONNEL_SUM, na.rm=TRUE),
                TOTAL_AERIAL = sum(TOTAL_AERIAL_SUM, na.rm=T),
                SUP_FS_PERC_MEAN = mean(SUP_PERCENT_FS, na.rm=T)
        ) %>% ungroup() 
# write to table
write_csv(state.summary, "data/tabular/mod/summaries/conus_state_summary_key_vars.csv")

# gaccs
gacc.summary <- spatial.conus %>%
        as_tibble() %>%
        group_by(GACCAbbrev) %>%
        summarize(
                FIRE_COUNT = n(),
                BURNED_AREA = sum(FINAL_ACRES, na.rm=T),
                FIRE_SIZE_MEAN = mean(FINAL_ACRES, na.rm=T),
                WF_FSR_MEAN = mean(WF_MAX_FSR, na.rm=T),
                WF_DURATION_MEAN = mean(WF_GROWTH_DURATION, na.rm=T),
                STR_THREAT_TOTAL = sum(STR_THREATENED_MAX, na.rm=T),
                EVAC_TOTAL = sum(PEAK_EVACUATIONS, na.rm=T),
                STR_DAMAGE_TOTAL = sum(STR_DAMAGED_TOTAL, na.rm=T),
                STR_DESTR_TOTAL = sum(STR_DESTROYED_TOTAL, na.rm=T),
                STR_DESTR_RES = sum(STR_DESTROYED_RES_TOTAL, na.rm=T),
                FATALITIES_P = sum(FATALITIES_PUBLIC, na.rm=T),
                FATALITIES_R = sum(FATALITIES_RESPONDER, na.rm=T),
                NUM_SITREPS_TOTAL = sum(INC_MGMT_NUM_SITREPS, na.rm=T),
                PROJ_IM_COSTS = sum(PROJECTED_FINAL_IM_COST, na.rm=T),
                TOTAL_PERSONNEL = sum(TOTAL_PERSONNEL_SUM, na.rm=TRUE),
                TOTAL_AERIAL = sum(TOTAL_AERIAL_SUM, na.rm=T),
                SUP_FS_PERC_MEAN = mean(SUP_PERCENT_FS, na.rm=T)
        ) %>% ungroup() 
# write to table
write_csv(gacc.summary, "data/tabular/mod/summaries/conus_gacc_summary_key_vars.csv")

# ecoregion level 3
eco.summary <- spatial.conus %>%
        as_tibble() %>%
        group_by(NA_L3CODE) %>%
        summarize(
                FIRE_COUNT = n(),
                BURNED_AREA = sum(FINAL_ACRES, na.rm=T),
                FIRE_SIZE_MEAN = mean(FINAL_ACRES, na.rm=T),
                WF_FSR_MEAN = mean(WF_MAX_FSR, na.rm=T),
                WF_DURATION_MEAN = mean(WF_GROWTH_DURATION, na.rm=T),
                STR_THREAT_TOTAL = sum(STR_THREATENED_MAX, na.rm=T),
                EVAC_TOTAL = sum(PEAK_EVACUATIONS, na.rm=T),
                STR_DAMAGE_TOTAL = sum(STR_DAMAGED_TOTAL, na.rm=T),
                STR_DESTR_TOTAL = sum(STR_DESTROYED_TOTAL, na.rm=T),
                STR_DESTR_RES = sum(STR_DESTROYED_RES_TOTAL, na.rm=T),
                FATALITIES_P = sum(FATALITIES_PUBLIC, na.rm=T),
                FATALITIES_R = sum(FATALITIES_RESPONDER, na.rm=T),
                NUM_SITREPS_TOTAL = sum(INC_MGMT_NUM_SITREPS, na.rm=T),
                PROJ_IM_COSTS = sum(PROJECTED_FINAL_IM_COST, na.rm=T),
                TOTAL_PERSONNEL = sum(TOTAL_PERSONNEL_SUM, na.rm=TRUE),
                TOTAL_AERIAL = sum(TOTAL_AERIAL_SUM, na.rm=T),
                SUP_FS_PERC_MEAN = mean(SUP_PERCENT_FS, na.rm=T)
        ) %>% ungroup() 
# write to table
write_csv(eco.summary, "data/tabular/mod/summaries/conus_ecol3_summary_key_vars.csv")

# neon domains
neon.summary <- spatial.conus %>%
        as_tibble() %>%
        group_by(NEON_DOMAIN) %>%
        summarize(
                FIRE_COUNT = n(),
                BURNED_AREA = sum(FINAL_ACRES, na.rm=T),
                FIRE_SIZE_MEAN = mean(FINAL_ACRES, na.rm=T),
                WF_FSR_MEAN = mean(WF_MAX_FSR, na.rm=T),
                WF_DURATION_MEAN = mean(WF_GROWTH_DURATION, na.rm=T),
                STR_THREAT_TOTAL = sum(STR_THREATENED_MAX, na.rm=T),
                EVAC_TOTAL = sum(PEAK_EVACUATIONS, na.rm=T),
                STR_DAMAGE_TOTAL = sum(STR_DAMAGED_TOTAL, na.rm=T),
                STR_DESTR_TOTAL = sum(STR_DESTROYED_TOTAL, na.rm=T),
                STR_DESTR_RES = sum(STR_DESTROYED_RES_TOTAL, na.rm=T),
                FATALITIES_P = sum(FATALITIES_PUBLIC, na.rm=T),
                FATALITIES_R = sum(FATALITIES_RESPONDER, na.rm=T),
                NUM_SITREPS_TOTAL = sum(INC_MGMT_NUM_SITREPS, na.rm=T),
                PROJ_IM_COSTS = sum(PROJECTED_FINAL_IM_COST, na.rm=T),
                TOTAL_PERSONNEL = sum(TOTAL_PERSONNEL_SUM, na.rm=TRUE),
                TOTAL_AERIAL = sum(TOTAL_AERIAL_SUM, na.rm=T),
                SUP_FS_PERC_MEAN = mean(SUP_PERCENT_FS, na.rm=T)
        ) %>% ungroup() 
# write to table
write_csv(neon.summary, "data/tabular/mod/summaries/conus_neon_summary_key_vars.csv")

# clean up
rm(states, gaccs, ecol3, neon, state.summary, gacc.summary, eco.summary, neon.summary)




### MTBS summary

mtbs.summary.west <- mtbs.west %>%
 as_tibble() %>%
 group_by(STUSPS) %>%
 summarise(MTBS_ACRES = sum(BurnBndAc, na.rm=TRUE),
           MTBS_HA = MTBS_ACRES * 0.404686) %>%
 ungroup()
