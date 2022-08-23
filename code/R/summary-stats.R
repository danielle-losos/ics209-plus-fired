##################
library(tidyverse)
library(sf)
##################

# Environments
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd("../../")

# Load the latest ICS-209-PLUS spatial points
# Incident Summary Reports
ics.points <- st_read("data/spatial/raw/wf-incidents/ics-209-plus-2.0/ics209plus-wf_incidents_spatial_us_1999to2020.gpkg")

# Bring in boundary data to subset incidents, add attributes
states <- st_read("../data/boundaries/political/TIGER/tl19_us_states_w_ak_lambert.gpkg") %>%
 st_make_valid() %>%
 st_transform(crs=st_crs(ics.points)) %>%
 select(STUSPS)

gaccs <- st_read("../data/boundaries/political/GACC/national_gacc_current.gpkg") %>%
 st_make_valid() %>%
 st_transform(crs=st_crs(ics.points)) %>%
 select(GACCAbbrev)

ecol3 <- st_read("../data/boundaries/ecological/ecoregion/na_cec_eco_l3.gpkg") %>%
 st_make_valid() %>%
 st_transform(crs=st_crs(ics.points)) %>%
 select(NA_L3CODE, NA_L3NAME)

neon <- st_read("../data/boundaries/bioclimatic/neon_domains.gpkg") %>%
 st_make_valid() %>%
 st_transform(crs=st_crs(ics.points)) %>%
 select(DomainName) %>% 
 rename(NEON_DOMAIN=DomainName)


# add attributes from the boundary data
spatial.summaries <- ics.points %>%
        st_join(states) %>%
        st_join(gaccs) %>%
        st_join(ecol3) %>%
        st_join(neon) %>%
 dplyr::select(-STUSPS.x) %>%
 rename(STUSPS = STUSPS.y)

glimpse(spatial.summaries)

# Save the spatial layer
st_write(spatial.summaries,
         "data/spatial/raw/wf-incidents/ics-209-plus-2.0/ics209plus-wf_incidents_spatial_us_1999to2020.gpkg",
         delete_dsn=TRUE)



# create summary tables of key variables across spatial domains

# states
state.summary <- spatial.summaries %>%
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
gacc.summary <- spatial.summaries %>%
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
eco.summary <- spatial.summaries %>%
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
neon.summary <- spatial.summaries %>%
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
