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


Copy.

```{r}
# Total acres
acres <- ggplot() +
 geom_sf(data = counties.conus, fill = "grey", color = "gray89", lwd = 0.2) +
 geom_sf(data = counties.sf.conus, 
         aes(fill = log(FOD_FINAL_ACRES)),
         color = "gray89", lwd = 0.2) +
 labs(title="(a) Burned area (acres)") +
 theme_void() + 
 scico::scale_fill_scico(palette = "lajolla",
                         begin = 0,
                         na.value = "grey",
                         name = "Total burned acres (log scale)") +
 theme(legend.title=element_text(size=7, hjust=0.2), 
       legend.text=element_text(size=6),
       legend.position=c(0.2, 0.1)) +
 guides(fill = guide_colourbar(direction = "horizontal",
                               label.position = "bottom",
                               title.position="top",
                               barwidth = 6.5, barheight = 0.7))  

ggsave(acres, file = paste("C:/Users/mccoo/OneDrive/mcook/ics209-plus-fired/figs/",
                           "ICS209-PLUS_version2.0_KeyVars_Acres.png", sep=""),
       width=5, height=4, dpi = 700, bg="white") # adjust dpi accordingly


# Max spread rate
fsr <- ggplot() +
 geom_sf(data = counties.conus, fill = "grey", color = "gray89", lwd = 0.2) +
 geom_sf(data = counties.sf.conus, 
         aes(fill = log(WF_MAX_FSR)),
         color = "gray89", lwd = 0.2) +
 labs(title = "(b) Max fire spread rate (acres/day)") +
 theme_void() + 
 scico::scale_fill_scico(palette = "lajolla",
                         begin = 0,
                         na.value = "grey",
                         name = "Maximum fire spread rate (log scale)") +
 theme(legend.title=element_text(size=7, hjust=0.2), 
       legend.text=element_text(size=6),
       legend.position=c(0.2, 0.1)) +
 guides(fill = guide_colourbar(direction = "horizontal",
                               label.position = "bottom",
                               title.position="top",
                               barwidth = 6.5, barheight = 0.7)) 

ggsave(fsr, file = paste("C:/Users/mccoo/OneDrive/mcook/ics209-plus-fired/figs/",
                         "ICS209-PLUS_version2.0_KeyVars_FSR.png", sep=""),
       width=5, height=4, dpi = 700, bg="white") # adjust dpi accordingly

# Max spread rate
fsr2 <- ggplot() +
 geom_sf(data = counties.conus, fill = "grey", color = "gray89", lwd = 0.2) +
 geom_sf(data = counties.sf.conus, 
         aes(fill = log(WF_MEAN_FSR)),
         color = "gray89", lwd = 0.2) +
 labs(title = "(b) Average max fire spread rate (acres/day)") +
 theme_void() + 
 scico::scale_fill_scico(palette = "lajolla",
                         begin = 0,
                         na.value = "grey",
                         name = "Average max fire spread rate (log scale)") +
 theme(legend.title=element_text(size=7, hjust=0.2), 
       legend.text=element_text(size=6),
       legend.position=c(0.2, 0.1)) +
 guides(fill = guide_colourbar(direction = "horizontal",
                               label.position = "bottom",
                               title.position="top",
                               barwidth = 6.5, barheight = 0.7)) 

ggsave(fsr2, file = paste("C:/Users/mccoo/OneDrive/mcook/ics209-plus-fired/figs/",
                          "ICS209-PLUS_version2.0_KeyVars_FSRmean.png", sep=""),
       width=5, height=4, dpi = 700, bg="white") # adjust dpi accordingly

# Count of fires
threat <- ggplot() +
 geom_sf(data = counties.conus, fill = "grey", color = "gray89", lwd = 0.2) +
 geom_sf(data = counties.sf.conus, 
         aes(fill = log(STR_THREATENED)),
         color = "gray89", lwd = 0.2) +
 labs(title="(e) Total structures threatened") +
 theme_void() + 
 scico::scale_fill_scico(palette = "lajolla",
                         begin = 0,
                         na.value = "grey",
                         name = "Structures threatened (log scale)") +
 theme(legend.title=element_text(size=7, hjust=0.2), 
       legend.text=element_text(size=6),
       legend.position=c(0.2, 0.1)) +
 guides(fill = guide_colourbar(direction = "horizontal",
                               label.position = "bottom",
                               title.position="top",
                               barwidth = 6.5, barheight = 0.7)) 

ggsave(threat, file = paste("C:/Users/mccoo/OneDrive/mcook/ics209-plus-fired/figs/",
                            "ICS209-PLUS_version2.0_KeyVars_Threat.png", sep=""),
       width=5, height=4, dpi = 700, bg="white") # adjust dpi accordingly


# Structures destroyed (log)
struct <- ggplot() +
 geom_sf(data = counties.conus, fill = "grey", color = "gray89", lwd = 0.2) +
 geom_sf(data = counties.sf.conus, 
         aes(fill = log(STR_DESTROYED_TOTAL)),
         color = "gray89", lwd = 0.2) +
 labs(title="(f) Total structures destroyed") +
 theme_void() +
 scico::scale_fill_scico(palette = "lajolla",
                         begin = 0,
                         na.value = "grey",
                         name = "Total structures destroyed (log scale)") +
 theme(legend.title=element_text(size=7, hjust=0.2), 
       legend.text=element_text(size=6),
       legend.position=c(0.2, 0.1)) +
 guides(fill = guide_colourbar(direction = "horizontal",
                               label.position = "bottom",
                               title.position="top",
                               barwidth = 6.5, barheight = 0.7)) 

ggsave(struct, file = paste("C:/Users/mccoo/OneDrive/mcook/ics209-plus-fired/figs/",
                            "ICS209-PLUS_version2.0_KeyVars_Destr.png", sep=""),
       width=5, height=4, dpi = 700, bg="white") # adjust dpi accordingly

# Personnel costs
personnel <- ggplot() +
 geom_sf(data = counties.conus, fill = "grey", color = "gray89", lwd = 0.2) +
 geom_sf(data = counties.sf.conus, 
         aes(fill = log(TOTAL_PERSONNEL_SUM)),
         color = "gray89", lwd = 0.2) +
 labs(title="(c) Total assigned personnel") +
 theme_void() + 
 scico::scale_fill_scico(palette = "lajolla",
                         begin = 0,
                         na.value = "grey",
                         name = "Assigned personnel (log scale)") +
 theme(legend.title=element_text(size=7), 
       legend.text=element_text(size=6),
       legend.position=c(0.2, 0.1)) +
 guides(fill = guide_colourbar(direction = "horizontal",
                               label.position = "bottom",
                               title.position="top",
                               barwidth = 6.5, barheight = 0.7)) 

ggsave(personnel, file = paste("C:/Users/mccoo/OneDrive/mcook/ics209-plus-fired/figs/",
                               "ICS209-PLUS_version2.0_KeyVars_Personnel.png", sep=""),
       width=5, height=4, dpi = 700, bg="white") # adjust dpi accordingly

# Projected IM costs
cost <- ggplot() +
 geom_sf(data = counties.conus, fill = "grey", color = "gray89", lwd = 0.2) +
 geom_sf(data = counties.sf.conus, 
         aes(fill = log(PROJECTED_COSTS)),
         color = "gray89", lwd = 0.2) +
 labs(title="(c) Projected IM costs ($)") +
 theme_void() + 
 scico::scale_fill_scico(palette = "lajolla",
                         begin = 0,
                         na.value = "grey",
                         name = "Projected costs (log scale)") +
 theme(legend.title=element_text(size=7), 
       legend.text=element_text(size=6),
       legend.position=c(0.2, 0.1)) +
 guides(fill = guide_colourbar(direction = "horizontal",
                               label.position = "bottom",
                               title.position="top",
                               barwidth = 6.5, barheight = 0.7)) 

ggsave(cost, file = paste("C:/Users/mccoo/OneDrive/mcook/ics209-plus-fired/figs/",
                          "ICS209-PLUS_version2.0_KeyVars_ProjCost.png", sep=""),
       width=5, height=4, dpi = 700, bg="white") # adjust dpi accordingly


# Max evacuations
evacuation <- ggplot() +
 geom_sf(data = counties.conus, fill = "grey", color = "gray89", lwd = 0.2) +
 geom_sf(data = counties.sf.conus, 
         aes(fill = log(PEAK_EVACUATIONS)),
         color = "gray89", lwd = 0.2) +
 labs(title="(d) Total evacuations (2014-2020)") +
 theme_void() + 
 scico::scale_fill_scico(palette = "lajolla",
                         begin = 0,
                         na.value = "grey",
                         name = "Total evacuations (log scale)") +
 theme(legend.title=element_text(size=7), 
       legend.text=element_text(size=6),
       legend.position=c(0.2, 0.1)) +
 guides(fill = guide_colourbar(direction = "horizontal",
                               label.position = "bottom",
                               title.position="top",
                               barwidth = 6.5, barheight = 0.7)) 

ggsave(evacuation, file = paste("C:/Users/mccoo/OneDrive/mcook/ics209-plus-fired/figs/",
                                "ICS209-PLUS_version2.0_KeyVars_Evac.png", sep=""),
       width=5, height=4, dpi = 700, bg="white") # adjust dpi accordingly

```
