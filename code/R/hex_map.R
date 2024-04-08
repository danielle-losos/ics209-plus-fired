"""
Hexbin Maps of ICS-209 Attributes

"""

library(tidyverse)
library(sf)

# Define Lambert projection
lambert.prj <- "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs"


###############
# Load the data

# ICS-209 spatial points
ics <- st_read('data/spatial/raw/wf-incidents/ics-209-plus-2.0/ics209plus-wf_incidents_spatial_us_1999to2020.gpkg')

# State boundaries (CONUS)
states <- st_read('../data/boundaries/political/TIGER/tl19_us_states_conus.gpkg')


#####################
# Create the hex grid

hex <- st_make_grid(
 states, n=c(50,50), what="polygons", square=FALSE, 
 flat_topped=TRUE, crs=st_crs(lambert.prj)) %>%
 st_as_sf() %>%
 mutate(HEXID = floor(as.numeric(rownames(.))))

##########################
# Join to incident reports

hex.join <- st_join(ics, hex %>% st_transform(st_crs(lambert.prj)), join=st_within)

##########################
# Create the summary table

sums <- hex.join %>% group_by(HEXID) %>%
 st_drop_geometry() %>%
 summarize(counts = n()) %>%
 ungroup()

#######################
# Join back to hex grid

hex_ <- inner_join(hex %>% as_tibble(), sums, by="HEXID") %>% st_as_sf()

################
# Create the map

hexmap <- ggplot()+
 geom_sf(data=states, color="transparent", fill="gray90", size=0.5)+
 geom_sf(data=hex_, aes(fill=counts), color="transparent")+
 scale_fill_viridis_c(option="inferno", trans="log10", alpha=0.9)+
 geom_sf(data=states, color="gray30", fill="transparent", size=0.5)+
 theme_void() +
 guides(
  fill = guide_colourbar(
   position="left", barwidth = 0.4, barheight = 6, ticks=F,
   label.position = "right", title.position = "left")) +
 labs(fill="log(Fire Counts)") +
 theme(legend.title = element_text(angle = 90, size=10),
       legend.position = c(0.9, 0.3))

hexmap

# # Save
# ggsave(hexmap, file = "../../figures/ics209plus_fired_FireCounts_Hex_CONUS.png",
#        width=5, height=3.5, dpi = 500, bg="white") # adjust dpi accordingly
