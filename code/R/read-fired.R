library(tidyverse)
library(sf)
setwd("C:/Users/mccoo/OneDrive/mcook/")
# FIRED Events US-Canada
events <- st_read("fired/data/events/122021/fired_uscan_to2021121.shp") %>%
  st_set_crs(4326)
# Bring country boundaries
world <- st_read("data/boundaries/political/ne_50m_admin_0_countries.shp") %>%
  st_transform(4326) %>%
  filter(NAME_EN == "United States of America" | NAME_EN == "Canada")
#Base plot
ggplot()+
  geom_sf(data=events, fill="transparent", color="grey20", size=1.2) +
  theme_minimal()

base +
  geom_sf(data=events, fill="red", color="red")

# Export 
st_write(events, "fired_to2021091_events_wgs.gpkg")