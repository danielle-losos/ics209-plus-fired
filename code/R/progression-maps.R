

# This script creates fire progression maps for given perimeters

library(tidyverse)
library(sf)
library(terra)
library(ggspatial)

# Global vars

maindir <- '/Users/max/Library/CloudStorage/OneDrive-Personal/mcook/'

burndates <- paste0(maindir,"earth-lab/fastest-fires/data/spatial/mod/mcd64a1/")

# Define Lambert projection
lambert.prj <- "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs"

# MTBS perimeters

mtbs <- st_read(paste0(maindir,'data/mtbs/mtbs_perimeter_data/mtbs_perims_conus.gpkg'))

#################

# Grab the case study fires (Chetco Bar & Taylor Creek/Klondike)

ids <- c("OR4229712395420170712", "OR4252812357120180715", "OR4237012386020180716")

fires <- mtbs %>%
 filter(Event_ID %in% ids)

# Simple map

ggplot(data = fires) +
 geom_sf(aes(fill=Incid_Name)) +
 labs(x="",y="",fill="Incident Name")
 theme_light() +
 theme(legend.position="right",
       text = element_text(family = "Helvetica"),
       axis.text = element_text(size=8, angle=45),
       axis.text.x = element_text(vjust = 0.5))

# Grab the associated burndate rasters

# Load the burndate raster (MCD64A1)
mcd64.17 <- terra::rast(paste0(burndates,'mcd64a1_annual_burndate_y2017.tif'))
mcd64.18 <- terra::rast(paste0(burndates,'mcd64a1_annual_burndate_y2018.tif'))

########################################
# Plot the Chetco Bar fire progression #
########################################

fire <- fires %>% 
 filter(Incid_Name == "CHETCO BAR") %>% 
 st_transform(st_crs(lambert.prj))
# Convert to SpatVects
fire.sp <- vect(fire)

# Reproject and mask the burndate raster, crop to perimeter
mcd64.17 <- terra::project(mcd64.17,crs(lambert.prj, proj=TRUE))
mcd64.17 <- terra::crop(mcd64.17,fire.sp,mask=T)

# Plot the histogram of burn date
hist(mcd64.17,
     main = "Distribution of day of burn (DOY) for Chetco Bar Fire (2017)",
     xlab = "Day of Year", ylab = "Frequency",
     col = "red")

# Grab the range of burn days (original)
range.doy <- range(mcd64.17[],na.rm=TRUE)

# Cut of the sparse values > 255
mcd64.17[mcd64.17>258] = NA
start <- as.numeric(range.doy[1])
breaks <- abs(range.doy[1]-range.doy[2])

# Update raster values to get "days since ignition"
mcd64.17 <- mcd64.17 - start
range.dsi <- range(mcd64.17[],na.rm=TRUE) # Grab the new range

# Extract as DF for plotting
df <- as.data.frame(mcd64.17, xy = TRUE) %>% 
 rename(burndate = mcd64a1_annual_burndate_y2017)

# Plot
p1 <- df %>%
 drop_na() %>%
 ggplot() +
 geom_raster(aes(x = x, y = y, fill = burndate)) +
 scale_fill_fermenter(palette = "Reds") +
 # Add the fire boundary
 geom_sf(data=fire, fill="transparent", color="gray30", lwd=0.45) +
 coord_sf() +
 labs(fill="Days Since Ignition",x="",y="",
      title="Chetco Bar (2017) Fire Progression") +
 theme_light() +
 theme(legend.position="bottom",
       plot.title = element_text(hjust = 0.5),
       plot.tag = element_text(hjust = -2.5),
       text = element_text(family = "Helvetica"),
       axis.text = element_text(size=8, angle=45),
       axis.text.x = element_text(vjust = 0.5)) +
 guides(fill = element_blank()) +
 guides(fill = guide_colorbar(title.position="top", title.hjust = 0.5,
                              barwidth = 12, barheight = 0.5,
                              ticks=FALSE, draw.llim=FALSE)) +
 ggspatial::annotation_scale(location = "br", width_hint = 0.1,
                             pad_x = unit(0.2,"in"), pad_y = unit(0.2,"in"),
                             line_width = 0.5, text_pad = unit(0.15,"cm"),
                             height = unit(0.15,"cm")) +
 ggspatial::annotation_north_arrow(location = "br", which_north = "true",
                                   pad_x = unit(0.3,"in"), pad_y= unit(0.4,"in"),
                                   width = unit(0.8,"cm"), height = unit(0.8,"cm"),
                                   style = north_arrow_fancy_orienteering)
p1
gc()


#####################################################
# Plot the Taylor Creek / Klondike fire progression #
#####################################################

fire <- fires %>% 
 filter(Incid_Name == "TAYLOR CREEK" | Incid_Name == "KLONDIKE") %>% 
 st_transform(st_crs(lambert.prj))
# Convert to SpatVects
fire.sp <- vect(fire)

# Reproject and mask the burndate raster, crop to perimeter
mcd64.18 <- terra::project(mcd64.18,crs(lambert.prj, proj=TRUE))
mcd64.18 <- terra::crop(mcd64.18,fire.sp,mask=T)

# Grab the range of burn days (original)
(range.doy <- range(mcd64.18[],na.rm=TRUE))

start <- as.numeric(range.doy[1])

# Update raster values to get "days since ignition"
mcd64.18 <- mcd64.18 - start
range.dsi <- range(mcd64.18[],na.rm=TRUE) # Grab the new range

# Plot the histogram of burn date
hist(mcd64.18,
     main = "Distribution of day of burn (DOY) for Taylor Creek/Klondike Fire (2017)",
     xlab = "Day of Year", ylab = "Frequency",
     col = "red")

# Extract as DF for plotting
df <- as.data.frame(mcd64.18, xy = TRUE) %>% 
 rename(burndate = mcd64a1_annual_burndate_y2018)

# Plot
p2 <- df %>%
 drop_na() %>%
 ggplot() +
 geom_raster(aes(x = x, y = y, fill = burndate)) +
 scale_fill_fermenter(palette = "Reds") +
 # Add the fire boundary
 geom_sf(data=fire, fill="transparent", color="gray30", lwd=0.45) +
 # geom_sf(data=fires%>%filter(Incid_Name=="CHETCO BAR"), 
 #         fill="transparent", color="gray30", lwd=0.45) +
 # geom_sf_text(data=fires%>%filter(Incid_Name=="CHETCO BAR"), 
 #              aes(label = Incid_Name), size=2) + # labels
 coord_sf() +
 labs(fill="Days Since Ignition",x="",y="",
      title="Taylor Creek/Klondike (2018) Fire Progression") +
 theme_light() +
 theme(legend.position="bottom",
       plot.title = element_text(hjust = 0.5),
       plot.tag = element_text(hjust = -2.5),
       text = element_text(family = "Helvetica"),
       axis.text = element_text(size=8, angle=45),
       axis.text.x = element_text(vjust = 0.5)) +
 guides(fill = element_blank()) +
 guides(fill = guide_colorbar(title.position="top", title.hjust = 0.5,
                              barwidth = 12, barheight = 0.5,
                              ticks=FALSE, draw.llim=FALSE)) +
 ggspatial::annotation_scale(location = "br", width_hint = 0.1,
                             pad_x = unit(0.2,"in"), pad_y = unit(0.2,"in"),
                             line_width = 0.5, text_pad = unit(0.15,"cm"),
                             height = unit(0.15,"cm")) +
 ggspatial::annotation_north_arrow(location = "br", which_north = "true",
                                   pad_x = unit(0.3,"in"), pad_y= unit(0.4,"in"),
                                   width = unit(0.8,"cm"), height = unit(0.8,"cm"),
                                   style = north_arrow_fancy_orienteering)
p2
gc()


# Combine the plots and export

(arr <- ggpubr::ggarrange(p1,p2,nrow=1,ncol=2,common.legend=F,align="hv"))

ggsave(arr,dpi=300,file=paste0(maindir,"ics209-plus-fired/figures/progression-maps_v2.png"),
       width = 10, height=8.5)

