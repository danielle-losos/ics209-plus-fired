

# This script creates fire progression maps and ICS-209 situation report analysis
# for the East Troublesome Fire (2020)

# Load all required libraries
library(tidyverse)
library(sf)
library(terra)
library(ggspatial)
library(here)
library(ggpubr)
library(patchwork)
library(scales)
library(lubridate)

# Set up flexible directory structure
# This assumes the script is run from code/R/ and needs to access the parent project directory
if (!exists("project_root")) {
  # If running from RStudio, use rstudioapi to get script location
  if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    script_dir <- dirname(rstudioapi::getActiveDocumentContext()$path)
    project_root <- file.path(script_dir, "..", "..")
  } else {
    # Fallback: assume we're in code/R/ directory
    project_root <- file.path("..", "..")
  }
  project_root <- normalizePath(project_root)
}

# Set working directory to project root
setwd(project_root)

# Define data paths relative to project root
data_dir <- file.path(project_root, "data")
spatial_dir <- file.path(data_dir, "spatial")
burndates_dir <- file.path(spatial_dir, "raw", "modis", "mcd64a1")  # Create this directory structure
mtbs_dir <- file.path(spatial_dir, "raw", "mtbs")  # Create this directory structure
figures_dir <- file.path(project_root, "figures")

# Create directories if they don't exist
dir.create(burndates_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(mtbs_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(figures_dir, recursive = TRUE, showWarnings = FALSE)

# Define Lambert projection
lambert.prj <- "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs"

# MTBS perimeters

# If you need to convert from GeoJSON GEE export
# Read GeoJSON
#fire_perimeter <- st_read("data/spatial/raw/mtbs/mtbs_east_troublesome_fire_perimeter.geojson")
# Write as geopackage
#st_write(fire_perimeter, "data/spatial/raw/mtbs/mtbs_east_troublesome_fire_perimeter.gpkg")

# Note: This file needs to be downloaded and placed in the mtbs_dir
mtbs_file <- file.path(mtbs_dir, "mtbs_east_troublesome_fire_perimeter.gpkg")
if (!file.exists(mtbs_file)) {
  stop("MTBS perimeter file not found at: ", mtbs_file, 
       "\nPlease download MTBS data and place it in: ", mtbs_dir)
}
mtbs <- st_read(mtbs_file)

#################

# Define fire to analyze - East Troublesome Fire 2020
# Using specific Event_ID for precise identification
target_fire_name <- "EAST TROUBLESOME"
target_fire_year <- 2020
target_event_id <- "CO4020310623920201014"

# Select fire using Event_ID from MTBS data
fires <- mtbs %>%
  filter(Event_ID == target_event_id)

# Check if fire was found
if (nrow(fires) == 0) {
  cat("Fire not found in MTBS data using Event_ID:", target_event_id, "\n")
  cat("You may need to:\n")
  cat("1. Download the specific MTBS perimeter for", target_fire_name, "\n")
  cat("2. Check that the Event_ID is correct\n")
  cat("3. Use the GeoJSON file from Google Earth Engine export\n")
  
  # Alternative: try to load the GeoJSON file from GEE export
  geojson_file <- file.path(mtbs_dir, "mtbs_east_troublesome_fire_perimeter.geojson")
  if (file.exists(geojson_file)) {
    cat("Loading fire perimeter from GEE export...\n")
    fires <- st_read(geojson_file)
  } else {
    stop("No fire perimeter data found. Please run the Google Earth Engine script first.")
  }
} else {
  cat("Successfully found East Troublesome Fire using Event_ID:", target_event_id, "\n")
}

# Simple map

ggplot(data = fires) +
 geom_sf(aes(fill=Incid_Name)) +
 labs(x="",y="",fill="Incident Name") +
 theme_light() +
 theme(legend.position="right",
       text = element_text(family = "Helvetica"),
       axis.text = element_text(size=8, angle=45),
       axis.text.x = element_text(vjust = 0.5))

# Grab the associated burndate rasters

# Load the burndate raster (MCD64A1) for the target year
burndate_file <- file.path(burndates_dir, paste0('mcd64a1_annual_burndate_y', target_fire_year, '.tif'))

if (!file.exists(burndate_file)) {
  stop("Burn date raster not found at: ", burndate_file,
       "\nPlease download MCD64A1 data for year ", target_fire_year, " and place it in: ", burndates_dir)
}

cat("Loading burn date raster for year", target_fire_year, "\n")
mcd64_target <- terra::rast(burndate_file)

########################################
# Plot the target fire progression    #
########################################

if (nrow(fires) > 0) {
  # Use the first fire if multiple found
  fire <- fires[1,] %>% 
 st_transform(st_crs(lambert.prj))
  
# Convert to SpatVects
fire.sp <- vect(fire)

  # Check burn date values BEFORE reprojection
  cat("BEFORE reprojection (should be integers 288-323):\n")
  cat("  Range:", range(mcd64_target[], na.rm=TRUE), "\n")
  cat("  Sample values:", head(mcd64_target[][mcd64_target[] > 0], 10), "\n")
  cat("  Data type:", datatype(mcd64_target), "\n")
  
  # Reproject using nearest neighbor to preserve integer burn dates
  mcd64_target <- terra::project(mcd64_target, crs(lambert.prj, proj=TRUE), method="near")
  
  cat("AFTER reprojection (should still be integers):\n")
  cat("  Range:", range(mcd64_target[], na.rm=TRUE), "\n")
  cat("  Sample values:", head(mcd64_target[][mcd64_target[] > 0], 10), "\n")
  
  mcd64_target <- terra::crop(mcd64_target, fire.sp, mask=TRUE)
  
  cat("AFTER cropping:\n")
  cat("  Range:", range(mcd64_target[], na.rm=TRUE), "\n")
  
  # Check the original burn date values
  cat("Original burn date range:", range(mcd64_target[], na.rm=TRUE), "\n")
  cat("Unique values in first 20 pixels:", unique(mcd64_target[][1:20]), "\n")
  cat("Number of 0 values:", sum(mcd64_target[] == 0, na.rm=TRUE), "\n")
  cat("Number of valid burn pixels:", sum(mcd64_target[] > 0, na.rm=TRUE), "\n")
  
  # Plot the histogram of original burn dates (Day of Year)
  hist(mcd64_target,
       main = paste("RAW Distribution of day of burn (DOY) for", target_fire_name, "Fire (", target_fire_year, ")", sep=" "),
     xlab = "Day of Year", ylab = "Frequency",
     col = "red")

  cat("This first histogram shows RAW data including 0s (non-burned pixels)\n")
  
  # Clean the data: MCD64A1 valid range is 1-366, but also remove 0 values
  mcd64_clean <- mcd64_target
  mcd64_clean[mcd64_clean > 366] <- NA
  mcd64_clean[mcd64_clean < 1] <- NA
  mcd64_clean[mcd64_clean == 0] <- NA  # Remove 0 values which are non-burned pixels
  
  # Grab the range of valid burn days
  range.doy <- range(mcd64_clean[], na.rm=TRUE)
  cat("Clean burn date range:", range.doy, "\n")
  cat("Number of valid pixels after cleaning:", sum(!is.na(mcd64_clean[])), "\n")
  
  # Calculate days since ignition (subtract minimum burn date)
  start_doy <- as.numeric(range.doy[1])
  cat("Fire ignition DOY:", start_doy, "\n")
  
  # Create days since ignition raster
  mcd64_dsi <- mcd64_clean - start_doy
  range.dsi <- range(mcd64_dsi[], na.rm=TRUE)
  cat("Days since ignition range:", range.dsi, "\n")
  
  # Plot histogram of days since ignition
  hist(mcd64_dsi,
       main = paste("CLEANED: Days since ignition for", target_fire_name, "Fire (", target_fire_year, ")", sep=" "),
       xlab = "Days Since Ignition", ylab = "Frequency",
       col = "blue")
  
  cat("This second histogram shows CLEANED data (days since ignition)\n")
  
  # Extract as DF for plotting (use days since ignition)
  df <- as.data.frame(mcd64_dsi, xy = TRUE) %>% 
   rename(burndate = names(mcd64_dsi))  # Use the actual raster name

  # Find the first MODIS observation (day 0 of fire progression)
  # Get coordinates of pixels that burned on day 0
  first_burn_df <- as.data.frame(mcd64_dsi, xy = TRUE) %>%
    rename(burndate = names(mcd64_dsi)) %>%  # Use same naming as main df
    filter(!is.na(burndate) & burndate == 0)
  
  # Calculate centroid of first burn pixels as ignition location
  if(nrow(first_burn_df) > 0) {
    ignition_coords <- data.frame(
      x = mean(first_burn_df$x),
      y = mean(first_burn_df$y)
    )
    ignition_point <- st_as_sf(ignition_coords, coords = c("x", "y"), crs = st_crs(lambert.prj))
    cat("Found", nrow(first_burn_df), "pixels that burned on day 0 (first MODIS detection)\n")
  } else {
    cat("No day 0 pixels found, using fallback location\n")
    # Fallback: use a representative location from the fire area
    ignition_coords <- data.frame(x = mean(df$x, na.rm=TRUE), y = mean(df$y, na.rm=TRUE))
    ignition_point <- st_as_sf(ignition_coords, coords = c("x", "y"), crs = st_crs(lambert.prj))
  }
  
  # Calculate the first detection date (start DOY + 0 days since ignition)
  first_detection_doy <- start_doy  # This is the DOY of first detection
  first_detection_date <- as.Date(first_detection_doy - 1, origin = paste0(target_fire_year, "-01-01"))
  first_detection_label <- paste("First Detection:", format(first_detection_date, "%Y-%m-%d"))
  cat("First fire detection date:", format(first_detection_date, "%Y-%m-%d"), "\n")

# Plot
p1 <- df %>%
 drop_na() %>%
 ggplot() +
 geom_raster(aes(x = x, y = y, fill = burndate)) +
   scale_fill_fermenter(palette = "Reds", direction = 1, n.breaks = 5) +
 # Add the fire boundary
 geom_sf(data=fire, fill="transparent", color="gray30", lwd=0.45) +
  # Add ignition point (first MODIS observation)
  geom_sf(data=ignition_point, aes(shape="First Detection"), color="black", size=8) +  # Black diamond (100% larger)
 coord_sf() +
  labs(fill="Days Since Ignition", shape="", x="", y="", title="", tag="(a)") +
   scale_shape_manual(values=c("First Detection"=18), labels=first_detection_label) +
 theme_void() +
 theme(legend.position="bottom",
       plot.title = element_text(hjust = 0.5),
      plot.tag = element_text(hjust = -0.1, vjust = -0.3),
       text = element_text(family = "Helvetica"),
      legend.title = element_text(size = 12),
      legend.text = element_text(size = 12),
      legend.margin = margin(t = 0, b = 0, l = 0, r = 0),
      legend.spacing = unit(0, "cm"),
      plot.margin = margin(0.1, 0.1, 0.1, 0.1, "cm")) +
 guides(fill = guide_colorbar(title.position="top", title.hjust = 0.5,
                               barwidth = 18, barheight = 0.6,
                               ticks=TRUE, draw.llim=FALSE, order = 1),
         shape = guide_legend(title.position="top", title.hjust = 0.5, order = 2)) +
 ggspatial::annotation_scale(location = "br", width_hint = 0.15,
                             pad_x = unit(0.2,"in"), pad_y = unit(0.2,"in"),
                             line_width = 1.5, text_pad = unit(0.2,"cm"),
                             height = unit(0.25,"cm"), text_cex = 1.2) +
 ggspatial::annotation_north_arrow(location = "br", which_north = "true",
                                   pad_x = unit(0.3,"in"), pad_y= unit(0.6,"in"),
                                   width = unit(2.2,"cm"), height = unit(2.2,"cm"),
                                   style = north_arrow_fancy_orienteering)
  print(p1)
  
  # Save the plot
  output_filename <- paste0(target_fire_name, "_", target_fire_year, "_progression_map.png")
  output_path <- file.path(figures_dir, output_filename)
  
  ggsave(p1, filename = output_path, dpi = 300, width = 10, height = 8.5)
  cat("Map saved to:", output_path, "\n")
  
} else {
  cat("No fire data found. Please check that the required data files are available.\n")
}

gc()

# Script complete - fire progression map has been generated
cat("Fire progression analysis complete!\n")

# ============================================================================
# ICS-209 SITUATION REPORTS ANALYSIS
# ============================================================================

# Load the daily SITREPS
sitreps_data <- read_csv("data/tabular/raw/ICS/ics209-plus-wf_sitreps_1999to2020.csv") %>%
  filter(INCIDENT_ID == "2020_11966782_EAST TROUBLESOME") %>%
  mutate(REPORT_TO_DATE = as.Date(REPORT_TO_DATE)) %>%
  arrange(REPORT_TO_DATE)

cat("Loaded", nrow(sitreps_data), "situation reports for East Troublesome Fire\n")

# ============================================================================
# CREATE DATE BINS FOR COLOR COORDINATION
# ============================================================================

# Calculate the fire ignition date from the first detection DOY
ignition_date <- as.Date(paste(target_fire_year, first_detection_doy), format = "%Y %j")
cat("Fire ignition date:", as.character(ignition_date), "\n")

# Get the range of days since ignition from the raster data
days_since_ignition_range <- range(df$burndate, na.rm = TRUE)
max_days <- ceiling(days_since_ignition_range[2])
cat("Fire progression range: 0 to", max_days, "days since ignition\n")

# Create 10-day bins to match the fire progression color scheme (5 breaks = 4 intervals)
# We'll use the same binning approach as scale_fill_fermenter with n.breaks = 5
bin_breaks <- seq(0, max_days, length.out = 6)  # 6 breaks = 5 intervals
bin_labels <- paste0(round(bin_breaks[-length(bin_breaks)]), "-", round(bin_breaks[-1]), " days")

# Modify the last bin label to indicate it includes post-fire dates
bin_labels[length(bin_labels)] <- paste0(round(bin_breaks[length(bin_breaks)-1]), "+ days")

cat("Date bins created:\n")
for(i in 1:length(bin_labels)) {
  cat("  Bin", i, ":", bin_labels[i], "\n")
}

# Add date bins to ICS-209 data
sitreps_data <- sitreps_data %>%
  mutate(
    # Calculate days since ignition for each report
    days_since_ignition = as.numeric(REPORT_TO_DATE - ignition_date),
    # Assign to bins, with special handling for post-fire dates
    date_bin_raw = cut(days_since_ignition, breaks = c(bin_breaks, Inf), labels = c(bin_labels, "post-fire"), include.lowest = TRUE),
    # Collapse post-fire dates into the last bin
    date_bin = ifelse(date_bin_raw == "post-fire", bin_labels[length(bin_labels)], as.character(date_bin_raw)),
    date_bin = factor(date_bin, levels = bin_labels),
    # Create a numeric version for color mapping (1 to 5 to match fermenter palette)
    date_bin_numeric = as.numeric(date_bin)
  )

# Create color palette to match the fire progression map
# Using the same "Reds" palette with 5 breaks
reds_palette <- RColorBrewer::brewer.pal(5, "Reds")
names(reds_palette) <- bin_labels

# Create line segments data for gradient coloring
# Each segment connects consecutive points and takes the color of the earlier point
create_line_segments <- function(data, y_var) {
  if(nrow(data) <= 1) return(NULL)
  
  segments <- data.frame()
  for(i in 1:(nrow(data)-1)) {
    segment <- data.frame(
      x = data$REPORT_TO_DATE[i],
      xend = data$REPORT_TO_DATE[i+1],
      y = data[[y_var]][i],
      yend = data[[y_var]][i+1],
      segment_color = as.character(data$date_bin[i])  # Use color of earlier point
    )
    segments <- rbind(segments, segment)
  }
  return(segments)
}

cat("Color-coded", nrow(sitreps_data), "situation reports with date bins\n")

# Create ICS-209 situation report plots
# Simple Fire Spread Rate
fsr_segments <- create_line_segments(sitreps_data, "WF_FSR")
sitrep_p1 <- ggplot() +
  {if(!is.null(fsr_segments)) geom_segment(data=fsr_segments, aes(x=x, xend=xend, y=y, yend=yend, color=segment_color), size=1.8)} +
  geom_point(data=sitreps_data, aes(x=REPORT_TO_DATE, y=WF_FSR, fill=date_bin, color=date_bin), 
             shape=21, size = 2.4, stroke = 0.5, alpha=0.8) +
  scale_fill_manual(values = reds_palette, name = "Days Since Ignition", guide = "none") +
  scale_color_manual(values = reds_palette, guide = "none") +
  scale_y_continuous(labels = scales::label_number(suffix = " K", scale = 1e-3)) +
  scale_x_date(date_labels = "%b %d", date_breaks = "10 days", date_minor_breaks = "2 days") + 
  labs(y="Acres/Day", x="",
       title="Simple Fire Spread Rate (acr/day)", tag="(b)") +
  theme_classic() +
  theme(plot.title = element_text(size=12, vjust=1, hjust=0.5, face="bold"),
        axis.title = element_text(size = 10),
        #axis.title.x = element_text(margin = margin(t = 8)),
        axis.text.y = element_text(size=11),
        axis.text.x = element_text(size=11, angle=15),
        plot.margin = margin(t = 0, r = 5, b = 0, l = 5, unit = "pt"))
sitrep_p1

# Burned Area
acres_segments <- create_line_segments(sitreps_data, "ACRES")
sitrep_p2 <- ggplot() +
  {if(!is.null(acres_segments)) geom_segment(data=acres_segments, aes(x=x, xend=xend, y=y, yend=yend, color=segment_color), size=1.8)} +
  geom_point(data=sitreps_data, aes(x=REPORT_TO_DATE, y=ACRES, fill=date_bin, color=date_bin), 
             shape=21, size = 2.4, stroke = 0.5, alpha=0.8) +
  scale_fill_manual(values = reds_palette, name = "Days Since Ignition", guide = "none") +
  scale_color_manual(values = reds_palette, guide = "none") +
  scale_y_continuous(labels = scales::label_number(suffix = " K", scale = 1e-3)) +
  scale_x_date(date_labels = "%b %d", date_breaks = "10 days", date_minor_breaks = "2 days") + 
  labs(y="Acres", x="",
       title="Burned Area (acr)", tag="(c)") +
  theme_classic() +
  theme(plot.title = element_text(size=12, vjust=0.2, hjust=0.5, face="bold"),
        axis.title = element_text(size = 10),
        #axis.title.x = element_text(margin = margin(t = 8)),
        axis.text.y = element_text(size=11),
        axis.text.x = element_text(size=11, angle=15),
        plot.margin = margin(t = 0, r = 5, b = 0, l = 5, unit = "pt"))
sitrep_p2

# Threatened Structures
threatened_segments <- create_line_segments(sitreps_data, "STR_THREATENED")
sitrep_p3 <- ggplot() +
  {if(!is.null(threatened_segments)) geom_segment(data=threatened_segments, aes(x=x, xend=xend, y=y, yend=yend, color=segment_color), size=1.8)} +
  geom_point(data=sitreps_data, aes(x=REPORT_TO_DATE, y=STR_THREATENED, fill=date_bin, color=date_bin), 
             shape=21, size = 2.4, stroke = 0.5, alpha=0.8) +
  scale_fill_manual(values = reds_palette, name = "Days Since Ignition", guide = "none") +
  scale_color_manual(values = reds_palette, guide = "none") +
  scale_y_continuous(labels = scales::label_number(suffix = " K", scale = 1e-3)) +
  scale_x_date(date_labels = "%b %d", date_breaks = "10 days", date_minor_breaks = "2 days") +
  labs(y="# Structures", x="",
       title="Threatened Structures", tag="(e)") +
  theme_classic() +
  theme(plot.title = element_text(size=12, vjust=0.2, hjust=0.5, face="bold"),
        axis.title = element_text(size = 10),
        #axis.title.x = element_text(margin = margin(t = 8)),
        axis.text.y = element_text(size=11),
        axis.text.x = element_text(size=11, angle=15),
        plot.margin = margin(t = 0, r = 5, b = 0, l = 5, unit = "pt"))
sitrep_p3

# Destroyed Structures
destroyed_segments <- create_line_segments(sitreps_data, "STR_DESTROYED")
sitrep_p4 <- ggplot() +
  {if(!is.null(destroyed_segments)) geom_segment(data=destroyed_segments, aes(x=x, xend=xend, y=y, yend=yend, color=segment_color), size=1.8)} +
  geom_point(data=sitreps_data, aes(x=REPORT_TO_DATE, y=STR_DESTROYED, fill=date_bin, color=date_bin),
             shape=21, size = 2.4, stroke = 0.5, alpha=0.8) +
  scale_fill_manual(values = reds_palette, name = "Days Since Ignition", guide = "none") +
  scale_color_manual(values = reds_palette, guide = "none") +
  scale_x_date(date_labels = "%b %d", date_breaks = "10 days", date_minor_breaks = "2 days") +
  labs(y="# Structures", x="",
       title="Destroyed Structures", tag="(f)") +
  theme_classic() +
  theme(plot.title = element_text(size=12, vjust=0.2, hjust=0.5, face="bold"),
        axis.title = element_text(size = 10),
        #axis.title.x = element_text(margin = margin(t = 8)),
        axis.text.y = element_text(size=11),
        axis.text.x = element_text(size=11, angle=15),
        plot.margin = margin(t = 0, r = 5, b = 0, l = 5, unit = "pt"))
sitrep_p4

# Total Evacuations
evacuations_segments <- create_line_segments(sitreps_data, "TOTAL_EVACUATIONS")
sitrep_p5 <- ggplot() +
  {if(!is.null(evacuations_segments)) geom_segment(data=evacuations_segments, aes(x=x, xend=xend, y=y, yend=yend, color=segment_color), size=1.8)} +
  geom_point(data=sitreps_data, aes(x=REPORT_TO_DATE, y=TOTAL_EVACUATIONS, fill=date_bin, color=date_bin),
             shape=21, size = 2.4, stroke = 0.5, alpha=0.8) +
  scale_fill_manual(values = reds_palette, name = "Days Since Ignition", guide = "none") +
  scale_color_manual(values = reds_palette, guide = "none") +
  scale_y_continuous(labels = scales::label_number(suffix = "K", scale = 1e-3)) +
  scale_x_date(date_labels = "%b %d", date_breaks = "10 days", date_minor_breaks = "2 days") +
  labs(y="# Evacuations", x="",
       title="Total Evacuations", tag="(g)") +
  theme_classic() +
  theme(plot.title = element_text(size=12, vjust=0.2, hjust=0.5, face="bold"),
        axis.title = element_text(size = 10),
        #axis.title.x = element_text(margin = margin(t = 8)),
        axis.text.y = element_text(size=11),
        axis.text.x = element_text(size=11, angle=15),
        plot.margin = margin(t = 0, r = 5, b = 0, l = 5, unit = "pt"))
sitrep_p5

# Total Personnel
personnel_segments <- create_line_segments(sitreps_data, "TOTAL_PERSONNEL")
sitrep_p6 <- ggplot() +
  {if(!is.null(personnel_segments)) geom_segment(data=personnel_segments, aes(x=x, xend=xend, y=y, yend=yend, color=segment_color), size=1.8)} +
  geom_point(data=sitreps_data, aes(x=REPORT_TO_DATE, y=TOTAL_PERSONNEL, fill=date_bin, color=date_bin),
             shape=21, size = 2.4, stroke = 0.5, alpha=0.8) +
  scale_fill_manual(values = reds_palette, name = "Days Since Ignition", guide = "none") +
  scale_color_manual(values = reds_palette, guide = "none") +
  scale_x_date(date_labels = "%b %d", date_breaks = "10 days", date_minor_breaks = "2 days") +
  labs(y="# Personnel", x="Report Date",
       title="Total Personnel", tag="(d)") +
  theme_classic() +
  theme(plot.title = element_text(size=12, vjust=0.2, hjust=0.5, face="bold"),
        axis.title = element_text(size = 10),
        #axis.title.x = element_text(margin = margin(t = 8)),
        axis.text.y = element_text(size=11),
        axis.text.x = element_text(size=11, angle=15),
        plot.margin = margin(t = 0, r = 5, b = 0, l = 5, unit = "pt"))
sitrep_p6

# Total Aerial Resources
aerial_segments <- create_line_segments(sitreps_data, "TOTAL_AERIAL")
sitrep_p7 <- ggplot() +
  {if(!is.null(aerial_segments)) geom_segment(data=aerial_segments, aes(x=x, xend=xend, y=y, yend=yend, color=segment_color), size=1.8)} +
  geom_point(data=sitreps_data, aes(x=REPORT_TO_DATE, y=TOTAL_AERIAL, fill=date_bin, color=date_bin),
             shape=21, size = 2.4, stroke = 0.5, alpha=0.8) +
  scale_fill_manual(values = reds_palette, name = "Days Since Ignition", guide = "none") +
  scale_color_manual(values = reds_palette, guide = "none") +
  scale_x_date(date_labels = "%b %d", date_breaks = "10 days", date_minor_breaks = "2 days") +
  labs(y="# Aerial", x="Report Date",
       title="Total Aerial Resources", tag="(e)") +
  theme_classic() +
  theme(plot.title = element_text(size=12, vjust=0.2, hjust=0.5, face="bold"),
        axis.title = element_text(size = 10),
        #axis.title.x = element_text(margin = margin(t = 8)),
        axis.text.y = element_text(size=11),
        axis.text.x = element_text(size=11, angle=15),
        plot.margin = margin(t = 0, r = 5, b = 0, l = 5, unit = "pt"))
sitrep_p7

# Projected Final IM Cost
cost_segments <- create_line_segments(sitreps_data, "PROJECTED_FINAL_IM_COST")
sitrep_p8 <- ggplot() +
  {if(!is.null(cost_segments)) geom_segment(data=cost_segments, aes(x=x, xend=xend, y=y, yend=yend, color=segment_color), size=1.8)} +
  geom_point(data=sitreps_data, aes(x=REPORT_TO_DATE, y=PROJECTED_FINAL_IM_COST, fill=date_bin, color=date_bin),
             shape=21, size = 2.4, stroke = 0.5, alpha=0.8) +
  scale_fill_manual(values = reds_palette, name = "Days Since Ignition", guide = "none") +
  scale_color_manual(values = reds_palette, guide = "none") +
  scale_y_continuous(labels = scales::label_number(suffix = " M", scale = 1e-6)) +
  scale_x_date(date_labels = "%b %d", date_breaks = "10 days", date_minor_breaks = "2 days") +
  labs(y="Cost ($)", x="Report Date",
       title="Projected Final IM Cost", tag="(h)") +
  theme_classic() +
  theme(plot.title = element_text(size=12, vjust=0.2, hjust=0.5, face="bold"),
        axis.title = element_text(size = 10),
        #axis.title.x = element_text(margin = margin(t = 8)),
        axis.text.y = element_text(size=11),
        axis.text.x = element_text(size=11, angle=15),
        plot.margin = margin(t = 0, r = 5, b = 0, l = 5, unit = "pt"))
sitrep_p8

# ============================================================================
# CREATE COMPOSITE FIGURES
# ============================================================================

# Create ICS-209 situation report panels
# ORIGINAL VERSION: Create two ICS-209 panels (4 plots each)
# Panel 1: Fire spread, burned area, total personnel, aerial resources
sitrep_panel_1 <- (sitrep_p1 + sitrep_p2 + sitrep_p6 + sitrep_p7) + 
  plot_layout(ncol = 1)
sitrep_panel_1

# Panel 2: Threatened structures, destroyed structures, total evacuations, projected final IM cost
sitrep_panel_2 <- (sitrep_p3 + sitrep_p4 + sitrep_p5 + sitrep_p8) + 
  plot_layout(ncol = 1)
sitrep_panel_2

# SIX-CHART VERSION: Remove aerial resources and threatened structures
# Panel 1: Fire spread, burned area, total personnel
sitrep_panel_1_6chart <- (sitrep_p1 + plot_spacer() + sitrep_p2 + plot_spacer() + sitrep_p6) + 
  plot_layout(ncol = 1, heights = c(4, -0.8, 4, -0.8, 4))
sitrep_panel_1_6chart

# Panel 2: Destroyed structures, total evacuations, projected final IM cost
sitrep_panel_2_6chart <- (sitrep_p4 + plot_spacer() + sitrep_p5 + plot_spacer() + sitrep_p8) + 
  plot_layout(ncol = 1, heights = c(4, -0.8, 4, -0.8, 4))
sitrep_panel_2_6chart

# Create composite figures with fire progression map and situation reports
if (exists("p1")) {  # Check if fire progression map was created successfully
  
  # ORIGINAL 8-CHART COMPOSITE FIGURE
  composite_figure <- p1 + sitrep_panel_1 + sitrep_panel_2 + 
    plot_layout(ncol = 3, widths = c(1.5, 0.5, 0.5)) +
    plot_annotation(
      title = paste("East Troublesome Fire -", target_fire_year),
      #subtitle = "Fire progression map (left) and ICS-209+ situation report data (center and right)",
      theme = theme(
        plot.title = element_text(size = 20, face = "bold"),
        #plot.subtitle = element_text(size = 16)
      )
    )
  
  # Save the original composite figure
  composite_figure
  composite_output <- file.path(figures_dir, paste0(target_fire_name, "_", target_fire_year, "_composite_analysis.png"))
  ggsave(composite_figure, filename = composite_output, 
         width = 16, height = 10, dpi = 300, bg = "white")
  cat("Original composite figure (8 charts) saved to:", composite_output, "\n")
  
  # SIX-CHART COMPOSITE FIGURE
  composite_figure_6chart <- p1 + sitrep_panel_1_6chart + sitrep_panel_2_6chart + 
    plot_layout(ncol = 3, widths = c(1.5, 0.5, 0.5)) +
    plot_annotation(
      title = paste("East Troublesome Fire, CO -", target_fire_year),
      subtitle = "MODIS active fire detection product and linked ICS-209-PLUS",
      theme = theme(
        plot.title = element_text(size = 25, face = "bold"),
        plot.subtitle = element_text(size = 16)
      )
    )
  
  # Save the 6-chart composite figure
  composite_figure_6chart
  composite_output_6chart <- file.path(figures_dir, paste0(target_fire_name, "_", target_fire_year, "_composite_analysis_6chart.png"))
  ggsave(composite_figure_6chart, filename = composite_output_6chart, 
         width = 16, height = 8, dpi = 300, bg = "white")
  cat("Six-chart composite figure saved to:", composite_output_6chart, "\n")
  
  # Also save individual sitrep panels
  sitrep_output <- file.path(figures_dir, paste0(target_fire_name, "_", target_fire_year, "_sitreps_8chart.png"))
  sitrep_panel_full <- sitrep_panel_1 + sitrep_panel_2 + plot_layout(ncol = 2)
  ggsave(sitrep_panel_full, filename = sitrep_output, 
         width = 12, height = 10, dpi = 300, bg = "white")
  cat("Eight-chart situation reports panel saved to:", sitrep_output, "\n")
  
  sitrep_output_6chart <- file.path(figures_dir, paste0(target_fire_name, "_", target_fire_year, "_sitreps_6chart.png"))
  sitrep_panel_6chart_full <- sitrep_panel_1_6chart + sitrep_panel_2_6chart + plot_layout(ncol = 2)
  ggsave(sitrep_panel_6chart_full, filename = sitrep_output_6chart, 
         width = 12, height = 8, dpi = 300, bg = "white")
  cat("Six-chart situation reports panel saved to:", sitrep_output_6chart, "\n")
  
} else {
  cat("Fire progression map not available - saving only situation reports\n")
  sitrep_output <- file.path(figures_dir, paste0(target_fire_name, "_", target_fire_year, "_sitreps_8chart.png"))
  sitrep_panel_full <- sitrep_panel_1 + sitrep_panel_2 + plot_layout(ncol = 2)
  ggsave(sitrep_panel_full, filename = sitrep_output, 
         width = 12, height = 10, dpi = 300, bg = "white")
  cat("Eight-chart situation reports panel saved to:", sitrep_output, "\n")
  
  sitrep_output_6chart <- file.path(figures_dir, paste0(target_fire_name, "_", target_fire_year, "_sitreps_6chart.png"))
  sitrep_panel_6chart_full <- sitrep_panel_1_6chart + sitrep_panel_2_6chart + plot_layout(ncol = 2)
  ggsave(sitrep_panel_6chart_full, filename = sitrep_output_6chart, 
         width = 12, height = 8, dpi = 300, bg = "white")
  cat("Six-chart situation reports panel saved to:", sitrep_output_6chart, "\n")
}

cat("Analysis complete! All figures generated.\n")