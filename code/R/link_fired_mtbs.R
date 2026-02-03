### Linking MTBS and FIRED events



### Step 0: libraries & directories ###

## Libraries
# List of packages
packages <- c("tidyverse", "sf")

# Install/load packages
lapply(packages, function(x){
 if (require(x, character.only = TRUE) == TRUE){
  
  library(x, character.only = TRUE)
  
 } else {
  
  install.packages(x)
  
 }
})



### Step 1: define projection
# EPSG:5070 - NAD83 / Conus Albers (meters)
project_crs <- 5070



### Step 2: load and buffer area of interest
westUS <- st_read("data/spatial/raw/aoi/westUS_5070.gpkg") %>%
  st_transform(project_crs)  # ensure it's in project CRS

# Add buffer around AOI (in meters)
westUS_buffered <- westUS %>%
  st_buffer(dist = 10000)



### Step 3: load perimeters, reporject to project crs, filter to AOI ###

## FIRED - events
# Perimeters updated: June 2025
firedEvents <- st_read("data/spatial/raw/FIRED/fired_conus_ak_2000_to_2025_events/fired_conus_ak_2000_to_2025_events/fired_conus_ak_2000_to_2025_events.shp") %>%
  st_transform(project_crs) %>%  # reproject to project CRS
  st_filter(westUS_buffered) %>%  # spatially filter to buffered AOI
  # Add FIRED_ prefix to all columns except geometry
  rename_with(
    .fn = ~paste0("FIRED_", .x),
    .cols = -geometry
  )

# Print FIRED time range
cat("FIRED time range:", as.character(min(firedEvents$FIRED_ig_date, na.rm = TRUE)), "to", as.character(max(firedEvents$FIRED_late_date, na.rm = TRUE)), "\n")

## MTBS perimeters
# Downloaded from https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/MTBS_Fire/data/composite_data/burned_area_extent_shapefile/mtbs_perimeter_data.zip
mtbs <- st_read("data/spatial/raw/mtbs/mtbs_perimeter_data/mtbs_perims_DD.shp") %>%
  st_transform(project_crs) %>%  # reproject to project CRS
  st_filter(westUS_buffered) %>%  # spatially filter to buffered AOI
  mutate(Ig_Date = as.Date(Ig_Date)) %>%  # convert to date format
  # Add MTBS_ prefix to all columns except geometry
  rename_with(
    .fn = ~paste0("MTBS_", .x),
    .cols = -geometry
  )

# Print MTBS time range
cat("MTBS time range:", as.character(min(mtbs$MTBS_Ig_Date, na.rm = TRUE)), "to", as.character(max(mtbs$MTBS_Ig_Date, na.rm = TRUE)), "\n")

## verify everything is in the same CRS!
list(westUS = st_crs(westUS), westUS_buffered = st_crs(westUS_buffered), firedEvents = st_crs(firedEvents), mtbs = st_crs(mtbs))



### Step 4: Prepare geometries for spatial join ###

# Set attributes to be constant (i.e. fire acres, fire name etc. will remain the
# same across fire perimeters)
st_agr(firedEvents) <- "constant"
st_agr(mtbs) <- "constant"

# Ensure geometries and cast to MULTIPOLYGON
firedEvents <- firedEvents %>%
  st_make_valid() %>%
  st_cast("MULTIPOLYGON")

mtbs <- mtbs %>%
  st_make_valid() %>%
  st_cast("MULTIPOLYGON")



### Step 5: Spatial join by year with largest overlap ###

# Determine overlapping year range
fired_years <- unique(year(firedEvents$FIRED_ig_date))
mtbs_years <- unique(year(mtbs$MTBS_Ig_Date))
years <- intersect(fired_years, mtbs_years) %>% sort()

# Print the years we'll be using to join
cat("Join years between MTBS and FIRED:", min(years), "-", max(years), "\n")

# Loop through years, perform spatial join with largest overlap
join_list <- list()
for (y in 1:length(years)) {
  cat("Processing year:", years[y], "\n")
  
  # Filter FIRED events by ignition year
  fired_yr <- firedEvents[year(firedEvents$FIRED_ig_date) == years[y], ]
  st_agr(fired_yr) <- "constant"
  
  # Filter MTBS perimeters by ignition year
  mtbs_yr <- mtbs[year(mtbs$MTBS_Ig_Date) == years[y], ]
  st_agr(mtbs_yr) <- "constant"
  
  # Perform spatial join with largest overlap
  join_list[[y]] <- st_join(mtbs_yr, fired_yr, join = st_intersects, largest = TRUE, left = FALSE)
}

# Merge all years together
joined_data <- bind_rows(join_list)

cat("Total joined features:", nrow(joined_data), "\n")



### Step 6: Calculate differences and filter ###

# Calculate area and date differences
joined_data <- joined_data %>%
  # Remove rows without a successful join
  filter(!is.na(FIRED_id)) %>%
  mutate(
    # Convert MTBS area from acres to km2
    mtbs_km2 = MTBS_BurnBndAc * 0.00404686,
    # Calculate differences
    area_diff_km2 = abs(FIRED_tot_ar_km2 - mtbs_km2),
    date_diff = abs(as.numeric(difftime(MTBS_Ig_Date, FIRED_ig_date, units = "days"))),
    perc_diff = abs((FIRED_tot_ar_km2 - mtbs_km2) / mtbs_km2) * 100
  )

# Check for duplicate IDs before filtering
cat("\n--- Before filtering ---\n")
cat("Duplicate FIRED IDs:", sum(duplicated(joined_data$FIRED_id)), "\n")
cat("Duplicate MTBS IDs:", sum(duplicated(joined_data$MTBS_Event_ID)), "\n")


## Area differences check
cat("\nArea difference (km2) summary:\n")
print(summary(joined_data$area_diff_km2))

# quick vis
ggplot(data=joined_data, aes(x=area_diff_km2)) + 
 geom_histogram(bins=10,binwidth=10)


## Ignition date differences check
cat("\nDate difference summary:\n")
print(summary(joined_data$date_diff))

# quick vis
ggplot(data=joined_data, aes(x=date_diff)) + 
 geom_histogram(bins=10,binwidth=10)


## Filter out poor matches
# Keep only records where date difference <= 25 days AND 
# (area difference < 202.34 km2 [~50,000 acres] OR percentage difference < 50%)
joined_filtered <- joined_data %>%
  filter(
    date_diff <= 25,
    area_diff_km2 < 202.34 | perc_diff < 50
  ) %>%
  # Handle duplicates: group by FIRED ID and keep the best match (smallest percentage difference)
  group_by(FIRED_id) %>%
  slice(which.min(perc_diff)) %>%
  ungroup() %>%
  # Keep distinct rows
  distinct(FIRED_id, .keep_all = TRUE)

# Check for duplicates after filtering
cat("\n--- After filtering ---\n")
cat("Duplicate FIRED IDs:", sum(duplicated(joined_filtered$FIRED_id)), "\n")
cat("Duplicate MTBS IDs:", sum(duplicated(joined_filtered$MTBS_Event_ID)), "\n")
cat("Final joined features:", nrow(joined_filtered), "\n")

# Final summary statistics
cat("\nFiltered area difference (km2) summary:\n")
print(summary(joined_filtered$area_diff_km2))

cat("\nFiltered date difference summary:\n")
print(summary(joined_filtered$date_diff))



### Step 7: Create final dataset and export ###

## OPTION: Choose which geometry to use ("FIRED" or "MTBS")
use_geometry <- "FIRED"  # Change to "MTBS" if you want MTBS perimeters

if (use_geometry == "FIRED") {
  
  # Create final dataset with FIRED geometries
  # Start with original FIRED events to get FIRED geometries
  final_joined <- firedEvents %>%
    # Keep only FIRED events that matched with MTBS
    filter(FIRED_id %in% joined_filtered$FIRED_id) %>%
    # Convert to tibble for join
    as_tibble() %>%
    # Join with ONLY MTBS columns and calculated metrics from joined_filtered
    inner_join(
      joined_filtered %>% 
        as_tibble() %>% 
        select(FIRED_id, starts_with("MTBS_"), area_diff_km2, date_diff, perc_diff, mtbs_km2),
      by = "FIRED_id"
    ) %>%
    # Convert back to sf object with FIRED geometry
    st_as_sf() %>%
    st_transform(project_crs) %>%
    st_make_valid() %>%
    st_cast("MULTIPOLYGON")
  
  cat("\nUsing FIRED geometries\n")
  
} else if (use_geometry == "MTBS") {
  
  # Use MTBS geometries (joined_filtered already has everything we need)
  final_joined <- joined_filtered
  
  cat("\nUsing MTBS geometries\n")
  
} else {
  stop("use_geometry must be either 'FIRED' or 'MTBS'")
}

# Final check for duplicates
cat("\n--- Final dataset ---\n")
cat("Total features:", nrow(final_joined), "\n")
cat("Total columns:", ncol(final_joined), "\n")
cat("Duplicate FIRED IDs:", sum(duplicated(final_joined$FIRED_id)), "\n")
cat("Duplicate MTBS Event IDs:", sum(duplicated(final_joined$MTBS_Event_ID)), "\n")

# Check for duplicate column names (issue when exporting to some formats)
if (any(duplicated(toupper(names(final_joined))))) {
  warning("Duplicate column names found (case-insensitive):")
  print(names(final_joined)[duplicated(toupper(names(final_joined)))])
} else {
  cat("Column names OK for export\n")
}

# Optional: Write to file
st_write(final_joined, "data/spatial/mod/fired_mtbs_linked.gpkg", delete_dsn = TRUE)



### Step 8 (Optional): Visualize results ###

# Histogram of area differences
ggplot(data = final_joined, aes(x = area_diff_km2)) +
  geom_histogram(bins = 30, fill = "lightblue", alpha = 0.7) +
  labs(title = "Distribution of Area Differences (FIRED - MTBS)",
       x = "Area Difference (km²)",
       y = "Count") +
  theme_bw()

# Histogram of date differences
ggplot(data = final_joined, aes(x = date_diff)) +
  geom_histogram(bins = 30, fill = "darkviolet", alpha = 0.7) +
  labs(title = "Distribution of Date Differences (MTBS - FIRED)",
       x = "Date Difference (days)",
       y = "Count") +
  theme_bw()

# Scatterplot of MTBS vs FIRED burned area
ggplot(data = final_joined, aes(x = FIRED_tot_ar_km2, y = mtbs_km2)) +
  geom_point(alpha = 0.5) +
  geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed") +
  labs(title = "MTBS vs FIRED Burned Area",
       x = "FIRED Area (km²)",
       y = "MTBS Area (km²)") +
  scale_x_continuous(trans = "log10") +
  scale_y_continuous(trans = "log10") +
  theme_bw()
