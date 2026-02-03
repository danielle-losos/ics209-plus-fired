# ==============================================================================
# Script Name: structure_destruction_timeline.R
# Description: Analysis of structure destruction timelines from ICS-209 SITREPS
#              Identifies start and end dates of structure destruction for 
#              specified wildfire incidents
# 
# Author: Nate Hofford
# Email: nate.hofford@colorado.edu
# Date Created: 2026-01-05
# Last Modified: 2026-01-12
# Version: 1.0
# 
# Dependencies: tidyverse, lubridate
# ==============================================================================


# ==============================================================================
# Step 0: Libraries
# ==============================================================================

library(tidyverse)
library(lubridate)


# ==============================================================================
# Step 1: Load in ICS-209 Data
# Note: loading in two version (1999-2020) and (1999-2024) to ensure consistency
# ==============================================================================

# standard published data
sitreps_2020 <- read_csv("data/tabular/raw/ICS/ics209-plus-wf_sitreps_1999to2020.csv") %>%
 mutate(REPORT_TO_DATE = as.Date(REPORT_TO_DATE)) %>%
 arrange(INCIDENT_ID, REPORT_TO_DATE)

cat("Loaded", nrow(sitreps_2020), "total situation reports\n")



# ==============================================================================
# Step 2: Define target fires & filter ICS-209
# ==============================================================================

# Define fires with name and start year for precise matching
# Note: Marshall and Alexander Mtn. do not have full data yet since these are
# post 2020
target_fires_list <- tribble(
 ~fire_name,           ~start_year,
 "Marshall",           2021,
 "EAST TROUBLESOME",   2020,
 "Cameron Peak",       2020,
 "Spring Creek",       2018,
 "MULLEN",             2020,
 "Roosevelt",          2018,
 "Calwood",            2020,
 "Badger Hole",        2018,
 "MM 117",             2018,
 "Logan Fire",         2017,
 "Alexander Mountain", 2024
)


# Subset 2020 data - currently using this instead of 2023
# Note: finding inconsistencies between 2023 and 2020 data
# Ex) MM 117 fire daily structures destroyed
sitreps_subset <- sitreps_2020 %>%
  select(INCIDENT_ID, INCIDENT_NAME, CY, REPORT_TO_DATE, 
         STR_DESTROYED, STR_DESTROYED_RES, STR_DESTROYED_COMM,
         TOTAL_AERIAL, TOTAL_PERSONNEL, EST_IM_COST_TO_DATE)

# Join with target fires list and filter to matches
sitreps_filtered <- sitreps_subset %>%
  # Join by year
  inner_join(
    target_fires_list,
    by = c("CY" = "start_year"),
    relationship = "many-to-many"
  ) %>%
  # Keep only rows where fire name matches INCIDENT_ID or INCIDENT_NAME
  filter(
    str_detect(INCIDENT_ID, fixed(fire_name, ignore_case = TRUE)) | 
    str_detect(INCIDENT_NAME, fixed(fire_name, ignore_case = TRUE))
  ) %>%
  # Remove any duplicates (shouldn't be any, but just in case)
  distinct(INCIDENT_ID, REPORT_TO_DATE, .keep_all = TRUE) %>%
  # Sort by year, fire name, and date
  arrange(CY, fire_name, REPORT_TO_DATE)

# Print summary
cat("Found", length(unique(sitreps_filtered$INCIDENT_ID)), "matching fires\n")
cat("Total reports:", nrow(sitreps_filtered), "\n\n")

cat("Matched fires:\n")
sitreps_filtered %>%
  distinct(INCIDENT_ID, INCIDENT_NAME, CY, fire_name) %>%
  arrange(CY, fire_name) %>%
  print()


# ==============================================================================
# Step 3: Resource timeline
# Note: this version of data is missing cost and trucks
# ==============================================================================

# Create day-by-day timeline for each fire and variable
resource_timeline <- sitreps_filtered %>%
  # Replace NA values with 0 for each variable
  mutate(
    STR_DESTROYED = replace_na(STR_DESTROYED, 0),
    STR_DESTROYED_RES = replace_na(STR_DESTROYED_RES, 0),
    STR_DESTROYED_COMM = replace_na(STR_DESTROYED_COMM, 0),
    TOTAL_AERIAL = replace_na(TOTAL_AERIAL, 0),
    TOTAL_PERSONNEL = replace_na(TOTAL_PERSONNEL, 0),
    EST_IM_COST_TO_DATE = replace_na(EST_IM_COST_TO_DATE, 0)
  ) %>%
  # Calculate changes from previous report for each fire
  group_by(INCIDENT_ID, INCIDENT_NAME, fire_name, CY) %>%
  arrange(REPORT_TO_DATE) %>%
  mutate(
    # Calculate days since first report
    days_since_start = as.numeric(REPORT_TO_DATE - min(REPORT_TO_DATE)),
    # Calculate change from previous report for each variable
    str_destroyed_change = STR_DESTROYED - lag(STR_DESTROYED, default = 0),
    strRES_destroyed_change = STR_DESTROYED_RES - lag(STR_DESTROYED_RES, default = 0),
    strCOMM_destroyed_change = STR_DESTROYED_COMM - lag(STR_DESTROYED_COMM, default = 0),
    qty_aerial_change = TOTAL_AERIAL - lag(TOTAL_AERIAL, default = 0),
    pers_total_change = TOTAL_PERSONNEL - lag(TOTAL_PERSONNEL, default = 0),
    im_cost_change = EST_IM_COST_TO_DATE - lag(EST_IM_COST_TO_DATE, default = 0)
  ) %>%
  ungroup() %>%
  # Rename for clarity
  rename(start_year = CY) %>%
  # Arrange by fire and date
  arrange(start_year, fire_name, REPORT_TO_DATE)

cat("Created timeline with", nrow(resource_timeline), "daily reports\n")

# Create a summary table for quick reference
resource_summary <- resource_timeline %>%
  group_by(INCIDENT_ID, INCIDENT_NAME, fire_name, start_year) %>%
  summarize(
    # Timeline info
    first_report_date = min(REPORT_TO_DATE),
    last_report_date = max(REPORT_TO_DATE),
    total_reports = n(),
    duration_days = as.numeric(max(REPORT_TO_DATE) - min(REPORT_TO_DATE)),
    
    # Structure destruction
    peak_str_destroyed = max(STR_DESTROYED, na.rm = TRUE),
    first_str_destroyed_date = if_else(
      any(STR_DESTROYED > 0),
      min(REPORT_TO_DATE[STR_DESTROYED > 0]),
      as.Date(NA)
    ),
    last_str_destroyed_increase_date = if_else(
      any(str_destroyed_change > 0),
      max(REPORT_TO_DATE[str_destroyed_change > 0]),
      as.Date(NA)
    ),
    
    # Aerial resources
    peak_qty_aerial = max(TOTAL_AERIAL, na.rm = TRUE),
    mean_qty_aerial = mean(TOTAL_AERIAL, na.rm = TRUE),
    
    # Personnel
    peak_pers_total = max(TOTAL_PERSONNEL, na.rm = TRUE),
    mean_pers_total = mean(TOTAL_PERSONNEL, na.rm = TRUE),
    
    .groups = 'drop'
  ) %>%
  arrange(desc(peak_str_destroyed))

cat("Created summary for", nrow(resource_summary), "fires\n\n")

# Print summary table
print(resource_summary)

# ==============================================================================
# Step 4: Save outputs
# ==============================================================================

# Save detailed timeline
write_csv(resource_timeline, "data/tabular/mod/cowy_resource_timeline_detailed.csv")
cat("\nDetailed timeline saved to: data/tabular/mod/cowy_resource_timeline_detailed.csv\n")

# Save summary
write_csv(resource_summary, "data/tabular/mod/cowy_resource_timeline_summary.csv")
cat("Summary saved to: data/tabular/mod/cowy_resource_timeline_summary.csv\n")

# ==============================================================================
# Step 5: Optional - Create visualizations
# ==============================================================================

# Reshape data to long format for easier plotting
resource_timeline_long <- resource_timeline %>%
  select(INCIDENT_ID, INCIDENT_NAME, fire_name, start_year, REPORT_TO_DATE, 
         days_since_start, STR_DESTROYED, QTY_AERIAL, PERS_TOTAL) %>%
  pivot_longer(
    cols = c(STR_DESTROYED, QTY_AERIAL, PERS_TOTAL),
    names_to = "variable",
    values_to = "value"
  ) %>%
  mutate(
    variable_label = case_when(
      variable == "STR_DESTROYED" ~ "Structures Destroyed",
      variable == "QTY_AERIAL" ~ "Aerial Resources",
      variable == "PERS_TOTAL" ~ "Total Personnel",
      TRUE ~ variable
    )
  )

# Save long format for easy plotting
write_csv(resource_timeline_long, "data/tabular/mod/cowy_resource_timeline_long.csv")
cat("Long format timeline saved to: data/tabular/mod/cowy_resource_timeline_long.csv\n")

# Example: Plot timeline for each fire
# Uncomment to generate plots
# library(ggplot2)
# 
# for(fire_id in unique(resource_timeline$INCIDENT_ID)) {
#   fire_data <- resource_timeline_long %>% filter(INCIDENT_ID == fire_id)
#   fire_info <- unique(paste(fire_data$fire_name, fire_data$start_year))
#   
#   p <- ggplot(fire_data, aes(x = REPORT_TO_DATE, y = value, color = variable_label)) +
#     geom_line(linewidth = 1) +
#     geom_point(size = 2, alpha = 0.6) +
#     facet_wrap(~variable_label, scales = "free_y", ncol = 1) +
#     labs(
#       title = paste("Resource Timeline:", fire_info),
#       x = "Report Date",
#       y = "Value",
#       color = "Variable"
#     ) +
#     theme_minimal() +
#     theme(legend.position = "none")
#   
#   print(p)
# }

cat("\n=== Analysis Complete ===\n")