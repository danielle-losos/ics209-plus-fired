# ============================================================================
# STRUCTURE DESTRUCTION TIMELINE ANALYSIS
# Analysis of when structures began and stopped being destroyed across fires
# ============================================================================

library(tidyverse)
library(lubridate)

# ============================================================================
# LOAD ICS-209 SITREPS DATA
# ============================================================================

sitreps_all <- read_csv("data/tabular/raw/ICS/ics209-plus-wf_sitreps_1999to2020.csv") %>%
  mutate(REPORT_TO_DATE = as.Date(REPORT_TO_DATE)) %>%
  arrange(INCIDENT_ID, REPORT_TO_DATE)

cat("Loaded", nrow(sitreps_all), "total situation reports\n")

# Remove specific incidents that should be excluded
incidents_to_exclude <- c(
  "2015_2894019_EXPERIMENTAL STATION",
  "2015_2815385_PUMP STATION",
  "2015_2947665_STATION"
)

# ============================================================================
# DEFINE TARGET FIRES
# ============================================================================

# Define fires with name and start year for precise matching
target_fires_list <- tribble(
  ~fire_name,           ~start_year,
  "Marshall",           2021,
  "EAST TROUBLESOME",   2020,
  "Cameron Peak",       2020,
  "Spring Creek",       2018,
  "MULLEN",             2020,
  "Roosevelt",          2018,
  "STATION",            2015,
  "Calwood",            2020,
  "Badger Hole",        2018,
  "MM 117",             2018,
  "Logan Fire",         2017,
  "Muddy Slide",        2021,
  "Cold Springs",       2016,
  "Alexander Mountain", 2024
)

cat("Target fires to analyze:\n")
print(target_fires_list)
cat("\n")

# Find matching incident IDs by both name and year using CY column
target_fires <- sitreps_all %>%
  # Join with target fires list by year
  inner_join(
    target_fires_list,
    by = c("CY" = "start_year"),
    relationship = "many-to-many"
  ) %>%
  # Filter where the fire name appears in the INCIDENT_ID or INCIDENT_NAME
  filter(str_detect(INCIDENT_ID, fixed(fire_name, ignore_case = TRUE)) | 
         str_detect(INCIDENT_NAME, fixed(fire_name, ignore_case = TRUE))) %>%
  distinct(INCIDENT_ID, INCIDENT_NAME, CY, fire_name) %>%
  rename(start_year = CY) %>%
  arrange(start_year, INCIDENT_NAME)

cat("Found", nrow(target_fires), "matching fires:\n")
print(target_fires)
cat("\n")

target_fires <- target_fires %>%
 filter(!INCIDENT_ID %in% incidents_to_exclude)

# Create a vector of matched incident IDs
matched_incident_ids <- target_fires$INCIDENT_ID


# Quick check for spring creek
springCreek <- sitreps_all %>% 
 filter(INCIDENT_ID == "2018_9201669_SPRING CREEK")



# ============================================================================
# ANALYZE STRUCTURE DESTRUCTION TIMELINE
# ============================================================================

structure_destruction_timeline <- sitreps_all %>%
  filter(INCIDENT_ID %in% matched_incident_ids) %>%
  select(INCIDENT_ID, INCIDENT_NAME, REPORT_TO_DATE, STR_DESTROYED) %>%
  # Replace NA values with 0
  mutate(STR_DESTROYED = replace_na(STR_DESTROYED, 0)) %>%
  # Join with target fires to get fire info
  left_join(
    target_fires %>% select(INCIDENT_ID, fire_name, start_year),
    by = "INCIDENT_ID"
  ) %>%
  group_by(INCIDENT_ID, INCIDENT_NAME, fire_name, start_year) %>%
  arrange(REPORT_TO_DATE) %>%
  mutate(
    # Calculate change from previous report
    str_destroyed_change = STR_DESTROYED - lag(STR_DESTROYED, default = 0)
  ) %>%
  summarize(
    # Fire timeline
    first_report_date = min(REPORT_TO_DATE),
    last_report_date = max(REPORT_TO_DATE),
    total_reports = n(),
    
    # Structure destruction timeline
    first_destruction_date = min(REPORT_TO_DATE[STR_DESTROYED > 0], na.rm = TRUE),
    last_destruction_increase_date = max(REPORT_TO_DATE[str_destroyed_change > 0], na.rm = TRUE),
    
    # Summary statistics
    peak_str_destroyed = max(STR_DESTROYED, na.rm = TRUE),
    total_reports_with_destruction = sum(STR_DESTROYED > 0),
    days_with_increasing_destruction = sum(str_destroyed_change > 0),
    
    # Calculate duration
    destruction_duration_days = as.numeric(last_destruction_increase_date - first_destruction_date),
    
    .groups = 'drop'
  ) %>%
  # Handle cases where no structures were destroyed
  mutate(
    first_destruction_date = if_else(is.infinite(first_destruction_date), 
                                     as.Date(NA), first_destruction_date),
    last_destruction_increase_date = if_else(is.infinite(last_destruction_increase_date), 
                                             as.Date(NA), last_destruction_increase_date),
    destruction_duration_days = if_else(is.na(first_destruction_date) | is.na(last_destruction_increase_date),
                                       as.numeric(NA), destruction_duration_days)
  ) %>%
  arrange(desc(peak_str_destroyed))

# ============================================================================
# DISPLAY RESULTS
# ============================================================================

cat("=" %>% rep(80) %>% paste(collapse = ""), "\n")
cat("STRUCTURE DESTRUCTION TIMELINE SUMMARY\n")
cat("=" %>% rep(80) %>% paste(collapse = ""), "\n\n")

for(i in seq_len(nrow(structure_destruction_timeline))) {
  row <- structure_destruction_timeline[i,]
  
  cat("FIRE:", row$INCIDENT_NAME, "(", row$start_year, ")\n")
  cat("  Incident ID:", row$INCIDENT_ID, "\n")
  cat("  Report Period:", format(row$first_report_date, "%b %d, %Y"), "to", 
      format(row$last_report_date, "%b %d, %Y"), "\n")
  
  if(!is.na(row$first_destruction_date)) {
    cat("  \n")
    cat("  *** Structure Destruction Timeline ***\n")
    cat("  First destruction reported:", format(row$first_destruction_date, "%b %d, %Y"), "\n")
    cat("  Last increase in destruction:", format(row$last_destruction_increase_date, "%b %d, %Y"), "\n")
    cat("  Duration of active destruction:", row$destruction_duration_days, "days\n")
    cat("  Peak structures destroyed:", row$peak_str_destroyed, "\n")
    cat("  Reports with destruction > 0:", row$total_reports_with_destruction, "\n")
    cat("  Days with increasing destruction:", row$days_with_increasing_destruction, "\n")
  } else {
    cat("  \n")
    cat("  *** No structure destruction reported ***\n")
  }
  
  cat("\n")
}

# ============================================================================
# EXPORT SUMMARY TABLE
# ============================================================================

# Create a clean summary table
summary_table <- structure_destruction_timeline %>%
  select(
    INCIDENT_NAME,
    start_year,
    first_destruction_date,
    last_destruction_increase_date,
    destruction_duration_days,
    peak_str_destroyed,
    days_with_increasing_destruction,
    INCIDENT_ID
  ) %>%
  rename(
    "Fire Name" = INCIDENT_NAME,
    "Year" = start_year,
    "First Destruction Date" = first_destruction_date,
    "Last Destruction Date" = last_destruction_increase_date,
    "Duration (days)" = destruction_duration_days,
    "Peak Structures Destroyed" = peak_str_destroyed,
    "Days with Increases" = days_with_increasing_destruction,
    "Incident ID" = INCIDENT_ID
  )

# Save to CSV
output_file <- "data/tabular/mod/structure_destruction_timeline.csv"
write_csv(summary_table, output_file)
cat("Summary table saved to:", output_file, "\n\n")

# Also print the table
cat("=" %>% rep(80) %>% paste(collapse = ""), "\n")
cat("SUMMARY TABLE\n")
cat("=" %>% rep(80) %>% paste(collapse = ""), "\n\n")
print(summary_table)

# ============================================================================
# CREATE DETAILED TIMELINE FOR EACH FIRE (OPTIONAL)
# ============================================================================

# Create a detailed day-by-day view for fires with structure destruction
detailed_timeline <- sitreps_all %>%
  filter(INCIDENT_ID %in% matched_incident_ids) %>%
  select(INCIDENT_ID, INCIDENT_NAME, REPORT_TO_DATE, STR_DESTROYED, STR_THREATENED) %>%
  mutate(STR_DESTROYED = replace_na(STR_DESTROYED, 0),
         STR_THREATENED = replace_na(STR_THREATENED, 0)) %>%
  group_by(INCIDENT_ID) %>%
  arrange(REPORT_TO_DATE) %>%
  mutate(
    str_destroyed_change = STR_DESTROYED - lag(STR_DESTROYED, default = 0)
  ) %>%
  filter(STR_DESTROYED > 0 | STR_THREATENED > 0) %>%
  ungroup() %>%
  arrange(INCIDENT_NAME, REPORT_TO_DATE)

# Save detailed timeline
write.csv(detailed_timeline, "cowyEngine_ics209.csv", row.names = F)

cowyEngine_ics209 <- "data/tabular/mod/structure_destruction_detailed_timeline.csv"
write_csv(cowyEngine_ics209)
cat("\nDetailed timeline saved to:", detailed_output_file, "\n")

