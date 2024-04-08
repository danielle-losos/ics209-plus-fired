#########################################################################
# Summary of statewide structure losses from ICS-209-PLUS (version 3.0) #

library(tidyverse)
library(sf)

# Bring in the latest ICS-209-PLUS

icsdir <- '/Users/max/Library/CloudStorage/OneDrive-Personal/mcook/ics209-plus-fired/data/'

# Load the newest years (1999-2022), extract 2021, 2022
# LINK TO DATA: https://1drv.ms/u/s!Aq4s9rj0qHBimJpTLkqDe2mOUSIKbg?e=UWCDZN
ics.v3 <- read_csv(paste0(icsdir,"tabular/raw/wf-incidents/ics209-plus_v3/ics209-plus-wf_incidents_1999to2022.csv"))
ics.v3 <- ics.v3 %>% filter(START_YEAR > 2020)

# Load the 1999-2020 vintage (with complex associations)
# LINK TO DATA: https://1drv.ms/u/s!Aq4s9rj0qHBilOhfWSxj_WjsJlKYvQ?e=o4PvBB
ics.v2 <- read_csv(paste0(icsdir,"tabular/raw/wf-incidents/ics209-plus_v2/ics209plus-wildfire/ics209-plus-wf_incidents_1999to2020.csv"))


###############################################
# Handle complex associations for (1999-2020) #

# Load the complex associations table
#LINK TO DATA: https://1drv.ms/u/s!Aq4s9rj0qHBilOhjw_EcNeQIzhbIKQ?e=VX07vu
complex.df <- suppressMessages(
 read_csv(paste0(
  icsdir,
  "tabular/raw/wf-incidents/ics209-plus_v2/ics209plus-wildfire/ics209-plus-wf_complex_associations_1999to2020.csv"
 ), show_col_types = FALSE)
)

# Isolate non-complex fires
no.complexes <- ics.v2 %>% 
 # Remove some known "problem fires"
 filter(INCIDENT_ID != "2017_7211255_REDWOOD VALLEY INCIDENT",
        INCIDENT_ID != "2017_7293073_REDWOOD VALLEY INCIDENT") %>%
 # Filter incidents with 0 acres
 filter(
  FINAL_ACRES!=0,
  # remove incidents found in the complex associations table
  ! INCIDENT_ID %in% complex.df$MEMBER_INCIDENT_ID,  # member incidents
  ! INCIDENT_ID %in% complex.df$CPLX_INCIDENT_ID,  # complex fires
  # filter to fires where FPA-FOD shows 1 event or NULL
  FOD_FIRE_NUM==1 | is.na(FOD_FIRE_NUM)
 )

# Check the number of complex fires in the complex association table
length(unique(complex.df$CPLX_INCIDENT_ID))

# Isolate complex fires
complexes <- ics.v2 %>%
 filter(INCIDENT_ID %in% complex.df$CPLX_INCIDENT_ID)

# Check the number of complex fires in that match
dim(complexes)

# Bind back to the ICS209-PLUS
ics.v2 <- bind_rows(complexes,no.complexes)
rm(complexes,no.complexes,complex.df)
gc()

# Join back with the new data
ics <- bind_rows(ics.v2,ics.v3)
glimpse(ics)

rm(ics.v2, ics.v3)


################################################################
# Grab a statewide structure loss summary by year (western US) #
# For the full time period (1999-2022)

west <- c("AZ", "CO", "NV", "WY", "CA", "ID", "WA", "OR", "NM", "MT", "UT")

state.summary <- ics %>%
 filter(POO_STATE %in% west) %>%
 group_by(POO_STATE) %>%
 summarize(
  str_destroyed_total = as.integer(sum(STR_DESTROYED_TOTAL)),
  str_destroyed_res = as.integer(sum(STR_DESTROYED_RES_TOTAL)),
  area_burned_total = sum(FINAL_ACRES * 0.00404686),  # in km2
  str_destroyed_ba = str_destroyed_total / area_burned_total 
 )
glimpse(state.summary)

# Print a pretty table

library(flextable)

# Set font name for table
fontname <- "Times New Roman"
# Fix names and add units
names(state.summary) <- c(
 "State","Structures Destroyed (total)", "Structures Destroyed (residential)", "Area Burned (Km2)", "Loss Rate (#/km2)")
# Create the flextable
ft <- flextable(state.summary) %>%
 font(fontname = fontname, part = "all") %>%
 autofit() %>% 
 fit_to_width(7.5)
ft
print(ft, preview = "docx")

# Save the table
write_csv(state.summary, paste0(icsdir,"tabular/mod/ics209plus_1999-2022_west_str_loss.csv"))


###################
# Plot of CA structure loss rate by year

ca.summary <- ics %>%
 filter(POO_STATE == "CA") %>%
 group_by(START_YEAR) %>%
 summarize(
  str_destroyed_total = as.integer(sum(STR_DESTROYED_TOTAL)),
  str_destroyed_res = as.integer(sum(STR_DESTROYED_RES_TOTAL)),
  area_burned_total = sum(FINAL_ACRES * 0.00404686),  # in km2
  str_loss_rate = str_destroyed_total / area_burned_total
 )

# Bar chart
ggplot(data=ca.summary, aes(x=START_YEAR, y=str_loss_rate)) +
 geom_bar(stat="identity", fill="darkred", color="black") +
 labs(x="Fire Year", y="Structure Loss Rate (#/km2)",
      title="Structure Loss Rate by Fire Year in California (1999-2022)") +
 theme_bw()


