library(tidyverse)
getwd()

exp <- read_csv("maps/data/tabular/mod/exposure/BP_x_BUPR_gacc.csv")

exp_ <- exp %>%
 pivot_wider(id_cols = GACCAbbrev, names_from = BUPR_YEAR, values_from = bupr_sum) %>%
 rename(
  BUPR2000 = "2000",
  BUPR2005 = "2005",
  BUPR2010 = "2010",
  BUPR2015 = "2015"
 ) %>%
 relocate(BUPR2000, .after = GACCAbbrev) %>%
 relocate(BUPR2005, .after = BUPR2000) %>%
 relocate(BUPR2010, .after = BUPR2005)

write_csv(exp_, "data/tabular/mod/burnable/Burnable_Exposure_gacc.csv")
