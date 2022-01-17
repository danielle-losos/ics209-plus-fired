library(tidyverse)
library(sf)

setwd("C:/Users/mccoo/OneDrive/mcook/ics209/")
y2000 <- read.csv("data/table/BP_x_BUPR2000_zs.csv")
y2005 <- read.csv("data/table/BP_x_BUPR2005_zs.csv")
y2010 <- read.csv("data/table/BP_x_BUPR2010_zs.csv")
y2015 <- read.csv("data/table/BP_x_BUPR2015_zs.csv")

# bind them together
bnd <- bind_rows(y2000, y2005, y2010, y2015)
# Summarize
sums <- bnd %>% group_by(STUSPS, BUPR_YEAR) %>%
  summarize(bupr_sum = sum(SUM)) %>%
  ungroup()
glimpse(sums)
write.csv(sums, "data/table/Burnable_Exposure_byState.csv")
# Plot it
ggplot(data=sums, aes(x=reorder(STUSPS, -sum), y=sum, fill=factor(BUPR_YEAR))) +
  geom_bar(stat='identity', position="dodge") +
  scale_y_continuous(labels = scales::label_number(suffix = " K", scale = 1e-4)) +
  labs(fill="Semi-Decade", x="", y="Sum of Properties")

  