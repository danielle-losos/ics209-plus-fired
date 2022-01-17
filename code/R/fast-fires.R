#########
##
#########

# Subset ICS-209s to fast fires
fast.fires <- ics %>% 
  mutate(WF_MAX_FSR_HA = WF_MAX_FSR * 0.404686,
         EVACUATION_REPORTED = as.factor(EVACUATION_REPORTED)) %>%
  filter(WF_MAX_FSR_HA > 1620)

# Check some numbers
dim(fast.fires%>%filter(is.na(EVACUATION_REPORTED)))
summary(fast.fires$EVACUATION_REPORTED)

sum(fast.fires$STR_DESTROYED_TOTAL)
sum(fast.fires$STR_DESTROYED_RES_TOTAL)
