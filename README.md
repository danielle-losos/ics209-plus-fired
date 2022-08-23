# ICS209-PLUS; ICS+FIRED (Fire Events Delineation)

This repository contains the workflow used to generate a link between Incident Command Summary Reports from the ICS209-PLUS (1999-2020) database (St. Denis et al. 2022, in review) and satellite-derived wildfire events from the Fire Events Delineation (FIRED) algorithm (Balch et al. 2020, Mahood et al. 2022).

The process includes a multi-step approach using three different primary methods for spatially joining these two datasets; 1) joining based on available Monitoring Trends in Burn Severity (MTBS) footprints to identify largest overlapping FIRED events in space and time, 2) A buffer and spatial overlap approach using the latitude and longitude from the ICS209-PLUS Point of Origin (POO), and 3) a K-Nearest Neighbor index identifying incidents and events that are near in space.

For each of these methods, we apply spatial and temporal threshold to the joined records such that we retain matching incidents/events which have a high confidence. Additionaly, we performed extensive manual QA/QC examining outliers and well known wildfire incidents to help refine the database. This work is on-going but is now represented in St. Denis et al. (2022), in review, and includes the high confidence joins between these two important databases. Basic validation of the resulting product involves fitting a linear model to predicted final burned acres in the ICS209-PLUS using the satellite-mapped burned acres by FIRED. With R2 values of 0.95, these methods achieve high confidence joins between these two different databases.

![alt text](https://github.com/maxwellCcook/ics209-plus-fired/blob/main/figures/Westwide-PLUS_HomeLoss_byIncident_1999to2020.png?raw=true)

# Implications

This new ICS+FIRED product represents a significant database of wildfire characteristics from both incident command and wildfire behavior perspective. The ICS209-PLUS provides detailed information including values at risk, suppression resources and tactics, incident cost, and more. The FIRED database provides a new look at fire growth characteristics such as the simple fire spread rate (acre/day) and the maximum single-day fire growth (acres) along with estimates of the timing of maximum growth, the ignition location (fuzzy), and modal landcover types. 

The joined database produced in "ics-fired.Rmd" results in 14,796 incidents with co-occurring FIRED events representing an important subset of the data:

* 91.4% of residential structures destroyed
* 81.4% of total acres burned 
* 86.3% of projected/estimated supression costs

![alt text](https://github.com/maxwellCcook/ics209-plus-fired/blob/main/figures/ics209plus_ICS-FIRED_burnedArea_compare.png?raw=true)

Figure 2. (left) Results from linear model predicting ICS209-PLUS burned acres by co-occurring FIRED acres (ICS+FIRED). (right) Annual area burned reported by ICS209-PLUS and by ICS+FIRED (2001-2020).

# Additional Resources / Links

This repository also includes exploration and analysis of the ICS209-PLUS, FIRED and ICS+FIRED databases with emphasis on western U.S. wildfire incidents and residential structure loss. Check out these markdowns for visualizations of these data: ...

Links to data:

* [ICS209-PLUS (1999-2014)](https://figshare.com/articles/dataset/ICS209-PLUS_Cleaned_databases/8048252/14); 
* [ICS209-PLUS (1999-2020)]: Coming Soon ...
* [FIRED Events, CONUS + AK (2001-2021)](https://scholar.colorado.edu/concern/datasets/d504rm74m); 
* [FIREDpy GitHub Repo](https://github.com/earthlab/firedpy)

# References

St Denis, Lise A., Nathan P. Mietkiewicz, Karen C. Short, Mollie Buckland, and Jennifer K. Balch. "All-hazards dataset mined from the US National Incident Management System 1999–2014." Scientific data 7, no. 1 (2020): 1-18.

Balch, Jennifer K., Lise A. St. Denis, Adam L. Mahood, Nathan P. Mietkiewicz, Travis M. Williams, Joe McGlinchy, and Maxwell C. Cook. "Fired (Fire events delineation): An open, flexible algorithm and database of us fire events derived from the modis burned area product (2001–2019)." Remote Sensing 12, no. 21 (2020): 3498.

Mahood, Adam L., Estelle J. Lindrooth, Maxwell C. Cook, and Jennifer K. Balch. "Country-level fire perimeter datasets (2001–2021)." Scientific data 9, no. 1 (2022): 1-8.

St. Denis et al (2022) in review
