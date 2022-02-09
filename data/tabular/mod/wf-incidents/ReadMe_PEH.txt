Data file with “_PEH” appended to the end was created as follows: 

1. Save original data file (e.g., ics209plus_wf_incidents_west_1999to2020_qc.csv) as
.xlsx file (b/c Phil doesn't know how to deal with multiple delimiters).  

2. Replace "NA" with "NaN" (entire cell content!).

3. Open in Matlab and append new variable CAUSE_UPDATED to the last column. The variable
originally contains the same data as CAUSE, but is updated first based on information in
FOD_CAUSE, and then manually based on checking media, CalFire, and other reports. The
code of this section is included below, so as to document the fires manually updated. 

4. Save new files (.csv and .mat format) with CAUSE_UPDATED appended as last column, and
with “_PEH” appended to the file name.

*This process is contained in the Matlab file ICS209_PLUS_West_ImportData_UpdateCauseCode.m

P. Higuera, December 2021
Updated: Jan. 2022

MANUALLY UPDATED CAUSES: 
%% 3. Add causes manually for large fire from 2020, based on InciWeb
 
idx = find(strcmp(data.INCIDENT_NAME,'HOLIDAY FARM'));
data.CAUSE_UPDATED(idx) = {'H'};
 
idx = find(strcmp(data.INCIDENT_NAME,'EAST TROUBLESOME'));
data.CAUSE_UPDATED(idx) = {'H'};
 
idx = find(strcmp(data.INCIDENT_NAME,'CAMERON PEAK'));
data.CAUSE_UPDATED(idx) = {'H'};
 
idx = find(strcmp(data.INCIDENT_NAME,'CALWOOD'));
data.CAUSE_UPDATED(idx) = {'H'};
 
idx = find(strcmp(data.INCIDENT_ID,'2018_9220077_CAMP')); % Since there
    % are multimple fires named "CAMP" this is using full incident ID. 
data.CAUSE_UPDATED(idx) = {'H'};
 
idx = find(strcmp(data.INCIDENT_ID,'2018_9236519_CARR')); % Since there
    % are multimple fires named "CARR" this is using full incident ID. 
data.CAUSE_UPDATED(idx) = {'H'};
 
idx = find(strcmp(data.INCIDENT_NAME,'WOOLSEY'));
data.CAUSE_UPDATED(idx) = {'H'};
 
idx = find(strcmp(data.INCIDENT_NAME,'MENDOCINO COMPLEX'));
data.CAUSE_UPDATED(idx) = {'H'};
 
idx = find(strcmp(data.INCIDENT_NAME,'AUGUST COMPLEX'));
data.CAUSE_UPDATED(idx) = {'L'};
 
idx = find(strcmp(data.INCIDENT_NAME,'CZU AUG LIGHTNING'));
data.CAUSE_UPDATED(idx) = {'L'};

idx = find(strcmp(data.INCIDENT_ID,'2015_2915082_VALLEY'));
data.CAUSE_UPDATED(idx) = {'H'};
% https://www.fire.ca.gov/incidents/2015/9/12/valley-fire/

idx = find(strcmp(data.INCIDENT_ID,'2017_9258165_THOMAS'));
data.CAUSE_UPDATED(idx) = {'H'};
% https://www.fire.ca.gov/incidents/2017/12/4/thomas-fire/
