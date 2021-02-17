## code to post-proccess disabling and fisheries activity data and develop pseudo-absences
## writen by Heather Welch

source("functions/load_libraries.R")
library(pals)

# unique raster
template=raster("/data/environmental_and_behavioural_drivers/template.grd")
string=seq(1:ncell(template))
template[]=string

# dist shore raster
dist_shore=raster("/data/environmental_and_behavioural_drivers/dist_shore.grd")
dist_eez=raster("/data/environmental_and_behavioural_drivers/dist_eez.grd")

#--------------------------------------------- HANDLING GAPS DATA  ----------------------------------------------------------

# 1. HANDLE PRESENCES ###
rawgaps=read.csv("/Users/heatherwelch/Dropbox/IUU_GRW/data_2017_2019/gap_inputs_v20201209/gaps_v20201209_12hr_2017_to_2019.csv")
rawgaps2=rawgaps %>% filter(gap_hours>=12) %>% filter(off_distance_from_shore_m>=370400)
gaps_pos25hrb4=rawgaps2 %>% filter(positions_X_hours_before_sat >= 25) %>% 
  filter(positions_per_day_off >= 5) %>% 
  filter(positions_per_day_on>=5)%>% mutate(gaps=1)%>% 
  dplyr::select(off_lon,off_lat,gaps,vessel_class,flag,gap_hours)

gaps_pos25hrb4=gaps_pos25hrb4 %>% mutate(unique=raster::extract(template,gaps_pos25hrb4[,1:2])) %>% 
  mutate(fishing_days=NA) %>% mutate(vessel_days=NA)

# 2. Read in ABSENCES ###
FAa=read.csv("/Users/heatherwelch/Dropbox/IUU_GRW/data_2017_2019/gap_inputs_v20201209/vessel_presence_quarter_degree_v20201209_2017_to_2019.csv")

# starting with vessel_days - > this will be the dataset used in modelling ---------
FA=FAa %>%  
  mutate(unique=raster::extract(template,FAa[,2:1])) %>% mutate(dist_shore=raster::extract(dist_shore,FAa[,2:1]))%>% filter(dist_shore>370400) %>% 
    filter(vessel_days>0) %>% rename(off_lon=lon_bin) %>% rename(off_lat=lat_bin) %>% mutate(gaps=0) 

A=FA %>%  dplyr::select(c(off_lon,off_lat,gaps,vessel_class,unique,flag,fishing_days,vessel_days))  %>% 
  mutate(gap_hours=NA)

# combine and write out ###
pa_pos25hrb4=rbind(gaps_pos25hrb4,A)

write.csv(pa_pos25hrb4,glue("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/gaps_v20201209_200nm_12hr_pos25hrb4_VesselDays_12_21_20.csv"))

# now with fishing_days - > this will be the dataset used for everything else ---------
FA=FAa %>% filter(fishing_days>0) %>% rename(off_lon=lon_bin) %>% rename(off_lat=lat_bin) %>% mutate(gaps=0) %>% 
  mutate(unique=raster::extract(template,.[,2:1])) %>% mutate(dist_shore=raster::extract(dist_shore,.[,2:1]))%>% filter(dist_shore>370400) 
  
A=FA %>%  dplyr::select(c(off_lon,off_lat,gaps,vessel_class,unique,flag,fishing_days,vessel_days))  %>% 
  mutate(gap_hours=NA)

# combine and write out ###
pa_pos25hrb4=rbind(gaps_pos25hrb4,A)

write.csv(pa_pos25hrb4,glue("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/gaps_v20201209_200nm_12hr_pos25hrb4_FishingDays_12_21_20.csv"))

#--------------------------------------------- HANDLING FISHING DATA ----------------------------------------------------------

# 1. create fishing data
FAa=read.csv("/Users/heatherwelch/Dropbox/IUU_GRW/data_2017_2019/gap_inputs_v20201209/vessel_presence_quarter_degree_v20201209_2017_to_2019.csv")
FA=FAa %>% filter(fishing_days>0) %>% rename(on_lon=lon_bin) %>% rename(on_lat=lat_bin) %>% mutate(fishing=1) %>% 
  mutate(unique=raster::extract(template,.[,2:1])) %>% mutate(dist_shore=raster::extract(dist_shore,.[,2:1]))%>% filter(dist_shore>370400) 
  
FA=FA%>% dplyr::select(-c(class,hours,fishing_hours,mmsi_present))

# 2. Read in ABSENCES ###
absences=rasterToPoints(template) %>% as.data.frame() %>% rename(on_lat=y) %>% rename(on_lon=x) %>% 
  mutate(fishing=0) %>% dplyr::select(-Sea.level.anomaly) %>% mutate(vessel_class="absences")%>% 
  mutate(unique=raster::extract(template,.[,2:1])) %>% mutate(dist_shore=raster::extract(dist_shore,.[,2:1]))%>% filter(dist_shore>370400) %>% 
  mutate(flag=NA) %>% 
  mutate(vessel_days=NA)  %>% mutate(fishing_days=NA)

pa=rbind(FA,absences)
write.csv(pa,glue("/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/fishing_200nm_12_21_20.csv"))
