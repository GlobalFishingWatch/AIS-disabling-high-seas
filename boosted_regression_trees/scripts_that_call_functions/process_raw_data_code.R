## Applies filters to disabling dataset to identify intentional events, develops pseudo absences for both disabling and fisheries activity

source("utilities/load_libraries.R")
library(pals)
outdir="/Users/heatherwelch/Dropbox/IUU_GRW/plots/plots_06_17_22b";dir.create(outdir)
date="06_17_22c"

map.world = map_data(map="world")
testt=map.world %>% filter(long<=180)
df <- data.frame(xmin=-180,xmax=180,ymin=-90,ymax=90)
eez=st_read("/Users/heatherwelch/Dropbox/IUU_GRW/global_shp/World_EEZ_v10_20180221/eez_boundaries_v10.shp")%>% st_simplify(preserveTopology=TRUE, dTolerance = .2) 

# unique raster
template=raster("/Users/heatherwelch/Dropbox/IUU_GRW/Environmental_data/rasters/template/template.grd")
string=seq(1:ncell(template))
template[]=string

# dist shore raster
dist_gfw=read.csv("/Users/heatherwelch/Downloads/distance_to_shore_quarterdegree.csv") %>%
  dplyr::select(lon,lat,distance_from_shore_km) %>%
  # dist_gfw=read.csv("/Users/EcoCast/Dropbox/IUU_GRW/Environmental_data/distance_to_shore_quarterdegree.csv") %>% 
  dplyr::select(lon,lat,distance_from_shore_km) %>% 
  rasterFromXYZ(.,res=c(.25,.25)) #%>% flip(.,direction = "y")
test=dist_gfw
test[values(test)<92.6]=NA
plot(test,colNA="blue")

# read in datasets
rawgaps=read.csv("/Users/heatherwelch/Dropbox/IUU_GRW/data_2017_2019/gaps_inputs_v20210722/gap_events_features_v20220606_new.csv")
FAa=read.csv("/Users/heatherwelch/Dropbox/IUU_GRW/data_2017_2019/gaps_inputs_v20210722/vessel_presence_quarter_degree_v20210722_2017_to_2019.csv")

## testing Jenn's filters ####
test=rawgaps %>% filter(gap_hours>=12) %>% #5,474,028 gaps remaining
  filter(as.Date(gap_start) >= '2017-01-01' & as.Date(gap_end) <= '2019-12-31') %>% #5,474,028 gaps remaining
  filter(off_distance_from_shore_m > 1852*50) %>% #504,684 gaps remaining
  filter(positions_per_day_off > 10) %>% #240,264 gaps remaining
  filter(positions_12_hours_before_sat >= 14) #55,368 gaps gaps remaining

test2=rawgaps %>% filter(gap_hours>=12 &
                           as.Date(gap_start) >= '2017-01-01' &
                           as.Date(gap_end) <= '2019-12-31' &
                           off_distance_from_shore_m > 1852*50 &
                           positions_per_day_off > 10 &
                           positions_12_hours_before_sat >= 14)

# mapping function ####
make_map_gaps=function(df2=df,ThingToPlot,outdir2=outdir,presOrabs,type2=type,abs2=abs_g,dist2=dist,eez2=eez,testt2=testt){
  gg_hm =
    ggplot() +
    # geom_rect(data=df2,aes(xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax),color = "black",fill=NA)+
    geom_point(data=ThingToPlot,aes(x = off_lon, y = off_lat)) +
    scale_y_continuous(expand=c(0,0),limits = c(-90,90)) +
    scale_x_continuous(expand=c(0,0),limits = c(-180,180))+
    geom_sf(data=eez2,color="black",fill=NA)+
    geom_map(data=testt2,map=testt2,aes(map_id=region,x=long,y=lat),fill="#384a6d",color="#384a6d")+
    coord_sf()+
    theme(axis.ticks=element_blank(),
          axis.text.x=element_blank(), axis.text.y=element_blank(),
          panel.grid.major = element_blank(),
          panel.grid.minor = element_blank(),
          plot.margin = margin(0, 0, 0, 0, "cm"),
          axis.title.x=element_blank(), axis.title.y=element_blank())+
    ggtitle(glue("Gaps {type2} {abs2} {dist2}"))
  
  png(glue("{outdir2}/raw_gaps_{presOrabs}_{type2}_{abs2}_{dist2}.png"),width=34,height=22,units='cm',res=400,type = "cairo")
  par(ps=10)
  par(mar=c(4,4,1,1))
  par(cex=1)
  print({grid.arrange(gg_hm)})
  # gg_hm
  dev.off()
  
}
make_map_fishing=function(df2=df,ThingToPlot,outdir2=outdir,presOrabs,type2=type,abs2=abs_f,dist2=dist,eez2=eez,testt2=testt){
  gg_hm =
    ggplot() +
    geom_rect(data=df2,aes(xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax),color = "black",fill=NA)+
    geom_point(data=ThingToPlot,aes(x = off_lon, y = off_lat)) +
    scale_y_continuous(expand=c(0,0),limits = c(-90,90)) +
    scale_x_continuous(expand=c(0,0),limits = c(-180,180))+
    geom_sf(data=eez2,color="black",fill=NA)+
    geom_map(data=testt2,map=testt2,aes(map_id=region,x=long,y=lat),fill="#384a6d",color="#384a6d")+
    coord_sf()+
    theme(axis.ticks=element_blank(),
          axis.text.x=element_blank(), axis.text.y=element_blank(),
          panel.grid.major = element_blank(),
          panel.grid.minor = element_blank(),
          plot.margin = margin(0, 0, 0, 0, "cm"),
          axis.title.x=element_blank(), axis.title.y=element_blank())+
    ggtitle(glue("Fishing {type2} {abs2} {dist2}"))
  
  png(glue("{outdir2}/raw_fishing_{presOrabs}_{type2}_{abs2}_{dist2}.png"),width=34,height=22,units='cm',res=400,type = "cairo")
  par(ps=10)
  par(mar=c(4,4,1,1))
  par(cex=1)
  print({grid.arrange(gg_hm)})
  # gg_hm
  dev.off()
  
}

###----> gap models, p=gaps, a=fishing: ####
type="PresAbs"
abs_g="fishing_presence"
dist="50nm"

# ------- Gaps presences ####
gaps3=rawgaps %>% filter(gap_hours>=12) %>% 
  filter(as.Date(gap_start) >= '2017-01-01' & as.Date(gap_end) <= '2019-12-31') %>%
  filter(positions_per_day_off > 10) %>% 
  filter(positions_12_hours_before_sat >= 14)%>%
  mutate(gaps=1)%>% 
  dplyr::select(off_lon,off_lat,on_lon,on_lat,gaps,vessel_class,flag,gap_hours,gap_id,gap_start)

gaps4=gaps3 %>% mutate(unique=raster::extract(template,gaps3[,1:2])) %>% 
  mutate(dist_shore_off=raster::extract(dist_gfw,gaps3[,1:2])) %>%
  mutate(dist_shore_on=raster::extract(dist_gfw,gaps3[,3:4])) %>%
  filter(dist_shore_off>92.6) %>%
  mutate(fishing_days=NA) %>% mutate(hours=NA) %>% dplyr::select(-c(dist_shore_off,dist_shore_on,on_lon,on_lat))

gaps5=gaps4 %>% mutate(date=as.Date(gap_start)) %>% 
  mutate(year=year(date)) %>% 
  mutate(month=month(date)) %>% 
  mutate(month=str_pad(month,2,side="left",pad="0")) %>% 
  mutate(y_m=glue("{year}-{month}-01")) %>% 
  # rename(PA=gaps) %>% 
  dplyr::select(-c(flag,gap_hours,gap_id,gap_start,date,year,month))

make_map_gaps(ThingToPlot = gaps5,presOrabs = "presences")

# ------- Gaps absences ####
FA=FAa %>% filter(fishing_days>0) %>% rename(off_lon=lon_bin) %>% rename(off_lat=lat_bin) %>% mutate(gaps=0) %>% 
  mutate(unique=raster::extract(template,.[,2:1])) %>% mutate(dist_shore=raster::extract(dist_gfw,.[,2:1]))%>%
  filter(dist_shore>92.6)
gaps_absences=FA %>%  #dplyr::select(c(off_lon,off_lat,PA,vessel_class,unique,flag,fishing_days,hours))  %>% 
  mutate(y_m=NA) %>% 
  dplyr::select(-c(fishing_hours,mmsi_present,dist_shore,flag,class,vessel_days))

make_map_gaps(ThingToPlot = gaps_absences,presOrabs = "absences")

## combind and write out
gaps_final=rbind(gaps5,gaps_absences)
write.csv(gaps_final,glue("/Users/heatherwelch/Dropbox/IUU_GRW/modeling_data_06_03_22/gaps_v20201209_{dist}_12hr_{type}_{abs_g}_{date}.csv"))


###----> fishing models, p=fishing, a=vessel presence: ####
# (these will use the same input dataset)
type="PresAbs"
abs_f="vessel_presence"
dist="50nm"

# ------- Fishing presences ####
FA=FAa %>% filter(fishing_days>0) %>% rename(on_lon=lon_bin) %>% rename(on_lat=lat_bin) %>% mutate(fishing=1) %>% 
  mutate(unique=raster::extract(template,.[,2:1])) %>% mutate(dist_shore=raster::extract(dist_gfw,.[,2:1]))%>%
  filter(dist_shore>92.6) 

FA2=FA%>% dplyr::select(-c(class,fishing_hours,mmsi_present,flag,dist_shore,vessel_days)) %>% 
  mutate(y_m=NA)

# make_map_fishing(ThingToPlot = FA2,presOrabs = "presences")

# ------- Fishing absences ####
fish_abs=FAa %>% filter(vessel_days>0) %>% rename(on_lon=lon_bin) %>% rename(on_lat=lat_bin) %>% mutate(fishing=0) %>% 
  mutate(unique=raster::extract(template,.[,2:1])) %>% mutate(dist_shore=raster::extract(dist_gfw,.[,2:1]))%>%
  filter(dist_shore>92.6) 

fishing_absences=fish_abs%>% dplyr::select(-c(class,fishing_hours,mmsi_present,flag,dist_shore,vessel_days)) %>% 
  mutate(y_m=NA)

fishing_final=rbind(FA2,fishing_absences)
write.csv(fishing_final,glue("/Users/heatherwelch/Dropbox/IUU_GRW/modeling_data_06_03_22/fishing_{dist}_12hr_{type}_{abs_f}_{date}.csv"))


###----> vessel presence models, p=vessel presence, a=background: ####
# (these will use the same input dataset)
type="PresAbs"
abs_f="background"
dist="50nm"

# ------- Fishing presences ####
FA=FAa %>% filter(vessel_days>0) %>% rename(on_lon=lon_bin) %>% rename(on_lat=lat_bin) %>% mutate(fishing=1) %>% 
  mutate(unique=raster::extract(template,.[,2:1])) %>% mutate(dist_shore=raster::extract(dist_gfw,.[,2:1]))%>%
  filter(dist_shore>92.6) 

FA2=FA%>% dplyr::select(-c(class,fishing_hours,mmsi_present,flag,dist_shore,vessel_days)) %>% 
  mutate(y_m=NA)

# make_map_fishing(ThingToPlot = FA2,presOrabs = "presences")

# ------- Fishing absences ####
fishing_absences=rasterToPoints(template) %>% as.data.frame() %>% rename(on_lon=x) %>% rename(on_lat=y) %>% 
  mutate(fishing=0) %>% dplyr::select(-Sea.level.anomaly) %>% mutate(vessel_class="absences")%>% 
  mutate(unique=raster::extract(template,.[,1:2])) %>% mutate(dist_shore=raster::extract(dist_gfw,.[,1:2]))%>%
  filter(dist_shore>92.6) %>% 
  mutate(hours=NA,fishing_days=NA,y_m=NA) %>% 
  dplyr::select(-dist_shore)

# make_map_fishing(ThingToPlot = fishing_absences,presOrabs = "absences")

fishing_final=rbind(FA2,fishing_absences)
write.csv(fishing_final,glue("/Users/heatherwelch/Dropbox/IUU_GRW/modeling_data_06_03_22/fishing_{dist}_12hr_{type}_{abs_f}_{date}.csv"))



