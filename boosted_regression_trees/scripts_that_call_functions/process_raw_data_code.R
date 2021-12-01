## Applies filters to disabling dataset to identify intentional events, develops pseudo absences for both disabling and fisheries activity

source("utilities/load_libraries.R")
library(pals)
outdir="/Users/heatherwelch/Dropbox/IUU_GRW/plots/plots_08_23_21";dir.create(outdir)
date="08_23_21"

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
  rasterFromXYZ(.,res=c(.25,.25)) #%>% flip(.,direction = "y")
test=dist_gfw
test[values(test)<92.6]=NA
plot(test,colNA="blue")

# read in datasets
rawgaps=read.csv("/Users/heatherwelch/Dropbox/IUU_GRW/data_2017_2019/gaps_inputs_v20210722/gap_events_features_v20210722.csv")

FAa=read.csv("/Users/heatherwelch/Dropbox/IUU_GRW/data_2017_2019/gaps_inputs_v20210722/vessel_presence_quarter_degree_v20210722_2017_to_2019.csv")

## testing Jenn's filters ####
test=rawgaps %>% filter(gap_hours>=12) %>% 
  filter(as.Date(gap_start) >= '2017-01-01' & as.Date(gap_end) <= '2019-12-31') %>%
  filter(off_distance_from_shore_m > 1852*50) %>% 
  filter(on_distance_from_shore_m > 1852*50) %>% 
  filter(positions_per_day_off > 5 & positions_per_day_on > 5) %>% 
  filter(positions_X_hours_before_sat >= 19)

# mapping function ####
make_map_gaps=function(df2=df,ThingToPlot,outdir2=outdir,presOrabs,type2=type,abs2=abs_g,dist2=dist,eez2=eez,testt2=testt){
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
    geom_point(data=ThingToPlot,aes(x = on_lon, y = on_lat)) +
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

## 4 Gaps: pres/abs; abs are fishing presence, everything is 50nm from shore ####
## 4 Fishing: pres/abs; abs are backgorund, everything is 50nm from shore
type="PresAbs"
abs_g="fishing_presence"
abs_f="background"
dist="50nm"

# ------- Gaps presences ####
gaps3=rawgaps %>% filter(gap_hours>=12) %>% 
  filter(as.Date(gap_start) >= '2017-01-01' & as.Date(gap_end) <= '2019-12-31') %>%
  # filter(off_distance_from_shore_m > 1852*50) %>% 
  # filter(on_distance_from_shore_m > 1852*50) %>% 
  filter(positions_per_day_off > 5 & positions_per_day_on > 5) %>% 
  filter(positions_X_hours_before_sat >= 19)%>%
  mutate(gaps=1)%>% 
  dplyr::select(off_lon,off_lat,on_lon,on_lat,gaps,vessel_class,flag,gap_hours)

gaps4=gaps3 %>% mutate(unique=raster::extract(template,gaps3[,1:2])) %>% 
  mutate(dist_shore_off=raster::extract(dist_gfw,gaps3[,1:2])) %>%
  mutate(dist_shore_on=raster::extract(dist_gfw,gaps3[,3:4])) %>%
  filter(dist_shore_off>92.6) %>%
  filter(dist_shore_on>92.6) %>%
  mutate(fishing_days=NA) %>% mutate(vessel_days=NA) %>% dplyr::select(-c(dist_shore_off,dist_shore_on,on_lon,on_lat))

# ggplot(gaps4,aes(x=on_lon,y=on_lat))+geom_point(aes(color=dist_shore_on))

make_map_gaps(ThingToPlot = gaps4,presOrabs = "presences")

# ------- Gaps absences ####
FA=FAa %>% filter(fishing_days>0) %>% rename(off_lon=lon_bin) %>% rename(off_lat=lat_bin) %>% mutate(gaps=0) %>% 
  mutate(unique=raster::extract(template,.[,2:1])) %>% mutate(dist_shore=raster::extract(dist_gfw,.[,2:1]))%>%
  filter(dist_shore>92.6)
gaps_absences=FA %>%  dplyr::select(c(off_lon,off_lat,gaps,vessel_class,unique,flag,fishing_days,vessel_days))  %>% 
  mutate(gap_hours=NA)

make_map_gaps(ThingToPlot = gaps_absences,presOrabs = "absences")

# ------- Fishing presences ####
FA=FAa %>% filter(fishing_days>0) %>% rename(on_lon=lon_bin) %>% rename(on_lat=lat_bin) %>% mutate(fishing=1) %>% 
  mutate(unique=raster::extract(template,.[,2:1])) %>% mutate(dist_shore=raster::extract(dist_gfw,.[,2:1]))%>%
  filter(dist_shore>92.6) 

FA2=FA%>% dplyr::select(-c(class,hours,fishing_hours,mmsi_present))

make_map_fishing(ThingToPlot = FA2,presOrabs = "presences")

# ------- Fishing absences ####
fishing_absences=rasterToPoints(template) %>% as.data.frame() %>% rename(on_lat=y) %>% rename(on_lon=x) %>% 
  mutate(fishing=0) %>% dplyr::select(-Sea.level.anomaly) %>% mutate(vessel_class="absences")%>% 
  mutate(unique=raster::extract(template,.[,1:2])) %>% mutate(dist_shore=raster::extract(dist_gfw,.[,1:2]))%>%
  filter(dist_shore>92.6) %>% 
  mutate(flag=NA) %>% 
  mutate(vessel_days=NA)  %>% mutate(fishing_days=NA)

make_map_fishing(ThingToPlot = fishing_absences,presOrabs = "absences")

# ------- combine and write out ####
gaps_final=rbind(gaps4,gaps_absences)
write.csv(gaps_final,glue("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/gaps_v20201209_{dist}_12hr_{type}_{abs_g}_{date}.csv"))

fishing_final=rbind(FA2,fishing_absences)
write.csv(fishing_final,glue("/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/fishing_{dist}_12hr_{type}_{abs_f}_{date}.csv"))




