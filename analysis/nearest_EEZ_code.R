## nearest eez analysis for % disabling vs % fishing activity
## written by Heather Welch

source("/Users/heatherwelch/Dropbox/IUU_GRW/ais-gaps-and-reception/utilities/load_libraries.R")
outdir="/Users/heatherwelch/Dropbox/IUU_GRW/plots/plots_08_23_21"

#development data: https://unctadstat.unctad.org/en/classifications.html
development=read.csv("/Users/heatherwelch/Dropbox/IUU_GRW/global_data/development_status.csv") %>% mutate(country.code=as.integer(as.character(country.code)))

#country codes
codes=read.csv("https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv")
global=left_join(codes,development,by=c("country.code"="country.code"))

# governance data: https://databank.worldbank.org/source/worldwide-governance-indicators#
governance=read.csv("/Users/heatherwelch/Dropbox/IUU_GRW/global_data/Data_Extract_From_Worldwide_Governance_Indicators/governance.csv") %>% 
  mutate(a2017=as.numeric(as.character(a2017))) %>%  mutate(a2018=as.numeric(as.character(a2018))) %>% 
  mutate(bothyears=(a2017+a2018)/2) %>% group_by(Country.Name) %>% summarise(meanGov=mean(bothyears,na.rm=T)) %>% .[complete.cases(.),]

governance2=read.csv("/Users/heatherwelch/Dropbox/IUU_GRW/global_data/Data_Extract_From_Worldwide_Governance_Indicators/governance.csv")%>% 
  mutate(a2017=as.numeric(as.character(a2017))) %>%  mutate(a2018=as.numeric(as.character(a2018))) %>% 
  mutate(bothyears=(a2017+a2018)/2)

# balanced ### --------------------------------------------------------

# 1. nearest EEZ calculation and plot ####
# dist_eez=raster("/Users/heatherwelch/Dropbox/IUU_GRW/Environmental_data/rasters/static/dist_eez.grd")
dist_eez=raster("/Users/heatherwelch/Dropbox/IUU_GRW/Environmental_data/rasters_2017/static/dist_gfw.grd")
master=read.csv("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/gaps_v20201209_50nm_12hr_PresAbs_fishing_presence_08_23_21.csv") %>%
  mutate(new=glue("{flag}_{vessel_class}"))
eez_gaps=raster::extract(dist_eez,master[,2:3])
eez=st_read("/Users/heatherwelch/Dropbox/IUU_GRW/global_shp/World_EEZ_v10_20180221/eez_v10.shp") %>% 
  st_simplify(preserveTopology=TRUE, dTolerance = .2) 

##MATH###
# calculations: EEZ = 370.4 km
# calculations: EEZ + 100k = 470.4

## fishing
master2=master %>% mutate(dist_eez=eez_gaps) %>% filter(dist_eez<(470.4)&dist_eez>(370.4)) %>% ### fishing withing 100k which is where we know it peaks
  filter(gaps==0)
test=master2
coordinates(test)=~off_lon+off_lat
crs(test)=crs(eez)
test=st_as_sf(test)
c <- st_join(test, eez, st_nearest_feature)
st_write(c,glue("{outdir}/nearestEEZ100k_fishing.shp",delete_dsn=TRUE))

test=a %>% filter(vessel_class=="drifting_longlines"|vessel_class=="trawlers"|vessel_class=="tuna_purse_seines"|vessel_class=="tuna_purse_seines")
# gaps
master3=master %>% mutate(dist_eez=eez_gaps) %>% filter(dist_eez<(470.4)&dist_eez>(370.4)) %>% ### gaps withing 100k which is where we know it peaks
  filter(gaps==1) 
test=master3
coordinates(test)=~off_lon+off_lat
crs(test)=crs(eez)
test=st_as_sf(test)
d<- st_join(test, eez, st_nearest_feature)
st_write(d,glue("{outdir}/nearestEEZ100k_gaps.shp",delete_dsn=TRUE))

## gaps near eez polygons, testing overlapping claims ####
master3=master %>% mutate(dist_eez=eez_gaps) %>% filter(dist_eez<(470.4)&dist_eez>(92.6)) %>% ### gaps withing 100k which is where we know it peaks
  filter(gaps==1) 
test=master3
coordinates(test)=~off_lon+off_lat
crs(test)=crs(eez)
test=st_as_sf(test)
d<- st_join(test, eez, st_nearest_feature)
st_write(d,glue("{outdir}/nearestEEZ100k_gaps_eez_50k.shp",delete_dsn=TRUE))

master2=master %>% mutate(dist_eez=eez_gaps) %>% filter(dist_eez<(470.4)&dist_eez>(92.6)) %>% ### fishing withing 100k which is where we know it peaks
  filter(gaps==0)
test=master2
coordinates(test)=~off_lon+off_lat
crs(test)=crs(eez)
test=st_as_sf(test)
c <- st_join(test, eez, st_nearest_feature)
st_write(c,glue("{outdir}/nearestEEZ100k_fishing_eez_50k.shp",delete_dsn=TRUE))

d=st_read(glue("{outdir}/nearestEEZ100k_gaps_eez_50k.shp"))
d2=st_read(glue("{outdir}/nearestEEZ100k_fishing_eez_50k.shp"))

d=st_read(glue("{outdir}/nearestEEZ100k_gaps.shp"))
d2=st_read(glue("{outdir}/nearestEEZ100k_fishing.shp"))

test=d %>% group_by(Pol_typ,GeoName) %>% summarise(n=n()) %>% as.data.frame() %>% filter(Pol_typ=="Overlapping claim")
test2=d2 %>% group_by(Pol_typ,GeoName) %>% summarise(n_fish=n())%>% as.data.frame()%>% filter(Pol_typ=="Overlapping claim")
tt=d %>% filter(Pol_typ=="Overlapping claim") %>% pull(GeoName)%>% unique()
# test2=eez %>% as.data.frame()%>% group_by(Pol_type) %>% summarise(n2=n())

test3=left_join(test,test2,by=c("Pol_typ","GeoName"))
a=sum(test3$n_fish)
g=sum(test$n)
test4=test3 %>% mutate(n_boundaries=n_fish/a*100,n_gaps=n/g*100) %>% mutate(newidea=n_gaps/n_boundaries)

test=d %>% group_by(Pol_typ) %>% summarise(n=n()) %>% as.data.frame() #%>% filter(Pol_typ=="Overlapping claim")
test2=d2 %>% group_by(Pol_typ) %>% summarise(n_fish=n())%>% as.data.frame()#%>% filter(Pol_typ=="Overlapping claim")

test3=left_join(test,test2,by=c("Pol_typ"))
a=sum(test3$n_fish)
g=sum(test$n)
test4=test3 %>% mutate(n_boundaries=n_fish/a*100,n_gaps=n/g*100) %>% mutate(newidea=n_gaps/n_boundaries)
# https://www.dummies.com/education/math/statistics/how-to-compare-two-population-proportions/


## gaps near eez polygons, testing overlapping claims 400 ####
master3=master %>% mutate(dist_eez=eez_gaps) %>% filter(dist_eez<(770.4)&dist_eez>(92.6)) %>% ### gaps withing 100k which is where we know it peaks
  filter(gaps==1) 
test=master3
coordinates(test)=~off_lon+off_lat
crs(test)=crs(eez)
test=st_as_sf(test)
d<- st_join(test, eez, st_nearest_feature)
st_write(d,glue("{outdir}/nearestEEZ100k_gaps_eez_400k.shp",delete_dsn=TRUE))

master2=master %>% mutate(dist_eez=eez_gaps) %>% filter(dist_eez<(770.4)&dist_eez>(92.6)) %>% ### fishing withing 100k which is where we know it peaks
  filter(gaps==0)
test=master2
coordinates(test)=~off_lon+off_lat
crs(test)=crs(eez)
test=st_as_sf(test)
c <- st_join(test, eez, st_nearest_feature)
st_write(c,glue("{outdir}/nearestEEZ100k_fishing_eez_400k.shp",delete_dsn=TRUE))

d=st_read(glue("{outdir}/nearestEEZ100k_gaps_eez_400k.shp"))
d2=st_read(glue("{outdir}/nearestEEZ100k_fishing_eez_400k.shp"))

test=d %>% group_by(Pol_typ) %>% summarise(n=n()) %>% as.data.frame()
test2=d2 %>% group_by(Pol_typ) %>% summarise(n_fish=n())%>% as.data.frame()
# test2=eez %>% as.data.frame()%>% group_by(Pol_type) %>% summarise(n2=n())

test3=left_join(test2,test,by="Pol_typ")
a=sum(test3$n_fish)
g=sum(test$n)
test4=test3 %>% mutate(n_boundaries=n_fish/a*100,n_gaps=n/g*100) %>% mutate(newidea=n_gaps/n_boundaries)

### eez boundaries ####
eez2=st_read("/Users/heatherwelch/Dropbox/IUU_GRW/global_shp/World_EEZ_v10_20180221/eez_boundaries_v10.shp") %>% 
  st_simplify(preserveTopology=TRUE, dTolerance = .2) 

master3=master %>% mutate(dist_eez=eez_gaps) %>% filter(dist_eez<(470.4)&dist_eez>(92.6)) %>% ### gaps withing 100k which is where we know it peaks
  filter(gaps==1) 
test=master3
coordinates(test)=~off_lon+off_lat
crs(test)=crs(eez2)
test=st_as_sf(test)
d<- st_join(test, eez2, st_nearest_feature)
st_write(d,glue("{outdir}/nearestEEZ100k_gaps_eez_boundaries.shp",delete_dsn=TRUE))

master2=master %>% mutate(dist_eez=eez_gaps) %>% filter(dist_eez<(470.4)&dist_eez>(92.6)) %>% ### fishing withing 100k which is where we know it peaks
  filter(gaps==0)
test=master2
coordinates(test)=~off_lon+off_lat
crs(test)=crs(eez2)
test=st_as_sf(test)
c <- st_join(test, eez2, st_nearest_feature)
st_write(c,glue("{outdir}/nearestEEZ100k_fishing_boundaries.shp",delete_dsn=TRUE))

d=st_read(glue("{outdir}/nearestEEZ100k_gaps_eez_boundaries.shp"))
test=d %>% as.data.frame() %>% 
  mutate(Lin_typ=replace(Lin_typ,Lin_typ=="Unsettled median line","Unsettled"))%>%
  group_by(Lin_typ) %>% summarise(n=n())  %>% dplyr::select(c(Lin_typ,n))

test2=c %>% as.data.frame() %>% 
  mutate(Line_type=replace(Line_type,Line_type=="Unsettled median line","Unsettled"))%>%
  group_by(Line_type) %>% summarise(n_fish=n())  %>% dplyr::select(c(Line_type,n_fish)) %>% 
  rename(Lin_typ=Line_type)

test3=left_join(test2,test)
a=sum(test3$n_fish)
g=sum(test$n)
test4=test3 %>% mutate(n_boundaries=n_fish/a*100,n_gaps=n/g*100) %>% mutate(newidea=n/n_fish)

test2=eez2 %>% as.data.frame() %>% 
  mutate(Line_type=replace(Line_type,Line_type=="Unsettled median line","Unsettled"))%>%
  group_by(Line_type) %>% summarise(n2=n()) %>% dplyr::select(c(Line_type,n2))
test3=left_join(test2,test)
a=sum(test3$n2)
g=sum(test$n)
test4=test3 %>% mutate(n_boundaries=n2/a*100,n_gaps=n/g*100) %>% mutate(newidea=n/n2)

# 2. top 10 blame reduced ####
d1=st_read(glue("{outdir}/nearestEEZ100k_gaps.shp"))
d=st_read(glue("{outdir}/nearestEEZ100k_fishing.shp"))

d2=d1 %>% filter(as.character(flag)==as.character(ISO_Tr1))
nrow(d2)/nrow(d1)
map.world = map_data(map="world")
testt=map.world %>% filter(long<=180)

## fishing
test=d %>% as.data.frame() %>% dplyr::select(flag,ISO_Tr1,fshng_d) %>% group_by(flag,ISO_Tr1) %>%
  summarise(n=sum(fshng_d)) %>% ungroup() %>%  mutate(ISO_Tr1=as.character(ISO_Tr1))%>% mutate(type="Fishing") %>%
  dplyr::select(c(flag,ISO_Tr1,n,type))%>% filter(flag!="Unknown"&flag!="ATA") %>% .[complete.cases(.),]

## gaps
test1=d1 %>% as.data.frame() %>% dplyr::select(flag,ISO_Tr1,gaps) %>% group_by(flag,ISO_Tr1) %>%
  summarise(n=n()) %>% ungroup() %>%  mutate(ISO_Tr1=as.character(ISO_Tr1)) %>% 
  dplyr::select(c(flag,ISO_Tr1,n)) %>% mutate(type="Gaps")%>% filter(flag!="Unknown"&flag!="ATA")%>% .[complete.cases(.),]

## vulnerable
vfishing=test %>% group_by(ISO_Tr1) %>% summarise(total=sum(n)) %>% mutate(percentfishing=total/sum(total))%>% mutate(type="Fishing")
vgaps=test1 %>% group_by(ISO_Tr1) %>% summarise(total=sum(n)) %>% mutate(percentgaps=total/sum(total))%>% mutate(type="Gaps")
vulnerable=full_join(vfishing,vgaps,by="ISO_Tr1")%>%  mutate(ISO_Tr1=replace(ISO_Tr1,!(ISO_Tr1 %in% c("RUS","ARG","PYF","KIR","PER")),"Other"))

a=ggplot(vulnerable,aes(x=percentgaps,y=percentfishing,color=ISO_Tr1))+geom_point() 
ggplotly(a)

aVulnerable=ggplot(vulnerable,aes(x=percentgaps,y=percentfishing,color=ISO_Tr1))+geom_point(size=4)+geom_abline(slope=1,intercept=0)+
  ggtitle("A. EEZ-adjacent disabling by EEZ") +
  theme(text = element_text(size=18))+
  scale_color_manual("EEZ",values=c("RUS"="#0073C2FF",
                                    "ARG"="#EFC000FF",
                                    "PYF"= "#003C67FF",
                                    "Other"="lightgrey",
                                    "KIR"="#CD534CFF",
                                    "PER"="#8a131f"
  ),
  labels=c("RUS"="Russia",
           "ARG"="Argentina",
           "SLB"="Solomon Islands",
           "KIR"="Kiribati",
           "Other"="Other",
           "PYF"="PYF",
           "MHL"="Marshall Islands",
           "PER"="Peru",
           "JPN"="Japan"
           
  ))+theme_classic()+
  ylab("Percent of total fishing ")+xlab("Percent of total suspected disabling")+ 
  scale_y_continuous(labels = scales::percent_format(accuracy = 1),limits = c(0,.4543))+ scale_x_continuous(labels = scales::percent_format(accuracy = 1))+
  theme(legend.position = c(0.2, 0.7),
        legend.background = element_rect(fill=NULL,
                                         size=0.5, linetype="solid", 
                                         colour ="black"))

## blame
bfishing=test %>% group_by(flag) %>% summarise(total=sum(n)) %>% mutate(percentfishing=total/sum(total))%>% mutate(type="Fishing")
bgaps=test1 %>% group_by(flag) %>% summarise(total=sum(n)) %>% mutate(percentgaps=total/sum(total))%>% mutate(type="Gaps")
blame=full_join(bfishing,bgaps,by="flag")%>%  mutate(flag=replace(flag,!(flag %in% c("CHN","TWN","ESP","KOR","JPN")),"Other"))

a=ggplot(blame,aes(x=percentgaps,y=percentfishing,color=flag))+geom_point() 
ggplotly(a)

aBlame=ggplot(blame,aes(x=percentgaps,y=percentfishing,color=flag))+geom_point(size=4)+geom_abline(slope=1,intercept=0)+
  ggtitle("B. EEZ-adjacent disabling by flag state") +
  theme(text = element_text(size=18))+
  scale_color_manual("Flag state",values=c("CHN"="#0073C2FF",
                                           "TWN"="#EFC000FF",
                                           "ESP"="#8a131f",
                                           "KOR"= "#003C67FF",
                                           "Other"="lightgrey",
                                           "JPN"="#CD534CFF",
                                           "MHL"="darkgreen"
  ),
  labels=c("CHN"="China",
           "TWN"="Taiwan",
           "ESP"="Spain",
           "KOR"="Korea",
           "Other"="Other",
           "JPN"="Japan",
           "MLH"="Marshall Islands"
           
  ))+theme_classic()+
  ylab("Percent of total fishing ")+xlab("Percent of total suspected disabling")+ 
  scale_y_continuous(labels = scales::percent_format(accuracy = 1),limits = c(0,.439932))+
  scale_x_continuous(labels = scales::percent_format(accuracy = 1))+
  theme(legend.position = c(0.2, 0.7),
        legend.background = element_rect(fill=NULL,
                                         size=0.5, linetype="solid", 
                                         colour ="black"))


png(glue("{outdir}/ratios_new_blame_pos25hrb4.png"),width=10,height=10,units='cm',res=400)
par(ps=10)
par(mar=c(4,4,1,1))
par(cex=1)
aBlame
dev.off()


png(glue("{outdir}/ratios_new_vulnerable_pos25hrb4.png"),width=10,height=10,units='cm',res=400)
par(ps=10)
par(mar=c(4,4,1,1))
par(cex=1)
aVulnerable
dev.off()

png(glue("{outdir}/ratios.png"),width=20,height=10,units='cm',res=400)
par(ps=10)
par(mar=c(4,4,1,1))
par(cex=1)
grid.arrange(aVulnerable,aBlame,ncol=2)
dev.off()
  
 