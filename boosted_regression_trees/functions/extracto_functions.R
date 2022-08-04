## filter down your data functions
filter_down_your_data_2017_vessel_days_BalancedRestrictive_distinct=function(dat,vessel){
  if(vessel=="all"){
    
    pa=dat  
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    A1=pa %>% filter(gaps==1)%>% distinct(unique,.keep_all=T)
    A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))
    
    total=rbind(A1,A0)
    test=total %>% group_by(gaps) %>% summarise(n=n())
    
    A1=pa %>% filter(gaps==1)%>% .[sample(nrow(.),min(test$n)),]
    A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))%>%  .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(gaps) %>% summarise(n=n())
    
  }else {
    
    variable <- rlang::sym(vessel)
    pa=dat %>% filter(vessel ==variable)
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    A1=pa %>% filter(gaps==1)%>% distinct(unique,.keep_all=T)
    A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))
    
    total=rbind(A1,A0)
    test=total %>% group_by(gaps) %>% summarise(n=n())
    
    A1=pa %>% filter(gaps==1)%>% .[sample(nrow(.),min(test$n)),]
    A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))%>%  .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(gaps) %>% summarise(n=n())
    
  }
  
  return(master)
}
filter_down_your_data_fishing_2017_vessel_days_distinct=function(dat,vessel){
  if(vessel=="all"){
    
    
    presences=dat %>% filter(fishing==1) %>% distinct(unique,.keep_all=T) 
    
    absences=dat %>% filter(fishing==0) %>% filter(!(unique %in%presences$unique))
    pa=rbind(presences,absences)
    test=pa %>% group_by(fishing) %>% summarise(n=n())
    
    A0=pa %>% filter(fishing==0)%>% .[sample(nrow(.),min(test$n)),]
    A1=pa %>% filter(fishing==1)%>% .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(fishing) %>% summarise(n=n())
    
  }else {
    
    variable <- rlang::sym(vessel)
    presences=dat %>% filter(vessel ==variable)  %>%  filter(fishing==1) %>% mutate(group="P") %>% distinct(unique,.keep_all=T)
    # absences=dat %>% filter(vessel !=variable)%>% filter(!(unique %in%presences$unique))%>% mutate(group="A")
    absences=dat  %>% filter(vessel ==variable)%>% filter(fishing==0)%>% filter(!(unique %in%presences$unique))%>% mutate(group="A")
    
    pa=rbind(presences,absences)
    test=pa %>% group_by(group) %>% summarise(n=n())
    
    # if(nrow(presences)>30000){
    #   A0=pa %>% filter(group=="A")%>% .[sample(nrow(.),min(test$n)),]
    #   A1=pa %>% filter(group=="P")%>% .[sample(nrow(.),min(test$n)),]
    # } else {
    A0=pa %>% filter(group=="A")%>% .[sample(nrow(.),min(test$n)),]
    A1=pa %>% filter(group=="P")%>% .[sample(nrow(.),min(test$n)),]
    # }
    
    master=do.call("rbind",list(A0,A1)) %>% 
      mutate(fishing=case_when(group=="A"~0,
                               group=="P"~1))
    test=master %>% group_by(fishing) %>% summarise(n=n())
    
  }
  
  return(master)
}
filter_down_your_data_2017_vessel_days_BalancedRestrictive_distinct_reviewer4=function(dat,vessel){
  if(vessel=="all"){
    
    pa=dat  
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    A1=pa %>% filter(gaps==1)%>% distinct(unique,.keep_all=T)
    A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))
    
    total=rbind(A1,A0)
    test=total %>% group_by(gaps) %>% summarise(n=n())
    
    # A1=pa %>% filter(gaps==1)%>% .[sample(nrow(.),min(test$n)),]
    # A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))%>%  .[sample(nrow(.),min(test$n)),]
    
    master=total
    test=master %>% group_by(gaps) %>% summarise(n=n())
    
  }else {
    
    variable <- rlang::sym(vessel)
    pa=dat %>% filter(vessel ==variable)
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    A1=pa %>% filter(gaps==1)%>% distinct(unique,.keep_all=T)
    A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))
    
    total=rbind(A1,A0)
    test=total %>% group_by(gaps) %>% summarise(n=n())
    
    # A1=pa %>% filter(gaps==1)%>% .[sample(nrow(.),min(test$n)),]
    # A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))%>%  .[sample(nrow(.),min(test$n)),]
    
    master=total
    test=master %>% group_by(gaps) %>% summarise(n=n())
    
  }
  
  return(master)
}

## extract functions
extracto_static_wrapper_2017_vessel_days_BalancedRestrictive_off_distinct=function(vessel,outdir,csv_path,date,type2){
  print(vessel)
  if(type2=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type2=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type2=="all"){dat=read.csv(csv_path)}
  print(1)
  DF=filter_down_your_data_2017_vessel_days_BalancedRestrictive_distinct(dat=dat,vessel=vessel)%>%
    rename(sum_gaps=gaps)
  test=DF %>% group_by(sum_gaps) %>% summarise(n=n())
  rm(dat)
  nrow(DF)
  staticDir="/Users/heatherwelch/Dropbox/IUU_GRW/Environmental_data/rasters_2017/static"
  x="off_lon"
  y="off_lat"
  rasters=list.files(staticDir,full.names = T,pattern = ".grd")
  raster_names=list.files(staticDir,full.names = F,pattern = ".grd") %>% gsub(".grd","",.)
  list=list()
  test=mcmapply(extracto_static_bullshit,raster=rasters,colname=raster_names,MoreArgs=list(x=x,y=y,DF=DF,envDirs = staticDir,mapply=T,list=list))
  # names(test)=raster_names
  # new=lapply(test,function(x)dplyr::select(-c(on_lon,on_lat,sum_gaps)))
  new=do.call(cbind,test) %>% cbind(DF[,2:4],.)
  # new=test %>% reduce(left_join,by=c("on_lon","on_lat","sum_gaps")) %>% dplyr::select(-c(eez)) ## removing this as i think it's biasing and a problem
  new2=new %>% mutate(sum_gaps=DF$sum_gaps) %>% mutate(vessel_class=DF$vessel_class) %>% mutate(class=DF$class) %>% mutate(unique=DF$unique)
  head(new2)
  if(type2=="flag_vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}.csv"))}
  if(type2=="vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}_VC.csv"))}
  if(type2=="all"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}.csv"))}
  rm(DF,test,new,new2)
}
extracto_static_wrapper_fishing_2017_vessel_days_distinct=function(date,csv_path,vessel,outdir,type2){
  print(vessel)
  if(type2=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type2=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type2=="all"){dat=read.csv(csv_path)}
  DF=filter_down_your_data_fishing_2017_vessel_days_distinct(dat=dat,vessel=vessel)%>% 
    rename(sum_gaps=fishing)
  test=DF %>% group_by(sum_gaps) %>% summarise(n=n())
  rm(dat)
  
  staticDir="/Users/heatherwelch/Dropbox/IUU_GRW/Environmental_data/rasters_2017/static"
  x="on_lon"
  y="on_lat"
  rasters=list.files(staticDir,full.names = T,pattern = ".grd")
  raster_names=list.files(staticDir,full.names = F,pattern = ".grd") %>% gsub(".grd","",.)
  list=list()
  test=mcmapply(extracto_static_bullshit,raster=rasters,colname=raster_names,MoreArgs=list(x=x,y=y,DF=DF,envDirs = staticDir,mapply=T,list=list))
  # names(test)=raster_names
  # new=lapply(test,function(x)dplyr::select(-c(on_lon,on_lat,sum_gaps)))
  new=do.call(cbind,test) %>% cbind(DF[,2:4],.)
  # new=test %>% reduce(left_join,by=c("on_lon","on_lat","sum_gaps")) %>% dplyr::select(-c(eez)) ## removing this as i think it's biasing and a problem
  new2=new %>% mutate(sum_gaps=DF$sum_gaps) %>% mutate(vessel_class=DF$vessel_class) %>% mutate(class=DF$class) %>% mutate(unique=DF$unique)
  if(type2=="flag_vessel_class"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))}
  if(type2=="vessel_class"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}_VC.csv"))}
  if(type2=="all"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))}
  # write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))
  rm(DF,test,new,new2)
}
extracto_static_wrapper_2017_vessel_days_BalancedRestrictive_off_distinct_reviewer4=function(vessel,outdir,csv_path,date,type2){
  print(vessel)
  variable <- rlang::sym(vessel)
  if(type2=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type2=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type2=="all"){dat=read.csv(csv_path)}
  print(1)
  DF=filter_down_your_data_2017_vessel_days_BalancedRestrictive_distinct_reviewer4(dat=dat,vessel=vessel)%>%
    rename(sum_gaps=gaps)
  test=DF %>% group_by(sum_gaps) %>% summarise(n=n())
  rm(dat)
  nrow(DF)
  staticDir="/Users/heatherwelch/Dropbox/IUU_GRW/Environmental_data/rasters_2017/static"
  x="off_lon"
  y="off_lat"
  rasters=list.files(staticDir,full.names = T,pattern = ".grd")
  raster_names=list.files(staticDir,full.names = F,pattern = ".grd") %>% gsub(".grd","",.)
  list=list()
  test=mcmapply(extracto_static_bullshit,raster=rasters,colname=raster_names,MoreArgs=list(x=x,y=y,DF=DF,envDirs = staticDir,mapply=T,list=list))
  # names(test)=raster_names
  # new=lapply(test,function(x)dplyr::select(-c(on_lon,on_lat,sum_gaps)))
  new=do.call(cbind,test) %>% cbind(DF[,2:4],.)
  # new=test %>% reduce(left_join,by=c("on_lon","on_lat","sum_gaps")) %>% dplyr::select(-c(eez)) ## removing this as i think it's biasing and a problem
  new2=new %>% mutate(sum_gaps=DF$sum_gaps) %>% mutate(vessel_class=DF$vessel_class) %>% mutate(class=DF$class) %>% mutate(unique=DF$unique)
  head(new2)
  if(type2=="flag_vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}.csv"))}
  if(type2=="vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}_VC_reviewer4.csv"))}
  if(type2=="all"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}_reviewer4.csv"))}
  rm(DF,test,new,new2)
}
