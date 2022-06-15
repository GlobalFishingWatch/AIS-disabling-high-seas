## script of functions to extract env data from rasters
# need three different temporal options: 1. daily, 2. monthly, 3. static

extracto_static=function(x,y,DF,envDirs,raster,colname,mapply=F,list){
  pnts=DF %>% dplyr::select(x,y)
  print(raster)
  if(mapply==T){
    raster=raster(raster)
  }
  a=raster::extract(raster,pnts)
  pnts$new=a
  position=grep("new",names(pnts))
  colnames(pnts)[position]=colname
  final=pnts %>% mutate(sum_gaps=DF$sum_gaps)
  if(mapply==T){
    list[[length(list)+1]]=final
    return(list)
  }
  else{return(final)}
}

extracto_static_wrapper=function(date,positions,nm,csv_path,vessel,outdir){
  dat=read.csv(csv_path)
  DF=filter_down_your_data(dat=dat,vessel=vessel,positions=positions)%>% 
    rename(sum_gaps=gaps)
  rm(dat)
  
  staticDir="/Users/heatherwelch/Dropbox/IUU_GRW/Environmental_data/rasters/static"
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
  write_csv(new2,glue("{outdir}/extracto/gaps_classAB_sat_{positions}fracD_{nm}nm_12hr_{date}_{vessel}.csv"))
  rm(DF,test,new,new2)
}

extracto_static_wrapper_2017_old=function(date,positions,csv_path,vessel,outdir){
  print(vessel)
  # dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))
  # DF=filter_down_your_data_2017(dat=dat,vessel=vessel,positions=positions)%>% 
  #   rename(sum_gaps=gaps)
  # rm(dat)
  # 
  # staticDir="/Users/heatherwelch/Dropbox/IUU_GRW/Environmental_data/rasters_2017/static"
  # x="on_lon"
  # y="on_lat"
  # rasters=list.files(staticDir,full.names = T,pattern = ".grd")
  # raster_names=list.files(staticDir,full.names = F,pattern = ".grd") %>% gsub(".grd","",.)
  # list=list()
  # test=mcmapply(extracto_static_bullshit,raster=rasters,colname=raster_names,MoreArgs=list(x=x,y=y,DF=DF,envDirs = staticDir,mapply=T,list=list))
  # # names(test)=raster_names
  # # new=lapply(test,function(x)dplyr::select(-c(on_lon,on_lat,sum_gaps)))
  # new=do.call(cbind,test) %>% cbind(DF[,2:4],.)
  # # new=test %>% reduce(left_join,by=c("on_lon","on_lat","sum_gaps")) %>% dplyr::select(-c(eez)) ## removing this as i think it's biasing and a problem
  # new2=new %>% mutate(sum_gaps=DF$sum_gaps) %>% mutate(vessel_class=DF$vessel_class) %>% mutate(class=DF$class) %>% mutate(unique=DF$unique)
  # write_csv(new2,glue("{outdir}/gaps_classAB_sat_{positions}fracD_200nm_12hr_{date}_{vessel}.csv"))
  # rm(DF,test,new,new2)
}

extracto_static_wrapper_2017=function(vessel,outdir,positions,csv_path,date,type){
  print(vessel)
  if(type=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type=="all"){dat=read.csv(csv_path)}
  print(1)
  DF=filter_down_your_data_2017(dat=dat,vessel=vessel,positions=positions)%>%
    rename(sum_gaps=gaps)
  rm(dat)
  nrow(DF)
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
  head(new2)
  if(type=="flag_vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_{positions}fracD_200nm_12hr_{date}_{vessel}.csv"))}
  if(type=="vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_{positions}fracD_200nm_12hr_{date}_{vessel}_VC.csv"))}
  if(type=="all"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_{positions}fracD_200nm_12hr_{date}_{vessel}.csv"))}
  rm(DF,test,new,new2)
}

extracto_static_wrapper_2017_vessel_days=function(vessel,outdir,positions,csv_path,date,type){
  print(vessel)
  if(type=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type=="all"){dat=read.csv(csv_path)}
  print(1)
  DF=filter_down_your_data_2017_vessel_days(dat=dat,vessel=vessel,positions=positions)%>%
    rename(sum_gaps=gaps)
  rm(dat)
  nrow(DF)
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
  head(new2)
  if(type=="flag_vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_{positions}fracD_200nm_12hr_{date}_{vessel}.csv"))}
  if(type=="vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_{positions}fracD_200nm_12hr_{date}_{vessel}_VC.csv"))}
  if(type=="all"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_{positions}fracD_200nm_12hr_{date}_{vessel}.csv"))}
  rm(DF,test,new,new2)
}

extracto_static_wrapper_2017_vessel_days_BalancedRestrictive=function(vessel,outdir,csv_path,date,type){
  print(vessel)
  if(type=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type=="all"){dat=read.csv(csv_path)}
  print(1)
  DF=filter_down_your_data_2017_vessel_days_BalancedRestrictive(dat=dat,vessel=vessel)%>%
    rename(sum_gaps=gaps)
  rm(dat)
  nrow(DF)
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
  head(new2)
  if(type=="flag_vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}.csv"))}
  if(type=="vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}_VC.csv"))}
  if(type=="all"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}.csv"))}
  rm(DF,test,new,new2)
}

extracto_static_wrapper_2017_vessel_days_BalancedRestrictive_off=function(vessel,outdir,csv_path,date,type){
  print(vessel)
  if(type=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type=="all"){dat=read.csv(csv_path)}
  print(1)
  DF=filter_down_your_data_2017_vessel_days_BalancedRestrictive(dat=dat,vessel=vessel)%>%
    rename(sum_gaps=gaps)
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
  if(type=="flag_vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}.csv"))}
  if(type=="vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}_VC.csv"))}
  if(type=="all"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}.csv"))}
  rm(DF,test,new,new2)
}
extracto_static_wrapper_2017_vessel_days_BalancedRestrictive_off_Nondistinct=function(vessel,outdir,csv_path,date,type2){
  print(vessel)
  if(type2=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type2=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type2=="all"){dat=read.csv(csv_path)}
  print(1)
  DF=filter_down_your_data_2017_vessel_days_BalancedRestrictive(dat=dat,vessel=vessel)%>%
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

extracto_static_wrapper_2017_vessel_days_BalancedRestrictive_off_background=function(vessel,outdir,csv_path,date,type){
  print(vessel)
  if(type=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type=="all"){dat=read.csv(csv_path)}
  print(1)
  DF=filter_down_your_data_2017_vessel_days_BalancedRestrictive_background(dat=dat,vessel=vessel)%>%
    rename(sum_gaps=gaps)
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
  if(type=="flag_vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}.csv"))}
  if(type=="vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}_VC.csv"))}
  if(type=="all"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}.csv"))}
  rm(DF,test,new,new2)
}
extracto_static_wrapper_2017_vessel_days_BalancedRestrictive_off_background_distinct=function(vessel,outdir,csv_path,date,type2){
  print(vessel)
  if(type2=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type2=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type2=="all"){dat=read.csv(csv_path)}
  print(1)
  DF=filter_down_your_data_2017_vessel_days_BalancedRestrictive_background_distinct(dat=dat,vessel=vessel)%>%
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

extracto_static_wrapper_2017_tonnage=function(vessel,outdir,positions,csv_path,date,type){
  print(vessel)
  if(type=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type=="all"){dat=read.csv(csv_path)}
  print(1)
  DF=filter_down_your_data_2017(dat=dat,vessel=vessel,positions=positions)%>%
    rename(sum_gaps=gaps)
  rm(dat)
  nrow(DF)
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
  new2=new %>% mutate(sum_gaps=DF$sum_gaps) %>% mutate(vessel_class=DF$vessel_class) %>% mutate(class=DF$class) %>% mutate(unique=DF$unique) %>% 
    mutate(tonnage=DF$vessel_tonnage_gt)
  head(new2)
  if(type=="flag_vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_{positions}fracD_200nm_12hr_{date}_{vessel}.csv"))}
  if(type=="vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_{positions}fracD_200nm_12hr_{date}_{vessel}_VC.csv"))}
  if(type=="all"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_{positions}fracD_200nm_12hr_{date}_{vessel}.csv"))}
  rm(DF,test,new,new2)
}

extracto_static_wrapper_2017_rfmo=function(vessel,outdir,positions,csv_path,date,type){
  print(vessel)
  if(type=="rfmo"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{rfmo}")) %>% filter(vessel_class=="drifting_longlines"|vessel_class=="other_purse_seines"|vessel_class=="trollers"|vessel_class=="pole_and_line"|vessel_class=="tuna_purse_seines")}
  print(1)
  DF=filter_down_your_data_2017_rfmo(dat=dat,vessel=vessel,positions=positions)%>%
    rename(sum_gaps=gaps)
  rm(dat)
  nrow(DF)
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
  new2=new %>% mutate(sum_gaps=DF$sum_gaps) %>% mutate(vessel_class=DF$vessel_class) %>% mutate(class=DF$class) %>% mutate(unique=DF$unique) %>% 
    mutate(rfmo=DF$rfmo)
  head(new2)
  if(type=="rfmo"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_{positions}fracD_200nm_12hr_{date}_{vessel}.csv"))}
  rm(DF,test,new,new2)
}

extracto_static_wrapper_fishing=function(date,nm,csv_path,vessel){
  dat=read.csv(csv_path)
  DF=filter_down_your_data_fishing(dat=dat,vessel=vessel)%>% 
    rename(sum_gaps=fishing)
  rm(dat)
  
  staticDir="/Users/heatherwelch/Dropbox/IUU_GRW/Environmental_data/rasters/static"
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
  write_csv(new2,glue("/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling/extracto/fishing_classAB_sat_{nm}nm_12hr_{date}_{vessel}.csv"))
  rm(DF,test,new,new2)
}

extracto_static_wrapper_fishing_2017=function(date,csv_path,vessel,outdir,type){
  print(vessel)
  if(type=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type=="all"){dat=read.csv(csv_path)}
  DF=filter_down_your_data_fishing_2017(dat=dat,vessel=vessel)%>% 
    rename(sum_gaps=fishing)
  # test=DF %>% group_by(fishing) %>% summarise(n=n())
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
  if(type=="flag_vessel_class"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))}
  if(type=="vessel_class"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}_VC.csv"))}
  if(type=="all"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))}
  # write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))
  rm(DF,test,new,new2)
}
extracto_static_wrapper_fishing_2017_background_distinct=function(date,csv_path,vessel,outdir,type2){
  print(vessel)
  if(type2=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type2=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type2=="all"){dat=read.csv(csv_path)}
  DF=filter_down_your_data_fishing_2017_background_distinct(dat=dat,vessel=vessel)%>% 
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
extracto_static_wrapper_fishing_2017_background_distinct_reviewer4=function(date,csv_path,vessel,outdir,type2){
  print(vessel)
  variable <- rlang::sym(vessel)
  if(type2=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type2=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type2=="all"){dat=read.csv(csv_path)}
  DF=filter_down_your_data_fishing_2017_background_distinct_reviewer4(dat=dat,vessel=vessel)%>%
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
  if(type2=="flag_vessel_class"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}_reviewer4.csv"))}
  if(type2=="vessel_class"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}_VC_reviewer4.csv"))}
  if(type2=="all"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}_reviewer4.csv"))}
  # write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))
  rm(DF,test,new,new2)
}

extracto_static_wrapper_fishing_2017_background_Nondistinct=function(date,csv_path,vessel,outdir,type2){
  print(vessel)
  if(type2=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type2=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type2=="all"){dat=read.csv(csv_path)}
  DF=filter_down_your_data_fishing_2017_background_Nondistinct(dat=dat,vessel=vessel)%>% 
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
extracto_static_wrapper_fishing_2017_background_distinct_absIncldOtherGears=function(date,csv_path,vessel,outdir,type2){
  print(vessel)
  if(type2=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type2=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type2=="all"){dat=read.csv(csv_path)}
  DF=filter_down_your_data_fishing_2017_background_distinct_absIncldOtherGears(dat=dat,vessel=vessel)%>% 
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

extracto_static_wrapper_fishing_2017_tonnage=function(date,csv_path,vessel,outdir,type){
  print(vessel)
  if(type=="flag_vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))}
  if(type=="vessel_class"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{vessel_class}"))}
  if(type=="all"){dat=read.csv(csv_path)}
  DF=filter_down_your_data_fishing_2017(dat=dat,vessel=vessel)%>% 
    rename(sum_gaps=fishing)
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
  new2=new %>% mutate(sum_gaps=DF$sum_gaps) %>% mutate(vessel_class=DF$vessel_class) %>% mutate(class=DF$class) %>% mutate(unique=DF$unique) %>% 
    mutate(tonnage=DF$size_class)
  if(type=="flag_vessel_class"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))}
  if(type=="vessel_class"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}_VC.csv"))}
  if(type=="all"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))}
  # write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))
  rm(DF,test,new,new2)
}

extracto_static_wrapper_fishing_2017_rfmo=function(date,csv_path,vessel,outdir,type){
  print(vessel)
  if(type=="rfmo"){dat=read.csv(csv_path) %>% mutate(vessel=glue("{rfmo}"))}
  DF=filter_down_your_data_fishing_2017_rfmo(dat=dat,vessel=vessel)%>% 
    rename(sum_gaps=fishing)
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
  new2=new %>% mutate(sum_gaps=DF$sum_gaps) %>% mutate(vessel_class=DF$vessel_class) %>% mutate(class=DF$class) %>% mutate(unique=DF$unique) %>% 
    mutate(tonnage=DF$size_class)
  if(type=="flag_vessel_class"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))}
  if(type=="vessel_class"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}_VC.csv"))}
  if(type=="all"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))}
  # write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))
  rm(DF,test,new,new2)
}
  

extracto_static_bullshit=function(x,y,DF,envDirs,raster,colname,mapply=F,list){
  pnts=DF %>% dplyr::select(x,y)
  print(raster)
  if(mapply==T){
    raster=raster(raster)
  }
  a=raster::extract(raster,pnts)
  pnts$new=a
  position=grep("new",names(pnts))
  colnames(pnts)[position]=colname
  final=pnts %>% dplyr::select(-c(x,y))
  # final=pnts %>% mutate(sum_gaps=DF$sum_gaps)
  if(mapply==T){
    list[[length(list)+1]]=final
    return(list)
  }
  else{return(final)}
}

extracto_monthly=function(date,rasters,DF,listo,colname,dateCol,x,y){ ## this works, get better name, make it so it can work with just one raster
  print(date)
  subb=DF %>% filter(!!as.name(dateCol)==date)%>% dplyr::select(x,y)
  pile=grep(date,rasters,value = T) %>% stack()
  names=grep(date,rasters,value = T) %>% strsplit("/") %>% lapply(.,function(x)extract2(x,9)) %>% unlist() %>% gsub(".grd","",.)
  names(pile)=names
  a=raster::extract(pile,subb)%>%as.data.frame() %>%  mutate(!!dateCol := date)
  # subb=subb %>% mutate(!!colname := a) %>% mutate(!!dateCol := date)
  final=a
  full_names=grep("2018-07-01",rasters,value = T) %>% strsplit("/") %>% lapply(.,function(x)extract2(x,9)) %>% unlist() %>% gsub(".grd","",.)
  missing=setdiff(full_names,names(final))
  final[missing]=NA
  listo[[length(listo)+1]]=final
  return(listo)
}

# extracto_monthly_single=function(x,y,DF,envDirs,raster,colname,dateCol){
#   dates=unique(DF[,dateCol])
#   date_list=list()
#   
#   test=lapply(dates,FUN=inner_extracto,raster=raster,DF=DF,listo=date_list,colname=colname)
#   test2=do.call(rbind,test) 
#   test3=do.call(rbind,test2)
# 
#   return(test3)
# }
# 
# extracto_monthly_multiple=function(x,y,DF,envDirs,raster,colname,dateCol,mapply=F,list){
#   dates=unique(DF[,dateCol])
#   date_list=list()
#   print("one")
#   
#   if(mapply==T){
#     raster=raster(raster)
#     print("Mapply")
#   }
#   
#   test=lapply(dates,FUN=inner_extracto_multi,raster=raster,DF=DF,listo=date_list,colname=colname)
#   test2=do.call(rbind,test) 
#   test3=do.call(rbind,test2)
#   
#   if(mapply==T){
#     outer_list[[length(outer_list)+1]]=test3
#     return(outer_list)
#   }
#   else{return(test3)}
# }
# 
# DF=b %>% mutate(on_timestamp=ymd_hms(on_timestamp)%>% date()%>% substr(.,1,8) %>% paste0(.,"01"))
# dateCol="on_timestamp"
# # colname="bathy_sd"
# # bathy_sd=raster("/Volumes/SeaGate/IUU_GRW/Environmental_data/rasters/static/bathy_sd.grd")
# # test=extracto_monthly(x=x,y=y,DF=DF,envDirs = monthlyDir,raster = bathy_sd, colname = "bathy",dateCol = dateCol)
# 
# # inner_extracto_multi=function(date,raster,DF,listo,colname){
# #   subb=DF %>% filter(!!as.name(dateCol)==date)%>% dplyr::select(x,y)
# #   a=raster::extract(raster,subb)
# #   subb=subb %>% mutate(!!colname := a) %>% mutate(!!dateCol := date)
# #   final=subb
# #   print(class(final))
# #   listo[[length(listo)+1]]<-final
# #   return(listo)
# # }
# 
# dates=unique(DF[,dateCol])
# date=dates[length(dates)]
# rasters=list.files(monthlyDir,full.names = T,pattern = ".grd",recursive = T)
# listo=list()
# 
# current_col=full_names[4:length(full_names)]
# 
# inner_extracto_multi2=function(date,rasters,DF,listo,colname,dateCol,x,y){
#   print(date)
#   subb=DF %>% filter(!!as.name(dateCol)==date)%>% dplyr::select(x,y)
#   pile=grep(date,rasters,value = T) %>% stack()
#   names=grep(date,rasters,value = T) %>% strsplit("/") %>% lapply(.,function(x)extract2(x,9)) %>% unlist() %>% gsub(".grd","",.)
#   names(pile)=names
#   a=raster::extract(pile,subb)%>%as.data.frame() %>%  mutate(!!dateCol := date)
#   # subb=subb %>% mutate(!!colname := a) %>% mutate(!!dateCol := date)
#   final=a
#   print(class(final))
#   full_names=grep("2018-07-01",rasters,value = T) %>% strsplit("/") %>% lapply(.,function(x)extract2(x,9)) %>% unlist() %>% gsub(".grd","",.)
#   missing=setdiff(full_names,names(final))
#   final[missing]=NA
#   listo[[length(listo)+1]]=final
#   return(listo)
# }
# 
# test=lapply(dates,FUN=inner_extracto_multi2,rasters=rasters,DF=DF,colname="blank",dateCol=dateCol,x=x,y=y,listo=listo)
# test2=do.call(rbind,test)
# test3=do.call(rbind,test2)
# 
# rasters=list.files(monthlyDir,full.names = T,pattern = ".grd",recursive = T)
# raster_names=list.files(monthlyDir,full.names = F,pattern = ".grd",recursive = T) %>% gsub(".grd","",.) %>% substring(., 12)
# outer_list=list()
# testa=mapply(extracto_monthly_multiple,raster=rasters,colname=raster_names,MoreArgs=list(x=x,y=y,DF=DF,envDirs = monthlyDir,mapply=T,list=outer_list,dateCol = dateCol))
# new=testa %>% reduce(left_join,by=c("on_lon","on_lat","on_timestamp"))

# inner_extracto=function(date,raster,DF,listo,colname){
#   subb=DF %>% filter(!!as.name(dateCol)==date)%>% dplyr::select(x,y)
#   a=raster::extract(raster,subb)
#   subb=subb %>% mutate(!!colname := a) %>% mutate(!!dateCol := date)
#   final=subb
#   print(class(final))
#   listo[[length(listo)+1]]<-final
#   return(listo)
# }
# 
# date_list=list()
# test=lapply(dates,FUN=inner_extracto,raster=raster,DF=DF,listo=date_list)
# test2=do.call(rbind,test) 
# test3=do.call(rbind,test2)

# # applied examples 
# b=read.csv("/Volumes/SeaGate/IUU_GRW/data/raw_gaps_2018-10_2019/filtered_gaps_10_24_29.csv") %>% filter(frac_day_coverage>.2) %>% .[1:400,]
# dailyDir="/Volumes/SeaGate/IUU_GRW/Environmental_data/rasters/daily"
# monthlyDir="/Volumes/SeaGate/IUU_GRW/Environmental_data/rasters/monthly"
# staticDir="/Volumes/SeaGate/IUU_GRW/Environmental_data/rasters/static"
# DF=b
# x="on_lon"
# y="on_lat"
# 
# # extracto_static(), individual raster ####
# bathy_sd=raster("/Volumes/SeaGate/IUU_GRW/Environmental_data/rasters/static/bathy_sd.grd")
# out=extracto_static(x=x,y=y,DF=DF,envDirs = staticDir,raster = bathy_sd, colname = "bathy_sd",mapply=F)
# 
# # extracto_static(), all rasters at once ####
# rasters=list.files(staticDir,full.names = T,pattern = ".grd")
# raster_names=list.files(staticDir,full.names = F,pattern = ".grd") %>% gsub(".grd","",.)
# list=list()
# test=mcmapply(extracto_static,raster=rasters,colname=raster_names,MoreArgs=list(x=x,y=y,DF=DF,envDirs = staticDir,mapply=T,list=list))
# # system.time(mapply(extracto_static,raster=rasters,colname=raster_names,MoreArgs=list(x=x,y=y,DF=DF,envDirs = staticDir,mapply=T,list=list)))
# # system.time(mcmapply(extracto_static,raster=rasters,colname=raster_names,MoreArgs=list(x=x,y=y,DF=DF,envDirs = staticDir,mapply=T,list=list)))
# new=test %>% reduce(left_join,by=c("on_lon","on_lat"))

# # extracto_monthly(), individual raster ####
# # dateCol needs to be yyyy-mm-dd
# DF=b %>% mutate(on_timestamp=ymd_hms(on_timestamp)%>% date()%>% substr(.,1,8) %>% paste0(.,"01"))
# dateCol="on_timestamp"
# dates=unique(DF[,dateCol])
# rasters=list.files(monthlyDir,full.names = T,pattern = ".grd",recursive = T)
# listo=list()
# test=lapply(dates,FUN=extracto_monthly,rasters=rasters,DF=DF,colname="blank",dateCol=dateCol,x=x,y=y,listo=listo)
# test2=do.call(rbind,test)
# test3=do.call(rbind,test2)


# extracto_static_wrapper_2017=function(vessel,outdir,positions,csv_path,date){
#   print(vessel)
#   dat=read.csv(csv_path) %>% mutate(vessel=glue("{flag}_{vessel_class}"))
#   print(1)
#   DF=filter_down_your_data_2017(dat=dat,vessel=vessel,positions=positions)%>%
#     rename(sum_gaps=gaps)
#   rm(dat)
# nrow(DF)
#   staticDir="/Users/heatherwelch/Dropbox/IUU_GRW/Environmental_data/rasters_2017/static"
#   x="on_lon"
#   y="on_lat"
#   rasters=list.files(staticDir,full.names = T,pattern = ".grd")
#   raster_names=list.files(staticDir,full.names = F,pattern = ".grd") %>% gsub(".grd","",.)
#   list=list()
#   test=mcmapply(extracto_static_bullshit,raster=rasters,colname=raster_names,MoreArgs=list(x=x,y=y,DF=DF,envDirs = staticDir,mapply=T,list=list))
#   # names(test)=raster_names
#   # new=lapply(test,function(x)dplyr::select(-c(on_lon,on_lat,sum_gaps)))
#   new=do.call(cbind,test) %>% cbind(DF[,2:4],.)
#   # new=test %>% reduce(left_join,by=c("on_lon","on_lat","sum_gaps")) %>% dplyr::select(-c(eez)) ## removing this as i think it's biasing and a problem
#   new2=new %>% mutate(sum_gaps=DF$sum_gaps) %>% mutate(vessel_class=DF$vessel_class) %>% mutate(class=DF$class) %>% mutate(unique=DF$unique)
#   head(new2)
#   write_csv(new2,glue("{outdir}/gaps_classAB_sat_{positions}fracD_200nm_12hr_{date}_{vessel}.csv"))
#   rm(DF,test,new,new2)
# }

