## script of functions to extract env data from rasters
## written by Heather Welch

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
  final=pnts %>% dplyr::select(-c(x,y))
  # final=pnts %>% mutate(sum_gaps=DF$sum_gaps)
  if(mapply==T){
    list[[length(list)+1]]=final
    return(list)
  }
  else{return(final)}
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
  test=mcmapply(extracto_static,raster=rasters,colname=raster_names,MoreArgs=list(x=x,y=y,DF=DF,envDirs = staticDir,mapply=T,list=list))
  new=do.call(cbind,test) %>% cbind(DF[,2:4],.)
  new2=new %>% mutate(sum_gaps=DF$sum_gaps) %>% mutate(vessel_class=DF$vessel_class) %>% mutate(class=DF$class) %>% mutate(unique=DF$unique)
  head(new2)
  if(type=="flag_vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}.csv"))}
  if(type=="vessel_class"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}_VC.csv"))}
  if(type=="all"){write_csv(new2,glue("{outdir}/gaps_classAB_sat_fracD_200nm_12hr_{date}_{vessel}.csv"))}
  write_csv(new2,glue("{outdir}/gaps_classAB_sat_200nm_{date}_{vessel}.csv"))
  rm(DF,test,new,new2)
}

extracto_static_wrapper_fishing_2017=function(date,csv_path,vessel,outdir,type){
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
  test=mcmapply(extracto_static,raster=rasters,colname=raster_names,MoreArgs=list(x=x,y=y,DF=DF,envDirs = staticDir,mapply=T,list=list))
  new=do.call(cbind,test) %>% cbind(DF[,2:4],.)
  new2=new %>% mutate(sum_gaps=DF$sum_gaps) %>% mutate(vessel_class=DF$vessel_class) %>% mutate(class=DF$class) %>% mutate(unique=DF$unique)
  if(type=="flag_vessel_class"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))}
  if(type=="vessel_class"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}_VC.csv"))}
  if(type=="all"){write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))}
  write_csv(new2,glue("{outdir}/fishing_classAB_sat_200nm_{date}_{vessel}.csv"))
  rm(DF,test,new,new2)
}



