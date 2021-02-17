## Code to calculate a series of performance metrics for fitted BRTs
## written by Heather Welch

source("functions/load_libraries.R")
source("functions/BRTs_ModelEvaluation_fcns.R")

## evaluate fishing and disabling models
modDir="/Users/heatherwelch/Dropbox/IUU_GRW/SDMs/brt_2017"
outdir="/Users/heatherwelch/Dropbox/IUU_GRW/plots/plots_12_21_20"

date="12_21_20"
full_modVC_fishing=list.files(modDir,pattern=date,full.names = T)  %>% grep("fishing",.,invert=F,value = T)
namesVC_fishing=full_modVC_fishing%>% 
  gsub(modDir,"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr_{date}_step_lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr_{date}_step_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr_{date}_step_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr_{date}_step_lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(".rds","",.) %>% gsub("_full_model","",.) %>% 
  gsub("_VC","",.)

date="12_21_20"
full_modVC_gaps=list.files(modDir,pattern=date,full.names = T)  %>% grep("gaps",.,invert=F,value = T)
namesVC_gaps=full_modVC_gaps%>% 
  gsub(modDir,"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr_{date}_pos25hrb4_step_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr_{date}_pos25hrb4_step_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr_{date}_step_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr_{date}_step_lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(".rds","",.) %>% gsub("_full_model","",.) %>% 
  gsub("_VC","",.)

full_mod=c(full_modVC_gaps,full_modVC_fishing)
names=c(namesVC_gaps,namesVC_fishing)

library(foreach)
library(doParallel, quietly = TRUE)
registerDoParallel(8)
system.time(print(
 a<-foreach(i=1:length(names),.export = c("full_mod","dev_eval3","eval_7525_heather_fixed","ratio"),.combine=rbind,.packages = c("gbm","glue","tidyverse"),.verbose=T) %dopar% {
    print(names[i])
    full=readRDS(full_mod[i]) %>% dev_eval3()

    mod=readRDS(full_mod[i])

    if(names[i]!="all"&grepl("gaps",full_mod[i])){
      main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/extracto/gaps_classAB_sat_fracD_200nm_12hr_12_21_20_pos25hrb4_{names[i]}_VC.csv"))%>%
        dplyr::select(c(unique,loitering_hours,dist_nta_200k,dist_eez_200k,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears_200k))
    }
    if(names[i]=="all"&grepl("gaps",full_mod[i])){
      main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/extracto/gaps_classAB_sat_fracD_200nm_12hr_12_21_20_pos25hrb4_{names[i]}.csv"))%>%
        dplyr::select(c(unique,loitering_hours,dist_nta_200k,dist_eez_200k,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears_200k))
    }
    if(names[i]!="all"&grepl("fishing",full_mod[i])){
      main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/extracto/fishing_classAB_sat_200nm_12_21_20_{names[i]}_VC.csv"))%>%
        dplyr::select(c(unique,loitering_hours,dist_nta_200k,dist_eez_200k,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears_200k))
    }
    if(names[i]=="all"&grepl("fishing",full_mod[i])){
      main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/extracto/fishing_classAB_sat_200nm_12_21_20_{names[i]}.csv"))%>%
        dplyr::select(c(unique,loitering_hours,dist_nta_200k,dist_eez_200k,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears_200k))
    }
    
    main=main %>% mutate(random=sample(1:nrow(main)))
    main=main %>% mutate(sum_gaps=as.integer(round(sum_gaps))) %>% .[complete.cases(.),]
    
    gbm.x=c("loitering_hours","dist_eez_200k","l.chl","sst","sst_temporal_sd","l.eke","random","dist_nta_200k","dist_ASAM_events_allyears_200k")
     
    family="bernoulli"
    if(main %>% filter(sum_gaps==1) %>% nrow()<500){lr=0.001}
    if(main %>% filter(sum_gaps==1) %>% nrow()>=500){lr=0.01}
    tc=mod$interaction.depth
    bf=0.6
    tolerance = .00001
    a75_25=eval_7525_heather_fixed_50(main,family=family,gbm.y="sum_gaps",lr=lr,tc=tc,tolerance.method="fixed",tolerance=tolerance,gbm.x=gbm.x,response = "sum_gaps")

    rat=ratio(main,mod,response = "sum_gaps")
    # "Deviance","AUC","TSS","True_positive_rate","True_negative_rate"
    
    if(grepl("gaps",full_mod[i])){type="gaps"}
    if(grepl("fishing",full_mod[i])){type="fishing"}
    
    dat=a75_25 %>% mutate(vessel=as.character(names[i]),devianceFull=as.numeric(full),ratio=as.numeric(rat),type=as.character(type))
    
  }
))

write.csv(a,glue("{outdir}/modeleval2.csv"))
a=read.csv(glue("{outdir}/modeleval2.csv")) %>% dplyr::select(-c(X,devianceFull,ratio)) %>% group_by(vessel,type) %>% summarise_all(c(mean,sd),na.rm=T) %>% 
  arrange(desc(type),vessel) %>% mutate_at(vars(matches("_fn2")),~round(.,digits=3))%>% mutate_at(vars(matches("_fn2")),~str_pad(.,width=5,side = "right",pad="0")) %>% 
  mutate_at(vars(matches("_fn1")),~round(.,digits=2))%>% mutate_at(vars(matches("_fn1")),~str_pad(.,width=4,side = "right",pad="0")) %>% 
  mutate(Deviance_fn1=round(as.numeric(Deviance_fn1),digits=2)) %>%   mutate(Deviance_fn2=round(as.numeric(Deviance_fn2),digits=2)) %>% 
  mutate_at(vars(matches("_fn1")),~glue("{.}+-{vars(matches(_fn2))}"))

write.csv(a,glue("{outdir}/modeleval2_cleaned.csv"))

