## Code to calculate a series of performance metrics for fitted BRTs
## written by Heather Welch

source("functions/load_libraries.R")
source("functions/BRTs_ModelEvaluation_fcns.R")

modDir="/Users/heatherwelch/Dropbox/IUU_GRW/SDMs/brt_2017"
outdir="/Users/heatherwelch/Dropbox/IUU_GRW/plots/plots_08_23_21"

# gaps full
date="08_23_21"
dist="50nm"
distance="_400kmpa"
type="PresAbs"
abs_g="fishing_presence"
abs_f="background"
date2=glue("{dist}_{type}_{abs_g}_{date}.csv{distance}")
date_fish=glue("{dist}_{type}_{abs_f}_{date}.csv{distance}")
combination="5A111"
date2=glue("{dist}_{type}_{abs_g}_{date}.csv{distance}")

full_modVC_fishing=list.files(modDir,pattern=date_fish,full.names = T)  %>% grep("fishing",.,invert=F,value = T)
namesVC_fishing=full_modVC_fishing%>% 
  gsub(modDir,"",.) %>% 
  gsub(date_fish,"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr__step_lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr__step_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr__lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr__lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr__step_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr__step_lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr__lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr__lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr_step_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr_step_lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr_lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(".rds","",.) %>% gsub("_full_model","",.) %>% 
  gsub("_VC","",.)

full_modVC_gaps=list.files(modDir,pattern=date2,full.names = T)  %>% grep("gaps",.,invert=F,value = T)
namesVC_gaps=full_modVC_gaps%>% 
  gsub(modDir,"",.) %>% 
  gsub(date2,"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr__step_lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr__step_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr__lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr__lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr_step_lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr_step_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/gaps_classAB_sat_200nm_12hr_lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr__step_lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr__step_lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr__lr0.01_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
  gsub(glue("/fishing_classAB_sat_200nm_12hr__lr0.001_tc3_bf0.6_tol1e-05_bernoulli_"),"",.) %>% 
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
    variable <- rlang::sym(names[i])
    full=readRDS(full_mod[i]) %>% dev_eval3()
    
    mod=readRDS(full_mod[i])
    
    if(names[i]!="all"&grepl("brt_2017/gaps",full_mod[i])){
      main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/extracto/gaps_classAB_sat_fracD_200nm_12hr_50nm_PresAbs_fishing_presence_08_23_21.csv_{names[i]}_VC_reviewer4.csv"))%>%
        dplyr::select(c(unique,loitering_hours,dist_mpa,dist_gfw,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears))
    }
    if(names[i]=="all"&grepl("brt_2017/gaps",full_mod[i])){
      main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/extracto/gaps_classAB_sat_fracD_200nm_12hr_50nm_PresAbs_fishing_presence_08_23_21.csv_{names[i]}_reviewer4.csv"))%>%
        dplyr::select(c(unique,loitering_hours,dist_mpa,dist_gfw,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears))
    }
    if(names[i]!="all"&grepl("brt_2017/fishing",full_mod[i])){
      main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/extracto/fishing_classAB_sat_200nm_50nm_PresAbs_background_08_23_21.csv_{names[i]}_VC_reviewer4.csv"))%>%
        dplyr::select(c(unique,loitering_hours,dist_mpa,dist_gfw,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears))
    }
    if(names[i]=="all"&grepl("brt_2017/fishing",full_mod[i])){
      main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/extracto/fishing_classAB_sat_200nm_50nm_PresAbs_background_08_23_21.csv_{names[i]}_reviewer4.csv"))%>%
        dplyr::select(c(unique,loitering_hours,dist_mpa,dist_gfw,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears))
    }
    
    main=main %>% mutate(random=sample(1:nrow(main))) %>%
      mutate(dist_gfw=replace(dist_gfw,dist_gfw<92.6,92.6)) %>% 
      mutate(dist_gfw=replace(dist_gfw,dist_gfw>770.4,770.4)) %>% 
      mutate(dist_mpa=replace(dist_mpa,dist_mpa>400000,400000)) %>% 
      mutate(dist_ASAM_events_allyears=replace(dist_ASAM_events_allyears,dist_ASAM_events_allyears>400000,400000))  
    main=main %>% mutate(sum_gaps=as.integer(round(sum_gaps))) %>% .[complete.cases(.),]
    
    gbm.x=c("loitering_hours","dist_gfw","l.chl","sst","sst_temporal_sd","l.eke","random","dist_mpa","dist_ASAM_events_allyears")
    
    family="bernoulli"
    if(main %>% filter(sum_gaps==1) %>% nrow()<500){lr=0.001}
    if(main %>% filter(sum_gaps==1) %>% nrow()>=500){lr=0.01}
    tc=mod$interaction.depth
    bf=0.6
    tolerance = .00001
    
    if(names[i]!="all"&grepl("brt_2017/gaps",full_mod[i])){eval_name="VC_gaps"}
    if(names[i]=="all"&grepl("brt_2017/gaps",full_mod[i])){eval_name="all_gaps"}
    if(names[i]!="all"&grepl("brt_2017/fishing",full_mod[i])){eval_name="VC_fishing"}
    if(names[i]=="all"&grepl("brt_2017/fishing",full_mod[i])){eval_name="all_fishing"}
    
    a75_25=eval_7525_heather_fixed_50_reviewer4(main,family=family,gbm.y="sum_gaps",lr=lr,tc=tc,tolerance.method="fixed",tolerance=tolerance,gbm.x=gbm.x,response = "sum_gaps",eval_name=eval_name,variable=variable)
    
    rat=ratio(main,mod,response = "sum_gaps")
    # "Deviance","AUC","TSS","True_positive_rate","True_negative_rate"
    
    if(grepl("brt_2017/gaps",full_mod[i])){type="gaps"}
    if(grepl("brt_2017/fishing",full_mod[i])){type="fishing"}
    
    # if(grepl("noloitering",full_mod[i])){
    #   type="noloitering"
    # } else if (grepl("noeez",full_mod[i])) {
    #   type="noeez"
    # } else{type="full"}
    
    
    # dat=data.frame(vessel=as.character(names[i]),
    #                deviance=as.numeric(full),
    #                ratio=as.numeric(rat),
    #                deviance7525=as.numeric(a75_25$Deviance),
    #                AUC7525=as.numeric(a75_25$AUC),
    #                TSS7525=as.numeric(a75_25$TSS),
    #                TPR=as.numeric(a75_25$TPR),
    #                TNR=as.numeric(a75_25$TNR),
    #                type=as.character(type),
    #                stringsAsFactors = F)
    # 
    dat=a75_25 %>% mutate(vessel=as.character(names[i]),devianceFull=as.numeric(full),ratio=as.numeric(rat),type=as.character(type))
    
  }
))

write.csv(a,glue("{outdir}/modeleval2.csv"))
a=read.csv(glue("{outdir}/modeleval2.csv")) %>% dplyr::select(-c(X,ratio)) %>% group_by(vessel,type) %>% summarise_all(c(mean,sd),na.rm=T) %>% 
  arrange(desc(type),vessel) %>% mutate_at(vars(matches("_fn2")),~round(.,digits=3))%>% mutate_at(vars(matches("_fn2")),~str_pad(.,width=5,side = "right",pad="0")) %>% 
  mutate_at(vars(matches("_fn1")),~round(.,digits=2))%>% mutate_at(vars(matches("_fn1")),~str_pad(.,width=4,side = "right",pad="0")) %>% 
  mutate(Deviance_fn1=round(as.numeric(Deviance_fn1),digits=2)) %>%   mutate(Deviance_fn2=round(as.numeric(Deviance_fn2),digits=2)) #%>% 
#mutate_at(vars(matches("_fn1")),~glue("{.}+-{vars(matches(_fn2))}"))

write.csv(a,glue("{outdir}/modeleval2_cleaned.csv"))