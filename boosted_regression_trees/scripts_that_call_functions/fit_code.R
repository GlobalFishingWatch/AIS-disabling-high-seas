## code to parallelize the fitting of boosted regression tree models for AIS disabling and fishing activity
## written by Heather Welch

source("functions/load_libraries.R")

vessels=c("squid_jigger","drifting_longlines","tuna_purse_seines","trawlers","all")
library(foreach)
library(doParallel, quietly = TRUE)
date="08_23_21"

#### 5111A #####
## 15 Fishing: pres/abs; abs background, everything is 50nm from shore MPA instead of NTA ####
date="08_23_21"
type="PresAbs"
dist="50nm"
abs_f="background"
abs_g=NA
date_fish=glue("{dist}_{type}_{abs_f}_{date}.csv")
date2=NA
distance_vars="400kmpa"

registerDoParallel(5)
system.time(print(
  foreach(i=1:length(vessels),.export=c("type","abs_g","abs_f","dist","date2","date_fish","distance_vars"),.packages = c("gbm","glue"),.verbose=T) %dopar% {
    print(vessels[i])
    
    tryCatch(
      expr ={
        if(vessels[i]!="all"){
          main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/extracto/fishing_classAB_sat_200nm_{date_fish}_{vessels[i]}_VC.csv"))%>% 
            dplyr::select(c(unique,loitering_hours,dist_mpa,dist_gfw,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears))
        }
        if(vessels[i]=="all"){
          main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/extracto/fishing_classAB_sat_200nm_{date_fish}_{vessels[i]}.csv"))%>%
            dplyr::select(c(unique,loitering_hours,dist_mpa,dist_gfw,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears))
        }
        
        main=main %>% mutate(random=sample(1:nrow(main))) %>%
          mutate(dist_gfw=replace(dist_gfw,dist_gfw<92.6,92.6)) %>% 
          mutate(dist_gfw=replace(dist_gfw,dist_gfw>770.4,770.4)) %>% 
          mutate(dist_mpa=replace(dist_mpa,dist_mpa>400000,400000)) %>% 
          mutate(dist_ASAM_events_allyears=replace(dist_ASAM_events_allyears,dist_ASAM_events_allyears>400000,400000))  
        main=main %>% mutate(sum_gaps=as.integer(round(sum_gaps))) %>% .[complete.cases(.),]
        
        #full model no reception
        gbm.x=c("loitering_hours","dist_gfw","l.chl","sst","sst_temporal_sd","l.eke","random","dist_mpa","dist_ASAM_events_allyears")
        family="bernoulli"
        if(main %>% filter(sum_gaps==1) %>% nrow()<500){lr=0.001}
        if(main %>% filter(sum_gaps==1) %>% nrow()>=500){lr=0.01}
        tc=3
        bf=0.6
        tolerance = .00001
        type=glue("{vessels[i]}_VC")
        
        if(vessels[i]=="all"){
          type=glue("{vessels[i]}")
        }
        
        tryCatch(
          expr ={
            gap_brt_poiss_step = gbm.fixed(main,gbm.x=gbm.x,gbm.y="sum_gaps",family=family,learning.rate = lr, tree.complexity =tc, bag.fraction = bf,n.trees = 2000,keep.data = T)
            
            if(gap_brt_poiss_step$n.trees<2000){
              print("N trees too low, refitting with smaller LR")
              lr=lr/10*5
              gap_brt_poiss_step = gbm.fixed(main,gbm.x=gbm.x,gbm.y="sum_gaps",family=family,learning.rate = lr, tree.complexity =tc, bag.fraction = bf,n.trees = 2000)
            }
            
            if(gap_brt_poiss_step$n.trees<2000){
              print("N trees too low, refitting with smaller LR")
              lr=lr/10*5
              gap_brt_poiss_step = gbm.fixed(main,gbm.x=gbm.x,gbm.y="sum_gaps",family=family,learning.rate = lr, tree.complexity =tc, bag.fraction = bf,n.trees = 2000)
            }
            
            name=glue("/Users/heatherwelch/Dropbox/IUU_GRW/SDMs/brt_2017/fishing_classAB_sat_200nm_12hr_{date_fish}_{distance_vars}_step_lr{lr}_tc{tc}_bf{bf}_tol{tolerance}_{family}_{type}.rds")
            write_rds(gap_brt_poiss_step,name)
          },
          error = function(e){
            message(glue("Model not working"))
            print(e)
          }
        )
      },
      error = function(e){
        message(glue("No data"))
        print(e)
      }
    )
  }
))











## 17 Gaps: pres/abs; abs are fishing_presence, everything is 50nm from shore MPA instead of NTA ####
date="08_23_21"
type="PresAbs"
abs_g="fishing_presence"
abs_f=NA
dist="50nm"
date2=glue("{dist}_{type}_{abs_g}_{date}.csv")
date_fish=NA
distance_vars="400kmpa"

registerDoParallel(5)
system.time(print(
  foreach(i=1:length(vessels),.export=c("type","abs_g","abs_f","dist","date2","date_fish","distance_vars"),.packages = c("gbm","glue"),.verbose=T) %dopar% {
    print(vessels[i])
    
    tryCatch(
      expr ={
        if(vessels[i]!="all"){
          main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/extracto/gaps_classAB_sat_fracD_200nm_12hr_{date2}_{vessels[i]}_VC.csv")) %>% 
            dplyr::select(c(unique,loitering_hours,dist_mpa,dist_gfw,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears))
        }
        if(vessels[i]=="all"){
          main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/extracto/gaps_classAB_sat_fracD_200nm_12hr_{date2}_{vessels[i]}.csv"))%>%
            dplyr::select(c(unique,loitering_hours,dist_mpa,dist_gfw,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears))
        }
        
        main=main %>% mutate(random=sample(1:nrow(main))) %>%
          mutate(dist_gfw=replace(dist_gfw,dist_gfw<92.6,92.6)) %>% 
          mutate(dist_gfw=replace(dist_gfw,dist_gfw>770.4,770.4)) %>% 
          mutate(dist_mpa=replace(dist_mpa,dist_mpa>400000,400000)) %>% 
          mutate(dist_ASAM_events_allyears=replace(dist_ASAM_events_allyears,dist_ASAM_events_allyears>400000,400000))  
        main=main %>% mutate(sum_gaps=as.integer(round(sum_gaps))) %>% .[complete.cases(.),]
        
        #full model no reception
        gbm.x=c("loitering_hours","dist_gfw","l.chl","sst","sst_temporal_sd","l.eke","random","dist_mpa","dist_ASAM_events_allyears")
        family="bernoulli"
        if(main %>% filter(sum_gaps==1) %>% nrow()<500){lr=0.001}
        if(main %>% filter(sum_gaps==1) %>% nrow()>=500){lr=0.01}
        tc=3
        bf=0.6
        tolerance = .00001
        type=glue("{vessels[i]}_VC")
        
        if(vessels[i]=="all"){
          type=glue("{vessels[i]}")
        }
        
        tryCatch(
          expr ={
            gap_brt_poiss_step = gbm.fixed(main,gbm.x=gbm.x,gbm.y="sum_gaps",family=family,learning.rate = lr, tree.complexity =tc, bag.fraction = bf,n.trees = 2000)
            
            if(gap_brt_poiss_step$n.trees<2000){
              print("N trees too low, refitting with smaller LR")
              lr=lr/10*5
              gap_brt_poiss_step = gbm.fixed(main,gbm.x=gbm.x,gbm.y="sum_gaps",family=family,learning.rate = lr, tree.complexity =tc, bag.fraction = bf,n.trees = 2000)
            }
            
            if(gap_brt_poiss_step$n.trees<2000){
              print("N trees too low, refitting with smaller LR")
              lr=lr/10*5
              gap_brt_poiss_step = gbm.fixed(main,gbm.x=gbm.x,gbm.y="sum_gaps",family=family,learning.rate = lr, tree.complexity =tc, bag.fraction = bf,n.trees = 2000)
            }
            
            name=glue("/Users/heatherwelch/Dropbox/IUU_GRW/SDMs/brt_2017/gaps_classAB_sat_200nm_12hr_{date2}_{distance_vars}_step_lr{lr}_tc{tc}_bf{bf}_tol{tolerance}_{family}_{type}.rds")
            write_rds(gap_brt_poiss_step,name)
          },
          error = function(e){
            message(glue("Model not working"))
            print(e)
          }
        )
      },
      error = function(e){
        message(glue("No data"))
        print(e)
      }
    )
  }
))


