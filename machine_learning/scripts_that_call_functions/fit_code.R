## code to parallelize the fitting of boosted regression tree models for AIS disabling and fishing activity
## written by Heather Welch

source("functions/load_libraries.R")

## 12.21.20. pos25hrb4 Vessel class gaps full model, 200k threshold for dist_eez, dist_NTA, dist_pirate ####
library(foreach)
library(doParallel, quietly = TRUE)

vessels=c("squid_jigger","drifting_longlines","tuna_purse_seines","trawlers","all")
registerDoParallel(4)
system.time(print(
  foreach(i=1:length(vessels),.packages = c("gbm","glue"),.verbose=T) %dopar% {
    print(vessels[i])
    
    tryCatch(
      expr ={
        if(vessels[i]!="all"){
          main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/extracto/gaps_classAB_sat_fracD_200nm_12hr_12_21_20_pos25hrb4_{vessels[i]}_VC.csv"))%>%
            dplyr::select(c(unique,loitering_hours,dist_nta_200k,dist_eez_200k,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears_200k))
        }
        if(vessels[i]=="all"){
          main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/extracto/gaps_classAB_sat_fracD_200nm_12hr_12_21_20_pos25hrb4_{vessels[i]}.csv"))%>%
            dplyr::select(c(unique,loitering_hours,dist_nta_200k,dist_eez_200k,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears_200k))
        }
        
        main=main %>% mutate(random=sample(1:nrow(main))) %>% mutate(dist_eez_200k=replace(dist_eez_200k,dist_eez_200k<26000,26000))
        main=main %>% mutate(sum_gaps=as.integer(round(sum_gaps))) %>% .[complete.cases(.),]
        
        #full model no reception
        gbm.x=c("loitering_hours","dist_eez_200k","l.chl","sst","sst_temporal_sd","l.eke","random","dist_nta_200k","dist_ASAM_events_allyears_200k")
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
            
            name=glue("/Users/heatherwelch/Dropbox/IUU_GRW/SDMs/brt_2017/gaps_classAB_sat_200nm_12hr_12_21_20_pos25hrb4_step_lr{lr}_tc{tc}_bf{bf}_tol{tolerance}_{family}_{type}.rds")
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




## 12_21_20. Vessel class fishing full model, 200k threshold for dist_eez, dist_NTA, dist_pirate ####
library(foreach)
library(doParallel, quietly = TRUE)

vessels=c("squid_jigger","drifting_longlines","tuna_purse_seines","trawlers","all")
registerDoParallel(6)
system.time(print(
  foreach(i=1:length(vessels),.packages = c("gbm","glue"),.verbose=T) %dopar% {
    print(vessels[i])
    
    tryCatch(
      expr ={
        if(vessels[i]!="all"){
          main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/extracto/fishing_classAB_sat_200nm_12_21_20_{vessels[i]}_VC.csv"))%>%
            dplyr::select(c(unique,loitering_hours,dist_nta_200k,dist_eez_200k,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears_200k))  
        }
        if(vessels[i]=="all"){
          main=read.csv(glue("/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/extracto/fishing_classAB_sat_200nm_12_21_20_{vessels[i]}.csv"))%>%
            dplyr::select(c(unique,loitering_hours,dist_nta_200k,dist_eez_200k,l.chl,sst,sst_temporal_sd,l.eke,sum_gaps,dist_ASAM_events_allyears_200k))  
        }
        main=main %>% mutate(random=sample(1:nrow(main)))%>% mutate(dist_eez_200k=replace(dist_eez_200k,dist_eez_200k<26000,26000))
        main=main %>% mutate(sum_gaps=as.integer(round(sum_gaps))) %>% .[complete.cases(.),]
        
        #full model no reception
        gbm.x=c("loitering_hours","dist_eez_200k","l.chl","sst","sst_temporal_sd","l.eke","random","dist_nta_200k","dist_ASAM_events_allyears_200k")
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
            
            name=glue("/Users/heatherwelch/Dropbox/IUU_GRW/SDMs/brt_2017/fishing_classAB_sat_200nm_12hr_12_21_20_step_lr{lr}_tc{tc}_bf{bf}_tol{tolerance}_{family}_{type}.rds")
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


