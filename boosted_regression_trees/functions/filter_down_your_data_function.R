library(rlang)

filter_down_your_data=function(dat,vessel,positions){
  if(vessel=="all"){
    
    pa=dat  %>% filter(positions_per_day>positions)
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    test=pa %>% group_by(gaps) %>% summarise(n=n())
    
    A0=pa %>% filter(gaps==0)%>% .[sample(nrow(.),min(test$n)),]
    A1=pa %>% filter(gaps==1)%>% .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(gaps) %>% summarise(n=n())
    
  }else {
    
    variable <- rlang::sym(vessel)
    pa=dat %>% filter(vessel_class ==variable) %>% filter(positions_per_day>positions)
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    test=pa %>% group_by(gaps) %>% summarise(n=n())
    
    A0=pa %>% filter(gaps==0)%>% .[sample(nrow(.),min(test$n)),]
    A1=pa %>% filter(gaps==1)%>% .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(gaps) %>% summarise(n=n())
    
  }
  
  return(master)
}
filter_down_your_data_2017=function(dat,vessel,positions){
  if(vessel=="all"){
    
    pa=dat  %>% filter(positions_per_day>positions)
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    test=pa %>% group_by(gaps) %>% summarise(n=n())
    
    A1=pa %>% filter(gaps==1)%>% .[sample(nrow(.),min(test$n)),]
    A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))%>%  .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(gaps) %>% summarise(n=n())
    
  }else {
    
    variable <- rlang::sym(vessel)
    pa=dat %>% filter(vessel ==variable) %>% filter(positions_per_day>positions)
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    test=pa %>% group_by(gaps) %>% summarise(n=n())
    
    A1=pa %>% filter(gaps==1)%>% .[sample(nrow(.),min(test$n)),]
    A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))%>%  .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(gaps) %>% summarise(n=n())
    
  }
  
  return(master)
}
filter_down_your_data_2017_vessel_days=function(dat,vessel,positions){
  if(vessel=="all"){
    
    pa=dat  %>% filter(positions_per_day>positions)
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    A1=pa %>% filter(gaps==1)
    A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))
    
    total=rbind(A1,A0)
    test=total %>% group_by(gaps) %>% summarise(n=n())
    
    A1=pa %>% filter(gaps==1)%>% .[sample(nrow(.),min(test$n)),]
    A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))%>%  .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(gaps) %>% summarise(n=n())
    
  }else {
    
    variable <- rlang::sym(vessel)
    pa=dat %>% filter(vessel ==variable) %>% filter(positions_per_day>positions)
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    A1=pa %>% filter(gaps==1)
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
filter_down_your_data_2017_vessel_days_BalancedRestrictive=function(dat,vessel){
  if(vessel=="all"){
    
    pa=dat  
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    A1=pa %>% filter(gaps==1)
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
    A1=pa %>% filter(gaps==1)
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
filter_down_your_data_2017_vessel_days_BalancedRestrictive_background=function(dat,vessel){
  if(vessel=="all"){
    
    pa=dat  
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    A1=pa %>% filter(gaps==1)
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
    A1=pa %>% filter(gaps==1)
    A0=dat %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))
    
    total=rbind(A1,A0)
    test=total %>% group_by(gaps) %>% summarise(n=n())
    
    A1=pa %>% filter(gaps==1)%>% .[sample(nrow(.),min(test$n)),]
    A0=dat %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))%>%  .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(gaps) %>% summarise(n=n())
    
  }
  
  return(master)
}
filter_down_your_data_2017_vessel_days_BalancedRestrictive_background_distinct=function(dat,vessel){
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
    A0=dat %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))
    
    total=rbind(A1,A0)
    test=total %>% group_by(gaps) %>% summarise(n=n())
    
    A1=pa %>% filter(gaps==1)%>% .[sample(nrow(.),min(test$n)),]
    A0=dat %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))%>%  .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(gaps) %>% summarise(n=n())
    
  }
  
  return(master)
}
filter_down_your_data_2017_rfmo=function(dat,vessel,positions){

    variable <- rlang::sym(vessel)
    pa=dat %>% filter(grepl(variable,rfmo)) %>% filter(positions_per_day>positions)
    # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
    # 
    # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    # pa=rbind(presences,absences)
    test=pa %>% group_by(gaps) %>% summarise(n=n())
    
    A1=pa %>% filter(gaps==1)%>% .[sample(nrow(.),min(test$n)),]
    A0=pa %>% filter(gaps==0)%>% filter(!(unique %in%A1$unique))%>%  .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(gaps) %>% summarise(n=n())

  return(master)
}

filter_down_your_data_fishing=function(dat,vessel){
  if(vessel=="all"){
    

    presences=pa %>% filter(fishing==1) %>% distinct(unique,.keep_all=T)%>% .[sample(nrow(.),30000),]

    absences=pa %>% filter(fishing==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
    pa=rbind(presences,absences)
    test=pa %>% group_by(fishing) %>% summarise(n=n())
    
    A0=pa %>% filter(fishing==0)%>% .[sample(nrow(.),min(test$n)),]
    A1=pa %>% filter(fishing==1)%>% .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(fishing) %>% summarise(n=n())
    
  }else {
    
    variable <- rlang::sym(vessel)
    presences=dat %>% filter(vessel_class ==variable)  %>%  filter(fishing==1)%>% distinct(unique,.keep_all=T)
    absences=dat %>% filter(vessel_class =="absences")  %>%  filter(fishing==0)%>% distinct(unique,.keep_all=T)%>% filter(!(unique %in%presences$unique))
    pa=rbind(presences,absences)
    test=pa %>% group_by(fishing) %>% summarise(n=n())
    
    A0=pa %>% filter(fishing==0)%>% .[sample(nrow(.),min(test$n)),]
    A1=pa %>% filter(fishing==1)%>% .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(fishing) %>% summarise(n=n())
    
  }
  
  return(master)
}
filter_down_your_data_fishing_2017=function(dat,vessel){
  if(vessel=="all"){
    
    
    presences=dat %>% filter(fishing==1) %>% .[sample(nrow(.),30000),]
    
    absences=dat %>% filter(fishing==0) %>% filter(!(unique %in%presences$unique))
    pa=rbind(presences,absences)
    test=pa %>% group_by(fishing) %>% summarise(n=n())
    
    A0=pa %>% filter(fishing==0)%>% .[sample(nrow(.),min(test$n)),]
    A1=pa %>% filter(fishing==1)%>% .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(fishing) %>% summarise(n=n())
    
  }else {
    
    variable <- rlang::sym(vessel)
    presences=dat %>% filter(vessel ==variable)  %>%  filter(fishing==1) %>% mutate(group="P")
    absences=dat %>% filter(vessel !=variable)%>% filter(!(unique %in%presences$unique))%>% mutate(group="A")
    
    pa=rbind(presences,absences)
    test=pa %>% group_by(group) %>% summarise(n=n())
    
    if(nrow(presences)>30000){
      A0=pa %>% filter(group=="A")%>% .[sample(nrow(.),min(test$n)),]
      A1=pa %>% filter(group=="P")%>% .[sample(nrow(.),min(test$n)),]
    } else {
    A0=pa %>% filter(group=="A")%>% .[sample(nrow(.),min(test$n)),]
    A1=pa %>% filter(group=="P")%>% .[sample(nrow(.),min(test$n)),]
    }
    
    master=do.call("rbind",list(A0,A1)) %>% 
      mutate(fishing=case_when(group=="A"~0,
                               group=="P"~1))
    test=master %>% group_by(fishing) %>% summarise(n=n())
    
  }
  
  return(master)
}
filter_down_your_data_fishing_2017_background_distinct=function(dat,vessel){
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
    absences=dat %>% filter(fishing==0)%>% filter(!(unique %in%presences$unique))%>% mutate(group="A")
    
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
filter_down_your_data_fishing_2017_background_distinct_reviewer4=function(dat,vessel){
  if(vessel=="all"){
    
    
    presences=dat %>% filter(fishing==1) %>% distinct(unique,.keep_all=T) 
    
    absences=dat %>% filter(fishing==0) %>% filter(!(unique %in%presences$unique))
    pa=rbind(presences,absences)
    test=pa %>% group_by(fishing) %>% summarise(n=n())
    
    # A0=pa %>% filter(fishing==0)%>% .[sample(nrow(.),min(test$n)),]
    # A1=pa %>% filter(fishing==1)%>% .[sample(nrow(.),min(test$n)),]
    
    master=pa
    test=master %>% group_by(fishing) %>% summarise(n=n())
    
  }else {
    
    variable <- rlang::sym(vessel)
    presences=dat %>% filter(vessel ==variable)  %>%  filter(fishing==1) %>% mutate(group="P") %>% distinct(unique,.keep_all=T)
    # absences=dat %>% filter(vessel !=variable)%>% filter(!(unique %in%presences$unique))%>% mutate(group="A")
    absences=dat %>% filter(fishing==0)%>% filter(!(unique %in%presences$unique))%>% mutate(group="A")
    
    pa=rbind(presences,absences)
    test=pa %>% group_by(group) %>% summarise(n=n())
    
    # # if(nrow(presences)>30000){
    # #   A0=pa %>% filter(group=="A")%>% .[sample(nrow(.),min(test$n)),]
    # #   A1=pa %>% filter(group=="P")%>% .[sample(nrow(.),min(test$n)),]
    # # } else {
    # A0=pa %>% filter(group=="A")%>% .[sample(nrow(.),min(test$n)),]
    # A1=pa %>% filter(group=="P")%>% .[sample(nrow(.),min(test$n)),]
    # # }
    
    master=pa %>% 
      mutate(fishing=case_when(group=="A"~0,
                               group=="P"~1))
    test=master %>% group_by(fishing) %>% summarise(n=n())
    
  }
  
  return(master)
}
filter_down_your_data_fishing_2017_background_Nondistinct=function(dat,vessel){
  if(vessel=="all"){
    
    
    presences=dat %>% filter(fishing==1) 
    
    absences=dat %>% filter(fishing==0) %>% filter(!(unique %in%presences$unique))
    pa=rbind(presences,absences)
    test=pa %>% group_by(fishing) %>% summarise(n=n())
    
    A0=pa %>% filter(fishing==0)%>% .[sample(nrow(.),min(test$n)),]
    A1=pa %>% filter(fishing==1)%>% .[sample(nrow(.),min(test$n)),]
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(fishing) %>% summarise(n=n())
    
  }else {
    
    variable <- rlang::sym(vessel)
    
    presences=dat %>% filter(vessel ==variable)  %>%  filter(fishing==1) %>% mutate(group="P")
    absences=dat %>% filter(vessel !=variable)%>% filter(!(unique %in%presences$unique))%>% mutate(group="A")
    
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
filter_down_your_data_fishing_2017_background_distinct_absIncldOtherGears=function(dat,vessel){
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
    
    presences=dat %>% filter(vessel ==variable)  %>%  filter(fishing==1) %>% mutate(group="P")%>% distinct(unique,.keep_all=T) 
    absences=dat %>% filter(vessel !=variable)%>% filter(!(unique %in%presences$unique))%>% mutate(group="A")
    
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
filter_down_your_data_fishing_2017_rfmo=function(dat,vessel){
  variable <- rlang::sym(vessel)
  pa=dat %>% filter(grepl(variable,rfmo))
  datPres=pa %>% filter(vessel_class=="drifting_longlines"|vessel_class=="other_purse_seines"|vessel_class=="trollers"|vessel_class=="pole_and_line"|vessel_class=="tuna_purse_seines")
  # datABS=pa %>% filter(vessel_class!="drifting_longlines"&vessel_class!="other_purse_seines"&vessel_class!="trollers"&vessel_class!="pole_and_line"&vessel_class!="tuna_purse_seines")
  # presences=pa %>% filter(gaps==1) %>% distinct(unique,.keep_all=T) 
  # 
  # absences=pa %>% filter(gaps==0)%>% distinct(unique,.keep_all=T) %>% filter(!(unique %in%presences$unique))
  # pa=rbind(presences,absences)
  # test=pa %>% group_by(gaps) %>% summarise(n=n())
  
  A1=datPres 
  A0=dat %>% filter(fishing==0) %>% filter(!(unique %in%A1$unique))%>%  .[sample(nrow(.),nrow(A1)),]
  
  master=do.call("rbind",list(A0,A1))
  test=master %>% group_by(fishing) %>% summarise(n=n())
  
  return(master)
}


# vessel="set_longlines"
# dat=read.csv("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data/gapsv20200207_classAB_sat_NAfracD_200nm_12hr_02_11_20.csv")
# positions = 200
# 
# vessel="all"
# test=filter_down_your_data(dat=dat,vessel=vessel,positions=positions)
