## code to ensure non-duplicate data and 1:1 ratios of presences to absences
## written by Heather Welch

library(rlang)

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
    
    master=do.call("rbind",list(A0,A1))
    test=master %>% group_by(group) %>% summarise(n=n())
    
  }
  
  return(master)
}



