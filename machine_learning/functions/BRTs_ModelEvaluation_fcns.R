## BRTs Model Evaluation functions
## Written by Heather Welch, Stephanie Brodie, and Elizabeth Becker

#Make function to 75/25 split AUC test. 
eval_7525_heather_fixed_50 <- function(dataInput, gbm.x, gbm.y, lr, tc=tc,tolerance.method,tolerance,family,response){
  DataInput <- dataInput
  if(family=="bernoulli"){
    Evaluations_7525 <- as.data.frame(matrix(data=0,nrow=50,ncol=5))
    colnames(Evaluations_7525) <- c("Deviance","AUC","TSS","TPR","TNR")
  }
  
  if(family=="poisson"){
    Evaluations_7525 <- as.data.frame(matrix(data=0,nrow=50,ncol=5))
    colnames(Evaluations_7525) <- c("pearson","spearman","Deviance","RMSE","AVE")
  }
  for(i in 1:50){
  DataInput_bound <- floor((nrow(DataInput)/4)*3)         #define % of training and test set
  DataInput_train<- DataInput[sample(nrow(DataInput),DataInput_bound),]
  DataInput_test<- DataInput[sample(nrow(DataInput),nrow(DataInput)-DataInput_bound),]
  DataInput.kfolds <- gbm.fixed(data=DataInput_train, gbm.x= gbm.x, gbm.y = gbm.y, 
                                family=family, tree.complexity=tc,
                                learning.rate = lr, bag.fraction = 0.6,n.trees = 2000 )
  preds <- predict.gbm(DataInput.kfolds, DataInput_test,
                       n.trees=DataInput.kfolds$gbm.call$best.trees, type="response")
  position=grep(response,names(dataInput))
  # dev <- calc.deviance(obs=DataInput_test[[response]], pred=preds, calc.mean=TRUE,family = family)
  null <- DataInput.kfolds$self.statistics$null.deviance
  res <- DataInput.kfolds$self.statistics$resid.deviance
  dev=((null - res)/null)*100 
  
  d <- cbind(DataInput_test[[response]], preds)
  test=as.data.frame(d) %>% group_by(V1) %>% summarise(mean(preds))
  if(family=="bernoulli"){
    pres <- d[d[,1]==1,2]
    abs <- d[d[,1]==0,2]
    e <- evaluate(p=pres, a=abs)
    thresh=threshold(e)$equal_sens_spec
    ind=which(abs(e@t-thresh)==min(abs(e@t-thresh)))
    Evaluations_7525[i,1] <- dev
    Evaluations_7525[i,2] <- e@auc
    Evaluations_7525[i,3] <- max(e@TPR + e@TNR-1)
    Evaluations_7525[i,4] <- e@TPR[ind]
    Evaluations_7525[i,5] <- e@TNR[ind]
    # Evaluations_7525[1,4] <- test[1,2]
    # Evaluations_7525[1,5] <- test[2,2]
  }
  if(family=="poisson"){
    colnames(d) <- c("observed","predicted")
    d <- as.data.frame(d)
    pear=cor(d$predicted, d$observed, use="na.or.complete",method = "pearson")
    spear=cor(d$predicted, d$observed, use="na.or.complete",method = "spearman")
    # lm(d$observed~d$predicted)
    rmse=sqrt(mean((d$observed - d$predicted)^2)) #RMSE
    ave=mean(d$observed - d$predicted) #AVE
    
    Evaluations_7525[1,1] <- pear
    Evaluations_7525[1,2] <- spear
    Evaluations_7525[1,3] <- dev
    Evaluations_7525[1,4] <- rmse
    Evaluations_7525[1,5] <- ave
  }
  }
  
  return(Evaluations_7525)}

#Percent deviance explained
dev_eval3=function(model_object){
  null <- model_object$self.statistics$null.deviance
  res <- model_object$self.statistics$resid.deviance
  dev=((null - res)/null)*100 
  return(dev)
}

#Ratio of observed to predicted values for study area 
ratio <- function(dataInput,model_object,response){
  ## Predict on model data using the best tree for predicting
  BRTpred <- predict.gbm(model_object, dataInput, n.trees = model_object$gbm.call$best.trees, "response")
  # calculate ratio of observed to predicted values for study area
  position=grep(response,names(dataInput))
  ratio.BRTpred <- sum(dataInput[position])/sum(BRTpred)
  return(ratio.BRTpred)
}
