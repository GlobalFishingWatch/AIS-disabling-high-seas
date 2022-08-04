### Extracts environmental and behavioral drivers to disabling and fishing activity datasets. 

source("functions/load_libraries.R")
source("functions/extracto_functions.R")
source("functions/filter_down_your_data_function.R")
library(purrr)

date="06_17_22c"


## 4 Gaps: pres/abs; abs are fishing presence, everything is 50nm from shore ####
## 4 Fishing: pres/abs; abs are backgorund, everything is 50nm from shore
type="PresAbs"
abs_g="fishing_presence"
dist="50nm"

# ------- Gaps ####
outdir="/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/extracto"
csv_path_gaps=glue("/Users/heatherwelch/Dropbox/IUU_GRW/modeling_data_06_03_22/gaps_v20201209_{dist}_12hr_{type}_{abs_g}_{date}.csv")
date2=glue("{dist}_{type}_{abs_g}_{date}.csv")
vesselNames=c("squid_jigger","drifting_longlines","tuna_purse_seines","trawlers")

extracto_static_wrapper_2017_vessel_days_BalancedRestrictive_off_distinct(date=date2,outdir=outdir,csv_path=csv_path_gaps,vessel="all",type2="all")
mclapply(vesselNames,FUN=extracto_static_wrapper_2017_vessel_days_BalancedRestrictive_off_distinct,outdir=outdir,csv_path=csv_path_gaps,date=date2,type2="vessel_class",mc.cores = 6)

# ------- fishing ####
abs_f="vessel_presence"
outdir="/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/extracto"
csv_path_fishing=glue("/Users/heatherwelch/Dropbox/IUU_GRW/modeling_data_06_03_22/fishing_{dist}_12hr_{type}_{abs_f}_{date}.csv")
date2=glue("{dist}_{type}_{abs_f}_{date}.csv")
vesselNames=c("squid_jigger","drifting_longlines","tuna_purse_seines","trawlers")

extracto_static_wrapper_fishing_2017_vessel_days_distinct(date=date2,outdir=outdir,csv_path=csv_path_fishing,vessel="all",type2="all")
mclapply(vesselNames,FUN=extracto_static_wrapper_fishing_2017_vessel_days_distinct,outdir=outdir,csv_path=csv_path_fishing,date=date2,type2="vessel_class",mc.cores = 6)

# ------- vessel presence ####
abs_f="background"
outdir="/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/extracto"
csv_path_fishing=glue("/Users/heatherwelch/Dropbox/IUU_GRW/modeling_data_06_03_22/fishing_{dist}_12hr_{type}_{abs_f}_{date}.csv")
date2=glue("{dist}_{type}_{abs_f}_{date}.csv")
vesselNames=c("squid_jigger","drifting_longlines","tuna_purse_seines","trawlers")

extracto_static_wrapper_fishing_2017_background_distinct(date=date2,outdir=outdir,csv_path=csv_path_fishing,vessel="all",type2="all")
mclapply(vesselNames,FUN=extracto_static_wrapper_fishing_2017_background_distinct,outdir=outdir,csv_path=csv_path_fishing,date=date2,type2="vessel_class",mc.cores = 6)





### secondary datasets used for model evaluation ####
# same as above, except pseudo-absences are not subsampled in order to grab new subsets of p/a for cross validation
type="PresAbs"
abs_g="fishing_presence"
abs_f="background"
dist="50nm"

# ------- Gaps ####
outdir="/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/extracto"
csv_path_gaps=glue("/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/gaps_v20201209_{dist}_12hr_{type}_{abs_g}_{date}.csv")
date2=glue("{dist}_{type}_{abs_g}_{date}.csv")
vesselNames=c("squid_jigger","drifting_longlines","tuna_purse_seines","trawlers")

extracto_static_wrapper_2017_vessel_days_BalancedRestrictive_off_distinct_reviewer4(date=date2,outdir=outdir,csv_path=csv_path_gaps,vessel="all",type2="all")
mclapply(vesselNames,FUN=extracto_static_wrapper_2017_vessel_days_BalancedRestrictive_off_distinct_reviewer4,outdir=outdir,csv_path=csv_path_gaps,date=date2,type2="vessel_class",mc.cores = 6)









