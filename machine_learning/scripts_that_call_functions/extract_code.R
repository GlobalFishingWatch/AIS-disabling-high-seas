### THIS DATA SET IS ONLY TO CREAT A MODELLING DATASET FOR GAPS THAT USES VESSEL DAYS (AS OPPOSED TO FISHING DAYS) FOR ABSENCES

source("functions/load_libraries.R")
source("functions/extracto_functions.R")
source("functions/filter_down_your_data_function.R")
library(purrr)

# all gaps pos25hrb4 ####
outdir="/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/extracto"
csv_path="/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/gaps_v20201209_200nm_12hr_pos25hrb4_VesselDays_12_21_20.csv"
date="12_21_20_pos25hrb4"
extracto_static_wrapper_2017_vessel_days_BalancedRestrictive_off(date=date,outdir=outdir,csv_path=csv_path,vessel="all",type="all")

# gaps vessel_class pos25hrb4 ####
outdir="/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/extracto"

vesselNames=c("squid_jigger","drifting_longlines","tuna_purse_seines","trawlers")

csv_path="/Users/heatherwelch/Dropbox/IUU_GRW/Gaps_data_2017_2019/gaps_v20201209_200nm_12hr_pos25hrb4_VesselDays_12_21_20.csv"
date="12_21_20_pos25hrb4"
mclapply(vesselNames,FUN=extracto_static_wrapper_2017_vessel_days_BalancedRestrictive_off,outdir=outdir,csv_path=csv_path,date=date,type="vessel_class",mc.cores = 6)


# all fishing ####

outdir="/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/extracto"
csv_path="/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/fishing_200nm_12_21_20.csv"
date="12_21_20"
extracto_static_wrapper_fishing_2017(date=date,outdir=outdir,csv_path=csv_path,vessel="all",type="all")

# fishing vessel_class ####
outdir="/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/extracto"

vesselNames=c("squid_jigger","drifting_longlines","tuna_purse_seines","trawlers")

csv_path="/Users/heatherwelch/Dropbox/IUU_GRW/fishing_activity_modeling_2017_2019/fishing_200nm_12_21_20.csv"
date="12_21_20"
mclapply(vesselNames,FUN=extracto_static_wrapper_fishing_2017,outdir=outdir,csv_path=csv_path,date=date,type="vessel_class",mc.cores = 6)






