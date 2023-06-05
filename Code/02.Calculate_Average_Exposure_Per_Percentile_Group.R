library(arrow)
library(tidyverse)
library(furrr)
library(future)

setwd("~/Trimester_Calculation/Data/Outputs")

dat.files <- list.files("TAVG_Exposure_partitioned")

#==============================================================================#
#
#==============================================================================#

Exposure_Percentiles <- function(Data){
  
  Extremes <- Data %>%
    select(percentile_95, percentile_90, percentile_10, percentile_05) %>%
    distinct() %>% 
    data.frame()
  
  Data <- Data %>%
    select(starts_with("Day")) %>%
    mutate(row = row_number()) %>%
    tidyr::pivot_longer(-row) %>%
    group_by(row) %>%
    summarise(mean_below_5th = mean(value[value < Extremes[,4]]),
              mean_5th_to_10th = mean(value[value >= Extremes[,4] & value <= Extremes[,3]], na.rm=T),
              mean_10th_to_90th = mean(value[value >= Extremes[,3] & value <= Extremes[,2]], na.rm=T),
              mean_90th_to_95th = mean(value[value >= Extremes[,2] & value <= Extremes[,1]], na.rm=T),
              mean_above_95th = mean(value[value > Extremes[,1]])) %>%
    bind_cols(Data, .) %>%
    data.frame() %>%
    select(RFA_ID, location, mean_below_5th, mean_5th_to_10th, mean_10th_to_90th, mean_90th_to_95th, mean_above_95th, Identifier)
  
  return(Data)
}

# Specify the range for which trimester you are curious about
start_col <- "Day_1"
end_col <- "Day_91"

# Run in parallel
setwd("~/Trimester_Calculation/Data/Outputs")
plan(multisession, workers = (availableCores() - 1))

# Set-up for loop to process 1 Zip Code at a time
for(i in 1:length(dat.files)){
  
  # Read in a single partitioned parquet file
  setwd("~/Trimester_Calculation/Data/Outputs/TAVG_Exposure_partitioned")
  
  dataset <- open_dataset(dat.files[i]) %>% 
    select(location, RFA_ID, percentile_95, percentile_90, percentile_10, percentile_05,
           Tr1, Tr3_end, all_of(start_col):all_of(end_col), Identifier) %>% 
    collect()
  
  Average_Exposure_Value <- Exposure_Percentiles(dataset)
  
  # Write the new calculated dataset out as a parquet file with the Zip Code as the end of the file name
  setwd("~/Trimester_Calculation/Data/Outputs/Percentiles_Exposure")
  write_parquet(Average_Exposure_Value, paste0("Average_Exposure_Value_",dataset$location[i],".parquet"))
  
}
