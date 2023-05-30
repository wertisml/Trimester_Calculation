library(arrow)
library(tidyverse)
library(furrr)
library(future)

setwd("~/Trimester_Calculation/Data/Outputs")

dat.files <- list.files("TAVG_Exposure_partitioned")

# Calculate the average value of temperature that fall within the range of percentile
# bins between the start and end day that we want

Exposure_Percentiles <- function(Data){
  
  Data <- Data %>%
    rowwise() %>%
    mutate(mean_below_5th = mean(c_across(starts_with(start_col):starts_with(end_col))[c_across(starts_with(start_col):starts_with(end_col)) < percentile_05], na.rm = TRUE),
           mean_5th_to_10th = mean(c_across(starts_with(start_col):starts_with(end_col))[c_across(starts_with(start_col):starts_with(end_col)) >= percentile_05 & c_across(starts_with(start_col):starts_with(end_col)) < percentile_10], na.rm = TRUE),
           mean_10th_to_90th = mean(c_across(starts_with(start_col):starts_with(end_col))[c_across(starts_with(start_col):starts_with(end_col)) >= percentile_05 & c_across(starts_with(start_col):starts_with(end_col)) <= percentile_90], na.rm = TRUE),
           mean_90th_to_95th = mean(c_across(starts_with(start_col):starts_with(end_col))[c_across(starts_with(start_col):starts_with(end_col)) > percentile_90 & c_across(starts_with(start_col):starts_with(end_col)) <= percentile_95], na.rm = TRUE),
           mean_above_95th = mean(c_across(starts_with(start_col):starts_with(end_col))[c_across(starts_with(start_col):starts_with(end_col)) > percentile_95], na.rm = TRUE)) %>%
    ungroup() %>%
    select(RFA_ID, ZIP, mean_below_5th, mean_5th_to_10th, mean_10th_to_90th, mean_90th_to_95th, mean_above_95th)
  
  return(Data)
}

# Set up the calculation to be ready to run in parallel
Exposure_Calculation_pipeline <- function(dataset) {
  
  test <- dataset %>%
    nest(data = c(-Identifier)) %>%
    mutate(calculate = future_map(data, Exposure_Percentiles)) %>%
    select(-data) %>% 
    unnest(cols = c(calculate), names_repair = "minimal")
}

# Specify the range for which trimester you are curious about
start_col <- "Day_1"
end_col <- "Day_91"

# Set-up for loop to process 1 Zip Code at a time
for(i in 1:length(dat.files)){
  
  # Read in a single partitioned parquet file
  setwd("~/Trimester_Calculation/Data/Outputs/TAVG_Exposure_partitioned")
  dataset <- open_dataset(dat.files[i]) %>% select(-Day_0) %>% collect()

  # Run in parallel
  setwd("~/Trimester_Calculation/Data/Outputs")
  plan(multisession, workers = (availableCores() - 1))
  Average_Exposure_Value <- Exposure_Calculation_pipeline(dataset)
  
  # Write the new calculated dataset out as a parquet file with the Zip Code as the end of the file name
  setwd("~/Trimester_Calculation/Data/Outputs/Percentiles_Exposure")
  write_parquet(Average_Exposure_Value, paste0("Average_Exposure_Value_",dataset$ZIP[1],".parquet"))

}
