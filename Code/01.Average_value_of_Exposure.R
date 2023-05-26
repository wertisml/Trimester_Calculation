library(tidyverse)
library(arrow)

setwd("~/Trimester_Calculation/Data/Files_For_Calculations")

#==============================================================================#
# Create a dataframe of percentiles of temperatures for each Zip Code
#==============================================================================#

Percentile_Data <- open_dataset("Heatwave.parquet") %>%
  rename(Exposure = TAVG,
         ZIP = Zip) %>% # Rename the exposure variable that we are curious about
  select(ZIP, Exposure) %>% 
  collect() %>%
  group_by(ZIP) %>%
  summarise(percentile_95 = quantile(Exposure, 0.95, na.rm = TRUE),
            percentile_90 = quantile(Exposure, 0.90, na.rm = TRUE),
            percentile_10 = quantile(Exposure, 0.10, na.rm = TRUE),
            percentile_05 = quantile(Exposure, 0.05, na.rm = TRUE))

#==============================================================================#
# Search over Birth data frame and get the average extremes temperatures experienced
#==============================================================================#

# Specify the range for which trimester you are curious about
start_col <- "Day_1"
end_col <- "Day_5"

# Calculate the mean of rows within the predefined ranges
result_df <- Percentile_Data %>%
  left_join(read_parquet("Outputs/TAVG_Exposure.parquet"), by = "ZIP") %>%
  rowwise() %>%
  mutate(mean_below_5th = mean(c_across(starts_with(start_col):starts_with(end_col))[c_across(starts_with(start_col):starts_with(end_col)) < percentile_05], na.rm = TRUE),
         mean_5th_to_10th = mean(c_across(starts_with(start_col):starts_with(end_col))[c_across(starts_with(start_col):starts_with(end_col)) >= percentile_05 & c_across(starts_with(start_col):starts_with(end_col)) < percentile_10], na.rm = TRUE),
         mean_10th_to_90th = mean(c_across(starts_with(start_col):starts_with(end_col))[c_across(starts_with(start_col):starts_with(end_col)) >= percentile_05 & c_across(starts_with(start_col):starts_with(end_col)) <= percentile_90], na.rm = TRUE),
         mean_90th_to_95th = mean(c_across(starts_with(start_col):starts_with(end_col))[c_across(starts_with(start_col):starts_with(end_col)) > percentile_90 & c_across(starts_with(start_col):starts_with(end_col)) <= percentile_95], na.rm = TRUE),
         mean_above_95th = mean(c_across(starts_with(start_col):starts_with(end_col))[c_across(starts_with(start_col):starts_with(end_col)) > percentile_95], na.rm = TRUE)) %>%
  ungroup() %>%
  select(ZIP, RFA_ID, mean_below_5th, mean_5th_to_10th, mean_10th_to_90th, mean_90th_to_95th, mean_above_95th)

# View the resulting data frame
result_df

setwd("~/Trimester_Calculation/Data/Outputs")
write_parquet(TAVG_Exposure, "TAVG_Average_Exposure.parquet")
