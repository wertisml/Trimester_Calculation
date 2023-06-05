library(tidyverse)
library(arrow)
library(furrr)
library(future)

setwd("~/Trimester_Calculation/Data")

#==============================================================================#
# Create a dataframe of percentiles of temperatures for each Zip Code
#==============================================================================#

# determine the percentile of temperatures per Zip Code
Percentile_Data <- open_dataset("Heatwave.parquet") %>%
  rename(Exposure = TAVG,
         location = Zip) %>% # Rename the exposure variable that we are curious about
  select(location, Exposure) %>% 
  collect() %>%
  group_by(location) %>%
  summarise(percentile_95 = quantile(Exposure, 0.95, na.rm = TRUE),
            percentile_90 = quantile(Exposure, 0.90, na.rm = TRUE),
            percentile_10 = quantile(Exposure, 0.10, na.rm = TRUE),
            percentile_05 = quantile(Exposure, 0.05, na.rm = TRUE))

# Calculate the mean of rows within the predefined ranges
dataset <- Percentile_Data %>%
  left_join(read_parquet("Outputs/TAVG_Exposure.parquet"), by = "location") %>%
  filter(!is.na(RFA_ID), !is.na(Day_1)) %>%
  mutate(Identifier = row_number())

write_dataset(dataset, "TAVG_Exposure_partitioned", partitioning = c("Location"))
