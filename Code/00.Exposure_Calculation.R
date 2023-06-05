library(future)
library(furrr)
library(tidyverse)
library(arrow)

setwd("~/Trimester_Calculation/Data")

# This is where we subset the date to fit our need
birth <- open_dataset("Trimester_Data.parquet") %>%
  rename(Date = ADMD,
         location = ZIP) %>% # Renaming the Admitantce column
  filter(location < 29945, location > 29000) %>% # Limit the data to individuals in SC
  collect() %>%
  mutate(Date = as.Date(Date, "1960-01-01"), # Changing the date format
         Pre = as.Date(Date) - ((GEST * 7) + (13*7)), # Calculating the start of Trimester 1 based on ADMD and gestation age
         Tr3_end = Date,
         Days_Between = interval(Pre, Tr3_end) %/% days(1), # Determine the number of days between the start of trimester 1 and birth
         Location = location) %>%
  filter(Days_Between <= 301) %>% # If the individual was pregnate for over 301 days they are removed, there are some errors with individuals pregnate for 600+ days
  select(location, Location, Date, RFA_ID, Pre, Tr3_end)

Temperature <- open_dataset("Heatwave.parquet") %>%
  rename(location = Zip) %>%
  collect() %>%
  filter(location < 29945, location > 29000) # Limits the data to just SC Zip codes

Exposure <- Temperature %>%
  rename(Exposure = TAVG) %>% # Rename the exposure variable that we are curious about
  select(Date, location, Exposure) 

#==============================================================================#
# Functions
#==============================================================================#

# This function takes a dataframe birth_data and adds new columns representing 
# each day between the start and end dates specified in the dataframe. The
# function modifies the original dataframe and returns the modified dataframe as 
# the output.

create_days <- function(birth_data){
  # Create new columns for each row
  for (i in 1:nrow(birth_data)) {
    start_date <- birth_data$Pre[i]
    end_date <- birth_data$Tr3_end[i]
    
    # Calculate the number of days between the start and end dates
    num_days <- as.integer(end_date - start_date)
    
    # Create new columns for each day
    for (j in 1:num_days) {
      new_col_name <- paste0("Day_", j)
      date_val <- start_date + j
      birth_data[i, new_col_name] <- date_val
    }
  }
  return(birth_data)
}

# Assign TAVG temps to each day
assign_Exposure_temperatures <- function(birth_data){
  # Iterate over the date columns in the birth dataset
  for (col in colnames(birth_data)[-c(1:5)]) {
    # Perform a left join to match dates and replace with temperature values
    merged_dataset <- birth_data %>%
      rename("Date_in_Question" = col) %>%
      left_join(Exposure, by = c("Date_in_Question" = "Date", "location" = "location")) %>%
      select("Date_in_Question", Exposure) %>%
      rename(!!col := Exposure)
    
    # Replace the original birth dataset column with the merged dataset column
    birth_data[, col] <- merged_dataset[, col]
  }
  return(birth_data)
}

# Runs the preivious two functions
Exposure_Calculation <- function(birth_data){
  
  birth_data %>%
    create_days() %>%
    assign_Exposure_temperatures()
}

Exposure_Calculation_pipeline <- function(birth_data) {
  
  test <- birth_data %>%
    nest(data = c(-location)) %>%
    mutate(calculate = future_map(data, Exposure_Calculation)) %>%
    select(-data) %>% 
    unnest(cols = c(calculate), names_repair = "minimal")
}

# Run in parallel
plan(multisession, workers = (availableCores() - 1))
TAVG_Exposure <- Exposure_Calculation_pipeline(birth)


setwd("~/Trimester_Calculation/Data/Outputs")
write_parquet(TAVG_Exposure, "TAVG_Exposure.parquet")


