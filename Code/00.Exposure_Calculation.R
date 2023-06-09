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
  filter(Days_Between <= 392) %>% # If the individual was pregnate for over 301 days they are removed, there are some errors with individuals pregnate for 600+ days
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
  
  birth_data <- birth_data %>%
    data.frame()
  
  # Calculate number of days between start and end date
  birth_data$duration <- as.numeric(birth_data$Tr3_end - birth_data$Pre) + 1
  
  start_day <- birth_data$Pre
  end_day <- birth_data$Tr3_end
  num_days <- birth_data$duration
  column_names <- paste0("Day_", 1:max(num_days))
  
  # Create a list of date ranges, padding shorter ranges with NA values
  date_ranges <- mapply(function(start, end, days) {
    seq(start, end, by = "day")[1:days]
  }, start_day, end_day, num_days)
  
  # Pad shorter date ranges with NA values
  max_length <- max(num_days)
  date_ranges <- lapply(date_ranges, function(x) {
    if (length(x) < max_length) {
      c(x, rep(NA, max_length - length(x)))
    } else {
      x
    }
  })
  
  birth_data[column_names] <- as.data.frame(do.call(rbind, date_ranges))
  
  # Find columns starting with "Day_"
  day_columns <- grep("^Day_", names(birth_data), value = TRUE)
  
  # Convert numeric columns to date format
  birth_data[, day_columns] <- lapply(birth_data[, day_columns], function(x) as.Date(as.numeric(x), origin = "1970-01-01"))
  
  birth_data <- birth_data %>%
    select(-duration)
  
  return(birth_data)
}

# Assign TAVG temps to each day
assign_Exposure_temperatures <- function(birth_data){
  
  birth_data <- birth_data %>%
    mutate(row = row_number())
  
  birth_data <- birth_data %>%
    select(starts_with("Day"), location, row) %>%
    tidyr::pivot_longer(c(-"location", -"row")) %>%
    group_by(location) %>%
    left_join(Exposure, by = c("location" = "location", "value" = "Date")) %>%
    select(-value) %>%
    pivot_wider(names_from = name, values_from = Exposure) %>%
    data.frame() %>%
    left_join(birth_data %>%
                select(location, row, Date, RFA_ID, Pre, Tr3_end),
              by = join_by(location, row)) %>%
    select(-row) %>%
    relocate(c("Date", "RFA_ID", "Pre", "Tr3_end", .after = "location"))
  
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
    nest(data = c(-Location)) %>%
    mutate(calculate = future_map(data, Exposure_Calculation)) %>%
    select(-data) %>% 
    unnest(cols = c(calculate), names_repair = "minimal")
}

# Run in parallel    
plan(multisession, workers = (availableCores() - 1))
system.time(TAVG_Exposure <- Exposure_Calculation_pipeline(birth))

setwd("~/Trimester_Calculation/Data/Outputs")
write_parquet(TAVG_Exposure, "TAVG_Exposure.parquet")


