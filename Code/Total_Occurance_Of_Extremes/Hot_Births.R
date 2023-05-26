library(tidyverse)
library(arrow)
library(data.table)

setwd("~/Trimester_Calculations/Data")

birth <- read_parquet("Trimester_Data.parquet")
Hot <- read_parquet("Heatwave.parquet")

#==============================================================================#
# Clean ZIP information
#==============================================================================#

birth$ZIP <- as.numeric(birth$ZIP)

birth <- birth %>%
  filter(ZIP < 29945) %>%
  filter(ZIP > 29000) %>%
  mutate(ADMD = as.Date(ADMD, "1960-01-01"))

Hot <- Hot %>%
  filter(Zip < 29945) %>%
  filter(Zip > 29000)

#==============================================================================#
#Set-up for the trimesters
#==============================================================================#

Cohorts <- birth
Cohorts$GEST <- as.numeric(Cohorts$GEST)

Cohorts$Pre <- as.Date(Cohorts$ADMD) - ((Cohorts$GEST * 7) + (13*7))
Cohorts$Tr1 <- as.Date(Cohorts$ADMD) - (Cohorts$GEST * 7)
Cohorts$Tr2 <- as.Date(Cohorts$ADMD) - ((Cohorts$GEST * 7) - (13*7))
Cohorts$Tr3 <- as.Date(Cohorts$ADMD) - ((Cohorts$GEST * 7) - (26*7))
Cohorts$Tr3_end <- Cohorts$ADMD

#==============================================================================#
#Set-up for Heatwave intensity
#==============================================================================#

low_intensity <- Hot %>%
  dplyr::select(Date, low_intensity, Zip) %>%
  filter(low_intensity == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

moderate_intensity <- Hot %>%
  dplyr::select(Date, moderate_intensity, Zip) %>%
  filter(moderate_intensity == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

high_intensity <- Hot %>%
  dplyr::select(Date, high_intensity, Zip) %>%
  filter(high_intensity == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

#==============================================================================#
#Set-up for Heatwave information
#==============================================================================#

No_Heatwave <- Hot %>%
  dplyr::select(Date, No_Heatwave, Zip) %>%
  filter(No_Heatwave == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

Heatwave <- Hot %>%
  dplyr::select(Date, Heatwave_days, Zip) %>%
  filter(Heatwave_days == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

Severe_Heatwave <- Hot %>%
  dplyr::select(Date, Severe_Heatwaves, Zip) %>%
  filter(Severe_Heatwaves == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

Extreme_Heatwave <- Hot %>%
  dplyr::select(Date, Extreme_Heatwaves, Zip) %>%
  filter(Extreme_Heatwaves == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

#==============================================================================#
#Set-up for Temperature Extremes information
#==============================================================================#

Above_95th <- Hot %>%
  dplyr::select(Date, Above_95th, Zip) %>%
  filter(Above_95th == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

Above_97th <- Hot %>%
  dplyr::select(Date, Above_97th, Zip) %>%
  filter(Above_97th == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

Above_99th <- Hot %>%
  dplyr::select(Date, Above_99th, Zip) %>%
  filter(Above_99th == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

#==============================================================================#
#Set-up for Preconception
#==============================================================================#

zips <- data.table(table(Hot$Zip))

Pre <- Cohorts %>%
  arrange(ZIP) %>%
  filter_at(vars(contains("ZIP")), any_vars(. %in% zips$V1)) %>%
  mutate(End = as.Date(Pre)+90,
         Number = cumsum(c(1,as.numeric(diff(ZIP))!=0)))%>%
  drop_na(Pre, End, ZIP) %>%
  dplyr::select(ZIP, ADMD, RFA_ID, Pre, End, Number)

#==============================================================================#
# For loop for Preconception
#==============================================================================#

data <- data.frame()
Data <- data.frame()

tic()
n= 1
for(i in Pre$Number){
  if(n == i){
    mydata2 <- Pre %>%
      filter(Number == i)
    n_heat <- No_Heatwave %>%
      filter(Number == i)
    heat <- Heatwave %>%
      filter(Number == i)
    s_heat <- Severe_Heatwave %>%
      filter(Number == i)
    e_heat <- Extreme_Heatwave %>%
      filter(Number == i)
    Above_95 <- Above_95th %>%
      filter(Number == i)
    Above_97 <- Above_97th %>%
      filter(Number == i)
    Above_99 <- Above_99th  %>%
      filter(Number == i)
    low_i <- low_intensity %>%
      filter(Number == i)
    moderate_i <- moderate_intensity %>%
      filter(Number == i)
    high_i <- high_intensity %>%
      filter(Number == i)
    
    mydata2$No_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(n_heat$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(heat$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Severe_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(s_heat$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Extreme_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(e_heat$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_95th <- apply(mydata2, 1, function(x) sum(as.Date(Above_95$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_97th <- apply(mydata2, 1, function(x) sum(as.Date(Above_97$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_99th <- apply(mydata2, 1, function(x) sum(as.Date(Above_99$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$low_intensity <- apply(mydata2, 1, function(x) sum(as.Date(low_i$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$moderate_intensity <- apply(mydata2, 1, function(x) sum(as.Date(moderate_i$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$high_intensity <- apply(mydata2, 1, function(x) sum(as.Date(high_i$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    
    data <- data.frame(mydata2)
    Data <- rbind(Data, data)
    
    print(n)
    n = n + 1
  }
}
toc()

fwrite(Data, "Preconception_Hot.csv")

#==============================================================================#
#Set-up for First Trimester
#==============================================================================#

zips <- data.table(table(Hot$Zip))

First <- Cohorts %>%
  arrange(ZIP) %>%
  filter_at(vars(contains("ZIP")), any_vars(. %in% zips$V1)) %>%
  mutate(End = as.Date(Tr1)+90,
         Number = cumsum(c(1,as.numeric(diff(ZIP))!=0)))%>%
  drop_na(Tr1, End, ZIP) %>%
  dplyr::select(ZIP, ADMD, RFA_ID, Tr1, End, Number)

#==============================================================================#
#For loop for First Trimester
#==============================================================================#

Data1 <- NULL
data <- NULL
data <- data.frame(data)
Data1 <- data.frame(Data1)

tic()
n= 1
for(i in First$Number){
  if(n == i){
    mydata2 <- First %>%
      filter(Number == i)
    n_heat <- No_Heatwave %>%
      filter(Number == i)
    heat <- Heatwave %>%
      filter(Number == i)
    s_heat <- Severe_Heatwave %>%
      filter(Number == i)
    e_heat <- Extreme_Heatwave %>%
      filter(Number == i)
    Above_95 <- Above_95th %>%
      filter(Number == i)
    Above_97 <- Above_97th %>%
      filter(Number == i)
    Above_99 <- Above_99th  %>%
      filter(Number == i)
    low_i <- low_intensity %>%
      filter(Number == i)
    moderate_i <- moderate_intensity %>%
      filter(Number == i)
    high_i <- high_intensity %>%
      filter(Number == i)
    
    mydata2$No_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(n_heat$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(heat$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Severe_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(s_heat$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Extreme_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(e_heat$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_95th <- apply(mydata2, 1, function(x) sum(as.Date(Above_95$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_97th <- apply(mydata2, 1, function(x) sum(as.Date(Above_97$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_99th <- apply(mydata2, 1, function(x) sum(as.Date(Above_99$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$low_intensity <- apply(mydata2, 1, function(x) sum(as.Date(low_i$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$moderate_intensity <- apply(mydata2, 1, function(x) sum(as.Date(moderate_i$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$high_intensity <- apply(mydata2, 1, function(x) sum(as.Date(high_i$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    
    data <- data.frame(mydata2)
    Data1 <- rbind(Data1, data)
     
     print(n)
     n = n + 1
  }
}
toc()

fwrite(Data1, "Trimester1_Hot.csv")

#==============================================================================#
#Set-up for Second Trimester
#==============================================================================#

zips <- data.table(table(Hot$Zip))

Second <- Cohorts %>%
  arrange(ZIP) %>%
  filter_at(vars(contains("ZIP")), any_vars(. %in% zips$V1)) %>%
  mutate(End = as.Date(Tr2)+90,
         Number = cumsum(c(1,as.numeric(diff(ZIP))!=0)))%>%
  filter(Tr3 < ADMD) %>%
  drop_na(Tr2, End, ZIP) %>%
  dplyr::select(ZIP, ADMD, RFA_ID, Tr2, End, Number)

#==============================================================================#
#For loop for Second Trimester
#==============================================================================#

Data2 <- NULL
data <- NULL
data <- data.frame(data)
Data2 <- data.frame(Data2)

tic()
n= 1
for(i in Second$Number){
  if(n == i){
    mydata2 <- Second %>%
      filter(Number == i)
    n_heat <- No_Heatwave %>%
      filter(Number == i)
    heat <- Heatwave %>%
      filter(Number == i)
    s_heat <- Severe_Heatwave %>%
      filter(Number == i)
    e_heat <- Extreme_Heatwave %>%
      filter(Number == i)
    Above_95 <- Above_95th %>%
      filter(Number == i)
    Above_97 <- Above_97th %>%
      filter(Number == i)
    Above_99 <- Above_99th  %>%
      filter(Number == i)
    low_i <- low_intensity %>%
      filter(Number == i)
    moderate_i <- moderate_intensity %>%
      filter(Number == i)
    high_i <- high_intensity %>%
      filter(Number == i)
    
    mydata2$No_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(n_heat$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(heat$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Severe_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(s_heat$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Extreme_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(e_heat$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_95th <- apply(mydata2, 1, function(x) sum(as.Date(Above_95$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_97th <- apply(mydata2, 1, function(x) sum(as.Date(Above_97$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_99th <- apply(mydata2, 1, function(x) sum(as.Date(Above_99$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$low_intensity <- apply(mydata2, 1, function(x) sum(as.Date(low_i$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$moderate_intensity <- apply(mydata2, 1, function(x) sum(as.Date(moderate_i$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$high_intensity <- apply(mydata2, 1, function(x) sum(as.Date(high_i$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    
    data <- data.frame(mydata2)
    Data2 <- rbind(Data2, data)
    
    print(n)
    n = n + 1
  }
}
toc()

fwrite(Data2, "Trimester2_Hot.csv")

#==============================================================================#
#Set-up for Second Trimester Births
#==============================================================================#

zips <- data.table(table(Hot$Zip))

Second_Birth <- Cohorts %>%
  arrange(ZIP) %>%
  filter_at(vars(contains("ZIP")), any_vars(. %in% zips$V1)) %>%
  mutate(End = as.Date(ADMD),
         Number = cumsum(c(1,as.numeric(diff(ZIP))!=0)))%>%
  filter(Tr3 > ADMD) %>%
  subset(Tr2 < ADMD) %>%
  drop_na(Tr2, End, ZIP) %>%
  dplyr::select(ZIP, ADMD, RFA_ID, Tr2, End, Number)

#==============================================================================#
#For loop for Second Trimester Births
#==============================================================================#

Data3 <- NULL
data <- NULL
data <- data.frame(data)
Data3 <- data.frame(Data3)

tic()
n= 1
for(i in Second_Birth$Number){
  if(n == i){
    mydata2 <- Second_Birth %>%
      filter(Number == i)
    n_heat <- No_Heatwave %>%
      filter(Number == i)
    heat <- Heatwave %>%
      filter(Number == i)
    s_heat <- Severe_Heatwave %>%
      filter(Number == i)
    e_heat <- Extreme_Heatwave %>%
      filter(Number == i)
    Above_95 <- Above_95th %>%
      filter(Number == i)
    Above_97 <- Above_97th %>%
      filter(Number == i)
    Above_99 <- Above_99th  %>%
      filter(Number == i)
    low_i <- low_intensity %>%
      filter(Number == i)
    moderate_i <- moderate_intensity %>%
      filter(Number == i)
    high_i <- high_intensity %>%
      filter(Number == i)
    
    mydata2$No_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(n_heat$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(heat$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Severe_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(s_heat$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Extreme_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(e_heat$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_95th <- apply(mydata2, 1, function(x) sum(as.Date(Above_95$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_97th <- apply(mydata2, 1, function(x) sum(as.Date(Above_97$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_99th <- apply(mydata2, 1, function(x) sum(as.Date(Above_99$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$low_intensity <- apply(mydata2, 1, function(x) sum(as.Date(low_i$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$moderate_intensity <- apply(mydata2, 1, function(x) sum(as.Date(moderate_i$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$high_intensity <- apply(mydata2, 1, function(x) sum(as.Date(high_i$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    
    data <- data.frame(mydata2)
    Data3 <- rbind(Data3, data)
    
    print(n)
    n = n + 1
  }
}
toc()

fwrite(Data3, "Trimester2_Preme_Hot.csv")

#==============================================================================#
#Set-up for Third Trimester
#==============================================================================#

zips <- data.table(table(Hot$Zip))

Third <- Cohorts %>%
  arrange(ZIP) %>%
  filter_at(vars(contains("ZIP")), any_vars(. %in% zips$V1)) %>%
  mutate(End = as.Date(Tr3_end),
         Number = cumsum(c(1,as.numeric(diff(ZIP))!=0)))%>%
  subset(Tr3 < ADMD) %>%
  drop_na(Tr3, End, ZIP) %>%
  dplyr::select(ZIP, ADMD, RFA_ID, Tr3, End, Number)

#==============================================================================#
#For loop for Third Trimester
#==============================================================================#

Data4 <- NULL
data <- NULL
data <- data.frame(data)
Data4 <- data.frame(Data4)

tic()
n= 1
for(i in Third$Number){
  if(n == i){
    mydata2 <- Third %>%
      filter(Number == i)
    n_heat <- No_Heatwave %>%
      filter(Number == i)
    heat <- Heatwave %>%
      filter(Number == i)
    s_heat <- Severe_Heatwave %>%
      filter(Number == i)
    e_heat <- Extreme_Heatwave %>%
      filter(Number == i)
    Above_95 <- Above_95th %>%
      filter(Number == i)
    Above_97 <- Above_97th %>%
      filter(Number == i)
    Above_99 <- Above_99th  %>%
      filter(Number == i)
    low_i <- low_intensity %>%
      filter(Number == i)
    moderate_i <- moderate_intensity %>%
      filter(Number == i)
    high_i <- high_intensity %>%
      filter(Number == i)
    
    mydata2$No_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(n_heat$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(heat$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Severe_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(s_heat$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Extreme_Heatwave <- apply(mydata2, 1, function(x) sum(as.Date(e_heat$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_95th <- apply(mydata2, 1, function(x) sum(as.Date(Above_95$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_97th <- apply(mydata2, 1, function(x) sum(as.Date(Above_97$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Above_99th <- apply(mydata2, 1, function(x) sum(as.Date(Above_99$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$low_intensity <- apply(mydata2, 1, function(x) sum(as.Date(low_i$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$moderate_intensity <- apply(mydata2, 1, function(x) sum(as.Date(moderate_i$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$high_intensity <- apply(mydata2, 1, function(x) sum(as.Date(high_i$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    
    data <- data.frame(mydata2)
    Data4 <- rbind(Data4, data)
    
    print(n)
    n = n + 1
  }
}
toc()

fwrite(Data4, "Trimester3_Hot.csv")
