library(arrow)
library(tidyverse)

setwd("~/Trimester_Calculations")

birth <- fread("E:\\Birth_cohorts\\zipcode.csv")
Chilly <- fread("E:\\Birth_cohorts\\trimesters\\temperature\\Coldwave_Runkle.csv")

#==============================================================================#
# Clean ZIP information
#==============================================================================#

birth$ZIP <- as.numeric(birth$ZIP)

birth <- birth %>%
  filter(ZIP < 29945) %>%
  filter(ZIP > 29000) %>%
  mutate(ADMD = as.Date(ADMD, "1960-01-01"))

Chilly <- Chilly %>%
  filter(Zip < 29945) %>%
  filter(Zip > 29000)

#======================================================================#
#Set-up for the trimesters
#======================================================================#

Cohorts <- birth
Cohorts$GEST <- as.numeric(Cohorts$GEST)

Cohorts$Pre <- as.Date(Cohorts$ADMD) - ((Cohorts$GEST * 7) + (13*7))
Cohorts$Tr1 <- as.Date(Cohorts$ADMD) - (Cohorts$GEST * 7)
Cohorts$Tr2 <- as.Date(Cohorts$ADMD) - ((Cohorts$GEST * 7) - (13*7))
Cohorts$Tr3 <- as.Date(Cohorts$ADMD) - ((Cohorts$GEST * 7) - (26*7))
Cohorts$Tr3_end <- Cohorts$ADMD

#==============================================================================#
#Set-up for Coldwave intensity
#==============================================================================#

low_intensity <- Chilly %>%
  dplyr::select(Date, low_intensity, Zip) %>%
  filter(low_intensity == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

moderate_intensity <- Chilly %>%
  dplyr::select(Date, moderate_intensity, Zip) %>%
  filter(moderate_intensity == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

high_intensity <- Chilly %>%
  dplyr::select(Date, high_intensity, Zip) %>%
  filter(high_intensity == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

#==============================================================================#
#Set-up for Coldwave information
#==============================================================================#

No_Coldwave <- Chilly %>%
  dplyr::select(Date, No_Coldwave, Zip) %>%
  filter(No_Coldwave == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

Coldwave <- Chilly %>%
  dplyr::select(Date, Coldwave_days, Zip) %>%
  filter(Coldwave_days == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

Severe_Coldwave <- Chilly %>%
  dplyr::select(Date, Severe_Coldwaves, Zip) %>%
  filter(Severe_Coldwaves == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

Extreme_Coldwave <- Chilly %>%
  dplyr::select(Date, Extreme_Coldwaves, Zip) %>%
  filter(Extreme_Coldwaves == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

#==============================================================================#
#Set-up for Temperature Extremes information
#==============================================================================#

Below_5th <- Chilly %>%
  dplyr::select(Date, Below_5th, Zip) %>%
  filter(Below_5th == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

Below_3rd <- Chilly %>%
  dplyr::select(Date, Below_3rd, Zip) %>%
  filter(Below_3rd == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

Below_1st <- Chilly %>%
  dplyr::select(Date, Below_1st, Zip) %>%
  filter(Below_1st == 1, Zip > 29000)%>%
  mutate(Number = cumsum(c(1,as.numeric(diff(Zip))!=0)))

#==============================================================================#
#Set-up for Preconception
#==============================================================================#

zips <- data.table(table(Chilly$Zip))

Pre <- Cohorts %>%
  arrange(ZIP) %>%
  filter_at(vars(contains("ZIP")), any_vars(. %in% zips$V1)) %>%
  mutate(End = as.Date(Pre)+90,
         Number = cumsum(c(1,as.numeric(diff(ZIP))!=0)))%>%
  drop_na(Pre, End, ZIP) %>%
  dplyr::select(ZIP, ADMD, RFA_ID, Pre, End, Number)

#==============================================================================#
#For loop for Preconception
#==============================================================================#

Datar <- NULL
data <- NULL
data <- data.frame(data)
Datar <- data.frame(Datar)

tic()
n= 1
for(i in Pre$Number){
  if(n == i){
    mydata2 <- Pre %>%
      filter(Number == i)
    n_Cold <- No_Coldwave %>%
      filter(Number == i)
    Cold <- Coldwave %>%
      filter(Number == i)
    s_Cold <- Severe_Coldwave %>%
      filter(Number == i)
    e_Cold <- Extreme_Coldwave %>%
      filter(Number == i)
    Below_5 <- Below_5th %>%
      filter(Number == i)
    Below_3 <- Below_3rd %>%
      filter(Number == i)
    Below_1 <- Below_1st  %>%
      filter(Number == i)
    low_i <- low_intensity %>%
      filter(Number == i)
    moderate_i <- moderate_intensity %>%
      filter(Number == i)
    high_i <- high_intensity %>%
      filter(Number == i)
    
    mydata2$No_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(n_Cold$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(Cold$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Severe_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(s_Cold$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Extreme_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(e_Cold$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_5th <- apply(mydata2, 1, function(x) sum(as.Date(Below_5$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_3rd <- apply(mydata2, 1, function(x) sum(as.Date(Below_3$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_1st <- apply(mydata2, 1, function(x) sum(as.Date(Below_1$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$low_intensity <- apply(mydata2, 1, function(x) sum(as.Date(low_i$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$moderate_intensity <- apply(mydata2, 1, function(x) sum(as.Date(moderate_i$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    mydata2$high_intensity <- apply(mydata2, 1, function(x) sum(as.Date(high_i$Date) %in% seq(as.Date(x["Pre"]), as.Date(x["End"]), by = "1 day")))
    
    data <- data.frame(mydata2)
    Datar <- rbind(Datar, data)
    
    print(n)
    n = n + 1
  }
}
toc()

fwrite(Datar, "Preconception_Cold.csv")

#==============================================================================#
#Set-up for First Trimester
#==============================================================================#

zips <- data.table(table(Chilly$Zip))

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

Datar1 <- NULL
data <- NULL
data <- data.frame(data)
Datar1 <- data.frame(Datar1)

tic()
n= 1
for(i in First$Number){
  if(n == i){
    mydata2 <- First %>%
      filter(Number == i)
    n_cold <- No_Coldwave %>%
      filter(Number == i)
    Cold <- Coldwave %>%
      filter(Number == i)
    s_Cold <- Severe_Coldwave %>%
      filter(Number == i)
    e_Cold <- Extreme_Coldwave %>%
      filter(Number == i)
    Below_5 <- Below_5th %>%
      filter(Number == i)
    Below_3 <- Below_3rd %>%
      filter(Number == i)
    Below_1 <- Below_1st  %>%
      filter(Number == i)
    low_i <- low_intensity %>%
      filter(Number == i)
    moderate_i <- moderate_intensity %>%
      filter(Number == i)
    high_i <- high_intensity %>%
      filter(Number == i)
    
    mydata2$No_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(n_Cold$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(Cold$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Severe_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(s_Cold$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Extreme_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(e_Cold$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_5th <- apply(mydata2, 1, function(x) sum(as.Date(Below_5$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_3rd <- apply(mydata2, 1, function(x) sum(as.Date(Below_3$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_1st <- apply(mydata2, 1, function(x) sum(as.Date(Below_1$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$low_intensity <- apply(mydata2, 1, function(x) sum(as.Date(low_i$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$moderate_intensity <- apply(mydata2, 1, function(x) sum(as.Date(moderate_i$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    mydata2$high_intensity <- apply(mydata2, 1, function(x) sum(as.Date(high_i$Date) %in% seq(as.Date(x["Tr1"]), as.Date(x["End"]), by = "1 day")))
    
    data <- data.frame(mydata2)
    Datar1 <- rbind(Datar1, data)
    
    print(n)
    n = n + 1
  }
}
toc()

fwrite(Datar1, "Trimester1_Cold.csv")

#==============================================================================#
#Set-up for Second Trimester
#==============================================================================#

zips <- data.table(table(Chilly$Zip))

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

Datar2 <- NULL
data <- NULL
data <- data.frame(data)
Datar2 <- data.frame(Datar2)

tic()
n= 1
for(i in Second$Number){
  if(n == i){
    mydata2 <- Second %>%
      filter(Number == i)
    n_Cold <- No_Coldwave %>%
      filter(Number == i)
    Cold <- Coldwave %>%
      filter(Number == i)
    s_Cold <- Severe_Coldwave %>%
      filter(Number == i)
    e_Cold <- Extreme_Coldwave %>%
      filter(Number == i)
    Below_5 <- Below_5th %>%
      filter(Number == i)
    Below_3 <- Below_3rd %>%
      filter(Number == i)
    Below_1 <- Below_1st  %>%
      filter(Number == i)
    low_i <- low_intensity %>%
      filter(Number == i)
    moderate_i <- moderate_intensity %>%
      filter(Number == i)
    high_i <- high_intensity %>%
      filter(Number == i)
    
    mydata2$No_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(n_Cold$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(Cold$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Severe_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(s_Cold$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Extreme_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(e_Cold$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_5th <- apply(mydata2, 1, function(x) sum(as.Date(Below_5$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_3rd <- apply(mydata2, 1, function(x) sum(as.Date(Below_3$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_1st <- apply(mydata2, 1, function(x) sum(as.Date(Below_1$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$low_intensity <- apply(mydata2, 1, function(x) sum(as.Date(low_i$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$moderate_intensity <- apply(mydata2, 1, function(x) sum(as.Date(moderate_i$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$high_intensity <- apply(mydata2, 1, function(x) sum(as.Date(high_i$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    
    data <- data.frame(mydata2)
    Datar2 <- rbind(Datar2, data)
    
    print(n)
    n = n + 1
  }
}
toc()

fwrite(Datar2, "Trimester2_Cold.csv")

#==============================================================================#
#Set-up for Second Trimester Births
#==============================================================================#

zips <- data.table(table(Chilly$Zip))

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

Datar3 <- NULL
data <- NULL
data <- data.frame(data)
Datar3 <- data.frame(Datar3)

tic()
n= 1
for(i in Second_Birth$Number){
  if(n == i){
    mydata2 <- Second_Birth %>%
      filter(Number == i)
    n_Cold <- No_Coldwave %>%
      filter(Number == i)
    Cold <- Coldwave %>%
      filter(Number == i)
    s_Cold <- Severe_Coldwave %>%
      filter(Number == i)
    e_Cold <- Extreme_Coldwave %>%
      filter(Number == i)
    Below_5 <- Below_5th %>%
      filter(Number == i)
    Below_3 <- Below_3rd %>%
      filter(Number == i)
    Below_1 <- Below_1st  %>%
      filter(Number == i)
    low_i <- low_intensity %>%
      filter(Number == i)
    moderate_i <- moderate_intensity %>%
      filter(Number == i)
    high_i <- high_intensity %>%
      filter(Number == i)
    
    mydata2$No_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(n_Cold$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(Cold$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Severe_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(s_Cold$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Extreme_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(e_Cold$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_5th <- apply(mydata2, 1, function(x) sum(as.Date(Below_5$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_3rd <- apply(mydata2, 1, function(x) sum(as.Date(Below_3$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_1st <- apply(mydata2, 1, function(x) sum(as.Date(Below_1$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$low_intensity <- apply(mydata2, 1, function(x) sum(as.Date(low_i$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$moderate_intensity <- apply(mydata2, 1, function(x) sum(as.Date(moderate_i$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    mydata2$high_intensity <- apply(mydata2, 1, function(x) sum(as.Date(high_i$Date) %in% seq(as.Date(x["Tr2"]), as.Date(x["End"]), by = "1 day")))
    
    data <- data.frame(mydata2)
    Datar3 <- rbind(Datar3, data)
    
    print(n)
    n = n + 1
  }
}
toc()

fwrite(Datar3, "Trimester2_Preme_Cold.csv")

#==============================================================================#
#Set-up for Third Trimester
#==============================================================================#

zips <- data.table(table(Chilly$Zip))

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

Datar4 <- NULL
data <- NULL
data <- data.frame(data)
Datar4 <- data.frame(Datar4)

tic()
n= 1
for(i in Third$Number){
  if(n == i){
    mydata2 <- Third %>%
      filter(Number == i)
    n_Cold <- No_Coldwave %>%
      filter(Number == i)
    Cold <- Coldwave %>%
      filter(Number == i)
    s_Cold <- Severe_Coldwave %>%
      filter(Number == i)
    e_Cold <- Extreme_Coldwave %>%
      filter(Number == i)
    Below_5 <- Below_5th %>%
      filter(Number == i)
    Below_3 <- Below_3rd %>%
      filter(Number == i)
    Below_1 <- Below_1st  %>%
      filter(Number == i)
    low_i <- low_intensity %>%
      filter(Number == i)
    moderate_i <- moderate_intensity %>%
      filter(Number == i)
    high_i <- high_intensity %>%
      filter(Number == i)
    
    mydata2$No_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(n_Cold$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(Cold$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Severe_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(s_Cold$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Extreme_Coldwave <- apply(mydata2, 1, function(x) sum(as.Date(e_Cold$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_5th <- apply(mydata2, 1, function(x) sum(as.Date(Below_5$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_3rd <- apply(mydata2, 1, function(x) sum(as.Date(Below_3$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$Below_1st <- apply(mydata2, 1, function(x) sum(as.Date(Below_1$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$low_intensity <- apply(mydata2, 1, function(x) sum(as.Date(low_i$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$moderate_intensity <- apply(mydata2, 1, function(x) sum(as.Date(moderate_i$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    mydata2$high_intensity <- apply(mydata2, 1, function(x) sum(as.Date(high_i$Date) %in% seq(as.Date(x["Tr3"]), as.Date(x["End"]), by = "1 day")))
    
    data <- data.frame(mydata2)
    Datar4 <- rbind(Datar4, data)
    
    print(n)
    n = n + 1
  }
}
toc()

fwrite(Datar4, "Trimester3_Cold.csv")
