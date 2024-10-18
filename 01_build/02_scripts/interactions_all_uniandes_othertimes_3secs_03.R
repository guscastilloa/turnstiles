# Import Libraries
library(foreign)
library(dplyr)
library(doBy)
library(plyr)
library(lubridate)

# Define directory locations ... UPDATE WITH PROPER LOCATIONS ...
source <- "../data/P2000"
store <- "01_build/03_output/"

# Import functions
source("01_build/02_scripts/all.full.edgelist.revisit.R")


### --------------------------- ###
# Using 3 seconds of distance ####
### --------------------------- ###
# Set time clock
start_time <- Sys.time() # Start counting time

#### MARZO ####
# --------  #
setwd(source)
mydata <- read.csv("marzo.csv")

# Format data
mydata <- mydata[mydata$modo == "Peatonal",]
mydata <- mydata[!is.na(mydata$carnet), ]
mydata <- mydata[, -which(names(mydata) %in%  c("X", "programa", "edificio", "modo", "dia", "NA.", "jornada", "fecha", "mesn"))] # Remove NA row
mydata <- mydata[!duplicated(mydata),] # Drop duplicates

# Format variables variables
mydata$date_time <- strptime(mydata$fecha_completa, format = '%Y.%m.%d %H:%M:%S')
mydata$torniquete <- as.character(mydata$torniquete)
mydata$year <- year(mydata$date_time)
mydata$porteria <- trimws(as.character(mydata$porteria))
mydata$accion <- as.character(mydata$accion)
mydata$action_in <- ifelse(mydata$accion == "IN ", 1, ifelse(mydata$accion == "OUT", 0, NA))    


# gen auxiliary tools
building <- c("W", "SD", "S", "RGB", "PB", "ÑF", "ÑE", "NAVAS", "ML", "MJ", "LL", "GA", "FRANCO", "CPM", "CORCAS", "CAI", "AU")
# building <- c("SD") # For testing


# Genera edge list
# ---------------- #
# Create auxiliary tools
edge.list.full.all <- matrix(nrow = 1, ncol = 9, rep(NA))
colnames(edge.list.full.all) <- c("carnet1", "carnet2", "date_time_carnet1", "torniquete_carnet1", "torniquete_carnet2", "porteria", "action", "day", "year")

for (y in 2016:2019) { # try w/one year
  for (b in building) { 
    for (a in 0:1) { # try outs only
      for (w in 1:5)  { # try mondays only
        
        my.data.filtered <- mydata[mydata$year == y, ]
        my.data.day <- my.data.filtered[ which(my.data.filtered$porteria==b & my.data.filtered$action_in == a & my.data.filtered$dia_semana == w), ] 
        if(nrow(my.data.day) == 0) {next}
        edge.list <- all.full.edgelist(my.data.day, building = b, action=a, day=w, year=y, time = 3)
        edge.list.full.all <- rbind(edge.list.full.all, edge.list)
      }
    }
  }
  
}

setwd(store)
edge.list.full.marzo.csv <- write.csv2(edge.list.full.all, "edge.list.full.marzo.3.csv")

end_time <- Sys.time() # stop counting time
time <- end_time - start_time
print(time) 
print("End of 3S March.")