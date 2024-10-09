# Turnstiles project
# Tatiana Velasco R
# December 13th, 2019


# -------------------------------------------------------------- #
#    CODING INTERACTIONS BETWEEN ALL INDIVIDUALS AT UNIANDES     #
# -------------------------------------------------------------- #

## Genera las listas de pares para cada base de mes a mes. No en loop sino separado
## La ventaja de hacerlo separado es que si el codigo se encuentra con un error en un mes dado, puede seguir al siguiente. 


rm(list = ls())

# install functions
# install.packages("foreign")
# install.packages("dplyr")
# install.packages("doBy")
# install.packages("plyr")
# install.packages("lubridate")

# Import Libraries
library(foreign)
library(dplyr)
library(doBy)
library(plyr)
library(lubridate)

# Define directory locations ... UPDATE WITH PROPER LOCATIONS ...
functions <- "/Users/tatianavelasco.ro/Dropbox/TC\ Columbia/Research/Turnstiles/Torniquetes_TRT/Data/GitHub/Turnstile_networks/Code" 
source <- "/Users/tatianavelasco.ro/Dropbox/TC\ Columbia/Research/Turnstiles/Torniquetes_TRT/Data/P2000"
store <- "/Users/tatianavelasco.ro/Dropbox/TC\ Columbia/Research/Turnstiles/Torniquetes_TRT/Data/data_processing"

# Import functions
# set working directory
setwd(functions)
source("all.full.edgelist.revisit.R")

# Set time clock
start_time <- Sys.time() # Start counting time

 #### ENERO ####
 # -------- #

# setwd(source)
# mydata <- read.csv("enero.csv")
# 
# # Format data
# mydata <- mydata[mydata$modo == "Peatonal",]
# mydata <- mydata[!is.na(mydata$carnet), ]
# mydata <- mydata[, -which(names(mydata) %in%  c("X", "programa", "edificio", "modo", "dia", "NA.", "jornada", "fecha", "mesn"))] # Remove NA row
# mydata <- mydata[!duplicated(mydata),] # Drop duplicates
# 
# # Format variables variables
# mydata$date_time <- strptime(mydata$fecha_completa, format = '%Y.%m.%d %H:%M:%S')
# mydata$torniquete <- as.character(mydata$torniquete)
# mydata$year <- year(mydata$date_time)
# mydata$porteria <- trimws(as.character(mydata$porteria))
# mydata$accion <- as.character(mydata$accion)
# mydata$action_in <- ifelse(mydata$accion == "IN ", 1, ifelse(mydata$accion == "OUT", 0, NA))    
# 
# 
# # gen auxiliary tools
# building <- c("W", "SD", "S", "RGB", "PB", "ÑF", "ÑE", "NAVAS", "ML", "MJ", "LL", "GA", "FRANCO", "CPM", "CORCAS", "CAI", "AU")
# # building <- c("SD") # For testing
# 
# 
# # Genera edge list
# # ---------------- #
# # Create auxiliary tools
# edge.list.full.all <- matrix(nrow = 1, ncol = 9, rep(NA))
# colnames(edge.list.full.all) <- c("carnet1", "carnet2", "date_time_carnet1", "torniquete_carnet1", "torniquete_carnet2", "porteria", "action", "day", "year")
# 
# for (y in 2016:2019) { # try w/one year
#   for (b in building) { 
#     for (a in 0:1) { # try outs only
#       for (w in 1:5)  { # try mondays only
#         
#       my.data.filtered <- mydata[mydata$year == y, ]
#       my.data.day <- my.data.filtered[ which(my.data.filtered$porteria==b & my.data.filtered$action_in == a & my.data.filtered$dia_semana == w), ] 
#       if(nrow(my.data.day) == 0) {next}
#       edge.list <- all.full.edgelist(my.data.day, building = b, action=a, day=w, year=y, time = 2)
#       edge.list.full.all <- rbind(edge.list.full.all, edge.list)
#       }
#     }
#   }
#   
# }
# 
# setwd(store)
# edge.list.full.enero.csv <- write.csv2(edge.list.full.all, "edge.list.full.enero.csv")

#### FEBRERO #### 
# -------- #
setwd(source)
mydata <- read.csv("febrero.csv")

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
        edge.list <- all.full.edgelist(my.data.day, building = b, action=a, day=w, year=y, time = 2) # Decirle a la funcion qué distancia es permitida
        edge.list.full.all <- rbind(edge.list.full.all, edge.list)
      }
    }
  }
  
}

setwd(store)
edge.list.full.febrero.csv <- write.csv2(edge.list.full.all, "edge.list.full.febrero.csv")

end_time <- Sys.time() # stop counting time
time <- end_time - start_time
time 
#### MARZO ####
# -------- #

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
        edge.list <- all.full.edgelist(my.data.day, building = b, action=a, day=w, year=y, time = 2)
        edge.list.full.all <- rbind(edge.list.full.all, edge.list)
      }
    }
  }
  
}

setwd(store)
edge.list.full.marzo.csv <- write.csv2(edge.list.full.all, "edge.list.full.marzo.csv")


#### ABRIL ####
# -------- #

setwd(source)
mydata <- read.csv("abril.csv")

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
        edge.list <- all.full.edgelist(my.data.day, building = b, action=a, day=w, year=y, time = 2)
        edge.list.full.all <- rbind(edge.list.full.all, edge.list)
      }
    }
  }
  
}

setwd(store)
edge.list.full.abril.csv <- write.csv2(edge.list.full.all, "edge.list.full.abril.csv")

#### MAYO ####
# -------- #

setwd(source)
mydata <- read.csv("mayo.csv")

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
        edge.list <- all.full.edgelist(my.data.day, building = b, action=a, day=w, year=y, time = 2)
        edge.list.full.all <- rbind(edge.list.full.all, edge.list)
      }
    }
  }
  
}

setwd(store)
edge.list.full.mayo.csv <- write.csv2(edge.list.full.all, "edge.list.full.mayo.csv")

#### AGOSTO ####
# --------- #

setwd(source)
mydata <- read.csv("agosto.csv")

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
        edge.list <- all.full.edgelist(my.data.day, building = b, action=a, day=w, year=y, time = 2)
        edge.list.full.all <- rbind(edge.list.full.all, edge.list)
      }
    }
  }
  
}

setwd(store)
edge.list.full.agosto.csv <- write.csv2(edge.list.full.all, "edge.list.full.agosto.csv")

#### SEPTIEMBRE #### 
# --------- #

setwd(source)
mydata <- read.csv("septiembre.csv")

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
        edge.list <- all.full.edgelist(my.data.day, building = b, action=a, day=w, year=y, time = 2)
        edge.list.full.all <- rbind(edge.list.full.all, edge.list)
      }
    }
  }
  
}

setwd(store)
edge.list.full.septiembre.csv <- write.csv2(edge.list.full.all, "edge.list.full.septiembre.csv")

#### OCTUBRE #### 
# ----------- #

setwd(source)
mydata <- read.csv("octubre.csv")

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
        edge.list <- all.full.edgelist(my.data.day, building = b, action=a, day=w, year=y, time = 2)
        edge.list.full.all <- rbind(edge.list.full.all, edge.list)
      }
    }
  }
  
}

setwd(store)
edge.list.full.octubre.csv <- write.csv2(edge.list.full.all, "edge.list.full.octubre.csv")

#### NOVIEMBRE ####
# -----------#

setwd(source)
mydata <- read.csv("noviembre.csv")

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
        edge.list <- all.full.edgelist(my.data.day, building = b, action=a, day=w, year=y, time = 2)
        edge.list.full.all <- rbind(edge.list.full.all, edge.list)
      }
    }
  }
  
}

setwd(store)
edge.list.full.noviembre.csv <- write.csv2(edge.list.full.all, "edge.list.full.noviembre.csv")


end_time <- Sys.time() # stop counting time
time <- end_time - start_time
time 

