
# Interactions All students
# -------------------------

all.full.edgelist <- function(my.data.filtered, building, action, day, year, time) {

# create vector 
all1 <- my.data.day[,c("carnet", "date_time", "torniquete")]
all1 <-all1[order(all1$date_time),]
# repeat vector 
all2 <- all1

# Create list of interactions w/time_date
auxPobresStartIndex <- 1 # Auxiliary variable to keep comparaisons at minimum # esto es lo que mas velocidad gana
minSeconds <- time #The number of seconds to consider an interaction
edg <- matrix(nrow = 1, ncol = 5, rep(NA))

# Recorre los vectores y calcula distancias
for (i in 1:nrow(all1)) { #Ricos are in the rows#
  updatedIndex <- FALSE
  for (j in auxPobresStartIndex:nrow(all2)) { 
  
  if(all1$carnet[i]!=all2$carnet[j]) { # Look at different students only #
   timediff<- as.numeric(difftime(all1$date_time[i], all2$date_time[j], units = "secs"))
  
     if(abs(timediff) < minSeconds){  
       row <- c(all1$carnet[i], all2$carnet[j], as.character(all1$date_time[i]), as.character(all1$torniquete[i]), as.character(all2$torniquete[j]) )
       edg <- rbind(edg, row)
       
       if(!updatedIndex){
         auxPobresStartIndex <- j
         updatedIndex <- TRUE
       }
     }
      if(timediff < -minSeconds){
        break;
      }
      if(!updatedIndex)
       auxPobresStartIndex <-j
  }
  }  
}


colnames(edg) <- c("carnet1", "carnet2", "date_time_carnet1", "torniquete_carnet1", "torniquete_carnet2")
edg <- cbind(edg, building, action, day, year)
edg <- edg[-1,]
return(edg)

}
