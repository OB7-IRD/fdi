# Author: P. CAUQUIL
# Creation date: 2016-05-26
#
# Description: This script federate a bunch of common miscellaneous functions
#
# Updates : 2017-09-29 - Added functions to collapse coordonates on 0.25° and 0.50° grids


#################################################################################################################
### Functions
#################################################################################################################

### Ask YES/NO to the user
askYesOrNo <- function() { 
  text <- readline(prompt="Do you want to continue (Y/N): ")
  
  if(!grepl("^[A-Z]$",text)) {
    return(askYesOrNo())
  }
  
  return(text)
}

#################################################################################################################

### Colapse coordonates on 0.25° grid
coordToGrid25 <- function(x) {
  int <- trunc(x)
  decabs <- abs(x - int)
  
  newdec <- ifelse(decabs>=0 & decabs<0.25,
                   0.00,
                   ifelse(decabs>=0.25 & decabs<0.50,
                          0.25,
                          ifelse(decabs>=0.50 & decabs<0.75,
                                 0.50,
                                 0.75
                          )
                   )
  )
  
  result <- ifelse(x<0, int-newdec, int+newdec)
  
  return(result)
}

#################################################################################################################

### Colapse coordonates on 0.50° grid
coordToGrid50 <- function(x) {
  int <- trunc(x)
  decabs <- abs(x - int)
  
  newdec <- ifelse(decabs>=0 & decabs<0.50,
                   0.00,
                   0.50
  )
  
  result <- ifelse(x<0, int-newdec, int+newdec)
  
  return(result)
}