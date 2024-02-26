library(dplyr)
library(tidyr)
library(gridExtra)

#install.packages("gridExtra")

#INFLUENZA/PNEUMONIA
rm(list = ls())

files<-list.files(
  "P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/normaltimeperiods/", 
  pattern="_none", all.files=TRUE, full.names=FALSE)


# Make a single dataframe containing all estimates -----------------------------

df <- NULL

for (i in 1:length(files)) {
  
  
#  tmp <- data.table::fread(paste0("P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/",files[i]),
#                           data.table = FALSE, sep = "")
  tmp <- read.table(paste0("P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/normaltimeperiods/",files[i])
                  ,sep = " ", header = FALSE, col.names = c("event", "term", "estimate", "conf.low", 
                  "conf.high", "std.error", "n.event", "subgroup", "median_follow_up"))
 
  tmp<-tmp[-1,] 


  tmp <- tmp[grepl("week",tmp$term),]
  
  
  tmp$adjustment <- gsub("_.*","",files[i])
  
  tmp$source <- files[i]
  

  tmp$infection <- ifelse(grepl("COVID", tmp$source),"Covid-19",
                          ifelse(grepl("FLU", tmp$source),"influenza/pneumonia", NA)) 
  
  
  df <- plyr::rbind.fill(df, tmp)
  
}

df$median_follow_up <-as.numeric(df$median_follow_up)

df <- df %>% mutate(across(c('median_follow_up'), round, 2))

#data.table::fwrite(df,"P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/normaltimeperiods/appended_results/FLU_results.csv")
data.table::fwrite(df, paste0("P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/normaltimeperiods/appended_results/FLU_",Sys.Date(),"_results.csv"))


#COVID-19
rm(list = ls())

files<-list.files(
  "P:/keenes/Projects/CCU002-04/results/COVID_cox_analysis/normaltimeperiods/", 
  pattern="_none", all.files=TRUE, full.names=FALSE)


# Make a single dataframe containing all estimates -----------------------------

df <- NULL

for (i in 1:length(files)) {
  
  
  #  tmp <- data.table::fread(paste0("P:/keenes/Projects/CCU002-04/results/COVID_cox_analysis/",files[i]),
  #                           data.table = FALSE, sep = "")
  tmp <- read.table(paste0("P:/keenes/Projects/CCU002-04/results/COVID_cox_analysis/normaltimeperiods/",files[i])
                    ,sep = " ", header = FALSE, col.names = c("event", "term", "estimate", "conf.low", 
                                                              "conf.high", "std.error", "n.event", "subgroup", "median_follow_up"))
  
  tmp<-tmp[-1,] 
  
  
  tmp <- tmp[grepl("week",tmp$term),]
  
  
  tmp$adjustment <- gsub("_.*","",files[i])
  
  tmp$source <- files[i]
  
  
  tmp$infection <- ifelse(grepl("COVID", tmp$source),"Covid-19",
                          ifelse(grepl("FLU", tmp$source),"influenza/pneumonia", NA)) 
  
  
  df <- plyr::rbind.fill(df, tmp)
  
}

df$median_follow_up <-as.numeric(df$median_follow_up)

df <- df %>% mutate(across(c('median_follow_up'), round, 2))

#data.table::fwrite(df,"P:/keenes/Projects/CCU002-04/results/COVID_cox_analysis/normaltimeperiods/appended_results/COVID_results.csv")
data.table::fwrite(df, paste0("P:/keenes/Projects/CCU002-04/results/COVID_cox_analysis/normaltimeperiods/appended_results/COVID_",Sys.Date(),"_results.csv"))

