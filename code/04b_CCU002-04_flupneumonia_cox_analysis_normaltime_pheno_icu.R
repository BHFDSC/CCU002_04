print("CCU002-04 R code adapted from Hoda and Samantha IP for project comparing CVD outcomes in influenza/pneumonia versus flu-19")
print("Spencer Keene")

rm(list = ls())

df <- data.table::fread("P:/keenes/Projects/CCU002-04/raw/CCU002_04_FLU_COHORT_20230322.csv",
                        data.table = FALSE)


###############################    packages   ################################

#install.packages("ggplot2")
#install.packages("incidence")
#install.packages("tidyverse")
#install.packages("epiR")
#install.packages("scales")
#install.packages("dplyr")
#install.packages("survival")
#install.packages("survminer")
#install.packages("gtsummary")
#install.packages("reshape2")
#install.packages("date")
#install.packages("lubridate")
#install.packages("stringr")
#install.packages("data.table")


library(ggplot2)
library(incidence)
library(tidyverse)
library(epiR)
library(scales)
library(dplyr)
library(survival)
library(survminer)
library(gtsummary)
library(reshape2)
library(date)
library(lubridate)
library(stringr)
library(data.table)


rm(list=setdiff(ls(), "df"))
sapply(df, function(x) sum(is.na(x)))


#REPLACE OLD COLUMN WITH NEW AND CREATE OBJECT FOR NAMING BASED ON NUMBER SCHEME BELOW:
#0: EXP1_PNEUMONIA_DATE, EXP1_PNEUMONIA_PHENOTYPE,
#1: EXP1_PNEUMONIA_DATE_v2, EXP1_PNEUMONIA_PHENOTYPE_v2, (Is very similar to main results)
#2: EXP2_FLU_DATE, EXP2_FLU_PHENOTYPE, 
#3: EXP3_BOTH_DATE, EXP3_BOTH_PHENOTYPE,  (Is very similar to main results)
#4: EXP4_BOTH_AND_OTHER_INFECTIONS_DATE, EXP4_BOTH_AND_OTHER_INFECTIONS_PHENOTYPE, (Is very similar to main results)

# df$EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE <- df$EXP1_PNEUMONIA_PHENOTYPE
# df$EXP_CONFIRMED_FLU_PNEUMONIA_DATE <- df$EXP1_PNEUMONIA_DATE

exp_def<-"_"


#Flu/Pneumonia dates are 2016-01-01 to 2019-12-31

#comment out two of these depending on the analysis
#need to add data$ to cohort_start_date and cohort_end_date in script
#vaccine<-"vaccine_start"
#vaccine<-"vaccine_end"

vaccine<-"main"

cohort_start_date_all <- as.Date("2016-01-01")
cohort_end_date_all <- as.Date("2019-12-31")


df$cohort_start_date <- as.Date("2016-01-01")
df$cohort_end_date <- as.Date("2019-12-31")

censor<-"censor" #censor OR nocensor


#clist <- c("icu") #"non-hospitalised"
# for(i in clist) {
#   pheno_of_interest <- i
#   print(pheno_of_interest)
  
  #pheno_of_interest<-c("icu", "hospitalised")
  pheno_of_interest<-"hospitalised"
  
  
  
  
  ################ introduce new summary 'history of' covariates that
  ################  combine  definition from individual covariates    ###############
  
  #df$hx_arterial_event<-pmax(df$COV_EVER_AMI, df$COV_EVER_STROKE_ISCH, 
  #                           df$COV_EVER_OTHER_ARTERIAL_EMBOLISM)
  #df$hx_venous_event<-pmax(df$COV_EVER_DVT_ICVT, df$COV_EVER_PE_VT)
  #df$hx_haematological_event<-pmax(df$COV_EVER_TT, df$COV_EVER_TCP)
  #df$hx_other_event<-pmax(df$COV_EVER_MESENTERIC_THROMBUS, df$COV_EVER_ARTERY_DISSECT, df$COV_EVER_LIFE_ARRHYTHMIA, 
  #                        df$COV_EVER_CARDIOMYOPATHY, df$COV_EVER_ANGINA, df$COV_EVER_HF, df$COV_EVER_STROKE_SPINAL, 
  #                        df$COV_EVER_STROKE_SAH_HS)
  df$hx_hypertension<-pmax(df$COV_EVER_HYPERTENSION, df$COV_BP_LOWERING_MEDS)
  df$COV_N_DISORDER[is.na(df$COV_N_DISORDER)] <-0
  df$COV_flu_pneumonia_hosp_or_infection<-pmax(df$COV_FLU_PNEUMONIA_HOSPITALISATION, df$COV_FLU_PNEUMONIA_INFECTION)
  
  df$hx_antiplat_anticoag<-pmax(df$COV_ANTICOAGULATION_MEDS, df$COV_ANTIPLATELET_MEDS)
  
  #df1<-subset(df, select = -c(COV_EVER_AMI, COV_REGION, COV_EVER_STROKE_ISCH, COV_EVER_OTHER_ARTERIAL_EMBOLISM, COV_EVER_DVT_ICVT, 
  #                            COV_EVER_PE_VT, COV_EVER_TCP, COV_EVER_HYPERTENSION, COV_BP_LOWERING_MEDS, COV_ANTICOAGULATION_MEDS,
  #                            COV_ANTIPLATELET_MEDS))
  

  
  #################### make separate outcomes and covariates dataframe with ids ########################
  df1<-df
  ids <- df1 %>% select(1)
  outcomes<-df1[, grep(pattern="^OUT", colnames(df1))]
  outcomes_names <- names(outcomes) # grep("^OUT", names(outcomes), value = TRUE)
  outcomes <- cbind(ids, outcomes)
  
  covariates <- df1[, grep(pattern="COV_|COM_|COINF|hx_|CARE_|RURAL_U", colnames(df1))]
  covariates <- cbind(ids, covariates)
  agesex <-     covariates[, grep(pattern="ALF_E|COV_AGE|COV_SEX", colnames(covariates))]
  covariates <- subset(covariates, select = -c(COV_AGE, COV_SEX))
  #df1 <- df1 %>% select(-starts_with("COV_") & -starts_with("COM_") & -starts_with("COINF") & -starts_with("hx_") & -starts_with("CARE_")) 
  df1 <- df1 %>% select(-starts_with(c("COV_","COM_","COINF","hx_","CARE_","RURAL_U"))) 
  
  rm("ids")
  outcomes[is.na(outcomes)]=0
  #head(outcomes)
  outcomes<-outcomes %>%
    mutate(across(starts_with('OUT_'), ~ifelse( .x>0,2,1), .names = 'BIN_OUT_{sub("OUT_", "", .col)}'))
  outcomes[outcomes==0]<-NA
  outcomes_date <- outcomes %>% select(-starts_with("BIN_OUT")) 
  outcomes_bin<-outcomes[, grep(pattern="^BIN_OUT", colnames(outcomes))]
  outcomes_bin[outcomes_bin>=1]<-outcomes_bin[outcomes_bin>=1] - 1
  outcomes <- cbind(outcomes_bin, outcomes_date)
  outcomes <- outcomes %>% select(ALF_E, everything())
  #write.csv(outcomes, "P:/keenes/Projects/CCU002-04/raw/CCU002_04_df1_outcomes.csv")
  
  
  #################### combine outcomes dataframe with main ########################
  
  # outcomes2 <- outcomes
  # outcomes2 <- subset(outcomes2, select = -c(ALF_E))
  df1 <- df1[grep("^OUT_", colnames(df1), invert = TRUE)]
  #data <- cbind(df1, outcomes2)
  data <- df1 %>% left_join(outcomes)
  rm("df1", "outcomes")
  
  
  ############### age subgroups, sex labels, ethnicity groups ####################
  #replaced data with covariates for below section
  
  ### Age group
  agebreaks <- c(0, 60, 500)
  agelabels <- c("<60", "60+")
  #agebreaks <- c(0, 40, 60, 80, 500)
  #agelabels <- c("<40", "40-59", "60-79", "+80")
  #agebreaks <- c(0, 500)
  #agelabels <- c("all")
  agesex$COV_AGE <- as.numeric(agesex$COV_AGE)
  agesex <- setDT(agesex)[ , agegroup := cut(COV_AGE, breaks = agebreaks, right = FALSE, labels = agelabels)]
  agesex$agegroup = relevel(agesex$agegroup, ref = "<60")
  
  agesex$age_sq <- agesex$COV_AGE^2
  
  ### Sex labels
  agesex$sex_label <- ifelse(agesex$COV_SEX==2, "Female", "Male")
  agesex$sex_label <- factor(agesex$sex_label, levels = c("Male", "Female"))
  agesex$sex_label = relevel(agesex$sex_label, ref = "Male")
  
  ### CVD history labels
  covariates$cvd_label <- ifelse(covariates$COV_EVER_ANY_CVD==1, "hx_of_CVD", "No_hx_of_CVD")
  covariates$cvd_label <- factor(covariates$cvd_label, levels = c("No_hx_of_CVD", "hx_of_CVD"))
  covariates$cvd_label = relevel(covariates$cvd_label, ref = "No_hx_of_CVD")
  
  ### Antiplatelet meds labels
  covariates$antiplatelet_label <- ifelse(covariates$hx_antiplat_anticoag==1, "Antiplatelet/anticoagulant_Meds", "No_antiplatelet/anticoagulant_medication")
  covariates$antiplatelet_label <- factor(covariates$antiplatelet_label, levels = c("No_antiplatelet/anticoagulant_medication", "Antiplatelet/anticoagulant_Meds"))
  covariates$antiplatelet_label = relevel(covariates$antiplatelet_label, ref = "No_antiplatelet/anticoagulant_medication")
  
  ### Smoking labels
  covariates$COV_SMOKING_STATUS <- factor(covariates$COV_SMOKING_STATUS, levels = c("Missing", "Never-smoked", "Ex-smoker", "Current-smoker"))
  covariates$COV_SMOKING_STATUS = relevel(covariates$COV_SMOKING_STATUS, ref = "Never-smoked")
  
  ###Deprivation
  covariates$COV_DEPRIVATION <- ifelse(is.na(covariates$COV_DEPRIVATION), 'Missing', covariates$COV_DEPRIVATION)
  covariates$COV_DEPRIVATION <- factor(covariates$COV_DEPRIVATION, levels = c("Missing", "1", "2", "3", "4", "5"))
  covariates$COV_DEPRIVATION = relevel(covariates$COV_DEPRIVATION, ref = "1")
  
  ###Rural/Urban
  covariates$RURAL_URBAN<-ifelse(grepl("Rural", covariates$RURAL_URBAN), "rural","urban")
  
  ##Ethnicity groups: Missing, White, and Other.
  #data$ethnicity_group <- ifelse((data$ethnicity!="White" | data$ethnicity!="Missing"), "Other", data$ethnicity)
  #data$ethnicity_group <- factor(data$ethnicity_group, levels = c("White", "Other", "Missing"))
  #data$ethnicity_group = relevel(data$ethnicity_group, ref = "White")
  
  
  
  ###############   set dates to NA if out of range of time period ###############
  ##shouldn't I change cohort_end_date to pt_end_date??
  
  set_dates_outofrange_na <- function(df, colname)
  {
    df <- df %>% mutate(
      !!sym(colname) := as.Date(ifelse((!!sym(colname) > cohort_end_date_all) | (!!sym(colname) < cohort_start_date_all), NA, !!sym(colname) ), origin='1970-01-01')
    )
    return(df)
  }
  
  schema <- sapply(data, is.Date) #used to be survival_data
  for (colname in names(schema)[schema==TRUE]){
    print(colname)
    data <- set_dates_outofrange_na(data, colname)
  }
  
  #names(data)[names(data) == 'EXPO_DATE'] <- 'expo_date'
  
  ################################ define end date ################################
  
  outcomes_names2 <- grep("^OUT_", names(data), value = TRUE)
  #outcomes2 <- outcomes2[outcomes2 != "BIN_OUT_FRACTURE"]
  outcomes_names2 <- outcomes_names2[outcomes_names2 != "OUT_TTP"]
  outcomes_names2 <- outcomes_names2[outcomes_names2 != "OUT_DVT_ICVT" & outcomes_names2 != 
                                       "OUT_OTHER_DVT" & outcomes_names2 != "OUT_MYOCARDITIS" & 
                                       outcomes_names2 != "OUT_PERICARDITIS" & outcomes_names2 != "OUT_PORTAL_VEIN_THROMBOSIS" &
                                       outcomes_names2 != "OUT_ARTERY_DISSECT" & outcomes_names2 != "OUT_THROMBOCYTOPENIA" 
                                     & outcomes_names2 != "OUT_HAEMATOLOGICAL_EVENT" & outcomes_names2 !="OUT_MESENTERIC_THROMBUS"]
  
  
  data[,outcomes_names2[2]]
  
  #print("Error below")
  
  data$ICU_OR_RESP_SUPPORT<-ifelse(data$ICU_OR_RESP_SUPPORT==1, "icu", NA)
  
  data$EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE <- ifelse(!is.na(data$ICU_OR_RESP_SUPPORT), data$ICU_OR_RESP_SUPPORT, data$EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE)
  
  if (censor!="censor") {
    
  pheno_of_interest<-"hospitalised"
  data$DATE_EXPO_CENSOR <- as.Date(ifelse((data$EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE %in% pheno_of_interest),
                                          data$EXP_CONFIRMED_FLU_PNEUMONIA_DATE,
                                          NA), origin='1970-01-01') #! taken out here after ifelse(
  pheno_of_interest<-"icu"
  data$expo_date <- as.Date(ifelse((data$EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE %in% pheno_of_interest),
                                     data$EXP_CONFIRMED_FLU_PNEUMONIA_DATE, NA), origin='1970-01-01') #phenotype: code added
  
  } else {
    
  pheno_of_interest<-"icu"
  data$DATE_EXPO_CENSOR <- as.Date(ifelse(!(data$EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE %in% pheno_of_interest),
                                          data$EXP_CONFIRMED_FLU_PNEUMONIA_DATE,
                                          NA), origin='1970-01-01') 
  
  data$expo_date <- as.Date(ifelse((!is.na(data$DATE_EXPO_CENSOR)) & 
                                     (data$EXP_CONFIRMED_FLU_PNEUMONIA_DATE >= data$DATE_EXPO_CENSOR), 
                                   NA, data$EXP_CONFIRMED_FLU_PNEUMONIA_DATE), origin='1970-01-01') #phenotype: code added

  }
    

  #print("Error above")
  
  memory.limit(size = 30000)
  
  
  #need to also add vaccination date to below for follow-up ending at vaccination.
  
  adjust<-"Extensive"
  time<-"normaltimeperiods"
  
  for(i in 1:length(outcomes_names2)){
    data$overall_event_date <- data[,outcomes_names2[i]]
    # data$overall_event_date <- as.Date(ifelse((!is.na(data$DATE_EXPO_CENSOR)) & 
    #                                             (data$overall_event_date >= data$DATE_EXPO_CENSOR), 
    #                                           NA, data$overall_event_date), origin='1970-01-01') #phenotype: code added
    
    
    # data$exclude <- ifelse((is.na(data$overall_event_date) | is.na(data$expo_date)),0,
    #                          ifelse((data$expo_date==data$overall_event_date),1,0))
    # data<-filter(data,exclude==0)
    # excl<-"exclsameday"  
    
    #  data[data$overall_event_date > data$EXP_CONFIRMED_FLU_PNEUMONIA_DATE, ]
    data<-transform(data, pt_end_date=pmin(DEATH_DATE,cohort_end_date,LOST_TO_FOLLOWUP_DATE, DATE_EXPO_CENSOR,
                                           GP_COVERAGE_END_DATE,overall_event_date, 
                                           na.rm=TRUE)) #phenotype: DATE_EXPO_CENSOR added
    data$overall_event<-data[,paste0("BIN_",outcomes_names2[i])]
#    data$overall_event <- ifelse((!is.na(data$DATE_EXPO_CENSOR)) & (data$overall_event_date >= data$DATE_EXPO_CENSOR), 0, data$overall_event) #phenotype: code added
    data$overall_event<-ifelse(((data$overall_event_date>data$pt_end_date) & (!is.na(data$overall_event_date))),0,data$overall_event)
#    data$overall_event = ifelse((data$overall_event==1) &
#                                  ( ((data$overall_event_date <= data$pt_end_date) & 
#                                       ((data$overall_event_date != data$DATE_EXPO_CENSOR) | is.na(data$DATE_EXPO_CENSOR))) |
#                                      ((data$overall_event_date <  data$pt_end_date) & (data$overall_event_date == data$DATE_EXPO_CENSOR)) ), 1, 0) #phenotype: code added
    data$overall_event_date <- as.Date(ifelse((!is.na(data$overall_event_date)) & 
                                                (data$overall_event_date > data$pt_end_date), 
                                              NA, data$overall_event_date), origin='1970-01-01') #phenotype: code added
    
    outcome_list<-paste0("BIN_",outcomes_names2[i])
    outcome_list2<-paste0(substring(outcome_list,9,40)) #added
    
    
    #################### make main exposure and overall outcome ########################
    
    #  data$flu_inf<-ifelse(data$EXP_CONFIRMED_FLU_PNEUMONIA_DATE>=cohort_start_date & data$EXP_CONFIRMED_FLU_PNEUMONIA_DATE<=data$pt_end_date,1,0)
    data$flu_inf<-ifelse((!is.na(data$expo_date) & (data$expo_date>=data$cohort_start_date & data$expo_date<=data$pt_end_date)),1,0) #phenotype: code added
    data$expo_date<-as.Date(ifelse((!is.na(data$expo_date) & (data$expo_date>=data$cohort_start_date 
                                                              & data$expo_date<=data$pt_end_date)),data$expo_date,NA), origin='1970-01-01') #phenotype: code added
    
    #  data$flu_inf[is.na(data$EXP_CONFIRMED_FLU_PNEUMONIA_DATE)]=0
    
    #  data$EXP_CONFIRMED_COVID_PHENOTYPE<-ifelse(data$flu_inf==0,data$EXP_CONFIRMED_COVID_PHENOTYPE=="",data$EXP_CONFIRMED_COVID_PHENOTYPE)
    data$fup<-as.numeric(difftime(data$pt_end_date, data$cohort_start_date, unit="days"))
    #  data$fup<-data$fup+1
    data$fup <- ifelse((!is.na(data$DATE_EXPO_CENSOR)) & (data$pt_end_date == data$DATE_EXPO_CENSOR), data$fup, (data$fup +1 )) #phenotype: code added
    data<-subset(data, fup>=0) 
    
    
    ################## 5% sample of uninfected non-cases #############################
    set.seed(137)
    noncase_frac <- 0.05
    data_sample_uninfected_noncases <- 
      data[sample(which(data$overall_event==0 & data$flu_inf==0), round(noncase_frac*length(which(data$overall_event==0 & data$flu_inf==0)))), ]
    data_infected_noncases <- data[which(data$overall_event==0 & data$flu_inf==1),]
    data_cases <- data[which(data$overall_event==1),]
    data_sample<-rbind(data_sample_uninfected_noncases, data_infected_noncases, data_cases)
    rm("data_sample_uninfected_noncases", "data_infected_noncases", "data_cases")
    sum(data$overall_event)
    sum(data_sample$overall_event)
    
    
    
    ############       split rows in those who were infected and 
    ###############    those who were not infected    ##############################
    infected_indiv <- data_sample[which(data_sample$flu_inf==1),]
    infected_indiv$delta_flu_inf<-as.numeric(difftime(infected_indiv$expo_date, infected_indiv$cohort_start_date, unit="days"))
    
    if(length(infected_indiv$fup[infected_indiv$fup==0])>0){
      infected_indiv$fup <- ifelse(infected_indiv$fup==0, infected_indiv$fup + 0.001, infected_indiv$fup)
    }
    excl<-"none"  
    
    # if(length(infected_indiv$fup[infected_indiv$fup==0])>0){
    #   infected_indiv$fup <- ifelse(infected_indiv$fup==0, infected_indiv$fup + 0.001, infected_indiv$fup)
    #   infected_indiv$fup <- ifelse(infected_indiv$expo_date==infected_indiv$overall_event_date
    #                                & (!is.na(infected_indiv$overall_event_date)), infected_indiv$fup + 5, infected_indiv$fup)
    # }
    # excl<-"add5days"  
    
    
    
    ################ variable list for long format transformation    ###############
    
    vars<-c("ALF_E", "fup", "EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE", 
            "expo_date", "overall_event_date", "pt_end_date"
    )
    
    ##################  data transform to long format for becoming infected #######################
    
    td_data <-
      tmerge(
        data1 = infected_indiv %>% select(all_of(vars), overall_event), #replaced overall_event_date with overall_event_time
        data2 = infected_indiv %>% select(all_of(vars), overall_event, delta_flu_inf, flu_inf), #'flu_inf' not needed, 'value' replaced with overall_event, 'fup' added
        id = ALF_E, 
        overall_event = event(fup,  overall_event), 
        flu_inf = tdc(delta_flu_inf)
      )
    
    
    
    ##################  split long format dataframe into pre and post infection #####################
    
    # cuts_weeks_since_expo <- c(1, 4, 16, 75, as.numeric(ceiling(difftime(cohort_end_date_all,cohort_start_date_all)/7))) 
    #was 1,4,12,36,72
    cuts_weeks_since_expo <- c(0.142857143,1, 4, 16, 75, as.numeric(ceiling(difftime(cohort_end_date_all,cohort_start_date_all)/7))) 
    
    
    with_expo_postexpo <- td_data %>% filter(flu_inf==1)
    with_expo_preexpo <- td_data %>% filter(flu_inf==0)
    with_expo_postexpo <- with_expo_postexpo %>% rename(t0=tstart, t=tstop) %>% mutate(tstart=0, tstop=t-t0)
    
    
    ###  data transform to long format for time from infection for those infected ####
    
    #------------------------for normal time periods-------------------------------
    with_expo_postexpo<-survSplit(Surv(tstop, overall_event) ~., with_expo_postexpo,
                                  cut=cuts_weeks_since_expo*7,
                                  #                     event = "overall_event",
                                  #                     start = "tstart",
                                  #                     end = "tstop",
                                  episode ="Weeks_category")
    with_expo_postexpo <- with_expo_postexpo %>% mutate(tstart=tstart+t0, tstop=tstop+t0) %>% dplyr::select(-c(t0,t))
    with_expo_preexpo$Weeks_category <- 0
    ls_with_expo <- list(with_expo_preexpo, with_expo_postexpo)
    with_expo <- do.call(rbind, lapply(ls_with_expo, function(x) x[match(names(ls_with_expo[[1]]), names(x))]))
    
    #################################################################################
    rm(list=c("ls_with_expo", "with_expo_preexpo", "with_expo_postexpo_reduced", "with_expo_postexpo"))
    
    with_expo  <- with_expo %>%
      group_by(ALF_E) %>% arrange(Weeks_category) %>% mutate(last_step = ifelse(row_number()==n(),1,0))
    #with_expo$tstop <- ifelse(with_expo$tstop ==0,  with_expo$tstop + 0.001, with_expo$tstop) # changed from fup to tstop after <-
    #with_expo$overall_event  <- with_expo$overall_event * with_expo$last_step
    
    ###hospitalised/non-hospitalised #this used to be below age group and was applied to the data_sample dataframe.
    # with_expo$EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE[with_expo$EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE =="" | with_expo$EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE ==" "] <- NA
    # with_expo <- mutate_at(with_expo, c("EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE"), ~replace(., is.na(.), 0))
    # 
    # with_expo$hospitalised <- ifelse(with_expo$EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE=="hospitalised",1, 0)
    # with_expo$hospitalised<-as.numeric(with_expo$hospitalised)
    # with_expo$nonhospitalised <- ifelse(with_expo$EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE=="hospitalised",0, 1)
    # with_expo$nonhospitalised<-as.numeric(with_expo$nonhospitalised)
    # 
    # with_expo$hosp_label <- ifelse(with_expo$hospitalised==1, "Hospitalised_Flu/Pneumonia", "Non-hospitalised_Flu/Pneumonia")
    # with_expo$hosp_label <- factor(with_expo$hosp_label, levels = c("Non-hospitalised_Flu/Pneumonia", "Hospitalised_Flu/Pneumonia"))
    # with_expo$hosp_label = relevel(with_expo$hosp_label, ref = "Non-hospitalised_Flu/Pneumonia")
    # with_expo$nonhosp_label <- ifelse(with_expo$nonhospitalised==1, "Non-hospitalised_Flu/Pneumonia", "Hospitalised_Flu/Pneumonia")
    # with_expo$nonhosp_label <- factor(with_expo$nonhosp_label, levels = c("Hospitalised_Flu/Pneumonia", "Non-hospitalised_Flu/Pneumonia"))
    # with_expo$nonhosp_label = relevel(with_expo$nonhosp_label, ref = "Hospitalised_Flu/Pneumonia")
    
    
    ###########  prepare dataframe for those  without infection    ##################
    without_expo <- data_sample[which(data_sample$flu_inf==0),]
    without_expo$tstart<- c(0)
    without_expo$tstop <- ifelse(without_expo$fup ==0,  without_expo$fup + 0.001, without_expo$fup) # changed from fup to tstop after <-
    without_expo$flu_inf<- c(0)
    without_expo$Weeks_category <- c(0)
    without_expo$last_step <- c(1)
    # without_expo$hospitalised <- c(1)
    # without_expo$nonhospitalised <- c(1)
    # without_expo$hosp_label <- ifelse(without_expo$hospitalised==1, "Hospitalised_Flu/Pneumonia", "Non-Hospitalised_Flu/Pneumonia")
    # without_expo$hosp_label <- factor(without_expo$hosp_label, levels = c("Non-Hospitalised_Flu/Pneumonia", "Hospitalised_Flu/Pneumonia"))
    # without_expo$hosp_label = relevel(without_expo$hosp_label, ref = "Non-Hospitalised_Flu/Pneumonia")
    # without_expo$nonhosp_label <- ifelse(without_expo$nonhospitalised==1, "Non-Hospitalised_Flu/Pneumonia", "Hospitalised_Flu/Pneumonia")
    # without_expo$nonhosp_label <- factor(without_expo$nonhosp_label, levels = c("Hospitalised_Flu/Pneumonia", "Non-Hospitalised_Flu/Pneumonia"))
    # without_expo$nonhosp_label = relevel(without_expo$nonhosp_label, ref = "Hospitalised_Flu/Pneumonia")
    without_expo_noncases <- without_expo[which(without_expo$overall_event==0),]
    noncase_ids <- unique(without_expo_noncases$ALF_E)
    
    
    ###########  combine uninfected and infected exposure dataframes    ##################
    with_expo_cols <- colnames(with_expo)
    without_expo <- without_expo %>% dplyr::select(all_of(with_expo_cols))
    data_surv <-rbind(with_expo, without_expo)
    rm(list=c("with_expo", "without_expo"))
    
    data_surv$cox_weights <- ifelse(data_surv$ALF_E %in% noncase_ids, 1/noncase_frac, 1)
    
    
    ################   Pivot Wide for time intervals (exposure)     ######################
    
    #------------------------for normal time periods-------------------------------
    
    interval_names <- mapply(function(x, y) ifelse(x == y, paste0("week", x), paste0("week", x, "_", y)),
                             lag(cuts_weeks_since_expo, default = 0)+1,
                             cuts_weeks_since_expo,
                             SIMPLIFY = FALSE)
    cat("...... interval_names...... \n")
    print(interval_names)
    intervals <- mapply(c, lag(cuts_weeks_since_expo, default = 0)+1, cuts_weeks_since_expo, SIMPLIFY = F)
    
    
    i<-0
    for (ls in mapply(list, interval_names, intervals, SIMPLIFY = F)){
      i <- i+1
      print(paste(c(ls, i), collapse="..."))
      data_surv[[ls[[1]]]] <- if_else(data_surv$Weeks_category==i, 1, 0)
    }
    
    data_surv %>% filter(tstart>0) %>% arrange(ALF_E)%>% head(10) %>% print(n = Inf) 
    data_surv %>% filter(expo_date==overall_event_date) %>% arrange(ALF_E)%>% head(10) %>% print(n = Inf) #replaced overall_event_date with overall_event_time
    
    
    data_surv <- data_surv %>% left_join(agesex)
    
    #=============================================================================
    # ============================= EVENTS COUNT =================================
    #=============================================================================

    df_overall_events <- data_surv %>% filter(overall_event==1)
    interval_names<-unlist(interval_names)
    week_cols <- df_overall_events %>% dplyr::select(all_of(interval_names))
    #  df_overall_events$expo_week <- names(week_cols)[which(week_cols == 1)] use below line instead but have to change if week categories are changed.

    df_overall_events$expo_week <- ifelse(week_cols$week1_0.142857143==1, "week1_0.142857143",
                                       ifelse(week_cols$week1.142857143_1==1, "week1.142857143_1",
                                            ifelse(week_cols$week2_4==1,"week2_4",
                                                 ifelse(week_cols$week5_16==1,"week5_16",
                                                        ifelse(week_cols$week17_75==1, "week17_75",
                                                               ifelse(week_cols$week76_209==1, "week76_209",NA))))))
    df_overall_events$expo_week <- ifelse(is.na(df_overall_events$expo_week),"pre expo", df_overall_events$expo_week)
    ls_data_surv <- split(df_overall_events, 1:nrow(df_overall_events))
    #   week_cols <- ls_data_surv %>% dplyr::select(all_of(interval_names))
    ls_data_surv <- do.call("rbind", ls_data_surv)
    tbl_overall_event_count_all <- aggregate(overall_event ~ expo_week, ls_data_surv, sum)
    tbl_overall_event_count_all[nrow(tbl_overall_event_count_all) + 1,] = c("all post expo", sum(tail(tbl_overall_event_count_all$overall_event, -1))  )

    #Event counts for males
    df_overall_events <- data_surv %>% filter(overall_event==1 & COV_SEX==1)
    interval_names<-unlist(interval_names)
    week_cols <- df_overall_events %>% dplyr::select(all_of(interval_names))
    #  df_overall_events$expo_week <- names(week_cols)[which(week_cols == 1)] use below 3 line instead but have to change if week categories are changed.

    df_overall_events$expo_week <- ifelse(week_cols$week1_0.142857143==1, "week1_0.142857143",
                                          ifelse(week_cols$week1.142857143_1==1, "week1.142857143_1",
                                                 ifelse(week_cols$week2_4==1,"week2_4",
                                                        ifelse(week_cols$week5_16==1,"week5_16",
                                                               ifelse(week_cols$week17_75==1, "week17_75",
                                                                      ifelse(week_cols$week76_209==1, "week76_209",NA))))))
    df_overall_events$expo_week <- ifelse(is.na(df_overall_events$expo_week),"pre expo", df_overall_events$expo_week)
    ls_data_surv <- split(df_overall_events, 1:nrow(df_overall_events))
    #   week_cols <- ls_data_surv %>% dplyr::select(all_of(interval_names))
    ls_data_surv <- do.call("rbind", ls_data_surv)
    tbl_overall_event_count_sex1 <- aggregate(overall_event ~ expo_week, ls_data_surv, sum)
    tbl_overall_event_count_sex1[nrow(tbl_overall_event_count_sex1) + 1,] = c("all post expo", sum(tail(tbl_overall_event_count_sex1$overall_event, -1))  )

    #Event counts for females
    df_overall_events <- data_surv %>% filter(overall_event==1 & COV_SEX==2)
    interval_names<-unlist(interval_names)
    week_cols <- df_overall_events %>% dplyr::select(all_of(interval_names))
    #  df_overall_events$expo_week <- names(week_cols)[which(week_cols == 1)] use below line instead but have to change if week categories are changed.

    df_overall_events$expo_week <- ifelse(week_cols$week1_0.142857143==1, "week1_0.142857143",
                                          ifelse(week_cols$week1.142857143_1==1, "week1.142857143_1",
                                                 ifelse(week_cols$week2_4==1,"week2_4",
                                                        ifelse(week_cols$week5_16==1,"week5_16",
                                                               ifelse(week_cols$week17_75==1, "week17_75",
                                                                      ifelse(week_cols$week76_209==1, "week76_209",NA))))))
    df_overall_events$expo_week <- ifelse(is.na(df_overall_events$expo_week),"pre expo", df_overall_events$expo_week)
    ls_data_surv <- split(df_overall_events, 1:nrow(df_overall_events))
    #   week_cols <- ls_data_surv %>% dplyr::select(all_of(interval_names))
    ls_data_surv <- do.call("rbind", ls_data_surv)
    tbl_overall_event_count_sex2 <- aggregate(overall_event ~ expo_week, ls_data_surv, sum)
    tbl_overall_event_count_sex2[nrow(tbl_overall_event_count_sex2) + 1,] = c("all post expo", sum(tail(tbl_overall_event_count_sex2$overall_event, -1))  )


    tbl_event_count <- list(tbl_overall_event_count_all, tbl_overall_event_count_sex1, tbl_overall_event_count_sex2) %>% reduce(left_join, by = "expo_week")

    event_count_levels <- c("pre expo", unlist(interval_names), "all post expo")
    tbl_event_count_levels <- data.frame(event_count_levels)
    names(tbl_event_count_levels) <- c("expo_week")

    tbl_event_count <- merge(tbl_event_count_levels, tbl_event_count, all.x = TRUE)
    tbl_event_count[is.na(tbl_event_count)] <- 0

    tbl_event_count <- tbl_event_count %>%
      arrange(factor(expo_week,
                     levels = event_count_levels),
              expo_week)

    names(tbl_event_count) <- c("expo_week", "events_total", "events_M", "events_F")


    #ind_any_zeroeventperiod <- any((tbl_event_count$events_total == 0) & (!identical(cuts_weeks_since_expo, c(4, 49))))

    write.csv(tbl_event_count, paste0("P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/event_counts/tbl_event_count_FLU_icu_",outcome_list2,"_",time,"_",vaccine,"_",exp_def,"_",censor,"_",excl,"_",Sys.Date(),".csv"), row.names = T)



    #=============================================================================
    # ============================= INCIDENCE RATE =================================
    #=============================================================================
    data_surv$fup2<-data_surv$tstop - data_surv$tstart
    infected<-data_surv[which(data_surv$flu_inf==1),]
    controls<-data_surv[which(data_surv$flu_inf==0),]
    total_fup_infected<-with(infected, sum(fup2))
    total_fup_controls<-with(controls, sum(fup2*cox_weights))
    #total_fup<-with(data_surv, sum(fup2))
    data_surv<-subset(data_surv, select = -c(fup2))


    incrate <- data.table::fread(paste0("P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/event_counts/tbl_event_count_FLU_icu_",outcome_list2,"_",time,"_",vaccine,"_",exp_def,"_",censor,"_",excl,"_",Sys.Date(),".csv"),
                                 data.table = FALSE)


    incrate<-subset(incrate, select = -c(V1, events_M, events_F))

    incrate <- incrate[!grepl("week",incrate$expo_week),]

    pre <- incrate[[1,2]]
    post <- incrate[[2,2]]
    total_events <- pre + post #added
    total_fup <- total_fup_controls + total_fup_infected #added


    library(epiR)
    library(scales)


    #################################    Overall          ################### #added
    inc_rate_overall = data.frame()
    tmp<-as.matrix(cbind(total_events,total_fup))
    inc_rate<-epi.conf(tmp, ctype = "inc.rate", method = "exact", design = 1, conf.level = 0.95) * 100000
    inc_rate_overall = rbind(inc_rate_overall, inc_rate)

    no_events<-as.data.frame(t(total_events))
    inc_rate3<-cbind(inc_rate_overall,outcome_list2,no_events,total_fup)
    #inc_rate3$col_outcomes<-gsub("BIN_OUT_", "", inc_rate3$col_outcomes)
    rownames(inc_rate3)<-inc_rate3[,4]
    inc_rate3<-inc_rate3[,-4]
    colnames(inc_rate3)<-c("event incidence_rate_(per_100,000_py)", "lower_95%_CI", "upper_95%_CI", "no._events", "follow-up")
    inc_rate3

    #################################   FLU/pneumonia infected          ###################
    inc_rate_infected = data.frame()
    tmp<-as.matrix(cbind(post,total_fup_infected))
    inc_rate<-epi.conf(tmp, ctype = "inc.rate", method = "exact", design = 1, conf.level = 0.95) * 100000
    inc_rate_infected = rbind(inc_rate_infected, inc_rate)

    no_events<-as.data.frame(t(post))
    inc_rate4<-cbind(inc_rate_infected,outcome_list2,no_events,total_fup_infected)
    #inc_rate4$col_outcomes<-gsub("BIN_OUT_", "", inc_rate4$col_outcomes)
    rownames(inc_rate4)<-inc_rate4[,4]
    inc_rate4<-inc_rate4[,-4]
    colnames(inc_rate4)<-c("event incidence_rate_(per_100,000_py)", "lower_95%_CI", "upper_95%_CI", "no._events", "follow-up")
    inc_rate4

    #################################    FLU/pneumonia controls          ###################
    inc_rate_controls = data.frame()
    tmp<-as.matrix(cbind(pre,total_fup_controls))
    inc_rate<-epi.conf(tmp, ctype = "inc.rate", method = "exact", design = 1, conf.level = 0.95) * 100000
    inc_rate_controls = rbind(inc_rate_controls, inc_rate)

    no_events<-as.data.frame(t(pre))
    inc_rate5<-cbind(inc_rate_controls,outcome_list2,no_events,total_fup_controls)
    #inc_rate5$col_outcomes<-gsub("BIN_OUT_", "", inc_rate5$col_outcomes)
    rownames(inc_rate5)<-inc_rate5[,4]
    inc_rate5<-inc_rate5[,-4]
    colnames(inc_rate5)<-c("event incidence_rate_(per_100,000_py)", "lower_95%_CI", "upper_95%_CI", "no._events", "follow-up")
    inc_rate5

    write.table(inc_rate3, paste0("P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/incidence/tbl_incidence_rate_FLU_icu_overall_",time,"_",vaccine,"_",exp_def,"_",censor,"_",excl,"_",Sys.Date(),".csv"), append=TRUE, col.name=!file.exists(paste0("P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/incidence/tbl_incidence_rate_FLU_icu_overall_",time,"_",vaccine,"_",exp_def,"_",censor,"_",excl,"_",Sys.Date(),".csv")))
    write.table(inc_rate4, paste0("P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/incidence/tbl_incidence_rate_FLU_icu_infected_",time,"_",vaccine,"_",exp_def,"_",censor,"_",excl,"_",Sys.Date(),".csv"), append=TRUE, col.name=!file.exists(paste0("P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/incidence/tbl_incidence_rate_FLU_icu_infected_",time,"_",vaccine,"_",exp_def,"_",censor,"_",excl,"_",Sys.Date(),".csv")))
    write.table(inc_rate5, paste0("P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/incidence/tbl_incidence_rate_FLU_icu_controls_",time,"_",vaccine,"_",exp_def,"_",censor,"_",excl,"_",Sys.Date(),".csv"), append=TRUE, col.name=!file.exists(paste0("P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/incidence/tbl_incidence_rate_FLU_icu_controls_",time,"_",vaccine,"_",exp_def,"_",censor,"_",excl,"_",Sys.Date(),".csv")))


    ####  Median person time of those with an event within each time period used for plotting figures. #####
    
    tmp <- data_surv[data_surv$overall_event ==1,c("ALF_E","Weeks_category","tstart","tstop")]
    tmp$person_time <- tmp$tstop - tmp$tstart
    
    tmp[,c("ALF_E","tstart","tstop")] <- NULL
    
    tmp <- tmp %>% group_by(Weeks_category) %>% 
      summarise(median_follow_up = median(person_time, na.rm = TRUE))
    
    tmp$median_follow_up <- (tmp$median_follow_up)/7 #SJK added
    
    
    ###################   left join all the outcomes and other covariates     ###########################
    #survival_data <- data_surv %>% left_join(outcomes) #data_surv replaces cohort_vac
    #survival_data <- survival_data %>% left_join(covariates) #data_surv replaces cohort_vac
    survival_data <- data_surv %>% left_join(covariates)
    survival_data$tstart <- as.Date(cohort_start_date_all) + survival_data$tstart
    survival_data$tstop <- as.Date(cohort_start_date_all) + survival_data$tstop
    survival_data$tstart<-as.numeric(survival_data$tstart)
    survival_data$tstop <-as.numeric(survival_data$tstop)
    
    ###################   last_step correction    ##########################################
    
    survival_data$overall_event <- survival_data$overall_event * survival_data$last_step
    survival_data <- survival_data[ ,!(names(survival_data) %in% outcome_list)]
    colnames(survival_data)[colnames(survival_data)=="overall_event"] <- outcome_list
    
    
    rm(list = c("data_sample", "data_surv", "outcomes_bin", "outcomes_date", "infected_indiv", "td_data", "ls_data_surv", "df_overall_events"))
    gc()
    
    if (outcome_list2=="ARTERIAL_EVENT" | outcome_list2=="VENOUS_EVENT") {
    
    #--------------------------------------------
    
    
    print(paste("Generating results for the following outcome type:", outcome_list))
    
    
    #-------------------------------------------------------------------------------
    #HR lowCI upperCI SE numberofevents
    
    cov_model <- "week1_0.142857143 + week1.142857143_1 + week2_4 + week5_16 + week17_75 + COV_AGE + COV_SEX + age_sq + COV_DEPRIVATION + COV_EVER_DEMENTIA + COV_EVER_CKD + hx_hypertension + COV_SMOKING_STATUS + COV_ELIXHAUSER_SCORE + COV_EVER_OBESITY + COV_EVER_COPD + COV_EVER_CANCER + COV_EVER_DIABETES + COV_EVER_ANY_CVD + COV_SURGERY_LASTYR + COV_LIPID_MEDS + hx_antiplat_anticoag + CARE_HOME + RURAL_URBAN + COV_EVER_DEPRESSION + COV_EVER_FRACTURE"
    cov <-c("week1_0.142857143 ","week1.142857143_1 ", "week2_4 ", "week5_16 ", "week17_75 ", "age ", "sex ", "age_squared ", "DEPRIVATIONMissing", "DEPRIVATION2", "DEPRIVATION3", "DEPRIVATION4", "DEPRIVATION5", "EVER_DEMENTIA", "EVER_CKD"
            , "hypertension ", "SMOKING_STATUS_Missing ", "SMOKING_STATUS_Ex-smoker ", "SMOKING_STATUS_Current-smoker ",
            "ELIXHAUSER_SCORE ", "EVER_OBESITY ", "EVER_COPD ", "EVER_CANCER ", "EVER_DIABETES ", 
            "EVER_ANY_CVD ", "SURGERY_LASTYR ", "LIPID_MEDS ", "antiplat_anticoag ", "CARE_HOME ",
            "RURAL_URBAN ", "COV_EVER_DEPRESSION ", "COV_EVER_FRACTURE ")
    
    #N_DISORDER was excluded and used to be after EVER ANY CVD
    
    write(c("event term estimate conf.low conf.high std.error n.event subgroup"), 
          paste0('P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/',time,'/',adjust,'_cox_results',exp_def,'_FLU_icu_',Sys.Date(),'_',vaccine,'_',outcome_list2,"_",censor,"_",excl,'.csv'), append = TRUE)
    
    purrr::map(outcome_list, function(x) {
      f <- as.formula(paste("Surv(time = tstart, time2 = tstop, event=", x, ") ~ ",cov_model))
      model <- coxph(f, survival_data, id = ALF_E, weights = survival_data$cox_weights, cluster = ALF_E, robust = T)
      model$call$formula <- f 
      s <- summary(model)
      cat(paste0(substring(x,9,40),' ',cov, apply(s$coefficients, 1, 
                                                  function(x) {
                                                    paste0(" ", round(exp(x[1]), 3), #change to cov1 and cov2 depending on the list of covariates used 
                                                           ' ', round(exp(x[1] - 1.96 * x[3]), 3),
                                                           ' ', round(exp(x[1] + 1.96 * x[3]), 3),
                                                           " ", round((x[3]), 4),
                                                           " ", summary(model)$nevent)}),
                 collapse = '\n'), '\n', sep = '', 
          file = paste0('P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/',time,'/',adjust,'_cox_results',exp_def,'_FLU_icu_',Sys.Date(),'_',vaccine,'_',outcome_list2,"_",censor,"_",excl,'.csv'), append = TRUE)
      invisible(model)
    })
    
    #Read in "Extensive..." results files after running Coxph then do the following
    estimates <- data.table::fread(paste0('P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/',time,'/',adjust,'_cox_results',exp_def,'_FLU_icu_',Sys.Date(),'_',vaccine,'_',outcome_list2,"_",censor,"_",excl,'.csv'),
                                   data.table = FALSE)
    estimates <- estimates %>% filter(term %in% term[grepl("^week",term)])
    tmp2<- data.frame(term=c("week1_0.142857143", "week1.142857143_1", "week2_4", "week5_16", "week17_75"),
                      Weeks_category=c(1,2,3,4,5))
    estimates<-merge(estimates, tmp2, by = "term", all.x = TRUE)
    estimates<- merge(estimates, tmp, by = "Weeks_category", all.x = TRUE)
    
    estimates <- estimates %>% dplyr::mutate(across(c(estimate,conf.low,conf.high,median_follow_up),as.numeric))
    
    #Calculate median follow-up for plotting
    estimates$median_follow_up <- as.numeric(estimates$median_follow_up)
    estimates$add_to_median <- sub("week","",estimates$term)
    estimates$add_to_median <- as.numeric(sub("\\_.*","",estimates$add_to_median))
    estimates$add_to_median <- as.numeric(estimates$add_to_median)
    estimates$add_to_median <- ifelse(estimates$term=="week1_0.142857143", 0.0,
                                      ifelse(estimates$term=="week1.142857143_1", 0.144, estimates$add_to_median))
    estimates$median_follow_up <- ((estimates$median_follow_up + estimates$add_to_median))#/7
    #estimates$add_to_median <- NULL
    
    #Filter to columns and terms of interest
    estimates <- estimates %>%
      select(event,term,estimate,conf.low,conf.high,std.error,n.event,subgroup,median_follow_up)
    
    estimates <- as.data.frame(estimates)
    
    #Then save to csv.
    write.table(estimates, paste0('P:/keenes/Projects/CCU002-04/results/FLU_cox_analysis/',time,'/',adjust,'_cox_results',exp_def,'_FLU_icu_',Sys.Date(),'_',vaccine,'_',outcome_list2,"_",censor,"_",excl,'.csv'), row.names = FALSE)
  
    
  } #this closes the loop for only looking at Arterial and Venous in cox models
} #this closes the first loop for(i in 1:length(outcomes_names2))
#} #this closes pheno_of_interest loop
