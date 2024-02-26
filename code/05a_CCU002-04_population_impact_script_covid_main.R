## =============================================================================
## Translating Hazard ratios into population impact estimates by using a life table approach 

## Keywords: Absolute Excess Risk (AER), population impact measures, life table, cardiovascular disease, COVID-19 infection

## Author: Xiyun Jiang
## =============================================================================

# 1) are these the only columns that are needed for the script to run?
#   
#   ID    COVID_date    disease_event_date   death_date    cohort_start_date   cohort_end_date
# 
# If I know exactly what the data looks like when it is fed into the script then it would be easier.
# 
# 2) What is the difference between cohort_start and start_date and cohort_end and end_date? I think they are the same but seem to be 
# 
# 3) Should I define max_follow_up_time as: cohort_end_date - cohort_start_date?
#   
# 4) Did you run these analyses outside of any TREs (i.e. you didn't have to access the data in a research environment but did it outside)?
#  
# 5) For the 2nd assumption (i.e. Disease of interest or death took place at the end of the day) is this why you add +1 to the to_calculate_futime function but only when not censoring at covid infection date? Shouldn't +1 be added to all ifelse statements?

##Project Background:
#COVID-19 infection induces a pro-thrombotic and pro-inflammatory state that may increase the risk of thrombotic disorders.
#Project CCU002_01 (paper: 'Association of COVID-19 with arterial and venous vascular diseases:population-wide cohort study of 48 million adults in England and Wales') calculated hazard ratios (HRs) for risks of cardiovascular disease (CVD) over time since diagnosis of COVID-19 compared to before or no COVID-19 diagnosis. 
#HRs are relative risk estimates and does not provide information on the likelihood of CVD occurring at the individual level (i.e. 1 in 10000, an absolute risk measure). HRs also cannot not tell us about the number of total excess CVD events attributable to COVID-19 infection. Estimates such as absolute risks and excess events are easier to interpret and have more direct public health implications. 
#Here, I use HRs generated from project CCU002_01 and calculated changes in absolute risk of CVD over time since COVID-19 diagnosis, compared to before or no COVID-19 diagnosis and the number of excess CVD events attributable to COVID-19 infection by using a life table approach.

##Aims:
#1. Estimate changes in absolute risk of arterial and venous thromboembolic events over time since COVID-19 diagnosis, compared to before or no COVID-19 diagnosis among CVD subtypes and population subgroups (e.g age, sex, ethnicity).
#2. Estimate number of excess arterial and venous thromboembolic events and CVD subtypes attributable to COVID-19 infection among population subgroups.

##Follow-up period: 2020-01-01 to 2020-12-07 --> unvaccinated period

rm(list=setdiff(ls(), "FLU_plot"))

##Load in the required packages 
library(data.table)
library(survival)
library(tidyverse)
library(gridExtra)
library(lubridate)
library(ggpubr)
library(ggplot2)
library(ggthemes) #optional package 

##Data format required (for example):
# ID COVID_date    disease_event_date   death_date
# x    NA               2020-01-10      2020-01-15
# y    NA                  NA               NA
# z   2020-01-02           NA               NA
# h   2020-01-04           NA           2020-05-30

##read in the functions: 
#summary_stat() outputs summary of the data
summary_stat<-function(dataset){
  dimension=dim(dataset)
  str=sapply(dataset, class)
  summarised=summary(dataset)
  my_return_list<-list(dimension=dimension,str=str,summarised=summarised)
  return(my_return_list)
}

#checks() function checks if the date in the columns of interests are outside of the follow-up period 
checks<-function(dataset){
  date_columns<-dataset %>%
    select_if(is.Date)
  
  result1<-sapply(date_columns, function(x) any(x<start_date, na.rm = T)) #start_date is the cohort follow-up start date 
  result2<-sapply(date_columns, function(x) any(x>end_date, na.rm = T)) #end_date is the cohort follow-up end date
  result_list<-list(any_before_start_date=result1, any_after_end_date=result2)
  return(result_list)
}

#count_columns () counts number of individuals with a date recorded in the column of interest (i.e. number of individuals with event/disease of interest)
count_columns<-function(dataset,column_names){
  date_columns<-dataset %>%
    select(all_of(column_names))
  
  return(sapply(date_columns, function(x)sum(!is.na(x)))) 
  
}

#to_calculate_futime() calculates the follow-up time for each individual spent disease (of interest)-free and COVID-19 diagnosis-free 

#This function performs censoring at the earliest date (whichever earlier): 
#Disease of interest occurred
#COVID-19 diagnosis confirmed
#Died 
#Cohort follow-up ended 

#Assumptions in the function: 
#1.Follow-up started at the start of day x and ended at end of day y
#2.Disease of interest or death took place at the end of the day 

#Spencer note: probably equivalent to what I have done before to calculate fup. Maybe use that instead.
#add other censoring columns to this as well
#also may need to define max_follow_up_time as cohort_end_date - cohort_start_date

to_calculate_futime<-function(cohort_start, covid_date, death_date, censor_date, gp_coverage, disease_event_date, max_follow_up_time){
  # futime<-ifelse(is.na(covid_date) & is.na(death_date) & is.na(disease_event_date), max_follow_up_time,
  #                ifelse(is.na(covid_date)& !is.na(disease_event_date), as.numeric(disease_event_date - cohort_start)+1,
  #                       ifelse(is.na(covid_date) & !is.na(death_date), as.numeric(death_date-cohort_start)+1,
  #                              ifelse(!is.na(covid_date) & is.na(disease_event_date), as.numeric(covid_date-cohort_start),
  #                                     ifelse(!is.na(covid_date) & disease_event_date<covid_date, as.numeric(disease_event_date-cohort_start)+1,
  #                                            ifelse(!is.na(covid_date) & disease_event_date>covid_date, as.numeric(covid_date-cohort_start),
  #                                                   ifelse(!is.na(covid_date) & disease_event_date==covid_date, as.numeric(covid_date-cohort_start),'to check')))))))
  
  futime<-ifelse(is.na(covid_date) & is.na(death_date) & is.na(disease_event_date) & is.na(censor_date) & is.na(gp_coverage), max_follow_up_time,
                 ifelse(is.na(covid_date)& !is.na(disease_event_date), as.numeric(disease_event_date - cohort_start)+1,
                        ifelse(is.na(covid_date) & !is.na(death_date), as.numeric(death_date-cohort_start)+1,
                               ifelse(is.na(covid_date) & !is.na(censor_date), as.numeric(censor_date-cohort_start)+1,
                                      ifelse(is.na(covid_date) & !is.na(gp_coverage), as.numeric(gp_coverage-cohort_start)+1,
                                             
                                             ifelse(!is.na(covid_date) & is.na(disease_event_date), as.numeric(covid_date-cohort_start),
                                                    ifelse(!is.na(covid_date) & disease_event_date<covid_date, as.numeric(disease_event_date-cohort_start)+1,
                                                           ifelse(!is.na(covid_date) & disease_event_date>covid_date, as.numeric(covid_date-cohort_start),
                                                                  ifelse(!is.na(covid_date) & disease_event_date==covid_date, as.numeric(covid_date-cohort_start),'to check')))))))))
  
  futime<-as.numeric(futime)
  futime<-ifelse(futime>max_follow_up_time, max_follow_up_time, futime)
  return(futime)
  
}

#average_daily_incidence() calculates average daily incidence of the disease of interest in the unexposed period 
#(i.e.period before or no COVID-19 diagnosis)
average_daily_incidence<-function(dataset, disease_event_date, covid_date,futime,max_follow_up_time){
  
  #Daily incidence formula in the unexposed period: #number of disease-of-interest events 
  #occurred during the day in the unexposed period/#number of disease-free (at risk) individuals at the start of the day in the unexposed period
  
  #Note:as we taking 'day' as the unit, the daily incidence calculated here is both daily incidence proportion and daily incidence rate
  
  
  #First, define disease status for each individual in the unexposed period: code 1 
  #for the disease of interest occurred in the unexposed period and 0 otherwise
  disease_status<-ifelse(is.na(disease_event_date),0,
                         ifelse(!is.na(disease_event_date) & is.na(covid_date),1,
                                ifelse(!is.na(disease_event_date) & disease_event_date>=covid_date,0,1))) 
  

  dataset$disease_status<-as.integer(disease_status)
  
  #Next, use the survfit() function in the survival package and the follow-up time 
  #calculated using to_calculate_futime() to summarise the number of daily disease event 
  #(nominator of the daily incidence formula) and the number of daily at risk individuals (denominator 
  #of the daily incidence formula) in the unexposed period
  fit<-survfit(Surv(futime,dataset$disease_status) ~1) 
  
  #Put the date, numbers of daily disease-event (nominator) and numbers of individuals at risk (denominator) into a new dataframe 
  daily_incidence<-data.frame(time=fit$time,
                              n_event=fit$n.event,
                              n_risk=fit$n.risk)
  
  #calculate daily incidence for disease of interest in the unexposed period
  daily_incidence$incidence_proportion<-daily_incidence$n_event/daily_incidence$n_risk
  
  #In the last step, average the daily incidence across the cohort follow-up period to get an average daily incidence for disease of interest 
  average_daily_incidence<-sum(daily_incidence$incidence_proportion)/max_follow_up_time 
  
  #two other useful summary level information to put in the output of this function
  event_count_unexposed<-sum(dataset$disease_status==1) #disease of interest counts in the unexposed period
  exposed_individual<-sum(!is.na(dataset$EXP_CONFIRMED_COVID19_DATE))  #number of individuals (exposed) with a COVID-19 diagnosis during cohort follow-up
  
  return_list<-c(event_count_unexposed=event_count_unexposed, 
                 average_daily_incidence=average_daily_incidence,
                 number_of_exposed_individuals=exposed_individual)
  
  return(return_list)
}

#incidence_rate() calculates an 'average' incidence rate of disease in the unexposed period (i.e.period before or no COVID-19 diagnosis)

#incidence rate formula in the unexposed period: #total number of disease-event occurred in the unexposed period/#total number of person-time contributed by at risk individuals during the unexposed period 

incidence_rate<-function(disease_event_date,covid_date,futime){
  
  #calculates the nominator
  total_event<-sum(!is.na(disease_event_date)) 
  event_exposed<-sum(disease_event_date>=covid_date,na.rm = T)
  event_unexposed<-total_event-event_exposed
  
  #calculates the denominator
  follow_up_time<-sum(futime)
  
  #calculates the incidence rate
  incidence_rate<-event_unexposed/follow_up_time
  
  return(incidence_rate)
}

#AER_calculation() builds the life table and calculates changes in absolute excess risk over time since COVID-19 diagnosis and outputs the number of excess disease events attributable to COVID-19 infection with 95% confidence intervals.
AER_calculation<-function(dataframe_name,event_name,subgroup_name,q,HR,HR_LB,HR_UB,covid_infected_population,total_num_rows_for_life_table,weeks_since_covid){
  event<-rep(event_name,total_num_rows_for_life_table) 
  subgroup<-rep(subgroup_name,total_num_rows_for_life_table)
  x_days_since_covid<-c(1:total_num_rows_for_life_table)
  weeks_since_covid<-weeks_since_covid
  q_estimated_average_incidence_rate<-rep(q, total_num_rows_for_life_table)
  life_table<-data.frame(event,subgroup,x_days_since_covid,weeks_since_covid,q_estimated_average_incidence_rate,HR,HR_LB,HR_UB)
  life_table$one_minus_q<-1-life_table$q_estimated_average_incidence_rate
  life_table$S<-cumprod(life_table$one_minus_q)
  life_table$qh<-life_table$q_estimated_average_incidence_rate*life_table$HR
  life_table$qh_LB<-life_table$q_estimated_average_incidence_rate*life_table$HR_LB
  life_table$qh_UB<-life_table$q_estimated_average_incidence_rate*life_table$HR_UB
  life_table$one_minus_qh<-1- life_table$qh
  life_table$SC<-cumprod(life_table$one_minus_qh)
  life_table$one_minus_qh_LB<-1- life_table$qh_LB
  life_table$SC_LB<-cumprod(life_table$one_minus_qh_LB)
  life_table$one_minus_qh_UB<-1- life_table$qh_UB
  life_table$SC_UB<-cumprod(life_table$one_minus_qh_UB)
  life_table$AER<-life_table$S- life_table$SC
  life_table$AER_percent<-life_table$AER*100
  life_table$AER_LB<-life_table$S- life_table$SC_LB
  life_table$AER_percent_LB<-life_table$AER_LB*100
  life_table$AER_UB<-life_table$S- life_table$SC_UB
  life_table$AER_percent_UB<-life_table$AER_UB*100
  assign(x = dataframe_name, value = life_table, envir = globalenv())
  excess_mean<-covid_infected_population*life_table[total_num_rows_for_life_table,20]
  excess_LB<-covid_infected_population*life_table[total_num_rows_for_life_table,22]
  excess_UB<-covid_infected_population*life_table[total_num_rows_for_life_table,24]
  return_list<-list(excess_events_mean=excess_mean, excess_events_LB=excess_LB, excess_events_UB=excess_UB)
  return(return_list) #this function returns changes in AER, total number of excess disease events attributable to COVID-19 infection and the 95% confidence intervals for excess events
}

#plot graphs() plots the graph for different population subgroups (up to 5 subgroups). 
plot_graph<-function(num_subgroups, life_table1,lifetable2,lifetable3,lifetable4,lifetable5,lifetable6,lifetable7, graph_title,graph_name,levels,legend_title,color_value,linetype){
  library(ggplot2)
  #library(ggthemes) optional package. If working in the English DAE (Data Access Environment), it does not have ggthemes package installed --> admin required to install this package 
  if(num_subgroups==0){#if there is no subgroup, i.e. plotting for all and not split by sex for example, put num_subgroups=0 in the function argument.
    
    life_table1$x_days_since_covid_converted<-life_table1$x_days_since_covid/7
    
    p_line<-ggplot(life_table1,
                   aes(x=x_days_since_covid_converted,
                       y=AER_percent,
                       lty = 'All')) +
      geom_line(size=1.5,colour=color_value)+
      scale_x_continuous(breaks = c(0,10,20,30,40,50,60,70,80),limits = c(0,80))+
      #      scale_y_continuous(breaks = c(0,1,2,3),limits = c(0,2.5))+
      scale_y_continuous(breaks = c(0,1,2),limits = c(0,2))+
      scale_color_manual(values = color_value)+
      labs(x='Weeks since COVID-19 hospitalisation',y='Difference in cumulative absolute risk (%)',
           title = graph_title)+
      theme(plot.title = element_text(hjust = 0.5))+
      #theme_hc()+ this background requires ggthemes package
      scale_linetype('')
    
    assign(x = graph_name, value = p_line, envir = globalenv())
    
    return(p_line)
  }
  
  if(num_subgroups==2){
    combined<-rbind(life_table1,lifetable2)
  }
  
  if(num_subgroups==3){
    combined<-rbind(life_table1,lifetable2,lifetable3)
  }
  
  if(num_subgroups==4){
    combined<-rbind(life_table1,lifetable2,lifetable3, lifetable4)
  }
  if(num_subgroups==5){
    combined<-rbind(life_table1,lifetable2,lifetable3,lifetable4,lifetable5)
  }
  if(num_subgroups==6){
    combined<-rbind(life_table1,lifetable2,lifetable3,lifetable4,lifetable5, lifetable6)
  }  
  if(num_subgroups==7){
    combined<-rbind(life_table1,lifetable2,lifetable3,lifetable4,lifetable5, lifetable6, lifetable7)
  }  
  
  combined$x_days_since_covid_converted<-combined$x_days_since_covid/7
  combined$subgroup<-factor(combined$subgroup,levels = levels)
  
  p_line<-ggplot(combined,
                 aes(x=x_days_since_covid_converted,
                     y=AER_percent,
                     colour=subgroup)) +
    geom_line(aes(linetype=subgroup, colour=subgroup), size=1)+
    scale_linetype_manual(values = linetype)+
    scale_color_manual(values = color_value)+
    scale_x_continuous(breaks = c(0,10,20,30,40,50,60,70,80),limits = c(0,80))+
    #    scale_y_continuous(breaks = c(0,1,2,3),limits = c(0,2.5))+
    scale_y_continuous(breaks = c(0,1,2),limits = c(0,2))+
    labs(x='Weeks since COVID-19 hospitalisation',y='Difference in cumulative absolute risk (%)',
         title = graph_title)+
    theme(plot.title = element_text(hjust = 0.5))+
    #theme_hc()+ this background requires ggthemes package
    theme(legend.key.size = unit(1.2,'cm'), legend.key = element_blank())+
    labs(color=legend_title,linetype=legend_title)
  
  assign(x = graph_name, value = p_line, envir = globalenv())
  
  return(p_line)
}

#save_graphs() saves the plot in .png format
save_plot<-function(file_location,data,filename,dpi, width,height){
  setwd(file_location)
  ggsave(plot=data, filename = paste0(filename, ".png"), dpi=dpi,width = width,height = height)
  
}


censor<-"nocensor" #censor OR nocensor


###NOTE ON EXCLUDING WEEK 1: 
#1. I got rid of week1 estimates
#2  Subtracted 7 from max follow-up time in AER_calculation function
#3 Changed y-axis back to 0 to 1 rather than 0 to 4. (2 SPOTS NEED CHANGING)
#4 got rid of week1 repitition from weeks_since_covid below.


##Example: Arterial Event (not split by subgroups)##

##Another example split by subgroups is given after this


#1.Set working directory and reading in the data
data<-fread("P:/keenes/Projects/CCU002-04/raw/CCU002_04_COVID_COHORT_20220630.csv",
            select = c('ALF_E','DEATH_DATE','GP_COVERAGE_END_DATE', 'LOST_TO_FOLLOWUP_DATE', 
                       'COV_SEX','COV_AGE',
                       'EXP_CONFIRMED_COVID19_DATE','OUT_VENOUS_EVENT', 'OUT_ARTERIAL_EVENT', 
                       'EXP_CONFIRMED_COVID_PHENOTYPE'))

#2.Define cohort start date and end date
start_date<-as.IDate('2020-01-01')
end_date<-as.IDate('2021-12-31')

max_follow_up_time<- end_date - start_date
max_follow_up_time
#3.Look at the data at the summary level
summary_stat(data)

#4.perform checks to see if any date variables have dates outside of the cohort start and end date 
checks(data) #here we can see that there are some individuals that have death date outside of the cohort follow-up period --> we will deal with this later on in the code

#5.count the number of individuals that died or have confirmed COVID-19 diagnosis or have arterial event in the dataset
count_columns(dataset=data, column_names = c('DEATH_DATE','EXP_CONFIRMED_COVID19_DATE','OUT_VENOUS_EVENT', 'OUT_ARTERIAL_EVENT'))




#6.calculate the follow-up time that individual spent at risk (no disease event of interest) and without COVID-19 diagnosis 
#censored when the individual (whichever is earliest):
#died 
#have disease of interest 
#confirmed COVID-19 diagnosis 
#reached the end of the cohort follow-up period

#the to_calculate_futime function() corrects for individuals with death date outside of the cohort follow-up time (see line 88). It restricts individuals with follow-up time longer than 342 (i.e. people with death date after cohort end time) to 342 days, which is the maximum follow-up time in this study.


#optional: you can also calculate the incidence rate considering the entire unexposed period as a whole (see line 134) but for the lifetable, it is better to use the average daily incidence calculated in step 7
#incidence_rate(disease_event_date=data$OUT_VENOUS_EVENT, covid_date=data$EXP_CONFIRMED_COVID19_DATE, futime=data$futime)

#8.Set up the lifetable
#Note: After average daily incidence is calculated, it is no longer necessary to keep working in the DAE. You can write down the average daily incidence estimate and work in the R environment (or excel) out of the DAE for the life table and plot graphs.

#weeks_since_covid<-c(rep('1',7), rep('2-4',21),rep('5-16',84),rep('17-75',413), rep('76-105',205))
weeks_since_covid<-c(
  rep('1',7), 
  rep('2-4',21),rep('5-16',84),rep('17-75',413), rep('76-105',205))

sink("P:/keenes/Projects/CCU002-04/results/COVID_cox_analysis/AER/Covid_excess_events_main.log",append=FALSE,split=TRUE)

if (censor=="censor") {
  data$CENSOR_DATE <- as.Date(ifelse((data$EXP_CONFIRMED_COVID_PHENOTYPE=="non-hospitalised"),
                                     data$EXP_CONFIRMED_COVID19_DATE,NA),origin='1970-01-01')
  
  data$EXP_CONFIRMED_COVID19_DATE <- as.Date(ifelse((data$EXP_CONFIRMED_COVID_PHENOTYPE=="non-hospitalised"), 
                                                    NA,data$EXP_CONFIRMED_COVID19_DATE),origin='1970-01-01')
  
  data<-transform(data, LOST_TO_FOLLOWUP_DATE=pmin(LOST_TO_FOLLOWUP_DATE, CENSOR_DATE,na.rm=TRUE)) #phenotype: DATE_EXPO_CENSOR added
  
} else {
  
  data$CENSOR_DATE <- as.Date(ifelse((data$EXP_CONFIRMED_COVID_PHENOTYPE=="non-hospitalised"),
                                     NA, data$EXP_CONFIRMED_COVID19_DATE),origin='1970-01-01')
  
  data$EXP_CONFIRMED_COVID19_DATE <- as.Date(ifelse((data$EXP_CONFIRMED_COVID_PHENOTYPE=="non-hospitalised"), 
                                                    NA,data$EXP_CONFIRMED_COVID19_DATE),origin='1970-01-01')
  
  data<-transform(data, LOST_TO_FOLLOWUP_DATE=pmin(LOST_TO_FOLLOWUP_DATE, CENSOR_DATE,na.rm=TRUE)) #phenotype: DATE_EXPO_CENSOR added
}

#--------------------------------------------------
#           VENOUS EVENT
#----------------------------------------------------

#I need to make data$EXP_CONFIRMED_COVID19_DATE = NA or blank if it is non-hosp

# data$futime<-to_calculate_futime(cohort_start = start_date, covid_date = data$EXP_CONFIRMED_COVID19_DATE,death_date = data$DEATH_DATE,
#                                  censor_date = data$LOST_TO_FOLLOWUP_DATE, gp_coverage = data$GP_COVERAGE_END_DATE, 
#                                  disease_event_date = data$OUT_VENOUS_EVENT, max_follow_up_time = max_follow_up_time) #342 is the maximum follow-up time in this particular study

data<-transform(data, min_date=pmin(EXP_CONFIRMED_COVID19_DATE, DEATH_DATE, LOST_TO_FOLLOWUP_DATE, 
                                    GP_COVERAGE_END_DATE, OUT_VENOUS_EVENT, end_date, na.rm=TRUE)) #phenotype: DATE_EXPO_CENSOR added
data$min_date<-as.IDate(data$min_date)
data$futime<-ifelse((is.na(data$min_date)), max_follow_up_time, as.numeric(data$min_date-start_date))

data<-subset(data, futime>=0) 
data$futime <- data$futime + 1


data$sex_label <- ifelse(data$COV_SEX==2, "Female", "Male")

agebreaks <- c(0, 60, 500)
agelabels <- c("<60", "60+")
data$COV_AGE <- as.numeric(data$COV_AGE)
data <- setDT(data)[ , agegroup := cut(COV_AGE, breaks = agebreaks, right = FALSE, labels = agelabels)]

data_female<-data[data$sex_label=='Female',]
data_male<-data[data$sex_label=='Male',]

data_old<-data[data$agegroup=='60+',]
data_young<-data[data$agegroup=='<60',]


print("COVID-19 excess events for Venous outcome")


#Female
print("Female")

return_daily_incidence<-average_daily_incidence(dataset=data_female, disease_event_date=data_female$OUT_VENOUS_EVENT, covid_date=data_female$EXP_CONFIRMED_COVID19_DATE, futime=data_female$futime,
                                                max_follow_up_time=max_follow_up_time)

HR<-c(
  rep(7.06,7),
  rep(13,21),rep(5.7,84),rep(1.74,413),rep(1.00,205))
HR_LB<-c(
  rep(2.6,7),
  rep(8.3,21),rep(3.9,84),rep(1.13,413),rep(1.00,205)) #HR upper 95% CI
HR_UB<-c(
  rep(18.8,7),
  rep(20,21),rep(8.4,84),rep(2.69,413),rep(1.00,205)) #HR lower 95% CI

AER_calculation(dataframe_name = 'life_table_venous_female',
                event_name = 'Venous_event',
                subgroup_name = 'Female',
                weeks_since_covid = weeks_since_covid,
                q=return_daily_incidence[[2]],
                HR=HR,
                HR_LB=HR_LB,
                HR_UB=HR_UB,
                covid_infected_population = return_daily_incidence[[3]],
                total_num_rows_for_life_table = max_follow_up_time)

print("additional information:")
print(return_daily_incidence)

#Male
print("Male")

return_daily_incidence<-average_daily_incidence(dataset=data_male, disease_event_date=data_male$OUT_VENOUS_EVENT, covid_date=data_male$EXP_CONFIRMED_COVID19_DATE, futime=data_male$futime,
                                                max_follow_up_time=max_follow_up_time)

HR<-c(
  rep(5.5,7),
  rep(27,21),rep(6.1,84),rep(2.5,413),rep(1.00,205))
HR_LB<-c(
  rep(1.75,7),
  rep(20,21),rep(4.1,84),rep(1.7,413),rep(1.00,205)) #HR upper 95% CI
HR_UB<-c(
  rep(16.9,7),
  rep(37,21),rep(9.1,84),rep(3.7,413),rep(1.00,205)) #HR lower 95% CI

AER_calculation(dataframe_name = 'life_table_venous_male',
                event_name = 'Venous_event',
                subgroup_name = 'Male',
                weeks_since_covid = weeks_since_covid,
                q=return_daily_incidence[[2]],
                HR=HR,
                HR_LB=HR_LB,
                HR_UB=HR_UB,
                covid_infected_population = return_daily_incidence[[3]],
                total_num_rows_for_life_table = max_follow_up_time)

print("additional information:")
print(return_daily_incidence)


#Young (<60)
print("Young (<60)")

return_daily_incidence<-average_daily_incidence(dataset=data_young, disease_event_date=data_young$OUT_VENOUS_EVENT, covid_date=data_young$EXP_CONFIRMED_COVID19_DATE, futime=data_young$futime,
                                                max_follow_up_time=max_follow_up_time)

HR<-c(
  rep(21.8,7),
  rep(50,21),rep(7.6,84),rep(2.4,413),rep(1.00,205))
HR_LB<-c(
  rep(8.2,7),
  rep(34,21),rep(4.5,84),rep(1.4,413),rep(1.00,205)) #HR upper 95% CI
HR_UB<-c(
  rep(58,7),
  rep(72,21),rep(12.9,84),rep(4.3,413),rep(1.00,205)) #HR lower 95% CI

AER_calculation(dataframe_name = 'life_table_venous_young',
                event_name = 'Venous_event',
                subgroup_name = '<60 years old',
                weeks_since_covid = weeks_since_covid,
                q=return_daily_incidence[[2]],
                HR=HR,
                HR_LB=HR_LB,
                HR_UB=HR_UB,
                covid_infected_population = return_daily_incidence[[3]],
                total_num_rows_for_life_table = max_follow_up_time)

print("additional information:")
print(return_daily_incidence)


#Old (60+)
print("Old (60+)")

return_daily_incidence<-average_daily_incidence(dataset=data_old, disease_event_date=data_old$OUT_VENOUS_EVENT, covid_date=data_old$EXP_CONFIRMED_COVID19_DATE, futime=data_old$futime,
                                                max_follow_up_time=max_follow_up_time)

HR<-c(
  rep(3.3,7),
  rep(13.2,21),rep(5.5,84),rep(2,413),rep(1.00,205))
HR_LB<-c(
  rep(1.07,7),
  rep(9.3,21),rep(4,84),rep(1.4,413),rep(1.00,205)) #HR upper 95% CI
HR_UB<-c(
  rep(10.3,7),
  rep(18.6,21),rep(7.6,84),rep(2.8,413),rep(1.00,205)) #HR lower 95% CI

AER_calculation(dataframe_name = 'life_table_venous_old',
                event_name = 'Venous_event',
                subgroup_name = '60+ years old',
                weeks_since_covid = weeks_since_covid,
                q=return_daily_incidence[[2]],
                HR=HR,
                HR_LB=HR_LB,
                HR_UB=HR_UB,
                covid_infected_population = return_daily_incidence[[3]],
                total_num_rows_for_life_table = max_follow_up_time)

print("additional information:")
print(return_daily_incidence)


#4. Plot changes version 2
plot_graph(num_subgroups = 4,#life_table1 = life_table_venous_hosp, 
           life_table1 = life_table_venous_female, lifetable2 = life_table_venous_male, 
           #           lifetable2 = life_table_venous_non_hosp_young, 
           #           lifetable2 = life_table_venous_hosp,
           lifetable3 = life_table_venous_young, 
           #           lifetable4 = life_table_venous_non_hosp_old, 
           lifetable4 = life_table_venous_old,
           graph_title = 'Venous Thrombosis',
           graph_name = 'Venous_event_graph_COVID2',
           levels = c(#'Hospitalised COVID-19 infection',
                      'Female','Male',
                      #                      'hospitalised COVID-19 infection',
                      #                      'non-hospitalised COVID-19 infection <60 years old',
                      '<60 years old',
                      #                      'non-hospitalised COVID-19 infection 60+ years old',
                      '60+ years old'
           ),
           legend_title = '',
           linetype = c(#'solid',
                        'twodash','dotted',
                        #                        'dashed',
                        'dotdash', 
                        'dotted' 
                        #                        'solid'
           ),
           color_value=c(
              # 'black',
             #'black',
             'springgreen4',
             #             'blue',
             'blue', 
             'goldenrod', 
             'red'
           ))




#--------------------------------------------------
#           ARTERIAL EVENT
#----------------------------------------------------
# data$futime<-to_calculate_futime(cohort_start = start_date, covid_date = data$EXP_CONFIRMED_COVID19_DATE,death_date = data$DEATH_DATE,
#                                  censor_date = data$CENSOR_DATE, gp_coverage = data$GP_COVERAGE_END_DATE, 
#                                  disease_event_date = data$OUT_ARTERIAL_EVENT, max_follow_up_time = max_follow_up_time) #342 is the maximum follow-up time in this particular study

data<-transform(data, min_date=pmin(EXP_CONFIRMED_COVID19_DATE, DEATH_DATE, LOST_TO_FOLLOWUP_DATE, 
                                    GP_COVERAGE_END_DATE, OUT_ARTERIAL_EVENT, end_date, na.rm=TRUE)) #phenotype: DATE_EXPO_CENSOR added
data$min_date<-as.IDate(data$min_date)
data$futime<-ifelse((is.na(data$min_date)), max_follow_up_time, as.numeric(data$min_date-start_date))

data<-subset(data, futime>=0) 
data$futime <- data$futime + 1


data$sex_label <- ifelse(data$COV_SEX==2, "Female", "Male")

agebreaks <- c(0, 60, 500)
agelabels <- c("<60", "60+")
data$COV_AGE <- as.numeric(data$COV_AGE)
data <- setDT(data)[ , agegroup := cut(COV_AGE, breaks = agebreaks, right = FALSE, labels = agelabels)]


data_female<-data[data$sex_label=='Female',]
data_male<-data[data$sex_label=='Male',]

data_old<-data[data$agegroup=='60+',]
data_young<-data[data$agegroup=='<60',]



print("COVID-19 excess events for Arterial outcome")


#Female
print("Female")

return_daily_incidence<-average_daily_incidence(dataset=data_female, disease_event_date=data_female$OUT_ARTERIAL_EVENT, covid_date=data_female$EXP_CONFIRMED_COVID19_DATE, futime=data_female$futime,
                                                max_follow_up_time=max_follow_up_time)

HR<-c(
  rep(3.1,7),
  rep(4.4,21),rep(2,84),rep(1.9,413),rep(1.00,205))
HR_LB<-c(
  rep(1.6,7),
  rep(3.14,21),rep(1.5,84),rep(1.5,413),rep(1.00,205)) #HR upper 95% CI
HR_UB<-c(
  rep(6,7),
  rep(6.1,21),rep(2.7,84),rep(2.3,413),rep(1.00,205)) #HR lower 95% CI

AER_calculation(dataframe_name = 'life_table_arterial_female',
                event_name = 'Arterial_event',
                subgroup_name = 'Female',
                weeks_since_covid = weeks_since_covid,
                q=return_daily_incidence[[2]],
                HR=HR,
                HR_LB=HR_LB,
                HR_UB=HR_UB,
                covid_infected_population = return_daily_incidence[[3]],
                total_num_rows_for_life_table = max_follow_up_time)

print("additional information:")
print(return_daily_incidence)

#Male
print("Male")

return_daily_incidence<-average_daily_incidence(dataset=data_male, disease_event_date=data_male$OUT_ARTERIAL_EVENT, covid_date=data_male$EXP_CONFIRMED_COVID19_DATE, futime=data_male$futime,
                                                max_follow_up_time=max_follow_up_time)

HR<-c(
  rep(3.5,7),
  rep(5,21),rep(2.1,84),rep(1.3,413),rep(1.00,205))
HR_LB<-c(
  rep(2,7),
  rep(3.7,21),rep(1.6,84),rep(1.03,413),rep(1.00,205)) #HR upper 95% CI
HR_UB<-c(
  rep(6,7),
  rep(6.6,21),rep(2.8,84),rep(1.6,413),rep(1.00,205)) #HR lower 95% CI

AER_calculation(dataframe_name = 'life_table_arterial_male',
                event_name = 'Arterial_event',
                subgroup_name = 'Male',
                weeks_since_covid = weeks_since_covid,
                q=return_daily_incidence[[2]],
                HR=HR,
                HR_LB=HR_LB,
                HR_UB=HR_UB,
                covid_infected_population = return_daily_incidence[[3]],
                total_num_rows_for_life_table = max_follow_up_time)

print("additional information:")
print(return_daily_incidence)


#Young (<60)
print("Young (<60)")

return_daily_incidence<-average_daily_incidence(dataset=data_young, disease_event_date=data_young$OUT_ARTERIAL_EVENT, covid_date=data_young$EXP_CONFIRMED_COVID19_DATE, futime=data_young$futime,
                                                max_follow_up_time=max_follow_up_time)

HR<-c(
  rep(4.09,7),
  rep(11.6,21),rep(2.8,84),rep(1.05,413),rep(1.00,205))
HR_LB<-c(
  rep(1.02,7),
  rep(7.3,21),rep(1.6,84),rep(0.63,413),rep(1.00,205)) #HR upper 95% CI
HR_UB<-c(
  rep(16.4,7),
  rep(18.4,21),rep(4.7,84),rep(1.75,413),rep(1.00,205)) #HR lower 95% CI

AER_calculation(dataframe_name = 'life_table_arterial_young',
                event_name = 'Arterial_event',
                subgroup_name = '<60 years old',
                weeks_since_covid = weeks_since_covid,
                q=return_daily_incidence[[2]],
                HR=HR,
                HR_LB=HR_LB,
                HR_UB=HR_UB,
                covid_infected_population = return_daily_incidence[[3]],
                total_num_rows_for_life_table = max_follow_up_time)

print("additional information:")
print(return_daily_incidence)


#Old (60+)
print("Old (60+)")

return_daily_incidence<-average_daily_incidence(dataset=data_old, disease_event_date=data_old$OUT_ARTERIAL_EVENT, covid_date=data_old$EXP_CONFIRMED_COVID19_DATE, futime=data_old$futime,
                                                max_follow_up_time=max_follow_up_time)

HR<-c(
  rep(3.3,7),
  rep(4.07,21),rep(2.02,84),rep(1.65,413),rep(1.00,205))
HR_LB<-c(
  rep(2.1,7),
  rep(3.2,21),rep(1.6,84),rep(1.41,413),rep(1.00,205)) #HR upper 95% CI
HR_UB<-c(
  rep(5.1,7),
  rep(5.2,21),rep(2.51,84),rep(1.92,413),rep(1.00,205)) #HR lower 95% CI

AER_calculation(dataframe_name = 'life_table_arterial_old',
                event_name = 'Arterial_event',
                subgroup_name = '60+ years old',
                weeks_since_covid = weeks_since_covid,
                q=return_daily_incidence[[2]],
                HR=HR,
                HR_LB=HR_LB,
                HR_UB=HR_UB,
                covid_infected_population = return_daily_incidence[[3]],
                total_num_rows_for_life_table = max_follow_up_time)

print("additional information:")
print(return_daily_incidence)


#4. Plot changes version 2
plot_graph(num_subgroups = 4,#life_table1 = life_table_arterial_hosp, 
           life_table1 = life_table_arterial_female, lifetable2 = life_table_arterial_male, 
           #           lifetable2 = life_table_arterial_non_hosp_young, 
           #            lifetable2 = life_table_arterial_hosp,
           lifetable3 = life_table_arterial_young, 
           #           lifetable4 = life_table_arterial_non_hosp_old, 
           lifetable4 = life_table_arterial_old,
           graph_title = 'Arterial Thrombosis',
           graph_name = 'Arterial_event_graph_COVID2',
           levels = c(#'Hospitalised COVID-19 infection',
                      'Female','Male',
                      #                      'hospitalised COVID-19 infection',
                      #                      'non-hospitalised COVID-19 infection <60 years old',
                      '<60 years old',
                      #                      'non-hospitalised COVID-19 infection 60+ years old',
                      '60+ years old'
           ),
           legend_title = '',
           linetype = c(#'solid',
                        'twodash','dotted',
                        #                        'dashed',
                        'dotdash', 
                        'dotted' 
                        #                        'solid'
           ),
           color_value=c(
             #  'black',
             #'black',
             'springgreen4',
             #                         'blue',
             'blue', 
             'goldenrod', 
             'red'
           ))


sink()
####################################################################################################
#grid.arrange(Venous_event_graph_COVID, Arterial_event_graph_COVID, ncol=2)

Venous_event_graph_COVID2<-Venous_event_graph_COVID2 + theme(axis.title.y = element_blank()) 
Arterial_event_graph_COVID2<-Arterial_event_graph_COVID2 + theme(axis.title.y = element_blank()) 

COVID_plot<-ggarrange(Arterial_event_graph_COVID2, Venous_event_graph_COVID2, common.legend=TRUE, ncol=2, legend="bottom")
ggarrange(Arterial_event_graph_COVID2, Venous_event_graph_COVID2, common.legend=TRUE, ncol=2, legend="bottom")

dev.print(file = "P:/keenes/Projects/CCU002-04/results/COVID_cox_analysis/AER/AER_COVID_main.png", device=png, width = 800, height = 400)