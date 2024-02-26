# ******************************************************************************
# Script:       4-1_CCU002-04_Analysis_Retrieve_Data.R
# SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK
#               CCU002-04: To compare the long-term risk of stroke/MI in patients
#               after COVID-19 infection with other respiratory infections
# About:        Load CCU002-04 data

# Author:       Hoda Abbasizanjani
#               Health Data Research UK, Swansea University, 2022
# ******************************************************************************
rm(list = ls())

# Setup DB2 connection ---------------------------------------------------------
library(RODBC)
source("S:/WMCC - WMCC - Wales Multi-morbidity Cardiovascular Covid-19 UK (0911)/CCU002/CCU002-04_Hoda/R_login/login_box.r");
login = getLogin();
sql = odbcConnect('PR_SAIL',login[1],login[2]);
login = 0

# Load COVID cohort ------------------------------------------------------------
 df <- sqlQuery(sql,"SELECT ALF_E,
 --                           WOB,
                            DEATH_DATE,
 --                           LSOA2011,
                            RURAL_URBAN,
                            LOST_TO_FOLLOWUP,
                            LOST_TO_FOLLOWUP_DATE,
                             CARE_HOME,
                            GP_COVERAGE_END_DATE,
                            -- EXPOSURE --------------------------------
                            EXP_CONFIRMED_COVID19_DATE,
                            EXP_CONFIRMED_COVID_PHENOTYPE_CCU002_01 as EXP_CONFIRMED_COVID_PHENOTYPE,
                            COINFECTION_FLU_PNEUMONIA,
                            -- COMPARISON GROUP ------------------------
                            COM_FLU_PNEUMONIA_INFECTION,
                            COM_NON_COVID_FLU_PNEUMONIA_INFECTION,
                            -- OUTCOMES --------------------------------
                            OUT_AMI,
                            OUT_ARTERY_DISSECT,
                            OUT_OTHER_ARTERIAL_EMBOLISM,
 --                            OUT_ANGINA,
 --                            OUT_UNSTABLE_ANGINA,
                            OUT_CARDIOMYOPATHY ,
 --                            OUT_DIC,
                            OUT_DVT_ICVT,
 --                            OUT_DVT_PREGNANCY,
                            OUT_DVT_DVT,
 --                            OUT_ICVT_PREGNANCY,
                            OUT_OTHER_DVT,
                            OUT_FRACTURE,
                            OUT_HF,
                            OUT_LIFE_ARRHYTHMIA,
                            OUT_MESENTERIC_THROMBUS,
                            OUT_MYOCARDITIS,
                            OUT_PE,
                            OUT_PERICARDITIS,
                            OUT_PORTAL_VEIN_THROMBOSIS,
 --                            OUT_RETINAL_INFARCTION,
                            OUT_THROMBOCYTOPENIA,
                            OUT_TTP,
 --                            OUT_STROKE_SAH,
 --                            OUT_STROKE_TIA,
 --                            OUT_STROKE_SPINAL,
 --                            OUT_STROKE_HS,
 --                            OUT_STROKE_IS,
 --                            OUT_STROKE_NOS,
                            OUT_STROKE_ISCH,
                            OUT_STROKE_SAH_HS,
                            -- COMBINED OUTCOMES -----------------------
 --                           OUT_ARTERIAL_EVENT,
                           OUT_ARTERIAL_EVENT,      -- based on CCU002-04 definition
 --                          OUT_ARTERIAL_EVENT_V2,    -- based on CCU002-01 definition
                            OUT_VENOUS_EVENT,
                            OUT_HAEMATOLOGICAL_EVENT,
                            -- COVARIATES -------------------------------
                            COV_SEX,
                            COV_AGE,
                            COV_ETHNICITY,
                            COV_DEPRIVATION, --WIMD2019, 1 = MOST DEPRIVED, 5 = LEAST DEPRIVED
                            COV_REGION,
 --                            COV_CHARLSON_SCORE_V1,
 --                            COV_CHARLSON_SCORE_V2,
                            COV_ELIXHAUSER_SCORE,
                            COV_SMOKING_STATUS,
                            COV_EVER_DEMENTIA,
--                          COV_EVER_LIVER_DISEASE,
                            COV_EVER_CKD,
                            COV_EVER_CANCER,
                            COV_SURGERY_LASTYR,
                            COV_EVER_HYPERTENSION,
                            COV_EVER_DIABETES,
                            COV_EVER_OBESITY,
                            COV_EVER_DEPRESSION,
                            COV_EVER_COPD,
--                            COV_EVER_STROKE_ISCH,
--                            COV_EVER_STROKE_SAH_HS,
 --                            COV_EVER_STROKE_TIA,
 --                            COV_EVER_STROKE_SPINAL,
 --                            COV_EVER_STROKE_NOS,
 --                            COV_EVER_STROKE_HS,
 --                            COV_EVER_STROKE_SAH,
 --                            COV_EVER_STROKE_IS,
 --                            COV_EVER_RETINAL_INFARCTION,
 --                           COV_EVER_ANY_STROKE,
 --                            COV_EVER_VENOUS_THROMBOSIS,
 --                            COV_EVER_PE,
 --                           COV_EVER_PE_VT,
 --                            COV_EVER_THROMBOPHILIA,
 --                            COV_EVER_TTP,
 --                           COV_EVER_TCP,-- THROMBOCYTOPENIA & TTP
--                            COV_EVER_OTHER_ARTERIAL_EMBOLISM,
 --                            COV_EVER_DIC,
 --                           COV_EVER_MESENTERIC_THROMBUS,
--                            COV_EVER_ARTERY_DISSECT,
--                            COV_EVER_LIFE_ARRHYTHMIA,
--                            COV_EVER_CARDIOMYOPATHY,
--                            COV_EVER_AMI,
--                            COV_EVER_ANGINA,
--                            COV_EVER_HF,
 --                            COV_EVER_VT,
--                            COV_EVER_DVT_ICVT,
--                            COV_EVER_DVT_DVT,
--                            COV_EVER_OTHER_DVT,
--                            COV_EVER_PORTAL_VEIN_THROMBOSIS,
--                            COV_EVER_PERICARDITIS,
--                            COV_EVER_MYOCARDITIS,
                            COV_EVER_FRACTURE,
                            COV_EVER_ANY_CVD,
                            COV_UNIQUE_MEDICATIONS,
                            COV_ANTIPLATELET_MEDS,
                            COV_LIPID_MEDS,
                            COV_BP_LOWERING_MEDS,
                            COV_ANTICOAGULATION_MEDS,
--                            COV_COCP_MEDS,
--                            COV_HRT_MEDS,
                            COV_N_DISORDER,
                            COV_COVID_VACCINATION,
 						               COV_COVID_VACCINATION_FIRST_DOSE_DATE,
 						                ICU_OR_RESP_SUPPORT
                     FROM SAILWWMCCV.CCU002_04_COVID_COHORT_20220630")



 data.table::fwrite(df,"P:/keenes/Projects/CCU002-04/raw/CCU002_04_COVID_COHORT_20220630.csv")

df <- data.table::fread("P:/keenes/Projects/CCU002-04/raw/CCU002_04_COVID_COHORT_20220630.csv",
                        data.table = FALSE)

#P:/keenes/Projects/CCU002-04/raw
# Load flu cohort --------------------------------------------------------------
df2<- sqlQuery(sql,"SELECT ALF_E,
--                           WOB,
                           DEATH_DATE,
--                           LSOA2011,
                           RURAL_URBAN,
                           LOST_TO_FOLLOWUP,
                           LOST_TO_FOLLOWUP_DATE,
                           CARE_HOME,
                           GP_COVERAGE_END_DATE,
                           -- EXPOSURE --------------------------------
                           EXP_CONFIRMED_FLU_PNEUMONIA_DATE,
                           EXP_CONFIRMED_FLU_PNEUMONIA_PHENOTYPE,
                           EXP_FLU,
                           EXP_PNEUMONIA,
                           EXP1_PNEUMONIA_DATE, EXP1_PNEUMONIA_PHENOTYPE,
                           EXP1_PNEUMONIA_DATE_v2, EXP1_PNEUMONIA_PHENOTYPE_v2,
                           EXP2_FLU_DATE, EXP2_FLU_PHENOTYPE,
                           EXP3_BOTH_DATE, EXP3_BOTH_PHENOTYPE,
                           EXP4_BOTH_AND_OTHER_INFECTIONS_DATE, EXP4_BOTH_AND_OTHER_INFECTIONS_PHENOTYPE,
                           -- COMPARISON GROUP ------------------------
                           COM_NON_FLU_PNEUMONIA_INFECTION,
                           -- OUTCOMES --------------------------------
                           OUT_AMI,
                           OUT_ARTERY_DISSECT,
                           OUT_OTHER_ARTERIAL_EMBOLISM,
--                           OUT_ANGINA,
--                           OUT_UNSTABLE_ANGINA,
                           OUT_CARDIOMYOPATHY ,
--                           OUT_DIC,
                           OUT_DVT_ICVT,
--                           OUT_DVT_PREGNANCY,
                           OUT_DVT_DVT,
--                           OUT_ICVT_PREGNANCY,
                           OUT_OTHER_DVT,
                           OUT_FRACTURE,
                           OUT_HF,
                           OUT_LIFE_ARRHYTHMIA,
                           OUT_MESENTERIC_THROMBUS,
                           OUT_MYOCARDITIS,
                           OUT_PE,
                           OUT_PERICARDITIS,
                           OUT_PORTAL_VEIN_THROMBOSIS,
--                           OUT_RETINAL_INFARCTION,
                           OUT_THROMBOCYTOPENIA,
                           OUT_TTP,
--                           OUT_STROKE_SAH,
--                           OUT_STROKE_TIA,
--                           OUT_STROKE_SPINAL,
--                           OUT_STROKE_HS,
--                           OUT_STROKE_IS,
--                           OUT_STROKE_NOS,
                           OUT_STROKE_ISCH,
                           OUT_STROKE_SAH_HS,
                           -- COMBINED OUTCOMES -----------------------
                           OUT_ARTERIAL_EVENT,
                           OUT_VENOUS_EVENT,
                           OUT_HAEMATOLOGICAL_EVENT,
                           -- COVARIATES -------------------------------
                           COV_SEX,
                           COV_AGE,
                           COV_ETHNICITY,
                           COV_DEPRIVATION, --WIMD2019, 1 = MOST DEPRIVED, 5 = LEAST DEPRIVED
                           COV_REGION,
--                           COV_CHARLSON_SCORE_V1,
--                           COV_CHARLSON_SCORE_V2,
                           COV_ELIXHAUSER_SCORE,
                           COV_SMOKING_STATUS,
                           COV_EVER_DEMENTIA,
--                           COV_EVER_LIVER_DISEASE,
                           COV_EVER_CKD,
                           COV_EVER_CANCER,
                           COV_SURGERY_LASTYR,
                           COV_EVER_HYPERTENSION,
                           COV_EVER_DIABETES,
                           COV_EVER_OBESITY,
                           COV_EVER_DEPRESSION,
                           COV_EVER_COPD,
--                           COV_EVER_STROKE_ISCH,
--                           COV_EVER_STROKE_SAH_HS,
--                          COV_EVER_STROKE_TIA,
--                           COV_EVER_STROKE_SPINAL,
--                           COV_EVER_STROKE_NOS,
--                           COV_EVER_STROKE_HS,
--                           COV_EVER_STROKE_SAH,
--                           COV_EVER_STROKE_IS,
--                           COV_EVER_RETINAL_INFARCTION,
--                           COV_EVER_ANY_STROKE,
--                           COV_EVER_VENOUS_THROMBOSIS,
--                           COV_EVER_PE,
--                           COV_EVER_PE_VT,
--                           COV_EVER_THROMBOPHILIA,
--                           COV_EVER_TTP,
--                           COV_EVER_TCP,-- THROMBOCYTOPENIA & TTP
--                           COV_EVER_OTHER_ARTERIAL_EMBOLISM,
--                           COV_EVER_DIC,
--                           COV_EVER_MESENTERIC_THROMBUS,
--                           COV_EVER_ARTERY_DISSECT,
--                           COV_EVER_LIFE_ARRHYTHMIA,
--                           COV_EVER_CARDIOMYOPATHY,
--                           COV_EVER_AMI,
--                           COV_EVER_ANGINA,
--                           COV_EVER_HF,
--                           COV_EVER_VT,
--                           COV_EVER_DVT_ICVT,
--                           COV_EVER_DVT_DVT,
--                           COV_EVER_OTHER_DVT,
--                           COV_EVER_PORTAL_VEIN_THROMBOSIS,
--                           COV_EVER_PERICARDITIS,
--                           COV_EVER_MYOCARDITIS,
                           COV_EVER_FRACTURE,
                           COV_EVER_ANY_CVD,
                           COV_UNIQUE_MEDICATIONS,
                           COV_ANTIPLATELET_MEDS,
                           COV_LIPID_MEDS,
                           COV_BP_LOWERING_MEDS,
                           COV_ANTICOAGULATION_MEDS,
--                           COV_COCP_MEDS,
--                           COV_HRT_MEDS,
                           COV_N_DISORDER,
                           COV_FLU_PNEUMONIA_INFECTION,
                           COV_FLU_PNEUMONIA_HOSPITALISATION,
                           COV_FLU_PNEUMONIA_VACCINATION,
                            ICU_OR_RESP_SUPPORT
                    FROM SAILWWMCCV.CCU002_04_FLU_COHORT_20230322")

data.table::fwrite(df2,"P:/keenes/Projects/CCU002-04/raw/CCU002_04_FLU_COHORT_20230322.csv")

#df2 <- data.table::fread("P:/keenes/Projects/CCU002-04/raw/CCU002_04_FLU_COHORT_20220329.csv",
#                         data.table = FALSE)




