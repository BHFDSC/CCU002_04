--************************************************************************************************
-- Script:       3-8_CCU002-04_COVID_Cohort.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
--               CCU002-04: To compare the long-term risk of stroke/MI in patients after
--               COVID-19 infection with other respiratory infections
-- About:        Create CCU002-04 COVID19 cohort

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2022
-- ***********************************************************************************************
-- Date parameters
CREATE OR REPLACE VARIABLE SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT DATE DEFAULT '2020-01-01';
CREATE OR REPLACE VARIABLE SAILWWMCCV.PARAM_CCU002_04_COVID_END_DT DATE DEFAULT '2021-12-31';
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 (
    alf_e                                    bigint,
    wob                                      date,
    death_date                               date,
    lsoa2011                                 char(10),
    rural_urban                              char(47),
    lost_to_followup                         char(1),
    lost_to_followup_date                    date,
    care_home                                smallint,
    gp_coverage_end_date                     date,
    -- Exposure --------------------------------
    exp_confirmed_covid19_date               date,
    exp_confirmed_covid_phenotype_ccu002_01  char(30),
    coinfection_flu_pneumonia                smallint,
    -- Comparison group ------------------------
    com_flu_pneumonia_infection              smallint,
    com_non_covid_flu_pneumonia_infection    smallint,
    -- Outcomes --------------------------------
    out_ami                                  date,
    out_artery_dissect                       date,
    out_other_arterial_embolism              date,
    out_angina                               date,
    out_unstable_angina                      date,
    out_cardiomyopathy                       date,
    out_dic                                  date,
    out_dvt_icvt                             date,
    out_dvt_pregnancy                        date,
    out_dvt_dvt                              date,
    out_icvt_pregnancy                       date,
    out_other_dvt                            date,
    out_fracture                             date,
    out_hf                                   date,
    out_life_arrhythmia                      date,
    out_mesenteric_thrombus                  date,
    out_pe                                   date,
    out_myocarditis                          date,
    out_pericarditis                         date,
    out_portal_vein_thrombosis               date,
    out_retinal_infarction                   date,
    out_thrombocytopenia                     date,
    out_ttp                                  date,
    out_stroke_sah                           date,
    out_stroke_tia                           date,
    out_stroke_spinal                        date,
    out_stroke_hs                            date,
    out_stroke_is                            date,
    out_stroke_nos                           date,
    out_stroke_isch                          date,
    out_stroke_sah_hs                        date,
    -- Combined outcomes -----------------------
    out_arterial_event                       date, -- Based on CCU002-04 protocol
    out_arterial_event_v2                    date, -- Based on CCU002-01 protocol
    out_venous_event                         date,
    out_haematological_event                 date,
    -- Covariates -------------------------------
    cov_sex                                  char(1),
    cov_age                                  int,
    cov_ethnicity                            char(15),
    cov_deprivation                          char(10), --WIMD2019, 1 = most deprived, 5 = least deprived
    cov_region                               char(50),
    cov_charlson_score_v1                    int,
    cov_charlson_score_v2                    int,
    cov_elixhauser_score                     int,
    cov_smoking_status                       char(20),
    cov_ever_dementia                        smallint,
    cov_ever_liver_disease                   smallint,
    cov_ever_ckd                             smallint,
    cov_ever_cancer                          smallint,
    cov_surgery_lastyr                       smallint,
    cov_ever_hypertension                    smallint,
    cov_ever_diabetes                        smallint,
    cov_ever_obesity                         smallint,
    cov_ever_depression                      smallint,
    cov_ever_copd                            smallint,
    cov_ever_stroke_isch                     smallint,
    cov_ever_stroke_sah_hs                   smallint,
    cov_ever_stroke_tia                      smallint,
    cov_ever_stroke_spinal                   smallint,
    cov_ever_stroke_nos                      smallint,
    cov_ever_stroke_hs                       smallint,
    cov_ever_stroke_sah                      smallint,
    cov_ever_stroke_is                       smallint,
    cov_ever_retinal_infarction              smallint,
    cov_ever_any_stroke                      smallint,
    cov_ever_venous_thrombosis               smallint,
    cov_ever_pe                              smallint,
    cov_ever_pe_vt                           smallint,
    cov_ever_thrombophilia                   smallint,
    cov_ever_ttp                             smallint,
    cov_ever_tcp                             smallint,-- Thrombocytopenia & TTP
    cov_ever_other_arterial_embolism         smallint,
    cov_ever_dic                             smallint,
    cov_ever_mesenteric_thrombus             smallint,
    cov_ever_artery_dissect                  smallint,
    cov_ever_life_arrhythmia                 smallint,
    cov_ever_cardiomyopathy                  smallint,
    cov_ever_ami                             smallint,
    cov_ever_angina                          smallint,
    cov_ever_hf                              smallint,
    cov_ever_vt                              smallint,
    cov_ever_dvt_icvt                        smallint,
    cov_ever_dvt_dvt                         smallint,
    cov_ever_other_dvt                       smallint,
    cov_ever_portal_vein_thrombosis          smallint,
    cov_ever_pericarditis                    smallint,
    cov_ever_myocarditis                     smallint,
    cov_ever_any_cvd                         smallint,
    cov_ever_fracture                        smallint,
    cov_unique_medications                   int,
    cov_antiplatelet_meds                    smallint,
    cov_lipid_meds                           smallint,
    cov_bp_lowering_meds                     smallint,
    cov_anticoagulation_meds                 smallint,
    cov_cocp_meds                            smallint,
    cov_hrt_meds                             smallint,
    cov_covid_vaccination                    smallint,
    cov_covid_vaccination_first_dose_date    date
    )
DISTRIBUTE BY HASH(alf_e);


--DROP TABLE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630;
--TRUNCATE TABLE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 IMMEDIATE;

INSERT INTO SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 (alf_e, cov_sex, wob, death_date, cov_age, lsoa2011, cov_deprivation,
                                                        rural_urban, gp_coverage_end_date, care_home)
    SELECT alf_e,
           gndr_cd,
           wob,
           dod_jl,  -- combined DOD
           der_age_,
           lsoa2011_inception,
           wimd2019_quintile_inception AS cov_deprivation,
           urban_rural_inception,
           gp_coverage_end_date,
           CASE WHEN carehome_ralf_inception IS NOT NULL THEN 1
           ELSE 0 END
    FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
    WHERE covid_cohort = 1;

-- ***********************************************************************************************
-- Determine lost to follow up cases
-------------------------------------------------------------------------------------------------
-- People who died within study period
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630
SET lost_to_followup = 1,
    lost_to_followup_date = death_date
WHERE death_date IS NOT NULL
AND death_date <= SAILWWMCCV.PARAM_CCU002_04_COVID_END_DT;

-- People with no GP registeration
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630
SET lost_to_followup = 1,
    lost_to_followup_date = NULL
WHERE gp_coverage_end_date IS NULL;

-- People whose GP registeration date ends before study start date
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630
SET lost_to_followup = 1,
    lost_to_followup_date = gp_coverage_end_date
WHERE gp_coverage_end_date < SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT;

-- People who moved out of Wales within study period
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630
SET lost_to_followup = 1,
    lost_to_followup_date = gp_coverage_end_date
WHERE gp_coverage_end_date >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT
AND gp_coverage_end_date < (SELECT max(cohort_end_date) FROM SAILWMCCV.C19_COHORT20);


-- ***********************************************************************************************
-- Update covariates using SAILWWMCCV.CCU002_04_COVID_COHORT_COVARIATES
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.cov_region                          = 'Wales',
    tgt.cov_charlson_score_v1               = src.charlson_score_v1,
    tgt.cov_charlson_score_v2               = src.charlson_score_v2,
    tgt.cov_elixhauser_score                = src.elixhauser_score,
    tgt.cov_smoking_status                  = CASE WHEN src.smoking_status IS NULL THEN 'Missing' ELSE src.smoking_status END,
    tgt.cov_ever_dementia                   = CASE WHEN src.dementia = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_liver_disease              = CASE WHEN src.liver_disease = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_ckd                        = CASE WHEN src.ckd = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_cancer                     = CASE WHEN src.cancer = 1 THEN 1 ELSE 0 END,
    tgt.cov_surgery_lastyr                  = CASE WHEN src.surgery = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_hypertension               = CASE WHEN src.hypertension_diag = 1 OR src.hypertension_medication = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_diabetes                   = CASE WHEN src.diabetes_medication = 1 OR src.diabetes_diag = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_obesity                    = CASE WHEN src.obesity = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_depression                 = CASE WHEN src.depression  = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_copd                       = CASE WHEN src.copd = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_stroke_isch                = CASE WHEN src.stroke_isch  = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_stroke_sah_hs              = CASE WHEN src.stroke_sah_hs  = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_stroke_tia                 = CASE WHEN src.stroke_tia = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_stroke_spinal              = CASE WHEN src.stroke_spinal = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_stroke_nos                 = CASE WHEN src.stroke_nos = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_stroke_hs                  = CASE WHEN src.stroke_hs = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_stroke_sah                 = CASE WHEN src.stroke_sah = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_stroke_is                  = CASE WHEN src.stroke_is = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_retinal_infarction         = CASE WHEN src.retinal_infarction  = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_any_stroke                 = CASE WHEN src.any_stroke = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_venous_thrombosis          = CASE WHEN src.venous_thrombosis  = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_pe                         = CASE WHEN src.pe = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_pe_vt                      = CASE WHEN src.pe_vt = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_thrombophilia              = CASE WHEN src.thrombophilia = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_ttp                        = CASE WHEN src.ttp = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_tcp                        = CASE WHEN src.tcp = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_other_arterial_embolism    = CASE WHEN src.other_arterial_embolism = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_dic                        = CASE WHEN src.dic = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_mesenteric_thrombus        = CASE WHEN src.mesenteric_thrombus = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_artery_dissect             = CASE WHEN src.artery_dissect = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_life_arrhythmia            = CASE WHEN src.life_arrhythmia = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_cardiomyopathy             = CASE WHEN src.cardiomyopathy = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_ami                        = CASE WHEN src.ami = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_angina                     = CASE WHEN src.angina = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_hf                         = CASE WHEN src.hf = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_vt                         = CASE WHEN src.vt = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_dvt_icvt                   = CASE WHEN src.dvt_icvt = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_dvt_dvt                    = CASE WHEN src.dvt_dvt = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_other_dvt                  = CASE WHEN src.other_dvt = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_portal_vein_thrombosis     = CASE WHEN src.portal_vein_thrombosis = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_pericarditis               = CASE WHEN src.pericarditis = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_myocarditis                = CASE WHEN src.myocarditis = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_any_cvd                    = CASE WHEN src.any_cvd = 1 THEN 1 ELSE 0 END,
    tgt.cov_ever_fracture                   = CASE WHEN src.fracture = 1 THEN 1 ELSE 0 END,
    tgt.cov_unique_medications              = CASE WHEN src.unique_medications >= 0 THEN src.unique_medications ELSE 0 END,
    tgt.cov_antiplatelet_meds               = CASE WHEN src.antiplatelet = 1 THEN 1 ELSE 0 END,
    tgt.cov_lipid_meds                      = CASE WHEN src.lipid_lowering = 1 THEN 1 ELSE 0 END,
    tgt.cov_bp_lowering_meds                = CASE WHEN src.bp_lowering = 1 THEN 1 ELSE 0 END,
    tgt.cov_anticoagulation_meds            = CASE WHEN src.anticoagulant = 1 THEN 1 ELSE 0 END,
    tgt.cov_cocp_meds                       = CASE WHEN src.cocp = 1 THEN 1 ELSE 0 END,
    tgt.cov_hrt_meds                        = CASE WHEN src.hrt = 1 THEN 1 ELSE 0 END
FROM SAILWWMCCV.CCU002_04_COVID_COHORT_COVARIATES_20220329 src
WHERE tgt.alf_e = src.alf_e;

-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630
SET cov_ever_pe_vt = 1
WHERE cov_ever_dvt_icvt = 1 OR cov_ever_pe_vt = 1;

-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.cov_covid_vaccination = src.covid_vaccination,
    tgt.cov_covid_vaccination_first_dose_date = src.covid_vaccination_first_dose_date
FROM SAILWWMCCV.CCU002_04_COVID_COHORT_COVARIATES_20220329 src
WHERE tgt.alf_e = src.alf_e;

-- ***********************************************************************************************
-- Add ethnicity using SAILWWMCCV.WMCC_COMB_ETHN_EHRD
-- ONS categories ('White', 'Mixed', 'Asian', 'Black', 'Other')
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.cov_ethnicity = src.ec_ons_desc
FROM SAILWWMCCV.WMCC_COMB_ETHN_EHRD src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630
SET cov_ethnicity = 'Missing'
WHERE cov_ethnicity IS NULL;

-- ***********************************************************************************************
-- Update exposure using SAILWWMCCV.CCU002_04_COVID19_HOSPITALISATION
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.exp_confirmed_covid19_date = src.covid19_confirmed_date,
    tgt.exp_confirmed_covid_phenotype_ccu002_01 = src.covid19_hospitalisation_ccu002_01
FROM SAILWWMCCV.CCU002_04_COVID19_HOSPITALISATION_20220630 src
WHERE tgt.alf_e = src.alf_e;


-- ***********************************************************************************************
-- Determine comparison group
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET com_flu_pneumonia_infection = 1
WHERE alf_e IN (-- PEDW
                SELECT alf_e
                FROM SAILWWMCCV.PHEN_PEDW_RESPIRATORY_INFECTIONS
                WHERE icd10_cd_category IN ('Viral influenza', 'Viral pneumonia', 'Bacterial pneumonia')
                AND admis_dt >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT
                AND admis_dt <= SAILWWMCCV.PARAM_CCU002_04_COVID_END_DT
                UNION ALL
                -- ICNARC
                SELECT alf_e
                FROM SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF a
                INNER JOIN SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE l
                ON a.system_id_e = l.system_id_e
                WHERE (raicu1_text LIKE '%pneumonia%' OR raicu2_text LIKE '%pneumonia%' OR uraicu_text LIKE '%pneumonia%' OR
                       raicu1_text LIKE '%influenza%' OR raicu2_text LIKE '%influenza%' OR uraicu_text LIKE '%influenza%'
                       )
                AND daicu >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT
                AND daicu <= SAILWWMCCV.PARAM_CCU002_04_COVID_END_DT
                UNION ALL
                -- WLGP
                SELECT alf_e
                FROM SAILWWMCCV.PHEN_WLGP_RESPIRATORY_INFECTIONS
                WHERE event_dt >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT
                AND event_dt <= SAILWWMCCV.PARAM_CCU002_04_COVID_END_DT
                UNION ALL
                -- WRRS
                SELECT alf_e
                FROM SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS
                WHERE test_code IN ('FLUAPCR',               -- 'Influenza A PCR'
                                    'FLUBPCR',               -- 'Influenza B PCR'
                                    'Mycoplasma pneumonia',  -- 'Mycoplasma pneumoniae'
                                    'MPCF',                  -- 'Mycoplasma pneumoniae'
                                    'Streptococcus pneumo'  -- 'Streptococcus pneumoniae,
                                    )
                 AND test_collected_date >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT
                 AND test_collected_date <= SAILWWMCCV.PARAM_CCU002_04_COVID_END_DT
                );


UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630
SET com_non_covid_flu_pneumonia_infection = 1
WHERE exp_confirmed_covid19_date IS NULL AND com_flu_pneumonia_infection IS NULL;

-- ***********************************************************************************************
-- Update outcomes using SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_ami = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'AMI') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_artery_dissect = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'ARTERY_DISSECT') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_other_arterial_embolism = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'ARTERIAL_EMBOLISM_OTHR') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_angina = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'ANGINA') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_unstable_angina = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'UNSTABLE_ANGINA') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_cardiomyopathy = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'CARDIOMYOPATHY') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_dic = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'DIC') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_icvt_pregnancy = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'ICVT_PREGNANCY') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_dvt_icvt = src.record_date
FROM (SELECT alf_e,
             min(record_date) AS record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name IN ('DVT_ICVT', 'ICVT_PREGNANCY')
      GROUP BY alf_e) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_dvt_pregnancy = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'DVT_PREGNANCY') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_dvt_dvt = src.record_date
FROM (SELECT alf_e,
             min(record_date) AS record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name IN ('DVT_DVT', 'DVT_PREGNANCY')
      GROUP BY alf_e) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_other_dvt = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'OTHER_DVT') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_fracture = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'FRACTURE') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_hf = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'HF') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_life_arrhythmia = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'LIFE_ARRHYTHM') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_mesenteric_thrombus = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'MESENTERIC_THROMBUS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_myocarditis = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'MYOCARDITIS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_pe = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'PE') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_pericarditis = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'PERICARDITIS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_portal_vein_thrombosis = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'PORTAL_VEIN_THROMBOSIS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_retinal_infarction = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'RETINAL_INFARCTION') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_thrombocytopenia = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'THROMBOCYTOPENIA') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_ttp = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'TTP') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_stroke_sah = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_SAH') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_stroke_tia = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_TIA') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_stroke_spinal = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_SPINAL') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_stroke_hs = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_HS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_stroke_is = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_IS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_stroke_nos = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_NOS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_stroke_isch = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_ISCH') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_stroke_sah_hs = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_SAH_HS') src
WHERE tgt.alf_e = src.alf_e;

-- ***********************************************************************************************
-- Update grouped outcomes
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_arterial_event = src.out_arterial_event
FROM (SELECT alf_e,
             min(record_date) AS out_arterial_event
      FROM (SELECT alf_e,
                   record_date
            FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
            WHERE name IN ('AMI','STROKE_ISCH','ARTERIAL_EMBOLISM_OTHR')
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_venous_event = src.out_venous_event
FROM (SELECT alf_e,
             min(record_date) AS out_venous_event
      FROM (SELECT alf_e,
                   record_date
            FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
            WHERE name IN ('PE', 'OTHER_DVT', 'DVT_ICVT', 'DVT_PREGNANCY', 'DVT_DVT',
                           'ICVT_PREGNANCY','PORTAL_VEIN_THROMBOSIS')
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_haematological_event = src.out_haematological_event
FROM (SELECT alf_e,
             min(record_date) AS out_haematological_event
      FROM (SELECT alf_e,
                   record_date
            FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
            WHERE name IN ('TTP', 'THROMBOCYTOPENIA')
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;

--***********************************************************************************************
-- Determine co-infection of another respiratory infection within 2 weeks before or after exposure
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630
SET coinfection_flu_pneumonia = 1
WHERE alf_e IN
    (SELECT c.alf_e
     FROM SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 c
     JOIN (-- PEDW
           SELECT alf_e,
                  admis_dt AS record_date
           FROM SAILWWMCCV.PHEN_PEDW_RESPIRATORY_INFECTIONS
           WHERE icd10_cd_category IN ('Viral influenza', 'Viral pneumonia', 'Bacterial pneumonia')
           AND admis_dt >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT - 14 DAY
           UNION ALL
           -- ICNARC
           SELECT alf_e,
                  daicu AS record_date
           FROM SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF a
           INNER JOIN SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE l
           ON a.system_id_e = l.system_id_e
           WHERE (raicu1_text LIKE '%pneumonia%' OR raicu2_text LIKE '%pneumonia%' OR uraicu_text LIKE '%pneumonia%' OR
                  raicu1_text LIKE '%influenza%' OR raicu2_text LIKE '%influenza%' OR uraicu_text LIKE '%influenza%'
                  )
           AND daicu >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT - 14 DAY
           UNION ALL
           -- WLGP
           SELECT alf_e,
                  event_dt AS record_date
           FROM SAILWWMCCV.PHEN_WLGP_RESPIRATORY_INFECTIONS
           WHERE event_dt >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT - 14 DAY
           UNION ALL
           -- WRRS
           SELECT alf_e,
                  test_collected_date AS record_date
           FROM SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS
           WHERE test_code IN ('FLUAPCR',              -- 'Influenza A PCR'
                               'FLUBPCR',              -- 'Influenza B PCR'
                               'Mycoplasma pneumonia', -- 'Mycoplasma pneumoniae'
                               'MPCF',                 -- 'Mycoplasma pneumoniae'
                               'Streptococcus pneumo'  -- 'Streptococcus pneumoniae
                                )
           AND test_collected_date >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT - 14 DAY
           ) f
     ON c.alf_e = f.alf_e
     WHERE f.record_date >= exp_confirmed_covid19_date - 14 DAY
     AND f.record_date <= exp_confirmed_covid19_date + 14 DAY
     );
-- ***********************************************************************************************
-- Number of Disorders (using Charlson codelist)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.cov_n_disorder = src.n_disorder
FROM (SELECT alf_e,
             count(DISTINCT event_cd) AS n_disorder
      FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
      WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_CHARLSON)
      AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT - 3 month
      AND event_dt < SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT
      GROUP BY alf_e) src
WHERE tgt.alf_e = src.alf_e


UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.cov_n_disorder = 0
WHERE cov_n_disorder IS NULL AND gp_coverage_end_date IS NOT NULL;


-- ***********************************************************************************************
-- Apply further exclusions
-------------------------------------------------------------------------------------------------
DELETE FROM SAILWWMCCV.CCU002_04_COVID_COHORT_20220630
WHERE cov_age >= 110
OR death_date < exp_confirmed_covid19_date
OR (cov_sex=1 AND cov_cocp_meds=1)
OR (cov_sex=1 AND cov_hrt_meds=1);


-------------------------------------------------------------------------------------------------
-- Add arterial events flag based on CCU002-01 definition
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.out_arterial_event_v2 = src.out_arterial_event_v2
FROM (SELECT alf_e,
             min(record_date) AS out_arterial_event_v2
      FROM (SELECT alf_e,
                   record_date
            FROM SAILWWMCCV.CCU002_04_COVID_COHORT_CVD_OUTCOMES_FIRST_20220329
            WHERE name IN ('AMI','STROKE_ISCH','ARTERIAL_EMBOLISM_OTHR')
            AND code NOT LIKE 'G45%' -- 'STROKE_TIA'
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;


-------------------------------------------------------------------------------------------------
-- Add covid vaccination info
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.cov_covid_vaccination = src.covid_vaccination,
    tgt.cov_covid_vaccination_first_dose_date = src.covid_vaccination_first_dose_date
FROM SAILWWMCCV.CCU002_04_COVID_COHORT_COVARIATES_20220329 src
WHERE tgt.alf_e = src.alf_e;


-- ***********************************************************************************************
-- Add new variables: ICU admission and respiratory support (invasive/non-invasive ventilation or ECMO)
-- Only for COVID admissions
-------------------------------------------------------------------------------------------------
ALTER TABLE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 ADD icu_admis char(1) NULL; -- within 28 days of infection
ALTER TABLE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 ADD icu_admis_dt date NULL;

ALTER TABLE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 ADD pedw_resp_support char(1) NULL;-- IMV, NIV, ECMO
ALTER TABLE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 ADD icnarc_ccds_resp_support char(1) NULL;
ALTER TABLE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 ADD icu_or_resp_support char(1) NULL;

-------------------------------------------------------------------------------------------------
-- ICU/CCU admission from ICNARC or CCDS (only for COVID admissions)
-------------------------------------------------------------------------------------------------
MERGE INTO SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
USING (SELECT alf_e, min(icu_admis_dt) AS icu_admis_dt
       FROM (SELECT c.alf_e, daicu AS icu_admis_dt
             FROM SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 c
             INNER JOIN SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF a
             ON c.alf_e = a.alf_e
             INNER JOIN SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE l
             ON a.system_id_e = l.system_id_e
             WHERE a.alf_e IS NOT NULL
             AND daicu - exp_confirmed_covid19_date <= 28
             AND TIMESTAMPDIFF(16,TIMESTAMP(daicu) - TIMESTAMP(exp_confirmed_covid19_date)) <= 28
             AND TIMESTAMPDIFF(16,TIMESTAMP(daicu) - TIMESTAMP(exp_confirmed_covid19_date)) >= 0
             UNION
             SELECT c.alf_e, date(a.admis_dt) AS icu_admis_dt
             FROM SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 c
             INNER JOIN SAILWMCCV.C19_COHORT_CCDS_CRITICAL_CARE_EPISODE a
             ON c.alf_e = a.alf_e
             WHERE a.alf_e IS NOT NULL
             AND TIMESTAMPDIFF(16,TIMESTAMP(a.admis_dt) - TIMESTAMP(exp_confirmed_covid19_date)) <= 28
             AND TIMESTAMPDIFF(16,TIMESTAMP(a.admis_dt) - TIMESTAMP(exp_confirmed_covid19_date)) >= 0
             )
      GROUP BY alf_e
      ) AS src
ON tgt.alf_e = src.alf_e
WHEN MATCHED AND exp_confirmed_covid_phenotype_ccu002_01 = 'hospitalised' THEN UPDATE
SET tgt.icu_admis = 1,
    tgt.icu_admis_dt = src.icu_admis_dt
;

-------------------------------------------------------------------------------------------------
-- Respiratory supprt recorded in PEDW (IMV, NIV, ECMO)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.pedw_resp_support = 1
WHERE exp_confirmed_covid_phenotype_ccu002_01 = 'hospitalised'
AND alf_e IN (SELECT c.alf_e
              FROM SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 c
              INNER JOIN SAILWWMCCV.WMCC_PEDW_SPELL s
              ON c.alf_e = s.alf_e
              INNER JOIN SAILWWMCCV.WMCC_PEDW_EPISODE e
              ON s.prov_unit_cd = e.prov_unit_cd
              AND s.spell_num_e = e.spell_num_e
              INNER JOIN SAILWWMCCV.WMCC_PEDW_OPER o
              ON e.prov_unit_cd = o.prov_unit_cd
              AND e.spell_num_e = o.spell_num_e
              AND e.spell_num_e = o.spell_num_e
              INNER JOIN SAILWWMCCV.PHEN_OPCS_VENTILATION v
              ON o.oper_cd = v.code OR LEFT(o.oper_cd,3) = v.code
              WHERE TIMESTAMPDIFF(16,TIMESTAMP(admis_dt) - TIMESTAMP(exp_confirmed_covid19_date)) <= 28
              AND TIMESTAMPDIFF(16,TIMESTAMP(admis_dt) - TIMESTAMP(exp_confirmed_covid19_date)) >= 0
              )
;
SELECT * FROM SAILWWMCCV.PHEN_OPCS_VENTILATION;
-------------------------------------------------------------------------------------------------
--- Respiratory supprt recorded in ICNARC
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.icnarc_ccds_resp_support = 1
WHERE exp_confirmed_covid_phenotype_ccu002_01 = 'hospitalised'
AND alf_e IN (SELECT c.alf_e
              FROM SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 c
              INNER JOIN SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF a
              ON c.alf_e = a.alf_e
              INNER JOIN SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE l
              ON a.system_id_e = l.system_id_e
              WHERE (arsd >= 1 OR vent = 1 OR brsd >= 1)
              AND TIMESTAMPDIFF(16,TIMESTAMP(daicu) - TIMESTAMP(exp_confirmed_covid19_date)) <= 28
              AND TIMESTAMPDIFF(16,TIMESTAMP(daicu) - TIMESTAMP(exp_confirmed_covid19_date)) >= 0
              )
;

-------------------------------------------------------------------------------------------------
-- ICU admission or any respiratory support
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_COVID_COHORT_20220630 tgt
SET tgt.icu_or_resp_support = 1
WHERE icnarc_ccds_resp_support = 1 OR pedw_resp_support = 1 OR icu_admis;

