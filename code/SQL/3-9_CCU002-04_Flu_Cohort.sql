--************************************************************************************************
-- Script:       3-9_CCU002-04_Flu_Cohort.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
--               CCU002-04: To compare the long-term risk of stroke/MI in patients after
--               COVID-19 infection with other respiratory infections
-- About:        Create CCU002-04 flu cohort

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2022
-- ***********************************************************************************************
-- Date parameters
CREATE OR REPLACE VARIABLE SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT DATE DEFAULT '2016-01-01';
CREATE OR REPLACE VARIABLE SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT DATE DEFAULT '2019-12-31';

-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 (
    alf_e                                  bigint,
    wob                                    date,
    death_date                             date,
    lsoa2011                               char(10),
    rural_urban                            char(47),
    lost_to_followup                       char(1),
    lost_to_followup_date                  date,
    care_home                              smallint,
    gp_coverage_end_date                   date,
    -- Exposure --------------------------------
    exp_confirmed_flu_pneumonia_date       date,
    exp_confirmed_flu_pneumonia_phenotype  char(30),
    -- Comparison group ------------------------
    com_non_flu_pneumonia_infection        smallint,
    -- Outcomes --------------------------------
    out_ami                                date,
    out_artery_dissect                     date,
    out_other_arterial_embolism            date,
    out_angina                             date,
    out_unstable_angina                    date,
    out_cardiomyopathy                     date,
    out_dic                                date,
    out_dvt_icvt                           date,
    out_dvt_pregnancy                      date,
    out_dvt_dvt                            date,
    out_icvt_pregnancy                     date,
    out_other_dvt                          date,
    out_fracture                           date,
    out_hf                                 date,
    out_life_arrhythmia                    date,
    out_mesenteric_thrombus                date,
    out_myocarditis                        date,
    out_pe                                 date,
    out_pericarditis                       date,
    out_portal_vein_thrombosis             date,
    out_retinal_infarction                 date,
    out_thrombocytopenia                   date,
    out_ttp                                date,
    out_stroke_sah                         date,
    out_stroke_tia                         date,
    out_stroke_spinal                      date,
    out_stroke_hs                          date,
    out_stroke_is                          date,
    out_stroke_nos                         date,
    out_stroke_isch                        date,
    out_stroke_sah_hs                      date,
    -- Combined outcomes -----------------------
    out_arterial_event                     date,
    out_venous_event                       date,
    out_haematological_event               date,
    -- Covariates -------------------------------
    cov_sex                                char(1),
    cov_age                                int,
    cov_ethnicity                          char(15),
    cov_deprivation                        char(10), --WIMD2019, 1 = most deprived, 5 = least deprived
    cov_region                             char(50),
    cov_charlson_score_v1                  int,
    cov_charlson_score_v2                  int,
    cov_elixhauser_score                   int,
    cov_smoking_status                     char(20),
    cov_ever_dementia                      smallint,
    cov_ever_liver_disease                 smallint,
    cov_ever_ckd                           smallint,
    cov_ever_cancer                        smallint,
    cov_surgery_lastyr                     smallint,
    cov_ever_hypertension                  smallint,
    cov_ever_diabetes                      smallint,
    cov_ever_obesity                       smallint,
    cov_ever_depression                    smallint,
    cov_ever_copd                          smallint,
    cov_ever_stroke_isch                   smallint,
    cov_ever_stroke_sah_hs                 smallint,
    cov_ever_stroke_tia                    smallint,
    cov_ever_stroke_spinal                 smallint,
    cov_ever_stroke_nos                    smallint,
    cov_ever_stroke_hs                     smallint,
    cov_ever_stroke_sah                    smallint,
    cov_ever_stroke_is                     smallint,
    cov_ever_retinal_infarction            smallint,
    cov_ever_any_stroke                    smallint,
    cov_ever_venous_thrombosis             smallint,
    cov_ever_pe                            smallint,
    cov_ever_pe_vt                         smallint,
    cov_ever_thrombophilia                 smallint,
    cov_ever_ttp                           smallint,
    cov_ever_tcp                           smallint,-- Thrombocytopenia & TTP
    cov_ever_other_arterial_embolism       smallint,
    cov_ever_dic                           smallint,
    cov_ever_mesenteric_thrombus           smallint,
    cov_ever_artery_dissect                smallint,
    cov_ever_life_arrhythmia               smallint,
    cov_ever_cardiomyopathy                smallint,
    cov_ever_ami                           smallint,
    cov_ever_angina                        smallint,
    cov_ever_hf                            smallint,
    cov_ever_vt                            smallint,
    cov_ever_dvt_icvt                      smallint,
    cov_ever_dvt_dvt                       smallint,
    cov_ever_other_dvt                     smallint,
    cov_ever_portal_vein_thrombosis        smallint,
    cov_ever_pericarditis                  smallint,
    cov_ever_myocarditis                   smallint,
    cov_ever_fracture                      smallint,
    cov_ever_any_cvd                       smallint,
    cov_unique_medications                 int,
    cov_n_disorder                         smallint,
    cov_antiplatelet_meds                  smallint,
    cov_lipid_meds                         smallint,
    cov_bp_lowering_meds                   smallint,
    cov_anticoagulation_meds               smallint,
    cov_cocp_meds                          smallint,
    cov_hrt_meds                           smallint,
    cov_flu_pneumonia_infection            smallint,
    cov_flu_pneumonia_hospitalisation      smallint,
    cov_flu_pneumonia_vaccination          smallint
    )
DISTRIBUTE BY HASH(alf_e);


--DROP TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329;
--TRUNCATE TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 IMMEDIATE;

INSERT INTO SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 (alf_e, cov_sex, wob, death_date, cov_age, lsoa2011, cov_deprivation,
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
    WHERE flu_cohort = 1;

SELECT * FROM SAILWWMCCV.CCU002_04_FLU_COHORT_20220329;

-- ***********************************************************************************************
-- Determine lost to follow up cases
-------------------------------------------------------------------------------------------------
-- People who died within study period
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329
SET lost_to_followup = 1,
    lost_to_followup_date = death_date
WHERE death_date < SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT
AND death_date >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT;

-- People with no GP registeration
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329
SET lost_to_followup = 1,
    lost_to_followup_date = NULL
WHERE gp_coverage_end_date IS NULL;

-- People whose GP registeration date ends before study start date
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329
SET lost_to_followup = 1,
    lost_to_followup_date = gp_coverage_end_date
WHERE gp_coverage_end_date < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT;

-- People who moved out of Wales within study period
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329
SET lost_to_followup = 1,
    lost_to_followup_date = gp_coverage_end_date
WHERE gp_coverage_end_date >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
AND gp_coverage_end_date < SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT;

SELECT count(DISTINCT alf_e) FROM SAILWWMCCV.CCU002_04_FLU_COHORT_20220329
WHERE lost_to_followup_date IS NULL;

-- ***********************************************************************************************
-- Update covariates using SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
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
    tgt.cov_hrt_meds                        = CASE WHEN src.hrt = 1 THEN 1 ELSE 0 END,
    tgt.cov_flu_pneumonia_infection         = CASE WHEN src.flu_pneumonia_infection = 1 THEN 1 ELSE 0 END,
    tgt.cov_flu_pneumonia_hospitalisation   = CASE WHEN src.flu_pneumonia_hospitalisation = 1 THEN 1 ELSE 0 END,
    tgt.cov_flu_pneumonia_vaccination       = CASE WHEN src.flu_pneumonia_vaccination = 1 THEN 1 ELSE 0 END
FROM SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 src
WHERE tgt.alf_e = src.alf_e;


UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329
SET cov_ever_pe_vt = 1
WHERE cov_ever_dvt_icvt = 1 OR cov_ever_pe_vt = 1;

-- ***********************************************************************************************
-- Add ethnicity using SAILWWMCCV.WMCC_COMB_ETHN_EHRD
-- ONS categories ('White', 'Mixed', 'Asian', 'Black', 'Other')
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.cov_ethnicity = src.ec_ons_desc
FROM SAILWWMCCV.WMCC_COMB_ETHN_EHRD src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329
SET cov_ethnicity = 'Missing'
WHERE cov_ethnicity IS NULL;

-- ***********************************************************************************************
-- Update exposure using SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.exp_confirmed_flu_pneumonia_date = src.flu_pneumonia_confirmed_date,
    tgt.exp_confirmed_flu_pneumonia_phenotype = src.flu_pneumonia_hospitalisation
FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20220329 src
WHERE tgt.alf_e = src.alf_e;

-- ***********************************************************************************************
-- Determine comparison group
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329
SET com_non_flu_pneumonia_infection =  1
WHERE alf_e NOT IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329
                    WHERE record_date IS NOT NULL);

SELECT count(DISTINCT alf_e) FROM SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 WHERE com_non_flu_pneumonia_infection = 1;
-- ***********************************************************************************************
-- Update outcomes using SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST
-------------------------------------------------------------------------------------------------
SELECT DISTINCT name FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_ami = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'AMI') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_artery_dissect = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'ARTERY_DISSECT') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_other_arterial_embolism = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'ARTERIAL_EMBOLISM_OTHR') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_angina = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'ANGINA') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_unstable_angina = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'UNSTABLE_ANGINA') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_cardiomyopathy = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'CARDIOMYOPATHY') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_dic = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'DIC') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_dvt_icvt = src.record_date
FROM (SELECT alf_e,
             min(record_date) AS record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name IN ('DVT_ICVT', 'ICVT_PREGNANCY')
      GROUP BY alf_e) src
WHERE tgt.alf_e = src.alf_e;

SELECT count(*) FROM SAILWWMCCV.CCU002_04_FLU_COHORT_20220329  WHERE out_dvt_icvt IS NOT NULL;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_dvt_pregnancy = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'DVT_PREGNANCY') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_dvt_dvt = src.record_date
FROM (SELECT alf_e,
             min(record_date) AS record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name IN ('DVT_DVT', 'DVT_PREGNANCY')
      GROUP BY alf_e) src
WHERE tgt.alf_e = src.alf_e;

SELECT count(*) FROM SAILWWMCCV.CCU002_04_FLU_COHORT_20220329  WHERE out_dvt_dvt IS NOT NULL;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_icvt_pregnancy = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'ICVT_PREGNANCY') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_other_dvt = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'OTHER_DVT') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_fracture = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'FRACTURE') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_hf = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'HF') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_life_arrhythmia = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'LIFE_ARRHYTHM') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_mesenteric_thrombus = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'MESENTERIC_THROMBUS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_myocarditis = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'MYOCARDITIS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_pe = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'PE') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_pericarditis = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'PERICARDITIS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_portal_vein_thrombosis = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'PORTAL_VEIN_THROMBOSIS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_retinal_infarction = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'RETINAL_INFARCTION') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_thrombocytopenia = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'THROMBOCYTOPENIA') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_ttp = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'TTP') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_stroke_sah = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_SAH') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_stroke_tia = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_TIA') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_stroke_spinal = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_SPINAL') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_stroke_hs = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_HS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_stroke_is = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_IS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_stroke_nos = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_NOS') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_stroke_isch = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_ISCH') src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_stroke_sah_hs = src.record_date
FROM (SELECT alf_e,
             record_date
      FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
      WHERE name = 'STROKE_SAH_HS') src
WHERE tgt.alf_e = src.alf_e;

-- ***********************************************************************************************
-- Update grouped outcomes
--------------------------------------------------------------------------------------------------
SELECT DISTINCT name FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220115;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_arterial_event = src.out_arterial_event
FROM (SELECT alf_e,
             min(record_date) AS out_arterial_event
      FROM (SELECT alf_e,
                   record_date
            FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
            WHERE name IN ('AMI','STROKE_ISCH','ARTERIAL_EMBOLISM_OTHR')
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_venous_event = src.out_venous_event
FROM (SELECT alf_e,
             min(record_date) AS out_venous_event
      FROM (SELECT alf_e,
                   record_date
            FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
            WHERE name IN ('PE', 'OTHER_DVT', 'DVT_ICVT', 'DVT_PREGNANCY', 'DVT_DVT',
                           'ICVT_PREGNANCY','PORTAL_VEIN_THROMBOSIS')
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.out_haematological_event = src.out_haematological_event
FROM (SELECT alf_e,
             min(record_date) AS out_haematological_event
      FROM (SELECT alf_e,
                   record_date
            FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
            WHERE name IN ('TTP', 'THROMBOCYTOPENIA')
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;
-- ***********************************************************************************************
-- Update medication related covariates using WLGP (instead of WDDS)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.cov_antiplatelet_meds = 1
WHERE alf_e IN (SELECT alf_e
                FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
                WHERE event_cd IN (SELECT code
                                   FROM SAILWWMCCV.PHEN_READ_DRUG_ANTIPLATELET
                                   WHERE is_latest = 1
                                   )
                AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 3 month
                AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                );
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.cov_lipid_meds = 1
WHERE alf_e IN (SELECT alf_e
                FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
                WHERE event_cd IN (SELECT code
                                   FROM SAILWWMCCV.PHEN_READ_DRUG_LIPID_LOWERING
                                   WHERE is_latest = 1
                                   )
                AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 3 month
                AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                );
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.cov_bp_lowering_meds = 1
WHERE alf_e IN (SELECT alf_e
                FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
                WHERE event_cd IN (SELECT code
                                   FROM SAILWWMCCV.PHEN_READ_DRUG_ANTIHYPERTENSIVE
                                   WHERE is_latest = 1
                                   )
                AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 3 month
                AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                );
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.cov_anticoagulation_meds = 1
WHERE alf_e IN (SELECT alf_e
                FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
                WHERE event_cd IN (SELECT code
                                   FROM SAILWWMCCV.PHEN_READ_DRUG_ANTICOAGULANT
                                   WHERE is_latest = 1
                                   )
                AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 3 month
                AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                );
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.cov_cocp_meds = 1
WHERE alf_e IN (SELECT alf_e
                FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
                WHERE event_cd IN (SELECT code
                                   FROM SAILWWMCCV.PHEN_READ_DRUG_COCP
                                   WHERE is_latest = 1
                                   )
                AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 3 month
                AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                );
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.cov_hrt_meds = 1
WHERE alf_e IN (SELECT alf_e
                FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
                WHERE event_cd IN (SELECT code
                                   FROM SAILWWMCCV.PHEN_READ_DRUG_HRT
                                   WHERE is_latest = 1
                                   )
                AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 3 month
                AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                );
-- ***********************************************************************************************
-- Number of Disorders (using Charlson codelist)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.cov_n_disorder = src.n_disorder
FROM (SELECT alf_e,
             count(DISTINCT event_cd) AS n_disorder
      FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
      WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_CHARLSON)
      AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 3 month
      AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
      GROUP BY alf_e) src
WHERE tgt.alf_e = src.alf_e


UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_20220329 tgt
SET tgt.cov_n_disorder = 0
WHERE cov_n_disorder IS NULL AND gp_coverage_end_date IS NOT NULL;

SELECT max(cov_n_disorder) FROM SAILWWMCCV.CCU002_04_FLU_COHORT_20220329;

-- ***********************************************************************************************
-- Apply further exclusions
-------------------------------------------------------------------------------------------------
DELETE FROM SAILWWMCCV.CCU002_04_FLU_COHORT_20220329
WHERE cov_age >= 110
OR death_date < exp_confirmed_flu_pneumonia_date
OR (cov_sex=1 AND cov_cocp_meds=1)
OR (cov_sex=1 AND cov_hrt_meds=1);

-- ***********************************************************************************************
SELECT count(*) FROM SAILWWMCCV.CCU002_04_FLU_COHORT_20220329
WHERE cov_unique_medications <> 0;

SELECT * FROM SAILWWMCCV.CCU002_04_FLU_COHORT_20220329
