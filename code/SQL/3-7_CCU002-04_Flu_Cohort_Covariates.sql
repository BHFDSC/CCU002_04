--************************************************************************************************
-- Script:       3-7_CCU002-04_Flu_Cohort_Covariates.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
--               CCU002-04: To compare the long-term risk of stroke/MI in patients after
--               COVID-19 infection with other respiratory infections
-- About:        Create covariates table for CCU002-04 flu cohort

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2022
-- ***********************************************************************************************
-- Date parameters
CREATE OR REPLACE VARIABLE SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT DATE DEFAULT '2016-01-01';
CREATE OR REPLACE VARIABLE SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT DATE DEFAULT '2019-12-31';
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 (
    alf_e                                      bigint,
    gndr_cd                                    char(1),
    wob                                        date,
    dod                                        date,
    gp_coverage_end_date                       date,
    charlson_score_v1                          int,
    charlson_score_v2                          int,
    elixhauser_score                           int,
    unique_medications                         int,
    antiplatelet                               smallint,
    bp_lowering                                smallint,
    lipid_lowering                             smallint,
    anticoagulant                              smallint,
    cocp                                       smallint,
    hrt                                        smallint,
    diabetes_medication                        smallint,
    diabetes_diag                              smallint,
    hypertension_medication                    smallint,
    hypertension_diag                          smallint,
    depression                                 smallint,
    cancer                                     smallint,
    copd                                       smallint,
    ckd                                        smallint,
    liver_disease                              smallint,
    dementia                                   smallint,
    bmi_obesity                                smallint,  -- Only "finding of obesity"
    bmi                                        decimal(31,8),
    obese_bmi                                  smallint,
    obesity                                    smallint,  -- bmi_obesity + obese_bmi
    smoking                                    smallint,
    smoking_status                             char(20),
    stroke_isch                                smallint,
    stroke_sah_hs                              smallint,
    stroke_tia                                 smallint,
    stroke_nos                                 smallint,
    stroke_spinal                              smallint,
    stroke_hs                                  smallint,
    stroke_sah                                 smallint,
    stroke_is                                  smallint,
    retinal_infarction                         smallint,
    any_stroke                                 smallint,
    ami                                        smallint,
    hf                                         smallint,
    angina                                     smallint,
    vt                                         smallint,
    dvt_icvt                                   smallint,
    dvt_dvt                                    smallint,
    other_dvt                                  smallint,
    portal_vein_thrombosis                     smallint,
    venous_thrombosis                          smallint,
    pe                                         smallint,
    pe_vt                                      smallint,
    thrombophilia                              smallint,
    tcp                                        smallint,  -- Thrombocytopenia & TTP
    ttp                                        smallint,
    other_arterial_embolism                    smallint,
    dic                                        smallint,
    mesenteric_thrombus                        smallint,
    artery_dissect                             smallint,
    life_arrhythmia                            smallint,
    cardiomyopathy                             smallint,
    pericarditis                               smallint,
    myocarditis                                smallint,
    any_cvd                                    smallint,
    fracture                                   smallint,
    surgery                                    smallint,
    flu_pneumonia_infection                    smallint,
    flu_pneumonia_hospitalisation              smallint,
    flu_pneumonia_vaccination                  smallint
    --consultation_rate                          int,
    --unique_bnf_chapters                        int,
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329;
--TRUNCATE TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 IMMEDIATE;


INSERT INTO SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 (alf_e, gndr_cd, wob, dod, gp_coverage_end_date)
    SELECT DISTINCT alf_e,
    gndr_cd,
    wob,
    dod_jl,
    gp_coverage_end_date
    FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
    WHERE flu_cohort = 1;
-------------------------------------------------------------------------------------------------
-- Number of unique drugs prescribed within 3 months prior to inception
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.unique_medications = src.unique_medications
FROM (SELECT DISTINCT alf_e,
             count(DISTINCT bnf_combined) AS unique_medications
      FROM (SELECT DISTINCT alf_e,
                   bnf_combined
            FROM SAILWMCCV.C19_COHORT20_RRDA_WDDS
            WHERE bnf_combined IS NOT NULL
            AND dt_prescribed >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 3 month
            AND dt_prescribed < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET unique_medications = 0
WHERE unique_medications IS NULL
AND alf_e IN (SELECT DISTINCT alf_e
              FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
              WHERE event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 3 month
              AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

SELECT * FROM SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
WHERE unique_medications IS NOT NULL;

-------------------------------------------------------------------------------------------------
-- Charlson score
-------------------------------------------------------------------------------------------------
-- Version 1: Original Charlson index
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.charlson_score_v1 = src.charlson_score
FROM (SELECT DISTINCT alf_e, sum(weight) AS charlson_score
      FROM (SELECT DISTINCT alf_e,
                   category,
                   CASE WHEN category = 'Acute myocardial infarction' THEN 1
                        WHEN category = 'Cancer' THEN 2
                        WHEN category = 'Cerebral vascular accident' THEN 1
                        WHEN category = 'Congestive heart failure' THEN 1
                        WHEN category = 'Connective tissue disorder' THEN 1
                        WHEN category = 'Dementia' THEN 1
                        WHEN category = 'Diabetes with long-term complications' THEN 2
                        WHEN category = 'Diabetes without long-term complications' THEN 1
                        WHEN category = 'Metastatic cancer' THEN 3
                        WHEN category = 'Mild or moderate liver disease' THEN 1
                        WHEN category = 'Paraplegia' THEN 2
                        WHEN category = 'Peptic ulcer' THEN 1
                        WHEN category = 'Peripheral vascular disease' THEN 1
                        WHEN category = 'Pulmonary disease' THEN 1
                        WHEN category = 'Renal disease' THEN 2
                        WHEN category = 'Severe liver disease' THEN 3
                        ELSE 0
                   END AS weight
            FROM (SELECT DISTINCT alf_e,
                         charlson_category AS category,
                         count(*)
                  FROM SAILWWMCCV.TEMP_PEDW_CHALSON_RECORDS
                  WHERE YEAR(admis_dt) < 2016
                  GROUP BY alf_e, charlson_category
                  )
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET charlson_score_v1 = 0
WHERE charlson_score_v1 IS NULL;


-- Version 2: Bottle & Aylin Charlson index
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.charlson_score_v2 = src.charlson_score
FROM (SELECT DISTINCT alf_e, sum(weight) AS charlson_score
      FROM (SELECT DISTINCT alf_e,
                   category,
                   CASE WHEN category = 'Acute myocardial infarction' THEN 5
                        WHEN category = 'Cancer' THEN 8
                        WHEN category = 'Cerebral vascular accident' THEN 11
                        WHEN category = 'Congestive heart failure' THEN 13
                        WHEN category = 'Connective tissue disorder' THEN 4
                        WHEN category = 'Dementia' THEN 14
                        WHEN category = 'Diabetes with long-term complications' THEN -1
                        WHEN category = 'Diabetes without long-term complications' THEN 3
                        WHEN category = 'Metastatic cancer' THEN 14
                        WHEN category = 'Mild or moderate liver disease' THEN 8
                        WHEN category = 'Paraplegia' THEN 1
                        WHEN category = 'Peptic ulcer' THEN 9
                        WHEN category = 'Peripheral vascular disease' THEN 6
                        WHEN category = 'Pulmonary disease' THEN 4
                        WHEN category = 'Renal disease' THEN 10
                        WHEN category = 'Severe liver disease' THEN 18
                        ELSE 0
                   END AS weight
            FROM (SELECT DISTINCT alf_e,
                         charlson_category AS category,
                         count(*)
                  FROM SAILWWMCCV.TEMP_PEDW_CHALSON_RECORDS
                  WHERE YEAR(admis_dt) < 2016
                  GROUP BY alf_e, charlson_category
                  )
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET charlson_score_v2 = 0
WHERE charlson_score_v2 IS NULL;

-------------------------------------------------------------------------------------------------
-- Elixhauser score
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.elixhauser_score = src.elixhauser_category
FROM (SELECT DISTINCT alf_e, sum(weight) AS elixhauser_category
      FROM (SELECT DISTINCT alf_e,
                   category,
                   CASE WHEN category = 'Alcohol abuse' THEN 0
                        WHEN category = 'Blood loss anaemia' THEN -2
                        WHEN category = 'Cardiac arrhythmias' THEN 5
                        WHEN category = 'Chronic pulmonary disease' THEN 3
                        WHEN category = 'Coagulopathy' THEN 3
                        WHEN category = 'Congestive heart failure' THEN 7
                        WHEN category = 'Deficiency anaemia' THEN -2
                        WHEN category = 'Depression' THEN -3
                        WHEN category = 'Diabetes, complicated' THEN 0
                        WHEN category = 'Diabetes, uncomplicated' THEN 0
                        WHEN category = 'Drug abuse' THEN -7
                        WHEN category = 'Fluid and electrolyte disorders' THEN 5
                        WHEN category = 'Hypertension' THEN 0
                        WHEN category = 'Hypothyroidism' THEN 0
                        WHEN category = 'Liver disease' THEN 11
                        WHEN category = 'Lymphoma' THEN 9
                        WHEN category = 'Metastatic cancer' THEN 12
                        WHEN category = 'Obesity' THEN -4
                        WHEN category = 'Other neurological disorders' THEN 6
                        WHEN category = 'Paralysis' THEN 7
                        WHEN category = 'Peptic ulcer disease, excluding bleeding' THEN 0
                        WHEN category = 'Peripheral vascular disorders' THEN 2
                        WHEN category = 'Psychoses' THEN 0
                        WHEN category = 'Pulmonary circulation disorders' THEN 4
                        WHEN category = 'Renal failure' THEN 5
                        WHEN category = 'Rheumatoid arthritis/collagen vascular diseases' THEN 0
                        WHEN category = 'Solid tumour without metastasis' THEN 4
                        WHEN category = 'Valvular disease' THEN -1
                        WHEN category = 'Weight loss' THEN 6
                        ELSE 0
                   END AS weight
            FROM (SELECT DISTINCT alf_e,
                         elixhauser_category AS category,
                         count(*)
                  FROM SAILWWMCCV.TEMP_PEDW_ELIXHAUSER_RECORDS
                  WHERE YEAR(admis_dt) < 2016
                  GROUP BY alf_e, elixhauser_category
                  )
            )
      GROUP BY alf_e
      ) src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET elixhauser_score = 0
WHERE elixhauser_score IS NULL;

--***********************************************************************************************
-------------------------------------------------------------------------------------------------
-- Hypertension
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.hypertension_diag = 1
WHERE tgt.alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE hypertension = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.hypertension_diag = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE hypertension = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.hypertension_diag IS NULL;
--------------------------------------------------------------------------------------------------
-- Diabetes diagnosis (from the start of records to index date)
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.diabetes_diag = 1
WHERE tgt.alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE diabetes_diag = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.diabetes_diag = 1
WHERE tgt.alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE diabetes = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.diabetes_diag IS NULL;
--------------------------------------------------------------------------------------------------
-- Cancer (from the start of records to index date)
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.cancer = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE cancer = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.cancer = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE cancer = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.cancer IS NULL;
--------------------------------------------------------------------------------------------------
-- Liver disease (from the start of records to index date)
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.liver_disease = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE liver_disease = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.liver_disease = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE liver_disease = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.liver_disease IS NULL;
--------------------------------------------------------------------------------------------------
-- Dementia (from the start of records to index date)
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.dementia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE dementia = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.dementia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE dementia = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.dementia IS NULL;
--------------------------------------------------------------------------------------------------
-- CKD (from the start of records to index date)
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.ckd = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE ckd = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.ckd = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE ckd = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.ckd IS NULL;
--------------------------------------------------------------------------------------------------
-- COPD (from the start of records to index date)
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.copd = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE copd = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.copd = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE copd = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.copd IS NULL;
--------------------------------------------------------------------------------------------------
-- Depression (from the start of records to index date)
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.depression = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE depression = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.depression = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE depression = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.depression IS NULL;
-------------------------------------------------------------------------------------------------
-- Smoking status (most recent record before index date)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.smoking = 1,
    tgt.smoking_status = CASE WHEN src.smoking_category = 'S' THEN 'Current-smoker'
                              WHEN src.smoking_category = 'E' THEN 'Ex-smoker'
                              WHEN src.smoking_category = 'N' THEN 'Never-smoked'
                         END
FROM (SELECT alf_e,
             event_dt,
             smoking_category,
             ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY event_dt DESC) AS row_num
      FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
      WHERE smoking = 1
      AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT) src
WHERE tgt.alf_e = src.alf_e
AND row_num = 1;
-------------------------------------------------------------------------------------------------
-- bmi_obesity (from the start of records to index date)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.bmi_obesity = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE obesity = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                    AND event_cd IN (SELECT code
                                     FROM SAILWWMCCV.PHEN_READ_OBESITY
                                     WHERE category = 'Diagnosis of Obesity'
                                     )
                   );

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.bmi_obesity = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE obesity = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.bmi_obesity IS NULL;

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET bmi_obesity = 0
WHERE bmi_obesity IS NULL;
--------------------------------------------------------------------------------------------------
-- BMI & OBESE_BMI (from the start of records to index date)
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.bmi = src.event_val,
    tgt.obese_bmi = CASE WHEN src.event_val >= 30 THEN 1
                         ELSE 0 END
FROM (SELECT alf_e,
             event_dt,
             event_cd,
             event_val,
             ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY event_dt DESC) AS row_num
      FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
      WHERE bmi = 1
      AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
      AND YEAR(event_dt) - YEAR(wob) >= 18
      AND event_val IS NOT NULL
      AND event_val >= 12
      AND event_val <= 100
      ) src
WHERE tgt.alf_e = src.alf_e
AND row_num = 1;

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET obese_bmi = 0
WHERE obese_bmi IS NULL;
-------------------------------------------------------------------------------------------------
-- Obesity (from the start of records to index date)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.obesity = CASE WHEN obese_bmi = 1 OR bmi_obesity = 1 THEN 1
                       WHEN obese_bmi = 0 AND bmi_obesity = 0 THEN 0
                       ELSE 0
                  END
WHERE alf_e IS NOT NULL;
-------------------------------------------------------------------------------------------------
-- Stroke ISCH
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_isch = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE stroke_isch = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_isch = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE stroke_isch = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.stroke_isch IS NULL;
-------------------------------------------------------------------------------------------------
-- Stroke SAH_HS
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_sah_hs = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE stroke_sah_hs = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_sah_hs = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE stroke_sah_hs = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.stroke_sah_hs IS NULL;
-------------------------------------------------------------------------------------------------
-- Stroke TIA
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_tia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE stroke_tia = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_tia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE stroke_tia = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.stroke_tia IS NULL;
-------------------------------------------------------------------------------------------------
-- Spinal stroke
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_spinal = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_CVD
                    WHERE outcome_icd10 IN (SELECT code
                                            FROM SAILWWMCCV.PHEN_ICD10_STROKE_SPINAL)
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);
-------------------------------------------------------------------------------------------------
-- Stroke NOS
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_nos = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE stroke_nos = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_nos = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE stroke_nos = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.stroke_nos IS NULL;
-------------------------------------------------------------------------------------------------
-- Stroke HS
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_hs = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE stroke_hs = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.stroke_hs IS NULL;
-------------------------------------------------------------------------------------------------
-- Stroke SAH
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_sah = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE stroke_sah = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_sah = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE stroke_sah = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.stroke_sah IS NULL;
-------------------------------------------------------------------------------------------------
-- Stroke IS
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_is = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE stroke_is = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.stroke_is = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE stroke_is = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.stroke_is IS NULL;
-------------------------------------------------------------------------------------------------
-- Retinal infarction
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.retinal_infarction = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE retinal_infarction = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.retinal_infarction = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE retinal_infarction = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.retinal_infarction IS NULL;
-------------------------------------------------------------------------------------------------
-- Any stroke
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET any_stroke = CASE WHEN stroke_sah_hs = 1 OR stroke_isch = 1 OR stroke_spinal = 1 OR retinal_infarction = 1
                      THEN 1
                      WHEN stroke_sah_hs = 0 AND stroke_isch = 0 AND stroke_spinal = 0 AND retinal_infarction = 1
                      THEN 0
                      ELSE 0
                 END
WHERE alf_e IS NOT NULL;
-------------------------------------------------------------------------------------------------
-- AMI
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.ami = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE ami = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.ami = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE ami = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.ami IS NULL;
-------------------------------------------------------------------------------------------------
-- HF
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.hf = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE heart_failure = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.hf = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE hf = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.hf IS NULL;
-------------------------------------------------------------------------------------------------
-- Angina
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.angina = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE angina = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.angina = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE angina = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.angina IS NULL;
-------------------------------------------------------------------------------------------------
-- VT
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.vt = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE vt = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET vt = 0
WHERE vt IS NULL;
-------------------------------------------------------------------------------------------------
-- DVT_ICVT
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.dvt_icvt = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE dvt_icvt = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.dvt_icvt = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE dvt_icvt = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.dvt_icvt IS NULL;
-------------------------------------------------------------------------------------------------
-- DVT_DVT
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.dvt_dvt = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE dvt_dvt = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);
-------------------------------------------------------------------------------------------------
-- Other DVT
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.other_dvt = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE other_dvt = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);
-------------------------------------------------------------------------------------------------
-- Portal vein thrombosis
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.portal_vein_thrombosis = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE portal_vein_thrombosis = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);
-------------------------------------------------------------------------------------------------
-- Venous thrombosis
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.venous_thrombosis = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE dvt_icvt = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET venous_thrombosis = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE (vt = 1 OR dvt_icvt = 1)
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET venous_thrombosis = 0
WHERE venous_thrombosis IS NULL;
-------------------------------------------------------------------------------------------------
-- PE
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.pe = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE pe = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.pe = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE pe = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.pe IS NULL;

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET pe = 0
WHERE pe IS NULL;
-------------------------------------------------------------------------------------------------
-- PE + VT
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET pe_vt = CASE WHEN pe =1 OR vt =1 THEN 1
                 WHEN pe = 0 AND vt = 0 THEN 0
                 ELSE 0
            END
WHERE alf_e IS NOT NULL;
-------------------------------------------------------------------------------------------------
-- Thrombophilia
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.thrombophilia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE thrombophilia = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.thrombophilia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE thrombophilia = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.thrombophilia IS NULL;
-------------------------------------------------------------------------------------------------
-- TCP (TTP + Thrombocytopenia)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.tcp = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE tcp = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.tcp = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE tcp = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.tcp IS NULL;
-------------------------------------------------------------------------------------------------
-- TTP
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.ttp = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE icd10_cd IN (SELECT code
                                       FROM SAILWWMCCV.PHEN_ICD10_TCP
                                       WHERE category = 'TTP')
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);
-------------------------------------------------------------------------------------------------
-- other_arterial_embolism
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.other_arterial_embolism = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE other_arterial_embolism = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);
-------------------------------------------------------------------------------------------------
-- DIC
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.dic = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE dic = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);
-------------------------------------------------------------------------------------------------
-- mesenteric_thrombus
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.mesenteric_thrombus = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE mesenteric_thrombus = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);
-------------------------------------------------------------------------------------------------
-- Artery dissect
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.artery_dissect = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE artery_dissect = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.artery_dissect = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE artery_dissect = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.artery_dissect IS NULL;
-------------------------------------------------------------------------------------------------
-- Life arrhythmia
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.life_arrhythmia = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE life_arrhythmia = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);
-------------------------------------------------------------------------------------------------
-- Cardiomyopathy
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.cardiomyopathy = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_WLGP_COVARIATES
                    WHERE cardiomyopathy = 1
                    AND event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.cardiomyopathy = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE cardiomyopathy = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT)
AND tgt.cardiomyopathy IS NULL;
-------------------------------------------------------------------------------------------------
-- Pericarditis
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.pericarditis = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE pericarditis = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);
-------------------------------------------------------------------------------------------------
-- Myocarditis
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.myocarditis = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE myocarditis = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);
-------------------------------------------------------------------------------------------------
-- Fracture
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.fracture = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWWMCCV.PHEN_PEDW_COVARIATES
                    WHERE fracture = 1
                    AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT);
-- EDDS
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.fracture = 1
WHERE tgt.alf_e IN (SELECT alf_e
                    FROM SAILWMCCV.C19_COHORT_EDDS_EDDS
                    WHERE admin_arr_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                    AND (diag_cd_1 IN ('03A', '03B', '03C', '03Z') OR
                         diag_cd_2 IN ('03A', '03B', '03C', '03Z') OR
                         diag_cd_3 IN ('03A', '03B', '03C', '03Z') OR
                         diag_cd_4 IN ('03A', '03B', '03C', '03Z') OR
                         diag_cd_5 IN ('03A', '03B', '03C', '03Z') OR
                         diag_cd_6 IN ('03A', '03B', '03C', '03Z')
                         )
                    AND (anat_area_cd_1 IN ('401', '403', '404', '405', '406') OR
                         anat_area_cd_2 IN ('401', '403', '404', '405', '406') OR
                         anat_area_cd_3 IN ('401', '403', '404', '405', '406') OR
                         anat_area_cd_4 IN ('401', '403', '404', '405', '406') OR
                         anat_area_cd_5 IN ('401', '403', '404', '405', '406') OR
                         anat_area_cd_6 IN ('401', '403', '404', '405', '406')
                         )
                    );
------------------------------------------------------------------------------------------------
-- Any CVD
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET any_cvd = 1
WHERE any_stroke = 1 OR ami = 1 OR hf = 1 OR angina = 1;

UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET any_cvd = 0
WHERE any_cvd IS NULL;

--***********************************************************************************************
-- Surgery (over the last year before index date)
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.surgery = 1
WHERE tgt.alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.WMCC_PEDW_SPELL s
                    LEFT JOIN SAILWWMCCV.WMCC_PEDW_EPISODE e
                    ON s.prov_unit_cd = e.prov_unit_cd
                    AND s.spell_num_e = e.spell_num_e
                    LEFT JOIN SAILWWMCCV.WMCC_PEDW_OPER d
                    ON e.prov_unit_cd = d.prov_unit_cd
                    AND e.spell_num_e = d.spell_num_e
                    AND e.epi_num = d.epi_num
                    WHERE d.oper_cd IS NOT NULL
                    AND oper_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                    AND oper_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 1 YEAR
                   );
--***********************************************************************************************
-- Infection and hospitalisation with flu or pneumonia (over the past year before index date) 
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329 tgt
SET tgt.flu_pneumonia_infection = 1
WHERE alf_e IN (-- PEDW
                SELECT alf_e
                       --admis_dt AS record_date
                FROM SAILWWMCCV.PHEN_PEDW_RESPIRATORY_INFECTIONS
                WHERE icd10_cd_category IN ('Viral influenza', 'Viral pneumonia', 'Bacterial pneumonia')
                AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                AND admis_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 1 YEAR
                UNION ALL
                -- ICNARC
                SELECT alf_e
                       --daicu AS record_date
                FROM SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF a
                INNER JOIN SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE l
                ON a.system_id_e = l.system_id_e
                WHERE (raicu1_text LIKE '%pneumonia%' OR raicu2_text LIKE '%pneumonia%' OR uraicu_text LIKE '%pneumonia%' OR
                       raicu1_text LIKE '%influenza%' OR raicu2_text LIKE '%influenza%' OR uraicu_text LIKE '%influenza%'
                       )
                AND daicu < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                AND daicu >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 1 YEAR
                UNION ALL
                -- WLGP
                SELECT alf_e
                FROM SAILWWMCCV.PHEN_WLGP_RESPIRATORY_INFECTIONS
                WHERE event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 1 YEAR
                UNION ALL
                -- WRRS
                SELECT alf_e
                FROM SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS
                WHERE test_code IN ('FLUAPCR',              -- 'Influenza A PCR'
                                    'FLUBPCR',              -- 'Influenza B PCR'
                                    'Mycoplasma pneumonia', -- 'Mycoplasma pneumoniae'
                                    'MPCF',                 -- 'Mycoplasma pneumoniae'
                                    'Streptococcus pneumo'  -- 'Streptococcus pneumoniae
                                    )
                 AND test_collected_date < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                 AND test_collected_date >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 1 YEAR
                );


UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET flu_pneumonia_hospitalisation = 1
WHERE alf_e IN (-- PEDW
                SELECT alf_e
                FROM SAILWWMCCV.PHEN_PEDW_RESPIRATORY_INFECTIONS
                WHERE icd10_cd_category IN ('Viral influenza', 'Viral pneumonia', 'Bacterial pneumonia')
                AND admis_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                AND admis_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 1 YEAR
                UNION ALL
                -- ICNARC
                SELECT alf_e
                FROM SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF a
                INNER JOIN SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE l
                ON a.system_id_e = l.system_id_e
                WHERE (raicu1_text LIKE '%pneumonia%' OR raicu2_text LIKE '%pneumonia%' OR uraicu_text LIKE '%pneumonia%' OR
                       raicu1_text LIKE '%influenza%' OR raicu2_text LIKE '%influenza%' OR uraicu_text LIKE '%influenza%'
                       )
                AND daicu < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                AND daicu >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 1 YEAR);
-------------------------------------------------------------------------------------------------
-- Flu or pneumonia vaccination
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_COVARIATES_20220329
SET flu_pneumonia_vaccination = 1
WHERE alf_e IN (SELECT alf_e
                FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED g
                JOIN (SELECT * FROM SAILWWMCCV.PHEN_READ_INFLUENZA_VACC WHERE is_latest = 1
                      UNION ALL
                      SELECT * FROM SAILWWMCCV.PHEN_READ_PNEUMOCOCCAL_VACC WHERE is_latest = 1
                      ) r
                ON g.event_cd = r.code
                WHERE event_dt < SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
                AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT - 1 YEAR);

