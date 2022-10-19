--************************************************************************************************
-- Script:       2-4_PHEN_covariates.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
-- About:        Create PHEN level covariates tables

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Create a table for PEDW covariates
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_PEDW_COVARIATES (
    alf_e                        bigint,
    gndr_cd                      char(1),
    admis_dt                     date,
    e_diag_cd_1234               char(4),
    d_diag_cd_123                char(3),
    d_diag_cd_1234               char(4),
    diabetes                     smallint,
    depression                   smallint,
    obesity                      smallint,
    cancer                       smallint,
    copd                         smallint,
    ckd                          smallint,
    liver_disease                smallint,
    hypertension                 smallint,
    dementia                     smallint,
    ami                          smallint,
    other_arterial_embolism      smallint,
    stroke_spinal                smallint,
    stroke_tia                   smallint,
    stroke_nos                   smallint,
    stroke_hs                    smallint,
    stroke_sah                   smallint,
    stroke_is                    smallint,
    stroke_isch                  smallint,
    stroke_sah_hs                smallint,
    retinal_infarction           smallint,
    pe                           smallint,
    vt                           smallint,
    dvt_icvt                     smallint,
    dvt_dvt                      smallint,
    other_dvt                    smallint,
    portal_vein_thrombosis       smallint,
    tcp                          smallint,
    life_arrhythmia              smallint,
    hf                           smallint,
    pericarditis                 smallint,
    myocarditis                  smallint,
    mesenteric_thrombus          smallint,
    cardiomyopathy               smallint,
    fracture                     smallint,
    angina                       smallint,
    dic                          smallint,
    artery_dissect               smallint,
    thrombophilia                smallint,
    icd10_cd                     char(4),
    icd10_cd_category            char(50),
    icd10_cd_desc                char(255),
    c19_cohort20                 smallint,
    c19_cohort16                 smallint
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.PHEN_PEDW_COVARIATES;
--TRUNCATE TABLE SAILWWMCCV.PHEN_PEDW_COVARIATES IMMEDIATE;

INSERT INTO SAILWWMCCV.PHEN_PEDW_COVARIATES (alf_e, gndr_cd, admis_dt, e_diag_cd_1234, d_diag_cd_123,
                                             d_diag_cd_1234, icd10_cd, icd10_cd_category,icd10_cd_desc,
                                             c19_cohort20, c19_cohort16)
    SELECT DISTINCT s.alf_e,
           s.gndr_cd,
           s.admis_dt,
           LEFT(e.diag_cd_1234,4),
           d.diag_cd_123,
           LEFT(d.diag_cd,4),
           c.code,
           c.category,
           c.desc,
           s.c19_cohort20,
           s.c19_cohort16
    FROM SAILWWMCCV.WMCC_PEDW_SPELL s
    INNER JOIN SAILWWMCCV.WMCC_PEDW_EPISODE e
    ON s.prov_unit_cd = e.prov_unit_cd
    AND s.spell_num_e = e.spell_num_e
    INNER JOIN SAILWWMCCV.WMCC_PEDW_DIAG d
    ON e.prov_unit_cd = d.prov_unit_cd
    AND e.spell_num_e = d.spell_num_e
    AND e.epi_num = d.epi_num
    JOIN (SELECT * FROM SAILWWMCCV.PHEN_ICD10_HYPERTENSION
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DIABETES
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_CANCER
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_LIVER_DISEASE
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DEMENTIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_CKD
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_OBESITY
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_COPD
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DEPRESSION
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_AMI_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_VT_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DVT_ICVT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PE_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_ISCH_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH_HS_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_THROMBOPHILIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_TCP
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ANGINA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERIAL_EMBOLISM_OTHR
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DIC
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_THROMBUS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_HF
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_NOS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DVT_DVT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_ANAEMIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_FRACTURE
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_RETINAL_INFARCTION
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_HS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_IS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SPINAL
          WHERE is_latest = 1
          ) c
    ON (LEFT(d.diag_cd,4) = c.code OR LEFT(d.diag_cd,3) = c.code);


-------------------------------------------------------------------------------------------------
-- Hypertension
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.hypertension = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_HYPERTENSION);
--------------------------------------------------------------------------------------------------
-- diabete
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.diabetes = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DIABETES);
--------------------------------------------------------------------------------------------------
-- cancer
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.cancer = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CANCER);
--------------------------------------------------------------------------------------------------
-- liver_disease
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.liver_disease = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_LIVER_DISEASE);
--------------------------------------------------------------------------------------------------
-- dementia
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.dementia = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DEMENTIA);
--------------------------------------------------------------------------------------------------
-- ckd
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.ckd = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CKD);
--------------------------------------------------------------------------------------------------
-- obesity
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.obesity = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_OBESITY);
--------------------------------------------------------------------------------------------------
-- COPD
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.copd = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_COPD);
--------------------------------------------------------------------------------------------------
-- Depression
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.depression = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DEPRESSION);
--------------------------------------------------------------------------------------------------
-- AMI
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.ami = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_AMI_CCU002);
--------------------------------------------------------------------------------------------------
-- VT
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.vt = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_VT_CCU002);
--------------------------------------------------------------------------------------------------
-- DVT_ICVT
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.dvt_icvt = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DVT_ICVT);
--------------------------------------------------------------------------------------------------
-- PE
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.pe = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_PE_CCU002);
--------------------------------------------------------------------------------------------------
-- Stroke_ISCH
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.stroke_isch = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_ISCH_CCU002);
--------------------------------------------------------------------------------------------------
-- Stroke_SAH_HS
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.stroke_sah_hs = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH_HS_CCU002);
--------------------------------------------------------------------------------------------------
-- stroke_TIA
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.stroke_tia = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA_CCU002);
--------------------------------------------------------------------------------------------------
-- stroke_NOS
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.stroke_nos = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_NOS);
--------------------------------------------------------------------------------------------------
-- thrombophilia
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.thrombophilia = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_THROMBOPHILIA);
--------------------------------------------------------------------------------------------------
-- TCP
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.tcp = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_TCP);
--------------------------------------------------------------------------------------------------
-- Angina
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.angina = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ANGINA);
--------------------------------------------------------------------------------------------------
-- other_arterial_embolism
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.other_arterial_embolism = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ARTERIAL_EMBOLISM_OTHR);
--------------------------------------------------------------------------------------------------
-- DIC
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.dic = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_DIC);
--------------------------------------------------------------------------------------------------
-- mesenteric_thrombus
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.mesenteric_thrombus= 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_THROMBUS);
--------------------------------------------------------------------------------------------------
-- artery_dissect
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.artery_dissect = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT_CCU002);
--------------------------------------------------------------------------------------------------
-- life_arrhythmia
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.life_arrhythmia = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM_CCU002);
--------------------------------------------------------------------------------------------------
-- cardiomyopathy
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.cardiomyopathy = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY_CCU002);
--------------------------------------------------------------------------------------------------
-- HF
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.hf = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_HF);
--------------------------------------------------------------------------------------------------
-- pericarditis
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.pericarditis = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS);
--------------------------------------------------------------------------------------------------
-- myocarditis
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.myocarditis = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS);
--------------------------------------------------------------------------------------------------
-- Spinal stroke
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.stroke_spinal = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_SPINAL);
--------------------------------------------------------------------------------------------------
-- Stroke HS
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.stroke_hs = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_HS);
--------------------------------------------------------------------------------------------------
-- Stroke SAH
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.stroke_sah = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH);
--------------------------------------------------------------------------------------------------
-- Stroke IS
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.stroke_is = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_STROKE_IS);
--------------------------------------------------------------------------------------------------
-- Retinal infaction
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.retinal_infarction = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_RETINAL_INFARCTION);
--------------------------------------------------------------------------------------------------
-- DVT_DVT & other_dvt & portal_vein_thrombosis
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.dvt_dvt = CASE WHEN src.category = 'DVT_DVT' THEN 1
                       ELSE NULL END,
    tgt.other_dvt = CASE WHEN src.category = 'other_DVT' THEN 1
                         ELSE NULL END,
    tgt.portal_vein_thrombosis = CASE WHEN src.category = 'portal_vein_thrombosis' THEN 1
                                      ELSE NULL END
FROM (SELECT code, category FROM SAILWWMCCV.PHEN_ICD10_VT_CCU002) src
WHERE tgt.icd10_cd = src.code;
--------------------------------------------------------------------------------------------------
-- Fracture
--------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_PEDW_COVARIATES tgt
SET tgt.fracture = 1
WHERE icd10_cd IN (SELECT code FROM SAILWWMCCV.PHEN_ICD10_FRACTURE);

-- ***********************************************************************************************
-- Create a table for WLGP covariates
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_WLGP_COVARIATES (
    alf_e                        bigint,
    gndr_cd                      char(1),
    wob                          date,
    event_cd                     char(5),
    event_val                    decimal(31,8),
    event_dt                     date,
    event_cd_desc                char(255),
    hypertension                 smallint,
    bmi                          smallint,
    pregnancy                    smallint,
    systolic_blood_pressure      smallint,
    heart_failure                smallint,
    diabetes_diag                smallint,
    diabetes_type2               smallint,
    depression                   smallint,
    obesity                      smallint,
    cancer                       smallint,
    copd                         smallint,
    ckd                          smallint,
    ckd_stage                    smallint,
    egfr                         smallint,
    liver_disease                smallint,
    dementia                     smallint,
    alcohol                      smallint,
    alcohol_status               smallint,
    smoking                      smallint,
    smoking_category             char(2),
    ami                          smallint,
    dvt_icvt                     smallint,
    pe                           smallint,
    stroke_nos                   smallint,
    stroke_is                    smallint,
    stroke_tia                   smallint,
    stroke_sah                   smallint,
    stroke_isch                  smallint,
    stroke_sah_hs                smallint,
    retinal_infarction           smallint,
    other_arterial_embolism      smallint,
    thrombophilia                smallint,
    tcp                          smallint,
    angina                       smallint,
    artery_dissect               smallint,
    cardiomyopathy               smallint,
    c19_cohort20                 smallint,
    c19_cohort16                 smallint
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.PHEN_WLGP_COVARIATES;
--TRUNCATE TABLE SAILWWMCCV.PHEN_WLGP_COVARIATES IMMEDIATE;

INSERT INTO SAILWWMCCV.PHEN_WLGP_COVARIATES (alf_e, gndr_cd, wob, event_cd, event_val, event_dt,
                                             event_cd_desc, c19_cohort20, c19_cohort16)
    SELECT DISTINCT alf_e,
           gndr_cd,
           wob,
           event_cd,
           event_val,
           event_dt,
           c.desc,
           c19_cohort20,
           c19_cohort16
    FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED g
    INNER JOIN (SELECT * FROM SAILWWMCCV.PHEN_READ_HYPERTENSION
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_BMI
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_PREGNANCY
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_BP
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_ALCOHOL
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_ALCOHOL_STATUS
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_SMOKING
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_DIABETES_TYPE2
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_DIABETES
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_DEPRESSION
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_OBESITY
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_CANCER
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_COPD
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_CKD
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_CKD_STAGE
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_EGFR
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_LIVER_DISEASE
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_DEMENTIA
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_HF
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_AMI
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_DVT_ICVT
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_PE
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_ISCH_CCU002
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_SAH_HS_CCU002
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_THROMBOPHILIA
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_TCP
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_ANGINA
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_ARTERY_DISSECT
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_CARDIOMYOPATHY
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_TIA
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_NOS
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_IS
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_RETINAL_INFARCTION
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_ARTERIAL_EMBOLISM_OTHR
                WHERE is_latest = 1
                UNION ALL
                SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_SAH
                WHERE is_latest = 1
                ) c
    ON g.event_cd = c.code;
    
SELECT count(*) FROM SAILWWMCCV.PHEN_WLGP_COVARIATES;
-------------------------------------------------------------------------------------------------
-- Hypertension
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.hypertension = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_HYPERTENSION);
-------------------------------------------------------------------------------------------------
-- BMI
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.bmi = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_BMI);
-------------------------------------------------------------------------------------------------
-- Pregnancy
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.pregnancy = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_PREGNANCY);
-------------------------------------------------------------------------------------------------
-- Systolic blood pressure (average of two most recent systolic blood pressure)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.systolic_blood_pressure = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_BP);
-------------------------------------------------------------------------------------------------
-- Heart failure
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.heart_failure = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_HF);
-------------------------------------------------------------------------------------------------
-- Alcohol consumption & alcohol category (most recent record)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.alcohol = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_ALCOHOL);
--1362.		Trivial drinker - <1u/DAY
--1363.		Light drinker - 1-2u/DAY
--1364.		Moderate drinker - 3-6u/DAY
--1365.		Heavy drinker - 7-9u/DAY
--1366.		Very heavy drinker - >9u/day
-------------------------------------------------------------------------------------------------
-- Alcohol status
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.alcohol_status = 1
WHERE event_cd IN (SELECT code
                   FROM SAILWWMCCV.PHEN_READ_ALCOHOL_STATUS
                   WHERE category NOT IN ('Non drinker', 'Drinker status not specified'));
-------------------------------------------------------------------------------------------------
-- Smoking status & smoking category
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.smoking = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_SMOKING);

UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.smoking_category = src.category
FROM SAILWWMCCV.PHEN_READ_SMOKING src
WHERE tgt.smoking = 1
AND tgt.event_cd = src.code;
-------------------------------------------------------------------------------------------------
-- Diabetes,type 2
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.diabetes_type2 = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_DIABETES_TYPE2);
-------------------------------------------------------------------------------------------------
-- Diabetes diag
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.diabetes_diag = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_DIABETES);
-------------------------------------------------------------------------------------------------
-- Cancer
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.cancer = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_CANCER);
-------------------------------------------------------------------------------------------------
-- Liver disease
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.liver_disease = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_LIVER_DISEASE);
-------------------------------------------------------------------------------------------------
-- Dementia
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.dementia = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_DEMENTIA);
-------------------------------------------------------------------------------------------------
-- CKD
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.ckd = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_CKD);
-------------------------------------------------------------------------------------------------
-- CKD stage
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.ckd_stage = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_CKD_STAGE);
-------------------------------------------------------------------------------------------------
-- EGFR
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.egfr = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_EGFR);
-------------------------------------------------------------------------------------------------
-- Obesity
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.obesity = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_OBESITY);
-------------------------------------------------------------------------------------------------
-- COPD
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.copd = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_COPD);
-------------------------------------------------------------------------------------------------
-- Depression
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.depression = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_DEPRESSION);
-------------------------------------------------------------------------------------------------
-- AMI
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.ami = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_AMI);
-------------------------------------------------------------------------------------------------
-- DVT_ICVT
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.dvt_icvt = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_DVT_ICVT);
-------------------------------------------------------------------------------------------------
-- PE
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.pe = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_PE);
-------------------------------------------------------------------------------------------------
-- Stroke ISCH
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.stroke_isch = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_STROKE_ISCH_CCU002);
-------------------------------------------------------------------------------------------------
-- Stroke SAH_HS
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.stroke_sah_hs = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_STROKE_SAH_HS_CCU002);
-------------------------------------------------------------------------------------------------
-- stroke TIA
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.stroke_tia = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_STROKE_TIA);
-------------------------------------------------------------------------------------------------
-- Stroke NOS
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.stroke_nos = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_STROKE_NOS);
-------------------------------------------------------------------------------------------------
-- Stroke IS
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.stroke_is = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_STROKE_IS);
-------------------------------------------------------------------------------------------------
-- Stroke SAH
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.stroke_sah = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_STROKE_SAH);
-------------------------------------------------------------------------------------------------
-- Retinal infarction
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.retinal_infarction = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_RETINAL_INFARCTION);
-------------------------------------------------------------------------------------------------
-- Other arterial embolism
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.other_arterial_embolism = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_ARTERIAL_EMBOLISM_OTHR);
-------------------------------------------------------------------------------------------------
-- Thrombophilia
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.thrombophilia = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_THROMBOPHILIA);
-------------------------------------------------------------------------------------------------
-- TCP
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.tcp = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_TCP);
-------------------------------------------------------------------------------------------------
-- Angina
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.angina = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_ANGINA);
-------------------------------------------------------------------------------------------------
-- artery_dissect
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.artery_dissect = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_ARTERY_DISSECT);
-------------------------------------------------------------------------------------------------
-- Cardiomyopathy
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.PHEN_WLGP_COVARIATES tgt
SET tgt.cardiomyopathy = 1
WHERE event_cd IN (SELECT code FROM SAILWWMCCV.PHEN_READ_CARDIOMYOPATHY);
