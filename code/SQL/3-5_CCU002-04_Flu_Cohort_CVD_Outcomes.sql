--************************************************************************************************
-- Script:       3-5_CCU002-04_Flu_Cohort_CVD_Outcomes.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
--               CCU002-04: To compare the long-term risk of stroke/MI in patients after
--               COVID-19 infection with other respiratory infections
-- About:        Create tables related to CVD outcomes (in primary care, hospital and death data) for CCU002-04 flu cohort

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- Date parameters
CREATE OR REPLACE VARIABLE SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT DATE DEFAULT '2016-01-01';
CREATE OR REPLACE VARIABLE SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT DATE DEFAULT '2019-12-31';
-- ***********************************************************************************************
-- Create a table containing all CVD outcomes for CCU002
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329 (
    alf_e  		           bigint,
    dod                    date,
    gndr_cd                char(1),
    record_date            date,
    code                   char(5),
    name                   char(50),     -- CVD category
    description            char(255),
    terminology            char(30),     -- ICD10, Read code
    arterial_event         smallint,
    venous_event           smallint,
    haematological_event   smallint,
    source                 char(5),      -- WLGP, Death, PEDW
    category               char(40),
    fatal_event            smallint,
    c19_cohort16           smallint
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329;
TRUNCATE TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329 IMMEDIATE;

-- CVD outcomes in Death data
INSERT INTO SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329
    SELECT alf_e,
           dod,
           gndr_cd,
           dod AS record_date,
           outcome_icd10 AS code,
           CASE WHEN outcome_name = 'AMI_CCU002' AND outcome_category = 'AMI' THEN 'AMI'
                WHEN outcome_name = 'STROKE_TIA_CCU002' THEN 'STROKE_TIA'
                WHEN outcome_name = 'STROKE_ISCH_CCU002' THEN 'STROKE_ISCH'
                WHEN outcome_name = 'STROKE_SAH_HS_CCU002' THEN 'STROKE_SAH_HS'
                WHEN outcome_name = 'PE_CCU002' THEN 'PE'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'DVT_DVT' THEN 'DVT_DVT'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'other_DVT' THEN 'OTHER_DVT'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'DVT_pregnancy' THEN 'DVT_PREGNANCY'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'ICVT_pregnancy' THEN 'ICVT_PREGNANCY'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'portal_vein_thrombosis' THEN 'PORTAL_VEIN_THROMBOSIS'
                WHEN outcome_name = 'TCP' AND outcome_category = 'TTP' THEN 'TTP'
                WHEN outcome_name = 'TCP' AND outcome_category = 'thrombocytopenia' THEN 'THROMBOCYTOPENIA'
                WHEN outcome_name = 'LIFE_ARRHYTHM_CCU002' THEN 'LIFE_ARRHYTHM'
                WHEN outcome_name = 'CARDIOMYOPATHY_CCU002' THEN 'CARDIOMYOPATHY'
                --
                -- additionals (not used in CCU002-04): DIC + ARTERY_DISSECT_CCU002
                WHEN outcome_icd10 IN ('I710','I720','I721') THEN 'ARTERY_DISSECT'
                WHEN outcome_name = 'ANGINA' AND outcome_category = 'Angina' THEN 'ANGINA'
                WHEN outcome_name = 'ANGINA' AND outcome_category = 'Unstable angina' THEN 'UNSTABLE_ANGINA'
                WHEN outcome_name = 'ARTERY_DISSECT_CCU002' THEN 'ARTERY_DISSECT'
                ELSE outcome_name
           END AS name,
           outcome_term AS description,
           'ICD10' AS terminology,
           CASE WHEN outcome_name IN ('AMI_CCU002', 'STROKE_ISCH_CCU002', 'ARTERIAL_EMBOLISM_OTHR')
                THEN 1 ELSE 0 END AS arterial_event,
           CASE WHEN outcome_name IN ('VT_CCU002', 'DVT_ICVT', 'PE_CCU002')
                THEN 1 ELSE 0 END AS venous_event,
           CASE WHEN outcome_name IN ('TCP')
                THEN 1 ELSE 0 END AS haematological_event,
           'Death' AS source,
           outcome_category AS category,
           1 AS fatal_event,
           c19_cohort20
    FROM SAILWWMCCV.PHEN_DEATH_CVD
    WHERE dod >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
    AND dod <= SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT
    AND alf_e IN (SELECT DISTINCT alf_e
                  FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                  WHERE flu_cohort = 1)
    AND (cod_underlying = outcome_icd10 OR LEFT(cod_underlying,3) = outcome_icd10) -- only when underlying reason is a CVD reason
    AND outcome_icd10 NOT IN ('O082', 'I252', 'I241', 'I670', 'I726') -- covariate only (VT_CCU002, AMI_CCU002, ARTERY_DISSECT_CCU002)
    AND outcome_name IN ('AMI_CCU002', -- AMI_covariate_only cases already excluded
                         'STROKE_IS', -- Ischaemic stroke
                         'STROKE_NOS', -- Unknown stroke
                         'STROKE_SPINAL', -- Spinal stroke
                         'RETINAL_INFARCTION', -- Retinal vascular occlusions
                         'ARTERIAL_EMBOLISM_OTHR', -- Other arterial embolism
                         'PE_CCU002',
                         'VT_CCU002', -- 'DVT_DVT' (deep vein), 'portal_vein_thrombosis', 'other_DVT' (Other vein thrombosis)
                                      -- 'DVT_pregnancy' (Thrombosis during pregnancy),'ICVT_pregnancy'  (venous thrombosis in pregnancy)
                         'DVT_ICVT',  --  Intracranial venous thrombosis
                         'DIC',
                         'TCP', -- 'TTP', 'thrombocytopenia'
                         'STROKE_HS', -- intracerebral hemorrhage(NOT 'STROKE_SAH_HS_CCU002')
                         'MESENTERIC_THROMBUS',
                         'ARTERY_DISSECT_CCU002',
                         'LIFE_ARRHYTHM_CCU002',
                         'CARDIOMYOPATHY_CCU002',
                         'HF',
                         'ANGINA',
                         'FRACTURE', -- lower limb fracture
                         -- Additioanl phenotypes
                         'STROKE_SAH', -- (stroke, subarachnoid hemorrhage), NOT IN COMPOSITE EVENTS
                         'STROKE_TIA_CCU002', -- NOT IN COMPOSITE EVENTS
                         'MYOCARDITIS',-- NOT IN COMPOSITE EVENTS
                         'PERICARDITIS',-- NOT IN COMPOSITE EVENTS
                         'STROKE_ISCH_CCU002',
                         'STROKE_SAH_HS_CCU002'
                         );

-----------------------------------------------------------------------------------------------
-- CVD outcomes in PEDW
INSERT INTO SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329
    SELECT alf_e,
           NULL AS dod,
           gndr_cd,
           admis_dt AS record_date,
           outcome_icd10 AS code,
           CASE WHEN outcome_name = 'AMI_CCU002' AND outcome_category = 'AMI' THEN 'AMI'
                WHEN outcome_name = 'STROKE_TIA_CCU002' THEN 'STROKE_TIA'
                WHEN outcome_name = 'STROKE_ISCH_CCU002' THEN 'STROKE_ISCH'
                -- 'ARTERIAL_EMBOLISM_OTHR', 'STROKE_NOS', 'RETINAL_INFARCTION', 'STROKE_IS', STROKE_SPINAL
                WHEN outcome_name = 'STROKE_SAH_HS_CCU002' THEN 'STROKE_SAH_HS'
                -- 'STROKE_SAH', 'STROKE_SAH'
                WHEN outcome_name = 'PE_CCU002' THEN 'PE'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'DVT_DVT' THEN 'DVT_DVT'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'other_DVT' THEN 'OTHER_DVT'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'DVT_pregnancy' THEN 'DVT_PREGNANCY'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'ICVT_pregnancy' THEN 'ICVT_PREGNANCY'
                WHEN outcome_name = 'VT_CCU002' AND outcome_category = 'portal_vein_thrombosis' THEN 'PORTAL_VEIN_THROMBOSIS'
                -- DVT_ICVT
                WHEN outcome_name = 'TCP' AND outcome_category = 'TTP' THEN 'TTP'
                WHEN outcome_name = 'TCP' AND outcome_category = 'thrombocytopenia' THEN 'THROMBOCYTOPENIA'
                WHEN outcome_name = 'LIFE_ARRHYTHM_CCU002' THEN 'LIFE_ARRHYTHM'
                WHEN outcome_name = 'CARDIOMYOPATHY_CCU002' THEN 'CARDIOMYOPATHY'
                --
                -- additionals (not used in CCU002-04): DIC + ARTERY_DISSECT_CCU002
                WHEN outcome_icd10 IN ('I710','I720','I721') THEN 'ARTERY_DISSECT'
                WHEN outcome_name = 'ANGINA' AND outcome_category = 'Angina' THEN 'ANGINA'
                WHEN outcome_name = 'ANGINA' AND outcome_category = 'Unstable angina' THEN 'UNSTABLE_ANGINA'
                WHEN outcome_name = 'ARTERY_DISSECT_CCU002' THEN 'ARTERY_DISSECT'
                ELSE outcome_name
           END AS name,
           outcome_term AS description,
           'ICD10' AS terminology,
           CASE WHEN outcome_name IN ('AMI_CCU002', 'STROKE_ISCH_CCU002', 'ARTERIAL_EMBOLISM_OTHR')
                THEN 1 ELSE 0 END AS arterial_event,
           CASE WHEN outcome_name IN ('VT_CCU002', 'DVT_ICVT', 'PE_CCU002')
                THEN 1 ELSE 0 END AS venous_event,
           CASE WHEN outcome_name IN ('TCP')
                THEN 1 ELSE 0 END AS haematological_event,
           'PEDW' AS SOURCE,
           outcome_category AS category,
           NULL as fatal_event,
           c19_cohort20
    FROM SAILWWMCCV.PHEN_PEDW_CVD
    WHERE admis_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
    AND admis_dt <= SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT
    AND primary_position = 1
    AND alf_e IN (SELECT DISTINCT alf_e
                  FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                  WHERE flu_cohort = 1)
    AND outcome_icd10 NOT IN ('O082', 'I252', 'I241', 'I670', 'I726') -- covariate only (VT_CCU002, AMI_CCU002, ARTERY_DISSECT_CCU002)
    AND outcome_name IN ('AMI_CCU002', -- AMI_covariate_only cases already excluded
                         'STROKE_IS', -- Ischaemic stroke
                         'STROKE_NOS', -- Unknown stroke
                         'STROKE_SPINAL', -- Spinal stroke
                         'RETINAL_INFARCTION', -- Retinal vascular occlusions
                         'ARTERIAL_EMBOLISM_OTHR', -- Other arterial embolism
                         'PE_CCU002',
                         'VT_CCU002', -- 'DVT_DVT' (deep vein), 'portal_vein_thrombosis', 'other_DVT' (Other vein thrombosis)
                                      -- 'DVT_pregnancy' (Thrombosis during pregnancy),'ICVT_pregnancy'  (venous thrombosis in pregnancy)
                         'DVT_ICVT',  --  Intracranial venous thrombosis
                         'DIC',
                         'TCP', -- 'TTP', 'thrombocytopenia'
                         'STROKE_HS', -- intracerebral hemorrhage(NOT 'STROKE_SAH_HS_CCU002')
                         'MESENTERIC_THROMBUS',
                         'ARTERY_DISSECT_CCU002',
                         'LIFE_ARRHYTHM_CCU002',
                         'CARDIOMYOPATHY_CCU002',
                         'HF',
                         'ANGINA',
                         'FRACTURE', -- lower limb fracture
                         'STROKE_SAH', -- (stroke, subarachnoid hemorrhage), NOT IN COMPOSITE EVENTS
                         'STROKE_TIA_CCU002',
                         'MYOCARDITIS',
                         'PERICARDITIS',
                         'STROKE_ISCH_CCU002',
                         'STROKE_SAH_HS_CCU002'
                         );

-----------------------------------------------------------------------------------------------
-- CVD outcomes in WLGP
INSERT INTO SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329
    SELECT alf_e,
           NULL AS dod,
           gndr_cd,
           event_dt AS record_date,
           outcome_readcode AS code,
           CASE WHEN outcome_name = 'STROKE_SAH_HS_CCU002' THEN 'STROKE_SAH_HS'
                WHEN outcome_name = 'STROKE_ISCH_CCU002' THEN 'STROKE_ISCH'
                ELSE outcome_name
           END AS name,
           outcome_term AS description,
           'Read code' AS terminology,
           CASE WHEN outcome_name IN ('AMI','STROKE_ISCH_CCU002','ARTERIAL_EMBOLISM_OTHR') THEN 1 ELSE 0 END AS arterial_event,
           NULL AS venous_event,
           NULL AS haematological_event,
           'WLGP' AS SOURCE,
           outcome_category AS category,
           NULL as fatal_event,
           c19_cohort20
    FROM SAILWWMCCV.PHEN_WLGP_CVD
    WHERE event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
    AND event_dt <= SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT
    AND alf_e IN (SELECT DISTINCT alf_e
                  FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                  WHERE flu_cohort = 1)
    AND outcome_name IN ('AMI',
                         'STROKE_IS', -- Ischaemic stroke yes
                         'STROKE_NOS', -- ( 'STROKE_ISCH_CCU002' = 'STROKE_NOS' + 'STROKE_IS')
                         'RETINAL_INFARCTION', -- retinal infarction
                         'ARTERIAL_EMBOLISM_OTHR', -- Other arterial embolism
                         'STROKE_SAH_HS_CCU002',  -- Intracerebral haemorrhage, yes (same as 'STROKE_HS')
                         'ARTERY_DISSECT', -- Dissection of artery
                         'STROKE_SAH',-- (stroke, subarachnoid hemorrhage), NOT IN COMPOSITE EVENTS
                         'STROKE_TIA', -- The LK table has more Read codes for 'Diagnosis of TIA' (compared to CCU002-04 protocol)
                         'STROKE_ISCH_CCU002'
    )
   AND outcome_category NOT IN ('History of MI (1)', 'History of TIA') -- For AMI & STROKE_TIA
   AND outcome_readcode NOT IN ('889A.','G38..','G380.','G381.''G384.','G38z.'); -- For AMI
-----------------------------------------------------------------------------------------------
-- Fracture in EDDS
INSERT INTO SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329
    SELECT alf_e,
           NULL AS dod,
           sex AS gndr_cd,
           admin_arr_dt AS record_date,
           CASE WHEN diag_cd_1 IN ('03A', '03B', '03C', '03Z') THEN diag_cd_1
                WHEN diag_cd_2 IN ('03A', '03B', '03C', '03Z') THEN diag_cd_2
                WHEN diag_cd_3 IN ('03A', '03B', '03C', '03Z') THEN diag_cd_3
                WHEN diag_cd_4 IN ('03A', '03B', '03C', '03Z') THEN diag_cd_4
                WHEN diag_cd_5 IN ('03A', '03B', '03C', '03Z') THEN diag_cd_5
                WHEN diag_cd_6 IN ('03A', '03B', '03C', '03Z') THEN diag_cd_6
                END AS code,
           'FRACTURE' AS name,
           CASE WHEN anat_area_cd_1 IN ('401', '403', '404', '405', '406') THEN anat_area_cd_1
                WHEN anat_area_cd_2 IN ('401', '403', '404', '405', '406') THEN anat_area_cd_2
                WHEN anat_area_cd_3 IN ('401', '403', '404', '405', '406') THEN anat_area_cd_3
                WHEN anat_area_cd_4 IN ('401', '403', '404', '405', '406') THEN anat_area_cd_4
                WHEN anat_area_cd_5 IN ('401', '403', '404', '405', '406') THEN anat_area_cd_5
                WHEN anat_area_cd_6 IN ('401', '403', '404', '405', '406') THEN anat_area_cd_6
                END AS description,
           'EDDS specific coding' AS terminology,
           NULL AS arterial_event,
           NULL AS venous_event,
           NULL AS haematological_event,
           'EDDS' AS SOURCE,
           'Lower Limb Fracture' AS category,
           NULL as fatal_event,
           1 AS c19_cohort20
    FROM SAILWMCCV.C19_COHORT_EDDS_EDDS
    WHERE admin_arr_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
    AND admin_arr_dt <= SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT
    AND alf_e IN (SELECT DISTINCT alf_e
                  FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                  WHERE flu_cohort = 1)
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
        );
-----------------------------------------------------------------------------------------------
-- Update date of death
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329 tgt
SET tgt.dod = src.dod_jl
FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329 src
WHERE tgt.alf_e = src.alf_e;

-- Delete events after death
DELETE FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329
WHERE dod < record_date;

-- Update fatal events
UPDATE SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329
SET fatal_event = 1
WHERE TIMESTAMPDIFF(16,TIMESTAMP(dod) - TIMESTAMP(record_date)) <= 28
OR source = 'Death';

-----------------------------------------------------------------------------------------------
SELECT * FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329
ORDER BY name;

SELECT source, count(*), count(DISTINCT alf_e)
FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329
GROUP BY source;
-- ***********************************************************************************************
-- Create a table containing first event per CVD category
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329 (
    alf_e  		      bigint,
    dod               date,
    gndr_cd           char(1),
    record_date       date,
    code              char(5),
    name              char(50),
    description       char(255),
    terminology       char(30),     -- ICD10, Read code
    source            char(5),      -- WLGP, Death, PEDW
    seq               smallint,
    fatal_event       smallint,
    c19_cohort16      smallint
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329;
TRUNCATE TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329 IMMEDIATE;

INSERT INTO SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220329
    SELECT * FROM (SELECT alf_e,
                          dod,
                          gndr_cd,
                          record_date,
                          code,
                          name,
                          description,
                          terminology,
                          source,
                          ROW_NUMBER() OVER(PARTITION BY alf_e, name ORDER BY record_date ASC) AS seq,
                          fatal_event,
                          c19_cohort16
                   FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329
                   ORDER BY alf_e
                   )
    WHERE seq = 1
    ORDER BY alf_e;


SELECT * FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_20220115;
-- ***********************************************************************************************
-- Create a table containing first arterial/event details
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_COMPOSITE_20220329 (
    alf_e  		                   bigint,
    arterial_event                 smallint,
    arterial_event_date            date,
    arterial_event_source          char(5),
    venous_event                   smallint,
    venous_event_date              date,
    venous_event_source            char(5),
    haematological_event           smallint,
    haematological_event_date      date,
    haematological_event_source    char(5),
    c19_cohort16                   smallint
    )
DISTRIBUTE BY HASH(alf_e);
--DROP TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_COMPOSITE_20220329;
TRUNCATE TABLE SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_COMPOSITE_20220329 IMMEDIATE;


INSERT INTO SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_COMPOSITE_20220329
          -- arterial_venous_haem
          SELECT CASE WHEN arterial_venous_id IS NOT NULL THEN arterial_venous_id
                      ELSE haem_id
                 END AS alf_e,
                 arterial_event,
                 arterial_event_date,
                 arterial_event_source,
                 venous_event,
                 venous_event_date,
                 venous_event_source,
                 haematological_event,
                 haematological_event_date,
                 haematological_event_source,
                 1 AS c19_cohort16
          FROM (-- arterial_venous
                SELECT arterial_id,
                       venous_id,
                       CASE WHEN arterial_id IS NOT NULL THEN arterial_id
                            ELSE venous_id
                       END AS arterial_venous_id,
                       arterial_event,
                       arterial_event_date,
                       arterial_event_source,
                       venous_event,
                       venous_event_date,
                       venous_event_source
                FROM (SELECT *
                      FROM (SELECT alf_e AS arterial_id,
                                   arterial_event,
                                   record_date AS arterial_event_date,
                                   source AS arterial_event_source,
                                   ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY record_date) AS seq
                            FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329
                            WHERE arterial_event = 1
                            )
                       WHERE seq =1
                       ) arterial_t
                 FULL OUTER JOIN (SELECT *
                                  FROM (SELECT alf_e AS venous_id,
                                               venous_event,
                                               record_date AS venous_event_date,
                                               source AS venous_event_source,
                                               ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY record_date) AS seq
                                         FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329
                                         WHERE venous_event = 1
                                         )
                                   WHERE seq =1
                                   ) venous_t
                 ON arterial_t.arterial_id = venous_t.venous_id
                ) arterial_venous_t
          -- arterial_venous ----------------------------------------------------
          FULL OUTER JOIN (SELECT *
                           FROM (SELECT alf_e AS haem_id,
                                        haematological_event,
                                        record_date AS haematological_event_date,
                                        source AS haematological_event_source,
                                        ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY record_date) AS seq
                                 FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_ALL_20220329
                                 WHERE haematological_event = 1
                                 )
                            WHERE seq =1
                            ) haem_t
           ON arterial_venous_t.arterial_venous_id = haem_t.haem_id;

-----------------------------------------------------------------------------------------
-- Some checks
SELECT * FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_COMPOSITE_20220115;

SELECT count(*), count(DISTINCT alf_e)
FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_COMPOSITE_20220115;

SELECT count(*), count(DISTINCT alf_e)
FROM SAILWWMCCV.CCU002_04_FLU_COHORT_CVD_OUTCOMES_FIRST_COMPOSITE_20220115
WHERE arterial_event IS NULL AND haematological_event IS NULL AND other_event IS NULL AND venous_event IS NULL;
