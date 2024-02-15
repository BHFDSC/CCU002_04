--************************************************************************************************
-- Script:       3-3_CCU002-04_Flu_Exposure.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
--               CCU002-04: To compare the long-term risk of stroke/MI in patients after
--               COVID-19 infection with other respiratory infections
-- About:        Create flu & pneumonia related tables for CCU002-04 project

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- **********************************************************************************************
-- Date parameters
CREATE OR REPLACE VARIABLE SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT DATE DEFAULT '2016-01-01';
CREATE OR REPLACE VARIABLE SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT DATE DEFAULT '2019-12-31';
-- ***********************************************************************************************
-- Create a table containing all flu & pneumonia records for CCU002_04 cohort
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329 (
    alf_e                bigint,
    record_date          date,
    status               char(40),
    source               char(4),   -- WRRS, PEDW, WLGP, ICNC
    code                 char(20),   -- ICD10 or Read code
    clinical_code        char(20),
    description          char(255),
    primary_position     char(1),
    flu                  char(1),
    pneumonia            char(1)
    )
DISTRIBUTE BY HASH(alf_e);
--DROP TABLE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329;
--TRUNCATE TABLE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329 IMMEDIATE;

------------------------------------------------------------------------------------------------
-- Add PCR test results from WRRS
INSERT INTO SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329
    SELECT alf_e,
           test_collected_date AS record_date,
           'Confirmed' AS status,
           'WRRS' AS source,
           'WRRS test code' AS code,
           test_code AS clinical_code,
           test_name AS description,
           NULL AS primary_position,
           CASE WHEN test_code IN ('FLUAPCR', 'FLUBPCR') THEN 1 ELSE 0 END AS flu,
           CASE WHEN test_code IN ('Mycoplasma pneumonia', 'MPCF', 'Streptococcus pneumo') THEN 1 ELSE 0 END AS pneumonia
    FROM SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS
    WHERE test_code IN ('FLUAPCR','Influenza A','Influenzae A result','POCTINFARNA'  -- 'Influenza A PCR'
                        'FLUBPCR','Influenza B','Influenzae B result''POCTINFBRNA'   -- 'Influenza B PCR'
                        'Mycoplasma pneumonia', -- 'Mycoplasma pneumoniae'
                        'MPCF',                 -- 'Mycoplasma pneumoniae'
                        'Streptococcus pneumo'  -- 'Streptococcus pneumoniae
                        )
    AND alf_e IN (SELECT DISTINCT alf_e 
                  FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                  WHERE flu_cohort = 1)
    AND test_collected_date >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
    AND test_collected_date <= SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT;

-------------------------------------------------------------------------------------------------
-- Add influenza & pneumonia related records from hospital data
INSERT INTO SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329
    SELECT alf_e,
           admis_dt AS record_date,
           'Confirmed' AS status,
           'PEDW' AS source,
           'ICD10' AS code,
           icd10_cd AS clinical_code,
           icd10_cd_desc AS description,
           primary_position,
           CASE WHEN icd10_cd_category IN ('Viral influenza') THEN 1 ELSE 0 END AS flu,
           CASE WHEN icd10_cd_category IN ('Viral pneumonia', 'Bacterial pneumonia') THEN 1 ELSE 0 END AS pneumonia
    FROM SAILWWMCCV.PHEN_PEDW_RESPIRATORY_INFECTIONS
    WHERE alf_e IN (SELECT DISTINCT alf_e 
                    FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                    WHERE flu_cohort = 1)
    AND icd10_cd_category IN ('Viral influenza', 'Viral pneumonia', 'Bacterial pneumonia')
    AND admis_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
    AND admis_dt <= SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT;
;
-------------------------------------------------------------------------------------------------
-- Add influenza & pneumonia related records from GP data
INSERT INTO SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329
    SELECT alf_e,
           event_dt AS record_date,
           'Confirmed' AS status,
           'WLGP' AS source,
           'Read code' AS code,
           event_cd AS clinical_code,
           event_cd_desc AS description,
           NULL AS primary_position,
           CASE WHEN (event_cd_desc LIKE '%influenza%' OR event_cd_desc LIKE '%Influenza%' OR event_cd IN ('4JU6.','4JU7.','4JU8.','XM0s0','Xa9J7','Hyu04')) THEN 1 ELSE 0 END AS flu,
           CASE WHEN (event_cd_desc LIKE '%pneumonia%' OR event_cd_desc LIKE '%Pneumonia%' OR event_cd IN ('A521.','A7850','H2600')) THEN 1 ELSE 0 END AS pneumonia
    FROM SAILWWMCCV.PHEN_WLGP_RESPIRATORY_INFECTIONS
    WHERE alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                    WHERE flu_cohort = 1)
    AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
    AND event_dt <= SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT;

-------------------------------------------------------------------------------------------------
-- Add influenza & pneumonia crirical care records
-- ICNARC (no need to use weekly version)
INSERT INTO SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329
    SELECT alf_e,
           daicu AS record_date, -- ICU admission date
           'Confirmed' AS status,
           'ICNC' AS source,
           NULL AS code,
           NULL AS clinical_code,
           'ICU admission with confirmed influenza or pneumonia' AS description,
           -- Only primary or ultimate primary reason for admission
           CASE WHEN raicu1_text LIKE '%pneumonia%' OR uraicu_text LIKE '%pneumonia%' OR raicu1_text LIKE '%influenza%' OR uraicu_text LIKE '%influenza%' THEN 1
                ELSE NULL END AS primary_position,
           CASE WHEN (raicu1_text LIKE '%influenza%' OR raicu2_text LIKE '%influenza%' OR uraicu_text LIKE '%influenza%') THEN 1 ELSE 0 END AS flu,
           CASE WHEN (raicu1_text LIKE '%pneumonia%' OR raicu2_text LIKE '%pneumonia%' OR uraicu_text LIKE '%pneumonia%') THEN 1 ELSE 0 END AS pneumonia
    FROM SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF a
    INNER JOIN SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE l
    ON a.system_id_e = l.system_id_e
    WHERE alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                    WHERE flu_cohort = 1)
    AND (raicu1_text LIKE '%pneumonia%' OR raicu2_text LIKE '%pneumonia%' OR uraicu_text LIKE '%pneumonia%' OR
         raicu1_text LIKE '%influenza%' OR raicu2_text LIKE '%influenza%' OR uraicu_text LIKE '%influenza%')
    AND daicu >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
    AND daicu <= SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT;

-- ***********************************************************************************************
-- Influenza & pneumonia with hospitalisation
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20220329 (
    alf_e                                      bigint,
    flu_pneumonia_confirmed_date               date,
    flu_pneumonia_pos_pcr_date                 date,
    flu_pneumonia_hospital_admis_primary_date  date,
    flu_pneumonia_hospitalisation              char(30)
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20220329;
--TRUNCATE TABLE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20220329 IMMEDIATE;

INSERT INTO SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20220329 (alf_e, flu_pneumonia_confirmed_date, flu_pneumonia_hospitalisation)
    SELECT alf_e,
           min(record_date) AS flu_pneumonia_confirmed_date,
           flu_pneumonia_hospitalisation
    FROM (SELECT a.alf_e,
                 a.record_date,
                 a.status,
                 b.flu_pneumonia_hospitalisation AS flu_pneumonia_hospitalisation
          FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329 a
          LEFT JOIN (SELECT DISTINCT alf_e,
                            'hospitalised' AS flu_pneumonia_hospitalisation,
                            days_between
                     FROM (SELECT t1.alf_e,
                                  t1.first_admission_date,
                                  t2.first_flu_pneumonia_event,
                                  TIMESTAMPDIFF(16,TIMESTAMP(first_admission_date) - TIMESTAMP(first_flu_pneumonia_event)) AS days_between
                           FROM (SELECT alf_e,
                                         min(record_date) AS first_admission_date
                                 FROM (SELECT alf_e,
                                              record_date
                                       FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329 a
                                       WHERE source IN ('PEDW', 'ICNC')
                                       AND status = 'Confirmed'
                                       AND primary_position = 1
                                      )
                                 GROUP BY alf_e
                                ) t1
                           LEFT JOIN (SELECT alf_e,
                                             min(record_date) AS first_flu_pneumonia_event
                                      FROM (SELECT alf_e,
                                                   record_date
                                            FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329 a
                                            WHERE status = 'Confirmed'
                                           )
                                      GROUP BY alf_e
                                       ) t2
                           ON t1.alf_e = t2.alf_e
                           ORDER BY t2.first_flu_pneumonia_event
                           )
                     WHERE days_between <= 28
                    ) b
          ON a.alf_e = b.alf_e
          WHERE a.status = 'Confirmed'
          )
    GROUP BY alf_e, flu_pneumonia_hospitalisation;


UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20220329
SET flu_pneumonia_hospitalisation = 'non-hospitalised'
WHERE flu_pneumonia_hospitalisation IS NULL;

-------------------------------------------------------------------------------------------------
-- First positive flu PCR test
UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20220329 tgt
SET tgt.flu_pneumonia_pos_pcr_date = src.flu_pneumonia_pos_pcr_date
FROM (SELECT alf_e,
             min(record_date) AS flu_pneumonia_pos_pcr_date
      FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329 a
      WHERE status = 'Confirmed'
      AND source = 'WRRS'
      GROUP BY alf_e
     ) src
WHERE tgt.alf_e = src.alf_e

-------------------------------------------------------------------------------------------------
-- First flu admission in primary position
UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20220329 tgt
SET tgt.flu_pneumonia_hospital_admis_primary_date = src.flu_pneumonia_hospital_admis_primary_date
FROM (SELECT alf_e,
             min(record_date) AS flu_pneumonia_hospital_admis_primary_date
      FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329 a
      WHERE source IN ('PEDW', 'ICNC')
      AND status = 'Confirmed'
      AND primary_position = 1
      GROUP BY alf_e
     ) src
WHERE tgt.alf_e = src.alf_e;

-- ***********************************************************************************************
-- New version of flu/pneumonia table with additional exposure variables
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 LIKE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329;

--DROP TABLE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322;
--TRUNCATE TABLE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 IMMEDIATE;

INSERT INTO SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 SELECT * FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20220329;

ALTER TABLE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 ADD other_pneumonia_infections char(1) DEFAULT 0;
-------------------------------------------------------------------------------------------------
-- Add additional records from WRRS
-------------------------------------------------------------------------------------------------
INSERT INTO SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322
    SELECT alf_e,
           test_collected_date AS record_date,
           'Confirmed' AS status,
           'WRRS' AS source,
           'WRRS test code' AS code,
           test_code AS clinical_code,
           test_name AS description,
           --test_value,
           NULL AS primary_position,
           CASE WHEN test_code IN ('Parainfluenza','PF1','PF2','PF3','PF4','PFPCR',
                                   'Paraflu 1','Paraflu 2','Paraflu 3',
                                   'Influenza A','Influenzae A result','Influenza B','Influenzae B result'
                                   ) THEN 1 ELSE 0 END AS flu,
           0 AS pneumonia,
           CASE WHEN test_code IN ('Haemophilus influenz') THEN 1 ELSE 0 END AS other_pneumonia_infections
    FROM SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS
    WHERE test_code IN ('Haemophilus influenz',
                        'Parainfluenza','PF1','PF2','PF3','PF4','Influenza A','Influenzae A result',
                        'Influenza B','Influenzae B result','PFPCR'
                        )
    AND alf_e IN (SELECT DISTINCT alf_e
                  FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                  WHERE flu_cohort = 1)
    AND test_collected_date >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
    AND test_collected_date <= SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT
    AND test_value NOT LIKE '%Inhibitory%';
-------------------------------------------------------------------------------------------------
-- Add additional records from WLGP using new codelist
-------------------------------------------------------------------------------------------------
INSERT INTO SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322
    SELECT alf_e,
           event_dt AS record_date,
           'Confirmed' AS status,
           'WLGP' AS source,
           'Read code' AS code,
           event_cd AS clinical_code,
           NULL AS description,
           NULL AS primary_position,
           0 AS flu,
           0 AS pneumonia,
           1 AS other_pneumonia_infections
    FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
    WHERE alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                    WHERE flu_cohort = 1)
    AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_FLU_START_DT
    AND event_dt <= SAILWWMCCV.PARAM_CCU002_04_FLU_END_DT
    AND event_cd IN ('43D3.','43wE.','A203.','A205.','A3B5.','A3BX4','A3BXA',
                     'A3BXB','A3By1','A3By4','A551.','A730.','A7850','AB405',
                     'AB415','AD04.','AyuK9','H0606','H0608','H060A','H200.',
                     'H203.','H5110','SP131','X73Rb'
                    );


-- ***********************************************************************************************
-- New version of the influenza/pneumonia hospitalisation table
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322 (
    alf_e                                      bigint,
    exp1_pneumonia_date                        date,
    exp1_pneumonia_phenotype                   char(20),
    exp1_pneumonia_date_v2                     date,
    exp1_pneumonia_phenotype_v2                char(20),
    exp2_flu_date                              date,
    exp2_flu_phenotype                         char(20),
    exp3_both_date                             date,
    exp3_both_phenotype                        char(20),
    exp4_both_and_other_infections_date        date,
    exp4_both_and_other_infections_phenotype   char(20)
    )
DISTRIBUTE BY HASH(alf_e);

TRUNCATE TABLE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322 IMMEDIATE;
-------------------------------------------------------------------------------------------------
-- Add all hospitalised ALFs
-------------------------------------------------------------------------------------------------
INSERT INTO SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322 (alf_e)
    SELECT DISTINCT alf_e
    FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
    WHERE status = 'Confirmed';

-------------------------------------------------------------------------------------------------
-- Exposure 1: Pneumonia hospitalisation (original version)
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322 tgt
SET tgt.exp1_pneumonia_date = src.exposure_date,
    tgt.exp1_pneumonia_phenotype = src.exposure_phenotype
FROM (SELECT alf_e,
           min(record_date) AS exposure_date,
           exposure_phenotype
    FROM (SELECT a.alf_e,
                 a.record_date,
                 a.status,
                 b.exposure_phenotype
          FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
          LEFT JOIN (SELECT DISTINCT alf_e,
                            'hospitalised' AS exposure_phenotype,
                            days_between
                     FROM (SELECT t1.alf_e,
                                  t1.first_admission_date,
                                  t2.first_related_event,
                                  TIMESTAMPDIFF(16,TIMESTAMP(first_admission_date) - TIMESTAMP(first_related_event)) AS days_between
                           FROM (SELECT alf_e,
                                        min(record_date) AS first_admission_date
                                 FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
                                 WHERE source IN ('PEDW', 'ICNC')
                                 AND status = 'Confirmed'
                                 AND primary_position = 1
                                 AND pneumonia = 1
                                 GROUP BY alf_e
                                ) t1
                           LEFT JOIN (SELECT alf_e,
                                             min(record_date) AS first_related_event
                                      FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
                                      WHERE status = 'Confirmed'
                                      AND pneumonia = 1
                                      GROUP BY alf_e
                                       ) t2
                           ON t1.alf_e = t2.alf_e
                           ORDER BY t2.first_related_event
                           )
                     WHERE days_between <= 28
                    ) b
          ON a.alf_e = b.alf_e
          WHERE a.status = 'Confirmed'
          AND pneumonia = 1
          )
    GROUP BY alf_e, exposure_phenotype) src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322
SET exp1_pneumonia_phenotype = 'non-hospitalised'
WHERE exp1_pneumonia_phenotype IS NULL
AND exp1_pneumonia_date IS NOT NULL;

-------------------------------------------------------------------------------------------------
-- Exposure 1 V2: Pneumonia hospitalisation
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322 tgt
SET tgt.exp1_pneumonia_date_v2 = src.exposure_date,
    tgt.exp1_pneumonia_phenotype_v2 = src.exposure_phenotype
FROM (SELECT alf_e,
           min(record_date) AS exposure_date,
           exposure_phenotype
    FROM (SELECT a.alf_e,
                 a.record_date,
                 a.status,
                 b.exposure_phenotype
          FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
          LEFT JOIN (SELECT DISTINCT alf_e,
                            'hospitalised' AS exposure_phenotype,
                            days_between
                     FROM (SELECT t1.alf_e,
                                  t1.first_admission_date,
                                  t2.first_related_event,
                                  TIMESTAMPDIFF(16,TIMESTAMP(first_admission_date) - TIMESTAMP(first_related_event)) AS days_between
                           FROM (SELECT alf_e,
                                        min(record_date) AS first_admission_date
                                 FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
                                 WHERE source IN ('PEDW', 'ICNC')
                                 AND status = 'Confirmed'
                                 AND primary_position = 1
                                 AND pneumonia = 1
                                 GROUP BY alf_e
                                ) t1
                           LEFT JOIN (SELECT alf_e,
                                             min(record_date) AS first_related_event
                                      FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
                                      WHERE status = 'Confirmed'
                                      AND (pneumonia = 1 OR other_pneumonia_infections = 1)----------
                                      GROUP BY alf_e
                                       ) t2
                           ON t1.alf_e = t2.alf_e
                           ORDER BY t2.first_related_event
                           )
                     WHERE days_between <= 28
                    ) b
          ON a.alf_e = b.alf_e
          WHERE a.status = 'Confirmed'
          AND (pneumonia = 1 OR other_pneumonia_infections = 1) ---------
          )
    GROUP BY alf_e, exposure_phenotype
    ORDER BY alf_e) src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322
SET exp1_pneumonia_phenotype_v2 = 'non-hospitalised'
WHERE exp1_pneumonia_phenotype_v2 IS NULL
AND exp1_pneumonia_date_v2 IS NOT NULL;

-------------------------------------------------------------------------------------------------
-- Exposure 2: Flu hospitalisation
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322 tgt
SET tgt.exp2_flu_date = src.exposure_date,
    tgt.exp2_flu_phenotype = src.exposure_phenotype
FROM (SELECT alf_e,
           min(record_date) AS exposure_date,
           exposure_phenotype
    FROM (SELECT a.alf_e,
                 a.record_date,
                 a.status,
                 b.exposure_phenotype
          FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
          LEFT JOIN (SELECT DISTINCT alf_e,
                            'hospitalised' AS exposure_phenotype,
                            days_between
                     FROM (SELECT t1.alf_e,
                                  t1.first_admission_date,
                                  t2.first_related_event,
                                  TIMESTAMPDIFF(16,TIMESTAMP(first_admission_date) - TIMESTAMP(first_related_event)) AS days_between
                           FROM (SELECT alf_e,
                                        min(record_date) AS first_admission_date
                                 FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
                                 WHERE source IN ('PEDW', 'ICNC')
                                 AND status = 'Confirmed'
                                 AND primary_position = 1
                                 AND flu = 1
                                 GROUP BY alf_e
                                ) t1
                           LEFT JOIN (SELECT alf_e,
                                             min(record_date) AS first_related_event
                                      FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
                                      WHERE status = 'Confirmed'
                                      AND flu = 1
                                      GROUP BY alf_e
                                       ) t2
                           ON t1.alf_e = t2.alf_e
                           ORDER BY t2.first_related_event
                           )
                     WHERE days_between <= 28
                    ) b
          ON a.alf_e = b.alf_e
          WHERE a.status = 'Confirmed'
          AND flu = 1
          )
    GROUP BY alf_e, exposure_phenotype
    ORDER BY alf_e) src
WHERE tgt.alf_e = src.alf_e;


UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322
SET exp2_flu_phenotype = 'non-hospitalised'
WHERE exp2_flu_phenotype IS NULL
AND exp2_flu_date IS NOT NULL;

-------------------------------------------------------------------------------------------------
-- Exposure 3: Flu or pneumonia hospitalisation
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322 tgt
SET tgt.exp3_both_date = src.exposure_date,
    tgt.exp3_both_phenotype = src.exposure_phenotype
FROM (SELECT alf_e,
           min(record_date) AS exposure_date,
           exposure_phenotype
    FROM (SELECT a.alf_e,
                 a.record_date,
                 a.status,
                 b.exposure_phenotype
          FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
          LEFT JOIN (SELECT DISTINCT alf_e,
                            'hospitalised' AS exposure_phenotype,
                            days_between
                     FROM (SELECT t1.alf_e,
                                  t1.first_admission_date,
                                  t2.first_related_event,
                                  TIMESTAMPDIFF(16,TIMESTAMP(first_admission_date) - TIMESTAMP(first_related_event)) AS days_between
                           FROM (SELECT alf_e,
                                        min(record_date) AS first_admission_date
                                 FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
                                 WHERE source IN ('PEDW', 'ICNC')
                                 AND status = 'Confirmed'
                                 AND primary_position = 1
                                 AND (pneumonia = 1 OR flu = 1)----------
                                 GROUP BY alf_e
                                ) t1
                           LEFT JOIN (SELECT alf_e,
                                             min(record_date) AS first_related_event
                                      FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
                                      WHERE status = 'Confirmed'
                                      AND (pneumonia = 1 OR flu = 1)
                                      GROUP BY alf_e
                                       ) t2
                           ON t1.alf_e = t2.alf_e
                           ORDER BY t2.first_related_event
                           )
                     WHERE days_between <= 28
                    ) b
          ON a.alf_e = b.alf_e
          WHERE a.status = 'Confirmed'
          AND (pneumonia = 1 OR flu = 1)
          )
    GROUP BY alf_e, exposure_phenotype
    ORDER BY alf_e) src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322
SET exp3_both_phenotype = 'non-hospitalised'
WHERE exp3_both_phenotype IS NULL
AND exp3_both_date IS NOT NULL;

-------------------------------------------------------------------------------------------------
-- Exposure 4: Flu, pneumonia or other pneumonia infections hospitalisation
-------------------------------------------------------------------------------------------------
UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322 tgt
SET tgt.exp4_both_and_other_infections_date = src.exposure_date,
    tgt.exp4_both_and_other_infections_phenotype = src.exposure_phenotype
FROM (SELECT alf_e,
           min(record_date) AS exposure_date,
           exposure_phenotype
    FROM (SELECT a.alf_e,
                 a.record_date,
                 a.status,
                 b.exposure_phenotype
          FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
          LEFT JOIN (SELECT DISTINCT alf_e,
                            'hospitalised' AS exposure_phenotype,
                            days_between
                     FROM (SELECT t1.alf_e,
                                  t1.first_admission_date,
                                  t2.first_related_event,
                                  TIMESTAMPDIFF(16,TIMESTAMP(first_admission_date) - TIMESTAMP(first_related_event)) AS days_between
                           FROM (SELECT alf_e,
                                        min(record_date) AS first_admission_date
                                 FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
                                 WHERE source IN ('PEDW', 'ICNC')
                                 AND status = 'Confirmed'
                                 AND primary_position = 1
                                 GROUP BY alf_e
                                ) t1
                           LEFT JOIN (SELECT alf_e,
                                             min(record_date) AS first_related_event
                                      FROM SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_ALL_20230322 a
                                      WHERE status = 'Confirmed'
                                      GROUP BY alf_e
                                       ) t2
                           ON t1.alf_e = t2.alf_e
                           ORDER BY t2.first_related_event
                           )
                     WHERE days_between <= 28
                    ) b
          ON a.alf_e = b.alf_e
          WHERE a.status = 'Confirmed'
          )
    GROUP BY alf_e, exposure_phenotype
    ORDER BY alf_e) src
WHERE tgt.alf_e = src.alf_e;

UPDATE SAILWWMCCV.CCU002_04_FLU_PNEUMONIA_HOSPITALISATION_20230322
SET exp4_both_and_other_infections_phenotype = 'non-hospitalised'
WHERE exp4_both_and_other_infections_phenotype IS NULL
AND exp4_both_and_other_infections_date IS NOT NULL;
