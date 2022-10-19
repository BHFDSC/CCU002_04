--************************************************************************************************
-- Script:       3-2_CCU002-04_COVID19_Exposure.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
--               CCU002-04: To compare the long-term risk of stroke/MI in patients after
--               COVID-19 infection with other respiratory infections
-- About:        Create COVID-19 related tables for CCU002-01 project

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- Date parameters
CREATE OR REPLACE VARIABLE SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT DATE DEFAULT '2020-01-01';
CREATE OR REPLACE VARIABLE SAILWWMCCV.PARAM_CCU002_04_COVID_END_DT DATE DEFAULT '2021-12-31';
-- ***********************************************************************************************
-- Create a table containing all COVID19 records for CCU002-04 cohort
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_04_COVID19_ALL_20220630 (
    alf_e                bigint,
    record_date          date,
    covid19_status       char(40),
    source               char(4),    -- PATD, PEDW, WLGP, ICNC, ICCD
    code                 char(20),   -- ICD10 or Read code
    clinical_code        char(5),
    description          char(255),
    primary_position     char(1)
    )
DISTRIBUTE BY HASH(alf_e);
--DROP TABLE SAILWWMCCV.CCU002_04_COVID19_ALL_20220630;
--TRUNCATE TABLE SAILWWMCCV.CCU002_04_COVID19_ALL_20220630 IMMEDIATE;
-------------------------------------------------------------------------------------------------
-- Add PCR test results
INSERT INTO SAILWWMCCV.CCU002_04_COVID19_ALL_20220630
    SELECT alf_e,
           test_date_cleaned AS record_date,
           'Confirmed COVID19' AS covid19_status,
           'PATD' AS source,
           'PATD test code' AS code,
           NULL AS clinical_code,
           'Coronavirus SARS CoV 2 PCR' AS description,
           NULL AS primary_position
    FROM SAILWWMCCV.PHEN_PATD_TESTRESULTS_COVID19
    WHERE covid19testresult = 'Positive'
    AND alf_e IN (SELECT DISTINCT alf_e
                  FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                  WHERE covid_cohort = 1)
    AND test_date_cleaned >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT
    AND test_date_cleaned <= SAILWWMCCV.PARAM_CCU002_04_COVID_END_DT;

-------------------------------------------------------------------------------------------------
-- Add COVID19 related records from hospital data
INSERT INTO SAILWWMCCV.CCU002_04_COVID19_ALL_20220630
    SELECT alf_e,
           admis_dt AS record_date,
           CASE WHEN diag_cd = 'U071' OR epi_diag_cd = 'U071' THEN 'Confirmed COVID19'
                WHEN diag_cd = 'U072' OR epi_diag_cd = 'U072' THEN 'Suspected COVID19'
           END AS covid19_status,
           'PEDW' AS source,
           'ICD10' AS code,
           CASE WHEN diag_cd IN ('U071', 'U072') THEN diag_cd
                WHEN epi_diag_cd IN ('U071', 'U072') THEN epi_diag_cd
                ELSE NULL
           END AS clinical_code,
           CASE WHEN diag_cd = 'U071' OR epi_diag_cd = 'U071' THEN 'Confirmed COVID19'
                WHEN diag_cd = 'U072' OR epi_diag_cd = 'U072' THEN 'Suspected COVID19'
           END AS description,
           primary_position
    FROM SAILWWMCCV.PHEN_PEDW_COVID19
    WHERE alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                    WHERE covid_cohort = 1)
    AND admis_dt >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT
    AND admis_dt <= SAILWWMCCV.PARAM_CCU002_04_COVID_END_DT;

-------------------------------------------------------------------------------------------------
-- Add COVID19 related records from Primary care data
INSERT INTO SAILWWMCCV.CCU002_04_COVID19_ALL_20220630
    SELECT alf_e,
           event_dt AS record_date,
           CASE WHEN event_cd_category IN ('Confirmed') THEN 'Confirmed COVID19'
                WHEN event_cd_category IN ('Suspected') THEN 'Suspected COVID19'
           END AS covid19_status,
           'WLGP' AS source,
           'Read code' AS code,
           event_cd AS clinical_code,
           event_cd_description AS description,
           NULL AS primary_position
    FROM SAILWWMCCV.PHEN_WLGP_COVID19
    WHERE alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                    WHERE covid_cohort = 1)
    AND event_dt >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT
    AND event_dt <= SAILWWMCCV.PARAM_CCU002_04_COVID_END_DT
    AND event_cd_category IN ('Confirmed', 'Suspected');


-------------------------------------------------------------------------------------------------
-- COVID19 crirical care records
-- ICNARC
-- raicu1_text = primary reason for admission
-- raicu2_text = secondary reason for admission
-- uraicu_text = ultimate primary reason for admission
INSERT INTO SAILWWMCCV.CCU002_04_COVID19_ALL_20220630
    SELECT alf_e,
           daicu AS record_date, -- ICU admission date
           CASE WHEN raicu1_text LIKE '%COVID-19, confirmed%' OR raicu2_text LIKE '%COVID-19, confirmed%' OR uraicu_text LIKE '%COVID-19, confirmed%'
                     THEN 'Confirmed COVID19'
                WHEN raicu1_text LIKE '%COVID-19, suspected%' OR raicu2_text LIKE '%COVID-19, suspected%' OR uraicu_text LIKE '%COVID-19, suspected%'
                     THEN 'Suspected COVID19'
           END AS covid19_status,
           'ICNC' AS source,
           NULL AS code,
           NULL AS clinical_code,
           CASE WHEN raicu1_text LIKE '%COVID-19, confirmed%' OR raicu2_text LIKE '%COVID-19, confirmed%' OR uraicu_text LIKE '%COVID-19, confirmed%'
                    THEN 'ICU admission with confirmed COVID19'
                WHEN raicu1_text LIKE '%COVID-19, suspected%' OR raicu2_text LIKE '%COVID-19, suspected%' OR uraicu_text LIKE '%COVID-19, suspected%'
                    THEN 'ICU admission with suspected COVID19'
           END AS description,
           -- Only primary or ultimate primary reason for admission
           CASE WHEN raicu1_text LIKE '%COVID-19, confirmed%' OR uraicu_text LIKE '%COVID-19, confirmed%' THEN 1
                ELSE NULL END AS primary_position
    FROM SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF a
    INNER JOIN SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE l
    ON a.system_id_e = l.system_id_e
    WHERE alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                    WHERE covid_cohort = 1)
    AND (raicu1_text LIKE '%COVID%' OR raicu2_text LIKE '%COVID%' OR uraicu_text LIKE '%COVID%')
    AND daicu >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT
    AND daicu <= SAILWWMCCV.PARAM_CCU002_04_COVID_END_DT;

-----------------------------------------------------------------------------------------------
-- ICNARC weekly
INSERT INTO SAILWWMCCV.CCU002_04_COVID19_ALL_20220630
    SELECT alf_e,
           daicu AS record_date, -- ICU admission date
           CASE WHEN raicu1_text LIKE '%COVID-19, confirmed%' OR raicu2_text LIKE '%COVID-19, confirmed%' OR uraicu_text LIKE '%COVID-19, confirmed%'
                     THEN 'Confirmed COVID19'
                WHEN raicu1_text LIKE '%COVID-19, suspected%' OR raicu2_text LIKE '%COVID-19, suspected%' OR uraicu_text LIKE '%COVID-19, suspected%'
                     THEN 'Suspected COVID19'
           END AS covid19_status,
           'ICCD' AS source,
           NULL AS code,
           NULL AS clinical_code,
           CASE WHEN raicu1_text LIKE '%COVID-19, confirmed%' OR raicu2_text LIKE '%COVID-19, confirmed%' OR uraicu_text LIKE '%COVID-19, confirmed%'
                    THEN 'ICU admission with confirmed COVID19'
                WHEN raicu1_text LIKE '%COVID-19, suspected%' OR raicu2_text LIKE '%COVID-19, suspected%' OR uraicu_text LIKE '%COVID-19, suspected%'
                    THEN 'ICU admission with suspected COVID19'
           END AS description,
           -- Only primary or ultimate primary reason for admission
           CASE WHEN raicu1_text LIKE '%COVID-19, confirmed%' OR uraicu_text LIKE '%COVID-19, confirmed%' THEN 1
                ELSE NULL END AS primary_position
    FROM SAILWMCCV.C19_COHORT_ICCD_ICNARC_LINKAGE_ALF a
    INNER JOIN SAILWMCCV.C19_COHORT_ICCD_ICNARC_LINKAGE l
    ON a.system_id_e = l.system_id_e
    WHERE alf_e IN (SELECT DISTINCT alf_e
                    FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
                    WHERE covid_cohort = 1)
    AND (raicu1_text LIKE '%COVID%' OR raicu2_text LIKE '%COVID%' OR uraicu_text LIKE '%COVID%')
    AND daicu >= SAILWWMCCV.PARAM_CCU002_04_COVID_START_DT
    AND daicu <= SAILWWMCCV.PARAM_CCU002_04_COVID_END_DT;

-- ***********************************************************************************************
-- COVID19 with hospitalisation
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.CCU002_04_COVID19_HOSPITALISATION_20220630 (
    alf_e                               bigint,
    covid19_confirmed_date              date,
    covid19_hospitalisation             char(30)
    )
DISTRIBUTE BY HASH(alf_e);
--DROP TABLE SAILWWMCCV.CCU002_04_COVID19_HOSPITALISATION_20220630;
--TRUNCATE TABLE SAILWWMCCV.CCU002_04_COVID19_HOSPITALISATION_20220630 IMMEDIATE;

INSERT INTO SAILWWMCCV.CCU002_04_COVID19_HOSPITALISATION_20220630 (alf_e, covid19_confirmed_date, covid19_hospitalisation)
    SELECT alf_e,
           min(record_date) AS covid19_confirmed_date,
           covid19_hospitalisation
    FROM (SELECT a.alf_e,
                 a.record_date,
                 a.covid19_status,
                 b.covid19_hospitalisation,
                 b.first_admission_date
          FROM SAILWWMCCV.CCU002_04_COVID19_ALL_20220630 a
          LEFT JOIN (SELECT DISTINCT alf_e,
                            'hospitalised' AS covid19_hospitalisation,
                            days_between,
                            first_admission_date
                     FROM (SELECT t1.alf_e,
                                  t1.first_admission_date,
                                  t2.first_covid_event,
                                  TIMESTAMPDIFF(16,TIMESTAMP(first_admission_date) - TIMESTAMP(first_covid_event)) AS days_between
                           FROM (SELECT alf_e,
                                         min(record_date) AS first_admission_date
                                 FROM (SELECT alf_e,
                                              record_date
                                       FROM SAILWWMCCV.CCU002_04_COVID19_ALL_20220630 a
                                       WHERE source IN ('PEDW', 'ICNC', 'ICCD')
                                       AND covid19_status = 'Confirmed COVID19'
                                       AND primary_position = 1
                                      )
                                 GROUP BY alf_e
                                ) t1
                           LEFT JOIN (SELECT alf_e,
                                             min(record_date) AS first_covid_event
                                      FROM (SELECT alf_e,
                                                   record_date
                                            FROM SAILWWMCCV.CCU002_04_COVID19_ALL_20220630 a
                                            WHERE covid19_status = 'Confirmed COVID19'
                                           )
                                      GROUP BY alf_e
                                       ) t2
                           ON t1.alf_e = t2.alf_e
                           ORDER BY t2.first_covid_event
                           )
                     WHERE days_between <= 28
                    ) b
          ON a.alf_e = b.alf_e
          WHERE a.covid19_status = 'Confirmed COVID19'
          )
    GROUP BY alf_e, covid19_hospitalisation;

UPDATE SAILWWMCCV.CCU002_04_COVID19_HOSPITALISATION_20220630
SET covid19_hospitalisation = 'non-hospitalised'
WHERE covid19_hospitalisation IS NULL;
