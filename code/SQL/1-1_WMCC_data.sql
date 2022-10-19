 --************************************************************************************************
-- Script:       1-1_WMCC_data.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
-- About:        Create curated data tables (WMCC tables)

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Patient Episode Database for Wales (PEDW)
-- https://web.www.healthdatagateway.org/dataset/4c33a5d2-164c-41d7-9797-dc2b008cc852
--------------------------------------------------------------------------------------------------
-- PEDW SPELL
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_PEDW_SPELL LIKE SAILWMCCV.C19_COHORT_PEDW_SPELL;
--TRUNCATE TABLE SAILWWMCCV.WMCC_PEDW_SPELL IMMEDIATE;

INSERT INTO SAILWWMCCV.WMCC_PEDW_SPELL
    (SELECT *
     FROM SAILWMCCV.C19_COHORT_PEDW_SPELL
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
     OR alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16)
    );
ALTER TABLE SAILWWMCCV.WMCC_PEDW_SPELL ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_PEDW_SPELL ADD c19_cohort16 SMALLINT NULL;

UPDATE SAILWWMCCV.WMCC_PEDW_SPELL
SET c19_cohort20 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20);

UPDATE SAILWWMCCV.WMCC_PEDW_SPELL
SET c19_cohort16 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16);
--------------------------------------------------------------------------------------------------
-- PEDW EPISODE
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_PEDW_EPISODE LIKE SAILWMCCV.C19_COHORT_PEDW_EPISODE;
--TRUNCATE TABLE SAILWWMCCV.WMCC_PEDW_EPISODE IMMEDIATE;

ALTER TABLE SAILWWMCCV.WMCC_PEDW_EPISODE ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_PEDW_EPISODE ADD c19_cohort16 SMALLINT NULL;

INSERT INTO SAILWWMCCV.WMCC_PEDW_EPISODE
    (SELECT e.*,
            s.c19_cohort20,
            s.c19_cohort16
     FROM SAILWWMCCV.WMCC_PEDW_SPELL s
     INNER JOIN SAILWMCCV.C19_COHORT_PEDW_EPISODE e
     ON s.prov_unit_cd = e.prov_unit_cd
     AND s.spell_num_e = e.spell_num_e
    );

--------------------------------------------------------------------------------------------------
-- PEDW DIAG
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_PEDW_DIAG LIKE SAILWMCCV.C19_COHORT_PEDW_DIAG;
--TRUNCATE TABLE SAILWWMCCV.WMCC_PEDW_DIAG IMMEDIATE;

ALTER TABLE SAILWWMCCV.WMCC_PEDW_DIAG ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_PEDW_DIAG ADD c19_cohort16 SMALLINT NULL;

INSERT INTO SAILWWMCCV.WMCC_PEDW_DIAG
    (SELECT * FROM (SELECT d.*,
                           e.c19_cohort20,
                           e.c19_cohort16
                    FROM SAILWWMCCV.WMCC_PEDW_EPISODE e
                    INNER JOIN SAILWMCCV.C19_COHORT_PEDW_DIAG d
                    ON e.prov_unit_cd = d.prov_unit_cd
                    AND e.spell_num_e = d.spell_num_e
                    AND e.epi_num = d.epi_num)
     WHERE diag_cd IS NOT null
    );

--------------------------------------------------------------------------------------------------
-- PEDW OPER
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_PEDW_OPER LIKE SAILWMCCV.C19_COHORT_PEDW_OPER;
--TRUNCATE TABLE SAILWWMCCV.WMCC_PEDW_OPER IMMEDIATE;

ALTER TABLE SAILWWMCCV.WMCC_PEDW_OPER ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_PEDW_OPER ADD c19_cohort16 SMALLINT NULL;

INSERT INTO SAILWWMCCV.WMCC_PEDW_OPER
    (SELECT o.*,
            e.c19_cohort20,
            e.c19_cohort16
     FROM SAILWWMCCV.WMCC_PEDW_EPISODE e
     INNER JOIN SAILWMCCV.C19_COHORT_PEDW_OPER o
     ON e.prov_unit_cd = o.prov_unit_cd
     AND e.spell_num_e = o.spell_num_e
     AND e.spell_num_e = o.spell_num_e
     WHERE o.oper_cd IS NOT null
    );

--------------------------------------------------------------------------------------------------
-- PEDW SUPER SPELL
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_PEDW_SUPERSPELL LIKE SAILWMCCV.C19_COHORT_PEDW_SUPERSPELL;
--TRUNCATE TABLE SAILWWMCCV.WMCC_PEDW_SUPERSPELL IMMEDIATE;

ALTER TABLE SAILWWMCCV.WMCC_PEDW_SUPERSPELL ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_PEDW_SUPERSPELL ADD c19_cohort16 SMALLINT NULL;

INSERT INTO SAILWWMCCV.WMCC_PEDW_SUPERSPELL
    (SELECT s.*,
            p.c19_cohort20,
            p.c19_cohort16
     FROM SAILWMCCV.C19_COHORT_PEDW_SUPERSPELL s
     JOIN SAILWWMCCV.WMCC_PEDW_SPELL p
     ON s.prov_unit_cd = p.prov_unit_cd
     AND s.spell_num_e = p.spell_num_e
    );

-- ***********************************************************************************************
-- Intensive Care National Audit & Research Centre (ICNARC)
-- https://web.www.healthdatagateway.org/dataset/add6226b-0f21-439a-84a6-51dc26cdc425
--------------------------------------------------------------------------------------------------
-- ICNARC ALF
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF LIKE SAILWMCCV.C19_COHORT_ICNC_ICNARC_LINKAGE_ALF;
--TRUNCATE TABLE SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF IMMEDIATE;

INSERT INTO SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF
    (SELECT *
     FROM SAILWMCCV.C19_COHORT_ICNC_ICNARC_LINKAGE_ALF
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
     OR alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16)
    );
ALTER TABLE SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF ADD c19_cohort16 SMALLINT NULL;

UPDATE SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF
SET c19_cohort20 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20);

UPDATE SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF
SET c19_cohort16 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16);

--------------------------------------------------------------------------------------------------
-- ICNARC EVENTS
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE LIKE SAILWMCCV.C19_COHORT_ICNC_ICNARC_LINKAGE;
--TRUNCATE TABLE SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE IMMEDIATE;

ALTER TABLE SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE ADD c19_cohort16 SMALLINT NULL;

INSERT INTO SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE
    (SELECT l.*,
            a.c19_cohort20,
            a.c19_cohort16
     FROM SAILWWMCCV.WMCC_ICNC_ICNARC_LINKAGE_ALF a
     INNER JOIN SAILWMCCV.C19_COHORT_ICNC_ICNARC_LINKAGE l
     ON a.system_id_e = l.system_id_e
    );

-- ***********************************************************************************************
-- Out Patient Dataset for Wales (OPDW)
-- https://web.www.healthdatagateway.org/dataset/d331159b-b286-4ab9-8b36-db39123ec229
--------------------------------------------------------------------------------------------------
-- OPDW 
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS LIKE SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS;
--TRUNCATE TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS IMMEDIATE;

INSERT INTO SAILWWMCCV.WMCC_OPDW_OUTPATIENTS
    (SELECT *
     FROM SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS
     WHERE (alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
           OR alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16))
    );

ALTER TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS ADD c19_cohort16 SMALLINT NULL;

UPDATE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS
SET c19_cohort20 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20);

UPDATE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS
SET c19_cohort16 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16);

--------------------------------------------------------------------------------------------------
-- OPDW DIAG
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_DIAG LIKE SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS_DIAG;
--TRUNCATE TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_DIAG IMMEDIATE;

ALTER TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_DIAG ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_DIAG ADD c19_cohort16 SMALLINT NULL;

INSERT INTO SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_DIAG
    (SELECT d.*,
            o.c19_cohort20,
            o.c19_cohort16
     FROM SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS_DIAG d
     INNER JOIN SAILWWMCCV.WMCC_OPDW_OUTPATIENTS o
     ON d.prov_unit_cd = o.prov_unit_cd
     AND d.case_rec_num_e = o.case_rec_num_e
     AND d.att_id_e = o.att_id_e
     AND d.attend_dt = o.attend_dt
    );

--------------------------------------------------------------------------------------------------
-- OPDW OPER
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_OPER LIKE SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS_OPER;
--TRUNCATE TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_OPER IMMEDIATE;

ALTER TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_OPER ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_OPER ADD c19_cohort16 SMALLINT NULL;

INSERT INTO SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_OPER
    (SELECT p.*,
            o.c19_cohort20,
            o.c19_cohort16
     FROM SAILWMCCV.C19_COHORT_OPDW_OUTPATIENTS_OPER p
     INNER JOIN SAILWWMCCV.WMCC_OPDW_OUTPATIENTS o
     ON p.prov_unit_cd = o.prov_unit_cd
     AND p.case_rec_num_e = o.case_rec_num_e
     AND p.att_id_e = o.att_id_e
     AND p.attend_dt = o.attend_dt
    );

-- ***********************************************************************************************
-- Welsh Longitudinal General Practice (WLGP)
-- https://web.www.healthdatagateway.org/dataset/33fc3ffd-aa4c-4a16-a32f-0c900aaea3d2
--------------------------------------------------------------------------------------------------
-- WLGP ALF
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED LIKE SAILWMCCV.C19_COHORT_WLGP_PATIENT_ALF_CLEANSED;
--TRUNCATE TABLE SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED IMMEDIATE;

INSERT INTO SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED
    (SELECT *
     FROM SAILWMCCV.C19_COHORT_WLGP_PATIENT_ALF_CLEANSED
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
     OR alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16)
    );

ALTER TABLE SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED ADD c19_cohort16 SMALLINT NULL;

UPDATE SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED
SET c19_cohort20 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20);

UPDATE SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED
SET c19_cohort16 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16);

----------------------------------------------------------------------------------------------------
-- WLGP EVENT
----------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED (
    alf_e  		      bigint,
    alf_sts_cd        char(2),
    alf_mtch_pct      decimal(7,6),
    gndr_cd           char(1),
    wob               date,
    lsoa_cd           char(10),
    prac_cd_e         bigint,
    event_cd          char(40),
    event_val         decimal(31,8),
    event_dt          date,
    episode           char(1),
    sequence          int
    )
DISTRIBUTE BY HASH(alf_e);
--TRUNCATE TABLE SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED IMMEDIATE;

INSERT INTO SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
    (SELECT alf_e, alf_sts_cd, alf_mtch_pct, gndr_cd, wob, lsoa_cd,
            prac_cd_e, event_cd, event_val, event_dt, episode, sequence
     FROM SAILWMCCV.C19_COHORT_WLGP_GP_EVENT_CLEANSED a
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED)
    );

ALTER TABLE SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED ADD c19_cohort16 SMALLINT NULL;

UPDATE SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
SET c19_cohort20 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED WHERE c19_cohort20 = 1);

UPDATE SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
SET c19_cohort16 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWWMCCV.WMCC_WLGP_PATIENT_ALF_CLEANSED WHERE c19_cohort16 = 1);

-- ***********************************************************************************************
-- Welsh Demographic Service Dataset (WDSD)
-- https://web.www.healthdatagateway.org/dataset/8a8a5e90-b0c6-4839-bcd2-c69e6e8dca6d
--------------------------------------------------------------------------------------------------
-- WDSD ADD
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_WDSD_AR_PERS_ADD (
    alf_e  		        bigint,
    gndr_cd             char(1),
    wob                 date,
    dod                 date,
    pers_id_e           bigint,
    ralf_e              bigint,
    from_dt             date,
    to_dt               date,
    lsoa2001_cd         char(10),
    lsoa2011_cd         char(10),
    uprn_qas_match_cd   char(50),
    row_sts             char(1),
    ralf_sts_cd         char(2),
    avail_from_dt       date
    )
DISTRIBUTE BY HASH(alf_e);
--TRUNCATE TABLE SAILWWMCCV.WMCC_WDSD_AR_PERS_ADD IMMEDIATE;

INSERT INTO SAILWWMCCV.WMCC_WDSD_AR_PERS_ADD
    (SELECT a.alf_e, a.gndr_cd, a.wob, a.dod, d.*
     FROM SAILWMCCV.C19_COHORT_WDSD_AR_PERS a
     JOIN SAILWMCCV.C19_COHORT_WDSD_AR_PERS_ADD d
     ON a.pers_id_e = d.pers_id_e
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
     OR alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16)
    );

ALTER TABLE SAILWWMCCV.WMCC_WDSD_AR_PERS_ADD ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_WDSD_AR_PERS_ADD ADD c19_cohort16 SMALLINT NULL;

UPDATE SAILWWMCCV.WMCC_WDSD_AR_PERS_ADD
SET c19_cohort20 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20);

UPDATE SAILWWMCCV.WMCC_WDSD_AR_PERS_ADD
SET c19_cohort16 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16);

--------------------------------------------------------------------------------------------------
-- WDSD GP
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_WDSD_AR_PERS_GP (
    alf_e  		        bigint,
    gndr_cd             char(1),
    wob                 date,
    dod                 date,
    pers_id_e           bigint,
    from_dt             date,
    to_dt               date,
    prac_cd_e           bigint,
    row_sts             char(1),
    avail_from_dt       date
    )
DISTRIBUTE BY HASH(alf_e);
--TRUNCATE TABLE SAILWWMCCV.WMCC_WDSD_AR_PERS_GP IMMEDIATE;

INSERT INTO SAILWWMCCV.WMCC_WDSD_AR_PERS_GP
    (SELECT a.alf_e, a.gndr_cd, a.wob, a.dod, g.*
     FROM SAILWMCCV.C19_COHORT_WDSD_AR_PERS a
     JOIN SAILWMCCV.C19_COHORT_WDSD_AR_PERS_GP g
     ON a.pers_id_e = g.pers_id_e
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
     OR alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16)
    );

ALTER TABLE SAILWWMCCV.WMCC_WDSD_AR_PERS_GP ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.WMCC_WDSD_AR_PERS_GP ADD c19_cohort16 SMALLINT NULL;

UPDATE SAILWWMCCV.WMCC_WDSD_AR_PERS_GP
SET c19_cohort20 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20);

UPDATE SAILWWMCCV.WMCC_WDSD_AR_PERS_GP
SET c19_cohort16 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16);

-- ***********************************************************************************************
-- Pathology data COVID-19 Daily (PATD)
-- https://web.www.healthdatagateway.org/dataset/f5f6d882-163d-4ef1-a53e-000fba409480
--------------------------------------------------------------------------------------------------
-- PATD LIMS ANTIBODYRESULTS
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_ANTIBODYRESULTS LIKE SAILWMCCV.C19_COHORT_PATD_DF_COVID_LIMS_ANTIBODYRESULTS;
--TRUNCATE TABLE SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_ANTIBODYRESULTS IMMEDIATE;

INSERT INTO SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_ANTIBODYRESULTS
    (SELECT *
     FROM SAILWMCCV.C19_COHORT_PATD_DF_COVID_LIMS_ANTIBODYRESULTS
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
    );

--------------------------------------------------------------------------------------------------
-- PATD LIMS TESTRESULTS
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_TESTRESULTS LIKE SAILWMCCV.C19_COHORT_PATD_DF_COVID_LIMS_TESTRESULTS;
--TRUNCATE TABLE SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_TESTRESULTS IMMEDIATE;

INSERT INTO SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_TESTRESULTS
    (SELECT *
     FROM SAILWMCCV.C19_COHORT_PATD_DF_COVID_LIMS_TESTRESULTS
     WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
    );

-- ***********************************************************************************************
-- Create a table containing all COVID19 related deaths
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.WMCC_DEATH_COVID19 LIKE SAILWMCCV.C19_COHORT20_MORTALITY;
--TRUNCATE TABLE SAILWWMCCV.WMCC_DEATH_COVID19 IMMEDIATE;

INSERT INTO SAILWWMCCV.WMCC_DEATH_COVID19
    SELECT *
    FROM SAILWMCCV.C19_COHORT20_MORTALITY
    WHERE covid_yn_underlying = 'y'
    OR covid_yn_underlying_qcovid = 'y'
    OR covid_yn_underlying_or_secondary = 'y'
    OR covid_yn_underlying_or_secondary_qcovid = 'y'
    OR covid_yn_secondary = 'y';

