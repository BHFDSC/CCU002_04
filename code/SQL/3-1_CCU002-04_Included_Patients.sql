--************************************************************************************************
-- Script:       3-1_CCU002-04_Included_Patients.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
--               CCU002-04: To compare the long-term risk of stroke/MI in patients after
--               COVID-19 infection with other respiratory infections
-- About:        Derive list of eligible individuals for CCU002-04 project

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Quality assurance
-- ***********************************************************************************************
SELECT count(DISTINCT alf_e) FROM SAILWMCCV.C19_COHORT20;
--------------------------------------------------------------------------------------------------
-- Rule 1: check if year(dod) < year(wob), also in Wales, dod < wob
--------------------------------------------------------------------------------------------------
SELECT count(DISTINCT alf_e) FROM SAILWMCCV.C19_COHORT20 WHERE year(dod) < year(wob); -- 0
SELECT count(DISTINCT alf_e) FROM SAILWMCCV.C19_COHORT20 WHERE dod < wob; -- 0
--------------------------------------------------------------------------------------------------
-- Rule 2: all ALFs, gndr_cd and wob should be known
--------------------------------------------------------------------------------------------------
SELECT count(*) FROM SAILWMCCV.C19_COHORT20 WHERE gndr_cd IN NULL; -- 0
SELECT count(*) FROM SAILWMCCV.C19_COHORT20 WHERE wob IN NULL; -- 0
--------------------------------------------------------------------------------------------------
-- Rule 3: check if wob is over the current date
--------------------------------------------------------------------------------------------------
SELECT min(wob), max(wob) FROM SAILWMCCV.C19_COHORT20;
SELECT count(*) FROM SAILWMCCV.C19_COHORT20 WHERE current_date - wob < 0;
--------------------------------------------------------------------------------------------------
-- Rule 4: remove those with invalid dod --> already done for C20 cohort
--------------------------------------------------------------------------------------------------
SELECT count(*) FROM SAILWMCCV.C19_COHORT20 WHERE year(dod) < 2020; -- 0
--------------------------------------------------------------------------------------------------
-- Rule 5: remove those where registered dod < actual dod
--------------------------------------------------------------------------------------------------
SELECT count(DISTINCT alf_e) FROM SAILWMCCV.C19_COHORT20
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT_ADDE_DEATHS
                WHERE death_dt > death_reg_dt); -- 0
--------------------------------------------------------------------------------------------------
-- Rule 6: check if patients have missing event dates in GP data
--------------------------------------------------------------------------------------------------
SELECT count(*) FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
WHERE event_dt IS NULL; -- 0
-- ***********************************************************************************************
-- Inclusion
-- ***********************************************************************************************
-- Create table CCU002_04_INCLUDED_PATIENTS
--------------------------------------------------------------------------------------------------
CREATE TABLE SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329 LIKE SAILWMCCV.C19_COHORT20;
--DROP TABLE SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329;
--TRUNCATE TABLE SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329 IMMEDIATE;

ALTER TABLE SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329 ADD COVID_COHORT SMALLINT NULL;
ALTER TABLE SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329 ADD FLU_COHORT SMALLINT NULL;
ALTER TABLE SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329 ADD COVID_FLU_COHORTS SMALLINT NULL;

INSERT INTO SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
    (SELECT *,
            1 AS covid_cohort,
            NULL AS flu_cohort,
            NULL AS covid_flu_cohorts
     FROM SAILWMCCV.C19_COHORT20
     WHERE der_age_ >= 18    -- Exclude individuals whose age is under 18 at cohort start date
     AND gndr_cd IS NOT NULL -- Exclude individuals whose gndr_cd is null
     AND migration = 0       -- Exclude those who moved into Wales or born after 2020-01-01
     AND (dod >= '2020-01-01' OR dod IS NULL)
    );


INSERT INTO SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
    (SELECT alf_e,
            pers_id_e,
            wob,
            dod,
            gndr_cd,
            adde_dod,
            adde_dod_reg_dt,
            adde_alf_sts_cd,
            adde_deathcause_diag_underlying_cd,
            adde_deathcause_diag_sec_cause_cd,
            adde_deathcause_diag_1_cd,
            adde_deathcause_diag_2_cd,
            adde_deathcause_diag_3_cd,
            adde_deathcause_diag_4_cd,
            adde_deathcause_diag_5_cd,
            adde_deathcause_diag_6_cd,
            adde_deathcause_diag_7_cd,
            adde_deathcause_diag_8_cd,
            adde_death_communal_establishment_cd,
            adde_death_nhs_establishment_ind_cd,
            adde_death_establishment_type_cd,
            NULL AS cdds_death_dt,
            NULL AS cdds_death_registeration_dt,
            NULL AS adde_death_dt,
            NULL AS addd_dt_reg,
            der_age_,
            wds_start_date,
            wds_end_date,
            ralf_inception,
            NULL AS from_dt,
            NULL AS to_dt,
            lsoa2011_inception,
            WIMD2019_QUINTILE_INCEPTION,
            WIMD2019_QUINTILE_DESC_INCEPTION,
            healthboard_inception,
            carehome_ralf_inception,
            NULL AS cch_carehome_ralf_inception,
            dod_jl,
            cohort_start_date,
            move_out_date,
            cohort_end_date,
            NULL AS wdsd_cleaned_cohort_end_date,
            gp_start_date,
            gp_end_date,
            urban_rural_inception,
            gp_coverage_end_date,
            age_group,
            NULL AS pedw_lsoa2011,
            NULL AS migration,
            NULL AS migration_date,
            NULL AS covid_cohort,
            1 AS flu_cohort,
            NULL AS covid_flu_cohorts
     FROM SAILWMCCV.C19_COHORT16
     WHERE der_age_ >= 18    -- Exclude individuals whose age is under 18 at cohort start date
     AND gndr_cd IS NOT NULL -- Exclude individuals whose gndr_cd is null
     AND (dod >= '2016-01-01' OR dod IS NULL)
     AND alf_e NOT IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20)
    );

UPDATE SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
SET covid_cohort = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20);

UPDATE SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
SET flu_cohort = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16
                WHERE der_age_ >= 18);

UPDATE SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
SET covid_flu_cohorts = 1
WHERE covid_cohort = 1 AND flu_cohort = 1;


-- ***********************************************************************************************
-- Exclusion
-- ***********************************************************************************************
-- Exclude people with a posistive test before '2020-01-01'
--------------------------------------------------------------------------------------------------
SELECT covid19testresult, count(*) FROM  SAILWWMCCV.WMCC_PATD_DF_COVID_LIMS_TESTRESULTS
GROUP BY covid19testresult;
-- Negative, Positive, In Progress, No Result

DELETE FROM SAILWWMCCV.CCU002_04_INCLUDED_PATIENTS_20220329
WHERE alf_e IN (SELECT DISTINCT alf_e
                FROM SAILWWMCCV.PHEN_PATD_TESTRESULTS_COVID19
                WHERE test_date_cleaned < '2020-01-01'
                AND covid19testresult = 'Positive');
