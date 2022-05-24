--************************************************************************************************
-- Script:       3-5_CCU002-04_Flu_Cohort_CVD_Outcomes-1.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
-- About:        Create CVD related PHEN tables (primary care, hospital and death data)

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Create a table containing all CVD related deaths for C20 & C16 cohorts
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_DEATH_CVD (
    alf_e                      bigint,
    wob                        date,
    dod                        date,    -- WDSD dod in C20
    gndr_cd                    char(1),
    age_at_death               decimal(7,2),
    cod_underlying             char(4),
    cod_secondary              char(4),
    cod_ons_cdds               char(17),
    cod_ons_more_detail_cdds   char(27),
    cod1                       char(4),
    cod2                       char(4),
    cod3                       char(4),
    cod4                       char(4),
    cod5                       char(4),
    cod6                       char(4),
    cod7                       char(4),
    cod8                       char(4),
    death_sources              char(9),
    outcome_icd10              char(5),
    outcome_name               char(40),
    outcome_term               char(255),
    outcome_category           char(40),
    c19_cohort20               smallint,
    c19_cohort16               smallint
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.PHEN_DEATH_CVD;
TRUNCATE TABLE SAILWWMCCV.PHEN_DEATH_CVD IMMEDIATE;
----------------------------------------------------------------------------------------------
--CVD deaths for C20 cohort using table C19_COHORT20_MORTALITY
INSERT INTO SAILWWMCCV.PHEN_DEATH_CVD
    SELECT alf_e,
           wob,
           dod,
           gndr_cd,
           age_at_death,
           cod_underlying,
           cod_secondary_adde,
           cod_ons_cdds,
           cod_ons_more_detail_cdds,
           cod1,
           cod2,
           cod3,
           cod4,
           cod5,
           cod6,
           cod7,
           cod8,
           death_sources,
           d.code AS outcome_icd10,
           d.name AS outcome_name,
           d.desc AS outcome_term,
           d.category AS outcome_category,
           1 AS c19_cohort20,
           NULL AS c19_cohort16
    FROM SAILWMCCV.C19_COHORT20_MORTALITY c
    JOIN (SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_AMI
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_AMI_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ANGINA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DIC
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DVT_DVT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DVT_ICVT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_VT_CCU002
          WHERE is_latest = 1
          AND category <> 'VT_covariate_only'
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_HF
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_ANAEMIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PE
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PE_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_SCD
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_HS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_IS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_NOS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_ISCH_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_TCP
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_FRACTURE
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_THROMBUS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERIAL_EMBOLISM_OTHR
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH_HS_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_THROMBOPHILIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_RETINAL_INFARCTION
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SPINAL
          WHERE is_latest = 1
         ) d
    ON (cod_underlying = code OR LEFT(cod_underlying,3) = code OR cod_secondary_adde = code OR LEFT(cod_secondary_adde,3) = code
        OR cod1 = code OR LEFT(cod1,3) = code OR cod2 = code OR LEFT(cod2,3) = code OR cod3 = code OR LEFT(cod3,3) = code
        OR cod4 = code OR LEFT(cod4,3) = code OR cod5 = code OR LEFT(cod5,3) = code OR cod6 = code OR LEFT(cod6,3) = code
        OR cod7 = code OR LEFT(cod7,3) = code OR cod8 = code OR LEFT(cod8,3) = code)
    WHERE is_latest = 1;

----------------------------------------------------------------------------------------------
-- CVD deaths for C16 cohort using table C19_COHORT16_MORTALITY
INSERT INTO SAILWWMCCV.PHEN_DEATH_CVD
    SELECT alf_e,
           wob,
           dod,
           gndr_cd,
           age_at_death,
           cod_underlying,
           cod_secondary_adde,
           cod_ons_cdds,
           cod_ons_more_detail_cdds,
           cod1,
           cod2,
           cod3,
           cod4,
           cod5,
           cod6,
           cod7,
           cod8,
           death_sources,
           d.code AS outcome_icd10,
           d.name AS outcome_name,
           d.desc AS outcome_term,
           d.category AS outcome_category,
           NULL AS c19_cohort20,
           1 AS c19_cohort16
    FROM SAILWMCCV.C19_COHORT16_MORTALITY c
    JOIN (SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_AMI
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_AMI_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ANGINA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DIC
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DVT_DVT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DVT_ICVT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_VT_CCU002
          WHERE is_latest = 1
          AND category <> 'VT_covariate_only'
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_HF
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_ANAEMIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PE
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PE_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_SCD
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_HS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_IS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_NOS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_ISCH_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_TCP
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_FRACTURE
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_THROMBUS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERIAL_EMBOLISM_OTHR
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH_HS_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_THROMBOPHILIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_RETINAL_INFARCTION
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SPINAL
          WHERE is_latest = 1
         ) d
    ON (cod_underlying = code OR LEFT(cod_underlying,3) = code OR cod_secondary_adde = code OR LEFT(cod_secondary_adde,3) = code
        OR cod1 = code OR LEFT(cod1,3) = code OR cod2 = code OR LEFT(cod2,3) = code OR cod3 = code OR LEFT(cod3,3) = code
        OR cod4 = code OR LEFT(cod4,3) = code OR cod5 = code OR LEFT(cod5,3) = code OR cod6 = code OR LEFT(cod6,3) = code
        OR cod7 = code OR LEFT(cod7,3) = code OR cod8 = code OR LEFT(cod8,3) = code)
    WHERE is_latest = 1;


SELECT * FROM SAILWWMCCV.PHEN_DEATH_CVD;
SELECT count(*), count(DISTINCT alf_e) FROM SAILWWMCCV.PHEN_DEATH_CVD;

SELECT count(*) FROM SAILWWMCCV.PHEN_DEATH_CVD WHERE c19_cohort16 = 1
AND alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20_MORTALITY);

-- ***********************************************************************************************
-- Create a table containing all CVD outcomes in PEDW for C20 & C16 cohorts
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_PEDW_CVD (
    alf_e                      bigint,
    prov_unit_cd               char(3),
    spell_num_e                bigint,
    epi_num                    char(3),
    gndr_cd                    char(1),
    record_order               int,
    admis_dt                   date,
    admis_mthd_cd              char(2),
    admis_source_cd            char(2),
    admis_spec_cd              char(3),
    disch_dt                   date,
    disch_mthd_cd              char(2),
    disch_spec_cd              char(3),
    epi_str_dt                 date,
    epi_end_dt                 date,
    epi_diag_cd                char(20),
    res_ward_cd                char(2),
    diag_num                   int,
    primary_position           char(1),
    diag_cd_123                char(5),
    diag_cd                    char(20),
    outcome_icd10              char(5),
    outcome_name               char(40),
    outcome_term               char(255),
    outcome_category           char(40),
    c19_cohort20               smallint,
    c19_cohort16               smallint
    )
DISTRIBUTE BY HASH(alf_e);

-- DROP TABLE SAILWWMCCV.PHEN_PEDW_CVD;
TRUNCATE TABLE SAILWWMCCV.PHEN_PEDW_CVD IMMEDIATE;

INSERT INTO SAILWWMCCV.PHEN_PEDW_CVD (alf_e,prov_unit_cd,spell_num_e,epi_num,gndr_cd,record_order,admis_dt,
                                      admis_mthd_cd,admis_source_cd,admis_spec_cd,disch_dt,disch_mthd_cd,
                                      disch_spec_cd,epi_str_dt,epi_end_dt,epi_diag_cd,res_ward_cd,diag_num,
                                      primary_position,diag_cd_123,diag_cd,outcome_icd10,outcome_name,outcome_term,
                                      outcome_category,c19_cohort20,c19_cohort16)
    SELECT s.alf_e,
           s.prov_unit_cd,
           s.spell_num_e,
           e.epi_num,
           s.gndr_cd,
           ROW_NUMBER() OVER(PARTITION BY s.alf_e ORDER BY s.admis_dt, e.epi_str_dt) AS record_order,
           s.admis_dt,
           s.admis_mthd_cd,
           s.admis_source_cd,
           s.admis_spec_cd,
           s.disch_dt,
           s.disch_mthd_cd,
           s.disch_spec_cd,
           e.epi_str_dt,
           e.epi_end_dt,
           e.diag_cd_1234,
           s.res_ward_cd,
           d.diag_num,
           CASE WHEN d.diag_num = 1 THEN 1
                ELSE 0 END AS primary_position,
           d.diag_cd_123,
           d.diag_cd,
           d.code AS outcome_icd10,
           d.name AS outcome_name,
           d.desc AS outcome_term,
           d.category AS outcome_category,
           s.c19_cohort20,
           s.c19_cohort16
    FROM SAILWWMCCV.WMCC_PEDW_SPELL s
    LEFT JOIN SAILWWMCCV.WMCC_PEDW_EPISODE e
    ON s.prov_unit_cd = e.prov_unit_cd
    AND s.spell_num_e = e.spell_num_e
    LEFT JOIN SAILWWMCCV.WMCC_PEDW_DIAG d
    ON e.prov_unit_cd = d.prov_unit_cd
    AND e.spell_num_e = d.spell_num_e
    AND e.epi_num = d.epi_num
    JOIN (SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_AMI
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_AMI_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ANGINA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DIC
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DVT_DVT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DVT_ICVT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_VT_CCU002
          WHERE is_latest = 1
          AND category <> 'VT_covariate_only'
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_HF
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_ANAEMIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PE
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PE_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_SCD
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_HS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_IS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_NOS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_ISCH_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_TCP
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_FRACTURE
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_THROMBUS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERIAL_EMBOLISM_OTHR
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH_HS_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_THROMBOPHILIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_RETINAL_INFARCTION
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SPINAL
          WHERE is_latest = 1
         ) d
    ON (d.diag_cd_1234 = code OR d.diag_cd = code OR LEFT(d.diag_cd,3) = code);

SELECT count(*), count(DISTINCT alf_e) FROM SAILWWMCCV.PHEN_PEDW_CVD;
SELECT max(admis_dt) FROM SAILWWMCCV.PHEN_PEDW_CVD;

SELECT YEAR(admis_dt), count(*) FROM SAILWWMCCV.PHEN_PEDW_CVD
GROUP BY YEAR(admis_dt);
-- ***********************************************************************************************
-- Create a table containing all CVD outcomes in OPDW for C20 & C16 cohorts
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_OPDW_CVD (
    alf_e                      bigint,
    prov_unit_cd               char(6),
    case_rec_num_e             bigint,
    att_id_e                   bigint,
    attend_dt                  date,
    gndr_cd                    char(1),
    record_order               int,
    priority_type_cd           char(1),
    outcome_cd                 char(1),
    diag_cd_123                char(3),
    diag_cd                    char(20),
    diag_num                   int,
    outcome_icd10              char(5),
    outcome_name               char(40),
    outcome_term               char(255),
    outcome_category           char(40),
    c19_cohort20               smallint,
    c19_cohort16               smallint
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.PHEN_OPDW_CVD;
TRUNCATE TABLE SAILWWMCCV.PHEN_OPDW_CVD IMMEDIATE;

INSERT INTO SAILWWMCCV.PHEN_OPDW_CVD (alf_e,prov_unit_cd,case_rec_num_e,att_id_e,attend_dt,gndr_cd,
                                      record_order,priority_type_cd,outcome_cd,diag_cd_123,diag_cd,
                                      diag_num,outcome_icd10,outcome_name,outcome_term,outcome_category,
                                      c19_cohort20,c19_cohort16)
    SELECT alf_e,
           o.prov_unit_cd,
           o.case_rec_num_e,
           o.att_id_e,
           o.attend_dt,
           gndr_cd,
           ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY o.attend_dt) AS record_order,
           priority_type_cd,
           outcome_cd,
           diag_cd_123,
           CONCAT(diag_cd_123, diag_cd_4),
           diag_num,
           d.code AS outcome_icd10,
           d.name AS outcome_name,
           d.desc AS outcome_term,
           d.category AS outcome_category,
           o.c19_cohort20,
           o.c19_cohort16
    FROM SAILWWMCCV.WMCC_OPDW_OUTPATIENTS o
    LEFT JOIN SAILWWMCCV.WMCC_OPDW_OUTPATIENTS_DIAG d
    ON o.prov_unit_cd = d.prov_unit_cd
    AND o.case_rec_num_e = d.case_rec_num_e
    AND o.att_id_e = d.att_id_e
    AND o.attend_dt = d.attend_dt
    JOIN (SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_TIA_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_AMI
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_AMI_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ANGINA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DIC
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERY_DISSECT_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_CARDIOMYOPATHY_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DVT_DVT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_DVT_ICVT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_VT_CCU002
          WHERE is_latest = 1
          AND category <> 'VT_covariate_only'
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_HF
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_LIFE_ARRHYTHM_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_ANAEMIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PE
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PE_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_SCD
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_HS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_IS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_NOS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_ISCH_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_TCP
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_FRACTURE
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MESENTERIC_THROMBUS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_MYOCARDITIS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_ARTERIAL_EMBOLISM_OTHR
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_PERICARDITIS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SAH_HS_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_THROMBOPHILIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_RETINAL_INFARCTION
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_ICD10_STROKE_SPINAL
          WHERE is_latest = 1
         ) d
    ON CONCAT(diag_cd_123, diag_cd_4) = code OR diag_cd_123 = diag_cd_123;

SELECT diag_cd, count(*)  FROM SAILWWMCCV.PHEN_OPDW_CVD
GROUP BY diag_cd;

-- ***********************************************************************************************
-- Create a table containing all CVD outcomes in primary care data for C20 & C16 cohorts
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_WLGP_CVD (
    alf_e  		      bigint,
    wob               date,
    gndr_cd           char(1),
    prac_cd_e         bigint,
    record_order      int,
    event_dt          date,
    event_cd          char(40),
    event_val         decimal(31,8),
    outcome_readcode  char(5),
    outcome_name      char(30),
    outcome_term      char(255),
    outcome_category  char(40),
    c19_cohort20      smallint,
    c19_cohort16      smallint
    )
DISTRIBUTE BY HASH(alf_e);

-- DROP TABLE SAILWWMCCV.PHEN_WLGP_CVD;
TRUNCATE TABLE SAILWWMCCV.PHEN_WLGP_CVD IMMEDIATE;

INSERT INTO SAILWWMCCV.PHEN_WLGP_CVD (alf_e, wob, gndr_cd, prac_cd_e, record_order, event_dt, event_cd,
                                             event_val, outcome_readcode, outcome_name, outcome_term,
                                             outcome_category, c19_cohort20, c19_cohort16)
    SELECT alf_e,
           wob,
           gndr_cd,
           prac_cd_e,
           ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY event_dt) AS record_order,
           event_dt,
           event_cd,
           event_val,
           code AS outcome_readcode,
           name AS outcome_name,
           CASE WHEN DESC IS NULL THEN desc_ukhd
                ELSE desc 
                END AS outcome_term,
           LEFT(category, 40) AS outcome_category,
           c19_cohort20,
           c19_cohort16
    FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED
    JOIN (SELECT * FROM SAILWWMCCV.PHEN_READ_AMI
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_ARTERY_DISSECT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_SCD
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_HS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_SAH
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_IS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_CARDIOMYOPATHY
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_HF
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_LIMB_AMPUTATION
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_PE
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_NOS
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_TIA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_DVT_DVT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_DVT_ICVT
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_ANGINA
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_ISCH_CCU002
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_RETINAL_INFARCTION
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_ARTERIAL_EMBOLISM_OTHR
          WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_STROKE_SAH_HS_CCU002
          WHERE is_latest = 1
         )
    ON event_cd = code;

SELECT year(event_dt), count(*) FROM SAILWWMCCV.PHEN_WLGP_CVD
GROUP BY year(event_dt);
