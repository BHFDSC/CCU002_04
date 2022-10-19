--************************************************************************************************
-- Script:       2-2_PHEN_RESPIRATORY_INFECTIONS.sql
-- SAIL project: WMCC - Wales Multi-morbidity Cardiovascular COVID-19 UK (0911)
-- About:        Creating COVID-19 related PHEN tables

-- Author:       Hoda Abbasizanjani
--               Health Data Research UK, Swansea University, 2021
-- ***********************************************************************************************
-- ***********************************************************************************************
-- Create a table containing all respiratory infections related records in hospital data
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_PEDW_RESPIRATORY_INFECTIONS (
    alf_e                      bigint,
    prov_unit_cd               char(3),
    spell_num_e                bigint,
    epi_num                    char(3),
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
    diag_cd_123                char(3),
    diag_cd                    char(20),
    icd10_cd                   char(4),
    icd10_cd_category          char(50),
    icd10_cd_desc              char(255),
    c19_cohort20               smallint,
    c19_cohort16               smallint
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.PHEN_PEDW_RESPIRATORY_INFECTIONS;
--TRUNCATE TABLE SAILWWMCCV.PHEN_PEDW_RESPIRATORY_INFECTIONS IMMEDIATE;

INSERT INTO SAILWWMCCV.PHEN_PEDW_RESPIRATORY_INFECTIONS (alf_e,prov_unit_cd,spell_num_e,epi_num,record_order,admis_dt,
                                          admis_mthd_cd,admis_source_cd,admis_spec_cd,disch_dt,disch_mthd_cd,
                                          disch_spec_cd,epi_str_dt,epi_end_dt,epi_diag_cd,res_ward_cd,diag_num,
                                          primary_position,diag_cd_123,diag_cd,icd10_cd,icd10_cd_category,icd10_cd_desc,
                                          c19_cohort20,c19_cohort16)
    SELECT s.alf_e,
           s.prov_unit_cd,
           s.spell_num_e,
           e.epi_num,
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
    JOIN (SELECT *
          FROM SAILWWMCCV.PHEN_ICD10_RESP_INFECTIONS
          WHERE is_latest = 1
          ) c
    ON (LEFT(d.diag_cd,4) = c.code OR LEFT(d.diag_cd,3) = c.code)
    WHERE s.admis_yr <= 2022
    ORDER BY s.alf_e, s.admis_dt, e.epi_str_dt;


-- ***********************************************************************************************
-- Create a table containing all respiratory infections related records in GP data
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_WLGP_RESPIRATORY_INFECTIONS (
    alf_e                      bigint,
    wob                        date,
    gndr_cd                    char(1),
    prac_cd_e                  integer,
    record_order               integer,
    event_dt                   date,
    event_cd                   char(40),
    event_cd_desc              char(255),
    event_cd_category          char(35),
    event_val                  decimal(31,8),
    c19_cohort20               smallint,
    c19_cohort16               smallint
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.PHEN_WLGP_RESPIRATORY_INFECTIONS;
--TRUNCATE TABLE SAILWWMCCV.PHEN_WLGP_RESPIRATORY_INFECTIONS IMMEDIATE;

INSERT INTO SAILWWMCCV.PHEN_WLGP_RESPIRATORY_INFECTIONS (alf_e, wob, gndr_cd, prac_cd_e, record_order,
                                                         event_dt, event_cd, event_cd_desc, event_cd_category,
                                                         event_val, c19_cohort20, c19_cohort16)
    SELECT alf_e,
           wob,
           gndr_cd,
           prac_cd_e,
           ROW_NUMBER() OVER(PARTITION BY alf_e ORDER BY event_dt) AS record_order,
           event_dt,
           event_cd,
           r.desc AS event_cd_desc,
           r.category AS event_cd_category,
           event_val,
           c19_cohort20,
           c19_cohort16
    FROM SAILWWMCCV.WMCC_WLGP_GP_EVENT_CLEANSED g
    JOIN (SELECT * FROM SAILWWMCCV.PHEN_READ_INFLUENZA WHERE is_latest = 1
          UNION ALL
          SELECT * FROM SAILWWMCCV.PHEN_READ_PNEUMONIA WHERE is_latest = 1
          ) r
    ON g.event_cd = r.code;

-- ***********************************************************************************************
-- Create a table containing all respiratory infections in WRRS for C20 cohort
-- ***********************************************************************************************
CREATE TABLE SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS (
    alf_e                      bigint,
    test_collected_date        date,
    test_received_date         date,
    test_code                  char(30),
    test_name                  char(90),
    test_value                 char(100),
    test_value_numeric         char(50),
    test_value_type            char(3),
    test_value_unit            char(25),
    test_abnormal_result       char(3)
    )
DISTRIBUTE BY HASH(alf_e);

--DROP TABLE SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS;
TRUNCATE TABLE SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS IMMEDIATE;


INSERT INTO SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS
     SELECT b.alf_e,
            q.spcm_collected_dt,
            q.spcm_received_dt,
            b.code,
            b.name,
            b.val,
            regexp_substr(b.val, '^\d\d*\.?\d* *$'),
            b.val_type,
            b.unitofmeasurement,
            q.abnormalresults
     FROM SAILWMCCV.C19_COHORT_WRRS_OBSERVATION_RESULT b
     LEFT JOIN SAILWMCCV.C19_COHORT_WRRS_OBSERVATION_REQUEST d
     ON b.alf_e = d.alf_e
     AND b.report_seq = d.report_seq
     AND b.request_seq = d.request_seq
     LEFT JOIN SAILWMCCV.C19_COHORT_WRRS_REPORT q
     ON d.alf_e = q.alf_e
     AND d.report_seq = q.report_seq 
	 WHERE (code IN ('FLUAPCR','FLUBPCR','HMPVPCR','Haemophilus influenz','RSV','RSV viral result',
		             'RSVPCR','ADPCR','BORPCR','Bordetella pertussis','CHPCR','MPAG','MPCF','MYCPCR',
		              'MYPC','Mycoplasma pneumonia','PFPCR','PNEP','PSEUCV','PSEUDOC','RHIPCR',
		              'Streptococcus pneumo','SCOV2PCR','PNEUM', -- (IgG Antibodies to Pneumoccocus)
		               'Parainfluenza','PF1','PF2','PF3','PF4','Paraflu 1','Paraflu 2','Paraflu 3'
		               'RSCF','RSV','RSVD','RSVPCR','RSVPOCT','POCTINFBRNA'
					   'Influenza A','Influenzae A result','POCTINFARNA','Influenza B','Influenzae B result')
            OR b.name IN ('Respiratory Syncytial Virus PCR', 'Respiratory syncytial virus',
                          'Respiratory Syncytial Virus', 'Respiratory Syncytial Virus POCT', 'Respiratory syncitia'
                         )
            )
      AND val IS NOT NULL
      AND val NOT LIKE '%Not%'
      AND val NOT LIKE '%NOT%'
      AND val NOT LIKE '%not%'
      AND val NOT LIKE '%Insufficient%'
      AND val NOT LIKE '%INSUFFICIENT%'
      AND val NOT LIKE '%telephoned%'
      AND val NOT LIKE '%assayed%'
      AND val NOT LIKE '%culture%'
      AND val NOT LIKE '%Negative%'
      AND val NOT LIKE '%negative%'
      AND val NOT LIKE '%NEG%'
      AND val NOT LIKE '%CONB%'
      AND val NOT LIKE '%to follow%'
      AND val NOT LIKE '%redacted%'
      AND val NOT LIKE '%repeat%'
      AND val NOT LIKE '%Repeat%'
      AND val NOT LIKE '%REPEAT%'
      AND val NOT LIKE '%requested%'
      AND val NOT LIKE '%What%'
      AND val NOT LIKE '%Report%'
      AND val NOT LIKE '%report%'
      AND val NOT LIKE '%contact%'
      AND val NOT LIKE '%sample%'
      AND val NOT LIKE '%See Comment%'
      AND val NOT LIKE '%N/A%'
      AND val NOT LIKE '%REQUESTED%'
      AND val NOT LIKE '%DUPLICATE%'
      AND val NOT LIKE '%LABORATORY%'
      AND val NOT LIKE '%tests.%'
      AND val NOT LIKE '%assistance%'
      AND val NOT LIKE '%PLEASE%'
      AND val NOT LIKE '%result.%'
      AND val NOT LIKE '%risk.%'
      AND val NOT LIKE '%Equivocal%'
      AND val NOT LIKE '%INDETERMINATE%'
      AND val NOT LIKE '%*%'
    AND (b.alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16)
    OR b.alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20))
;

ALTER TABLE SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS ADD c19_cohort20 SMALLINT NULL;
ALTER TABLE SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS ADD c19_cohort16 SMALLINT NULL;

UPDATE SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS
SET c19_cohort20 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT20);

UPDATE SAILWWMCCV.PHEN_WRRS_RESPIRATORY_INFECTIONS
SET c19_cohort16 = 1
WHERE alf_e IN (SELECT DISTINCT alf_e FROM SAILWMCCV.C19_COHORT16);
