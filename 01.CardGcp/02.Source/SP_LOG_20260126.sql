CREATE OR REPLACE PROCEDURE sp_log (
  IN in_prg_id STRING,
  IN in_prg_nm STRING,
  IN in_tbl_nm STRING,
  IN in_prg_tl_nm STRING,
  IN in_job_base_dt STRING,
  IN in_job_step_nbr INT64,
  IN in_frst_job_start_dtl_dttm STRING,
  IN in_job_start_dtl_dttm STRING,
  IN in_job_end_dtl_dttm STRING,
  IN in_data_prcs_cnt STRING,
  IN in_job_err_ltrs_nbr STRING,
  IN in_job_err_cntnt INT64,
  IN in_dml_gbn STRING,
  IN in_prg_sq STRING,
  OUT out_value STRING
)
BEGIN
  DECLARE v_runtime STRING;
  DECLARE v_runtime_1 STRING;
  DECLARE v_cnt INT64;
  DECLARE vs_job_step_nbr STRING;
  DECLARE vs_job_err_ltrs_nbr STRING;
  DECLARE vs_job_prgrs_rslt_cd STRING;
  DECLARE vs_job_err_cntnt STRING;
  
  -- 프로그램 분류 변수
  DECLARE vs_prg_gb STRING;
  DECLARE vs_sbj_gb STRING;
  DECLARE vs_ic_gb STRING;
  DECLARE vs_schema STRING;
  DECLARE vs_tbl_nm STRING;
  
  -- 카운트 변수
  DECLARE vs_ins_cnt INT64;
  DECLARE vs_del_cnt INT64;
  DECLARE vs_read_cnt INT64;
  DECLARE vs_rejt_cnt INT64;
  
  -- 날짜 변수
  DECLARE vs_yyyymm STRING;
  DECLARE vs_job_d STRING;
  DECLARE vs_work_d STRING;
  DECLARE vs_bse_smt STRING;
  DECLARE vs_bse_emt STRING;
  
  -- 파싱된 타임스탬프 (재사용)
  DECLARE dt_frst_start DATETIME;
  DECLARE dt_start DATETIME;
  DECLARE dt_end DATETIME;
  
  -- 프로그램 ID 파싱 변수 (한 번만 계산)
  DECLARE prg_id_upper STRING DEFAULT UPPER(in_prg_id);
  DECLARE prg_id_prefix_1 STRING DEFAULT UPPER(SUBSTR(in_prg_id, 1, 1));
  DECLARE prg_id_prefix_2 STRING DEFAULT UPPER(SUBSTR(in_prg_id, 2, 1));
  DECLARE prg_id_prefix_3 STRING DEFAULT UPPER(SUBSTR(in_prg_id, 1, 3));
  DECLARE prg_id_prefix_4 STRING DEFAULT UPPER(SUBSTR(in_prg_id, 1, 4));
  
  BEGIN
    -- 1. 타임스탬프 파싱 (한 번만 수행)
    SET dt_frst_start = PARSE_DATETIME("%Y%m%d%H%M%S", in_frst_job_start_dtl_dttm);
    SET dt_start = PARSE_DATETIME("%Y%m%d%H%M%S", in_job_start_dtl_dttm);
    SET dt_end = PARSE_DATETIME("%Y%m%d%H%M%S", in_job_end_dtl_dttm);
    
    -- 2. 런타임 계산 (초기화 개선)
    SET v_runtime_1 = COALESCE(
      FORMAT_TIME('%H%M%S', TIME_ADD(
        TIME '00:00:00',
        INTERVAL CAST(DATETIME_DIFF(dt_end, dt_frst_start, SECOND) AS INT64) SECOND
      )),
      'xx'
    );
    
    SET v_runtime = COALESCE(
      FORMAT_TIME('%H%M%S', TIME_ADD(
        TIME '00:00:00',
        INTERVAL CAST(DATETIME_DIFF(dt_end, dt_start, SECOND) AS INT64) SECOND
      )),
      'xx'
    );
    
    -- 3. 기본값 설정 (통합)
    SET vs_job_err_cntnt = CAST(in_job_err_cntnt AS STRING);
    SET vs_job_prgrs_rslt_cd = '99';
    SET vs_job_err_ltrs_nbr = in_job_err_ltrs_nbr;
    SET vs_job_step_nbr = LPAD(CAST(in_job_step_nbr AS STRING), 3, '0');
    
    -- 4. 카운트 초기화 및 DML 구분 처리 (CASE 문으로 통합)
    SET (vs_ins_cnt, vs_del_cnt, vs_read_cnt, vs_rejt_cnt) = (
      CASE WHEN in_dml_gbn = 'I' THEN CAST(COALESCE(in_data_prcs_cnt, '0') AS INT64) ELSE 0 END,
      CASE WHEN in_dml_gbn = 'D' THEN CAST(COALESCE(in_data_prcs_cnt, '0') AS INT64) ELSE 0 END,
      CASE WHEN in_dml_gbn = 'L' THEN CAST(COALESCE(in_data_prcs_cnt, '0') AS INT64) ELSE 0 END,
      CASE WHEN in_dml_gbn = 'R' THEN CAST(COALESCE(in_data_prcs_cnt, '0') AS INT64) ELSE 0 END
    );
    
    -- 5. 날짜 변수 설정 (LAST_DAY 오타 수정)
    SET vs_yyyymm = SUBSTR(in_job_base_dt, 1, 6);
    SET vs_job_d = in_job_base_dt;
    SET vs_work_d = CASE 
      WHEN LENGTH(TRIM(in_job_base_dt)) = 6 
      THEN CONCAT(SUBSTR(in_job_base_dt, 1, 6), '01')
      ELSE in_job_base_dt 
    END;
    SET vs_bse_smt = CONCAT(SUBSTR(in_job_base_dt, 1, 4), SUBSTR(in_job_base_dt, 5, 2), '01');
    SET vs_bse_emt = FORMAT_DATE(
      "%Y%m%d", 
      LAST_DAY(PARSE_DATE("%Y%m", SUBSTR(in_job_base_dt, 1, 6)))
    );
    
    -- 6. 프로그램 분류 로직 (최적화된 IF-ELSEIF 체인)
    IF prg_id_prefix_4 = 'MIG_' THEN
      SET vs_prg_gb = 'MG';
      SET vs_sbj_gb = 'MG';
      SET vs_ic_gb = 'INI';
      SET vs_schema = SUBSTR(in_tbl_nm, 1, STRPOS(in_tbl_nm, '.') - 1);
      SET vs_tbl_nm = SUBSTR(in_tbl_nm, STRPOS(in_tbl_nm, '.') + 1);
      
    ELSEIF prg_id_prefix_3 = 'AEB' THEN
      SET vs_prg_gb = 'IF';
      SET vs_sbj_gb = UPPER(SUBSTR(in_prg_id, 4, 3));
      SET vs_ic_gb = 'CHG';
      SET vs_schema = vs_sbj_gb;
      SET vs_tbl_nm = in_tbl_nm;
      
    ELSEIF prg_id_prefix_3 = 'WF_' THEN
      SET vs_prg_gb = 'STG';
      SET vs_sbj_gb = UPPER(SUBSTR(in_prg_id, 7, 2));
      SET vs_ic_gb = IF(UPPER(SUBSTR(in_prg_id, 4, 1)) = 'C', 'CHG', 'INI');
      SET vs_schema = SUBSTR(in_tbl_nm, 1, STRPOS(in_tbl_nm, '.') - 1);
      SET vs_tbl_nm = SUBSTR(in_tbl_nm, STRPOS(in_tbl_nm, '.') + 1);
      
    ELSEIF prg_id_prefix_1 = 'M' THEN
      SET vs_prg_gb = 'DM';
      SET vs_sbj_gb = UPPER(SUBSTR(in_prg_id, 4, 2));
      SET vs_ic_gb = IF(prg_id_prefix_2 = 'I', 'INI', 'CHG');
      SET vs_schema = SUBSTR(in_tbl_nm, 1, STRPOS(in_tbl_nm, '.') - 1);
      SET vs_tbl_nm = SUBSTR(in_tbl_nm, STRPOS(in_tbl_nm, '.') + 1);
      
    ELSE
      SET vs_prg_gb = 'DW';
      SET vs_sbj_gb = UPPER(SUBSTR(in_prg_id, 4, 2));
      SET vs_ic_gb = IF(prg_id_prefix_2 = 'I', 'INI', 'CHG');
      SET vs_schema = SUBSTR(in_tbl_nm, 1, STRPOS(in_tbl_nm, '.') - 1);
      SET vs_tbl_nm = SUBSTR(in_tbl_nm, STRPOS(in_tbl_nm, '.') + 1);
    END IF;
    
    -- 7. 에러 상태 처리
    IF COALESCE(in_job_err_ltrs_nbr, '0') = '0' THEN
      SET vs_job_err_ltrs_nbr = '0';
      SET vs_job_err_cntnt = ' ';
      SET vs_job_prgrs_rslt_cd = '00';
    END IF;
    
    -- 8. 중복 체크 (인덱스 활용을 위한 필터 최적화)
    SET v_cnt = (
      SELECT COUNT(1) 
      FROM dm_log 
      WHERE 프로그램ID = in_prg_id
        AND 작업시작일시 = dt_frst_start
    );
    
    -- 9. 로그 삽입/업데이트 로직
    IF v_cnt = 0 THEN
      -- 최초 실행: dm_log와 dm_log_dtl 동시 삽입
      INSERT INTO dm_log (
        영역구분명, 주제영역명, 초기변경구분코드, 프로그램ID, 스키마, 테이블명,
        기준일자, 작업단계번호, 작업시작일시, 작업종료일시, 작업경과시간, 차수,
        INSERT건수, DELETE건수, READ건수, REJECT건수, 작업종료여부, 작업결과코드,
        작업에러문자번호, 작업에러내용
      )
      VALUES (
        vs_prg_gb, vs_sbj_gb, vs_ic_gb, in_prg_id, COALESCE(vs_schema, 'ZZ'), vs_tbl_nm,
        vs_job_d, vs_job_step_nbr, dt_frst_start, dt_end, v_runtime_1, COALESCE(in_prg_sq, ' '),
        vs_ins_cnt, vs_del_cnt, vs_read_cnt, vs_rejt_cnt, 'N', vs_job_prgrs_rslt_cd,
        COALESCE(vs_job_err_ltrs_nbr, ''), COALESCE(vs_job_err_cntnt, ' ')
      );
      
      INSERT INTO dm_log_dtl (
        영역구분명, 프로그램ID, 단위작업명, 기준일자, 작업단계번호, 작업시작일시,
        작업단위시작일시, 작업단위종료일시, 작업소요시분초, 작업건수, 작업결과코드,
        작업에러문자번호, 작업에러내용
      )
      VALUES (
        vs_prg_gb, in_prg_id, in_prg_tl_nm, vs_job_d, vs_job_step_nbr, dt_frst_start,
        dt_start, dt_end, v_runtime_1, CAST(COALESCE(in_data_prcs_cnt, '0') AS INT64),
        vs_job_prgrs_rslt_cd, COALESCE(vs_job_err_ltrs_nbr, ' '), COALESCE(vs_job_err_cntnt, ' ')
      );
      
    ELSE
      -- 후속 실행: 상세 로그 삽입 후 마스터 로그 업데이트
      INSERT INTO dm_log_dtl (
        영역구분명, 프로그램ID, 단위작업명, 기준일자, 작업단계번호, 작업시작일시,
        작업단위시작일시, 작업단위종료일시, 작업소요시분초, 작업건수, 작업결과코드,
        작업에러문자번호, 작업에러내용
      )
      VALUES (
        vs_prg_gb, in_prg_id, in_prg_tl_nm, vs_job_d, vs_job_step_nbr, dt_frst_start,
        dt_start, dt_end, v_runtime, CAST(COALESCE(in_data_prcs_cnt, '0') AS INT64),
        vs_job_prgrs_rslt_cd, COALESCE(vs_job_err_ltrs_nbr, ' '), COALESCE(vs_job_err_cntnt, ' ')
      );
      
      -- UPDATE 문 통합 (작업단계번호에 따른 분기 제거)
      UPDATE dm_log 
      SET 작업단계번호 = vs_job_step_nbr,
          작업종료일시 = dt_end,
          작업경과시간 = v_runtime_1,
          INSERT건수 = INSERT건수 + vs_ins_cnt,
          DELETE건수 = DELETE건수 + vs_del_cnt,
          READ건수 = READ건수 + vs_read_cnt,
          REJECT건수 = REJECT건수 + vs_rejt_cnt,
          작업종료여부 = IF(in_job_step_nbr >= 998, 'Y', 'N'),
          작업결과코드 = vs_job_prgrs_rslt_cd,
          작업에러문자번호 = COALESCE(vs_job_err_ltrs_nbr, ''),
          작업에러내용 = COALESCE(vs_job_err_cntnt, '')
      WHERE 작업단위일자 = '2025-05-01'   --> 파티션
        and 프로그램ID = in_prg_id
        AND 작업시작일시 = dt_frst_start;
    END IF;
    
    SET out_value = ' ';
    
  EXCEPTION WHEN ERROR THEN
    SET out_value = @@error.message;
  END;
END;