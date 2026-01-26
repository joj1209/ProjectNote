create or replace procedure sp_log (
in in_prg_id string,
in in_prg_nm  string,
in in_tbl_nm  string,
in in_prg_tl_nm  string,
in in_job_base_dt  string,
in in_job_step_nbr  int64,
in in_frst_job_start_dtl_dttm  string,
in in_job_start_dtl_dttm  string,
in in_job_end_dtl_dttm  string,
in in_data_prcs_cnt  string,
in in_job_err_ltrs_nbr  string,
in in_job_err_cntnt  int64,
in in_dml_gbn  string,
in in_prg_sq  string,
out out_value  string)

BEGIN
declare v_runtime  string;
declare v_runtime_1  string;
declare v_value  string;
declare v_msg  string;
declare v_err_cd  string;
declare v_err_msg  string;
declare v_cnt  int64;
declare v_ced  string;
declare vs_yyyymm  string;
declare vs_job_d  string;
declare vs_work_d  string;
declare vs_bse_smt  string;
declare vs_bse_emt  string;
declare vs_check_date  string;
declare vs_job_err_ltrs_nbr  string;
declare vs_job_prgrs_rslt_cd  string;

declare l_sqlerrm  string;

declare vs_job_step_mbr  string;
declare vs_job_err_cntnt  string;
declare vs_sbj_gb  string;
declare vs_ic_gb  string;
declare vs_schema  string;
declare vs_tbl_nm  string;

declare vs_prg_gb  string;
declare vs_ins_cnt  string;
declare vs_del_cnt  string;
declare vs_read_cnt  string;
declare vs_rejt_cnt  string;

set vs_job_err_cntnt = in_job_err_cntnt;
set vs_job_prgrs_rslt_cd = '99';

set vs_ins_cnt = 0;
set vs_del_cnt = 0;
set vs_read_cnt = 0;
set vs_rejt_cnt = 0;

set in_data_prcs_cnt = coalesce(in_data_prcs_cnt,0);

/* 작업단계번호 셋팅 */
set vs_job_err_ltrs_nbr = in_job_err_ltrs_nbr;
set vs_job_step_nbr = lpad(cast(in_job_step_nbr as string),3,'0');

/* 주제영역 */
set vs_sbj_gb = upper(substr(in_prg_id,4,2));

/* 초기변경구분 */
if upper(substr(in_prg_id,2,1)) = 'I' then set vs_ic_gb = 'INI';
else set vs_ic_gb = 'CHG';
end if;

/* 스키마/테이블명 셋팅 */
set vs_schema = substr(in_tbl_nm,1,strpos(in_tbl_nm,'.')-1);
set vs_tbl_nm = substr(in_tbl_nm,strpos(in_tbl_nm,'.')+1);

/* 영역구분/주제영역/초기변경구분 셋팅 */
if upper(substr(in_prg_id,1,4)) = 'MIG_'
then set vs_prg_gb = 'MG'; 
  set vs_sbj_gb = 'MG';
  set vs_ic_gb = 'INI';
  set vs_schema = substr(in_tbl_nm,1,strpos(in_tbl_nm,'.')-1);
elseif upper(substr(in_prg_id,1,3)) = 'AEB'
then set vs_prg_gb = 'IF';
  set vs_sbj_gb = upper(substr(in_prg_id,4,3));
  set vs_ic_gb = 'CHG';
  set vs_schema = vs_sbj_gb;
elseif upper(substr(in_prg_id,1,3)) = 'WF_'
then set vs_prg_gb = 'STG'
  set vs_sbj_gb = upper(substr(in_prg_id,7,2));
  if upper(substr(in_prg_id,1,1)) = 'C' then set vs_ic_gb = 'CHG'; else set vs_ic_gb = 'INI'; end if;
elseif upper(substr(in_prg_id,1,1)) = 'M' then set vs_prg_gb = 'DM';
else set vs_prg_gb = 'DW';
end if;

if in_dml_gbn = 'I' then set vs_ins_cnt = in_data_prcs_cnt;
elseif in_dml_gbn = 'I' then set vs_del_cnt = in_data_prcs_cnt;
elseif in_dml_gbn = 'L' then set vs_read_cnt = in_data_prcs_cnt;
elseif in_dml_gbn = 'R' then set vs_rejt_cnt = in_data_prcs_cnt;
else set vs_ins_cnt = 0;
end if; 

set ( vs_yyyymm, vs_job_d, vs_work_d, vs_bse_smt, vs_bse_emt )
= (select as struct substr(in_job_base_dt,1,6)
, in_job_base_dt
, case when length(trim(in_job_base_dt)) = 6 then substr(in_job_base_dt,1,6)||'01'
else in_job_base_dt end
, substr(in_job_base_dt,1,4)||substr(in_job_base_dt,5,2)||'01'
, format_date("%Y%m%d",lase_day(parse_date("%Y%m",substr(in_job_base_dt,1,6))))
);

/* 프로그램 최초 실행 시간 */
set v_run_time_1 = (select coalesce(format_time('%H%M%S',time_add(time 00:00:00',interval(unix_seconds(parse_timestamp('%Y%m%d%H%M%S',in_job_end_dtl_dttm)) - unix_seconds(parse_timestamp('%Y%m%d%H%M%S',in_frst_job_start_dtl_dttm))) second)), 'xx'));

set v_run_time = (select coalesce(format_time('%H%M%S',time_add(time 00:00:00',interval(unix_seconds(parse_timestamp('%Y%m%d%H%M%S',in_job_end_dtl_dttm)) - unix_seconds(parse_timestamp('%Y%m%d%H%M%S',in_job_start_dtl_dttm))) second)), 'xx'));

if coalesce(in_job_err_ltrs_nbr,'0')='0'
then set vs_joberr_ltrs_nbr = '0';
set vs_job_err_cntnt = ' ';
set vs_job_prgrs_rslt_cd = '00';
end if;

set v_cnt = (select count(1) from dm_log where 프로그램id = in_prg_id
and 작업시작일시 = parse_datetime("%Y%m%d%H%M%S",in_frst_job_start_dtl_dttm));

if v_cnt = 0 then
/* 처음 시작이고 프로그램 종료가 아닌경우 프로그램 시작로그 입력 */
insert into dm_log (영역구분명, 주제영역명, 초기변경구분코드, 프로그램ID, 스키마, 테이블명, 기준일자, 작업단계번호, 작업시작일시, 작업종료일시, 작업경과시간, 차수, INSERT건수, DELETE건수, READ건수, REJECT건수, 작업종료여부, 작업결과코드, 작업에러문자번호, 작업에러내용)
select vs_prg_gb as `영역구분명`
, vs_sbj_gb as `주제영역명`
, vs_ic_gb as `초기변경구분코드`
, in_prg_id as `프로그램ID`
, coalesce(vs_schema,'ZZ') as `스키마`
, vs_tbl_nm as `테이블명`
, vs_job_d as `기준일자`
, vs_job_step_nbr as `작업단계번호`
, parse_datetime("%Y%m%d%H%M%S",in_frst_job_start_dtl_dttm) as `작업시작일시`
, parse_datetime("%Y%m%d%H%M%S",in_job_end_dtl_dttm) as `작업종료일시`
, v_runtime_1 as `작업경과시간`
, coalesce(in_prg_sq, ' ') as `차수`
, coalesce(vs_int_cnt,0) as `INSERT건수`
, coalesce(vs_del_cnt,0) as `DELETE건수`
, coalesce(vs_read_cnt,0) as `READ건수`
, coalesce(vs_rejt_cnt,0) as `REJECT건수`
,'N' as `작업종료여부`
, vs_job_prgrs_rslt_cd as `작업결과코드`
, coalesce(vs_job_err_ltrs_nbr,'') as `작업에러문자번호`
, coalesce(vs_job_err_cntnt, ' ') as `작업에러내용`
;

/* 단위작업에도 입력 */
insert into dm_log_dtl (영역구분명, 프로그램ID, 단위작업명, 기준일자, 작업단계번호, 작업시작일시, 작업단위시작일시, 작업단위종료일시, 작업소요시분초, 작업건수, 작업결과코드, 작업에러문자번호, 작업에러내용)
select vs_prg_gb
, in_prg_id
, in_prg_tl_nm
, vs_job_d
, vs_job_step_nbr. -- 작업단계번호
, parse_datetime("%Y%m%d%H%M%S",in_frst_job_start_dtl_dttm) --작업시작일시
, parse_datetime("%Y%m%d%H%M%S",in_job_start_Dtl_dttm) --작업단위시작상세일시
, parse_datetime("%Y%m%d%H%M%S",in_job_end_dtl_dttm) --작업단위종료상세일시
, v_runtime_1
, in_data_prcs_cnt
, vs_job_prgrs_rslt_cd  --작업결과코드
, coalesce(vs_job_err_ltrs_nbr,' ') --작업에러문자번호
, coalesce(vs_job_err_cntnt, ' ') --작업에러내용
;

elseif v_cnt > 0 then

/* STEP에 대한 성공 로그 */
insert into dm_log_dtl (영역구분명, 프로그램ID, 단위작업명, 기준일자, 작업단계번호, 작업시작일시, 작업단위시작일시, 작업단위종료일시, 작업소요시분초, 작업건수, 작업결과코드, 작업에러문자번호, 작업에러내용)
select vs_prg_gb
, in_prg_id
, in_prg_tl_nm
, vs_job_d
, vs_job_step_nbr  --작업단계번호
, parse_datetime("%Y%m%d%H%M%S",in_frst_job_start_dtl_dttm) --작업시작일시
, parse_datetime("%Y%m%d%H%M%S",in_job_start_Dtl_dttm) --작업단위시작상세일시
, parse_datetime("%Y%m%d%H%M%S",in_job_end_dtl_dttm) --작업단위종료상세일시
, v_runtime
, in_data_prcs_cnt
, vs_job_prgrs_rslt_cd --작업결과코드
, coalesce(vs_job_err_ltrs_nbr,' ') --작업에러문자번호
, coalesce(vs_job_err_cntnt, ' ') --작업에러내용
;

if in_job_step_nbr < 998 then   --통계정보나 파티션 exchange가 아니면

update dm_log 
set `작업단계번호` = vs_job_step_nbr  
, `작업종료일시` = parse_datetime("%Y%m%d%H%M%S",in_job_end_dtl_dttm) 
, `작업경과시간` = v_runtime_1 
, `INSERT건수` = `INSERT건수` + vs_int_cnt 
, `DELETE건수` = `DELETE건수` + vs_del_cnt 
, `READ건수` = `READ건수` + vs_read_cnt
, `REJECT건수` = `REJECT건수` + vs_rejt_cnt 
, `작업종료여부` = 'N' --작업종료여부 
, `작업결과코드` = vs_job_prgrs_rslt_cd --작업결과코드 
, `작업에러문자번호` = coalesce(vs_job_err_ltrs_nbr,'') --작업에러문자번호 
, `작업에러내용` = coalesce(vs_job_err_cntnt,'') --작업에러내용  
where `프로그램ID` = in_prg_id 
and 작업시작일시 = parse_datetime("%Y%m%d%H%M%S",in_frst_job_start_dtl_dttm); --작업기준일자

elseif in_job_step_nbr >= 998 then  --통계정보나 파티션 exchange
update dm_log 
set `작업단계번호` = vs_job_step_nbr  
, `작업종료일시` = parse_datetime("%Y%m%d%H%M%S",in_job_end_dtl_dttm) 
, `작업경과시간` = v_runtime_1 
, `INSERT건수` = `INSERT건수` + vs_int_cnt 
, `DELETE건수` = `DELETE건수` + vs_del_cnt 
, `READ건수` = `READ건수` + vs_read_cnt
, `REJECT건수` = `REJECT건수` + vs_rejt_cnt 
, `작업종료여부` = 'Y' --작업종료여부 
, `작업결과코드` = vs_job_prgrs_rslt_cd --작업결과코드 
, `작업에러문자번호` = coalesce(vs_job_err_ltrs_nbr,'') --작업에러문자번호 
, `작업에러내용` = coalesce(vs_job_err_cntnt,'') --작업에러내용  
where `프로그램ID` = in_prg_id 
and 작업시작일시 = parse_datetime("%Y%m%d%H%M%S",in_frst_job_start_dtl_dttm); --작업기준일자
;
end if;

end if;

set out_value = ' ';
exception when error then
set out_value = @@error.message;
end;


위의 프로시저는 오라클의 로그 적재 프로시저를 빅쿼리로 변환했음
그런데 오라클에서는 로그 프로시져를 호출하여 적재하는데 시간이 금방 끝나는데 빅쿼리에서는 오라클에 비해서 많은 시간이 발생함, 예를 들어 스크립트안에 쿼리문이 10개이면 10번을 호출하는데 오라클에서는 빠른 속도로 동작했음, 위의 프로시저를 분석하여 튜닝포인트를 제시하시오


1.dm_log
영역구분명 string(10)
주제영역명 string(5)
초기변경구분코드 string(3)
프로그램ID string(100)
기준일자 string(8)
작업시작일시 datetime
작업종료일시 datetime
스키마 string(5)
테이블명 string(200)
작업단계번호 string(3)
작업경과시간 string(6)
차수 string(2)
insert건수 numeric(12)
delete건수 numeric(12)
read건수 numeric(12)
reject건수 numeric(12)
작업결과코드 string(2)
작업종료여부 string(1)
작업에러문자번호 string(200)
작업에러내용 string(1000)

dm_log_dtl
영역구분명 string(10)
프로그램ID string(100)
기준일자 string(8)
작업시작일시 datetime
작업종료일시 datetime
단위작업명 string(100)
작업단위시작일시 datetime
작업단위종료일시 datetime
작업소요시분초 string(6)
작업건수 numeric(12)
작업결과코드 string(2)
작업에러문자번호 string(200)
작업에러내용 string(1000)


2. 로그 프로시저는 프로그램 1개에서 최소 2번 이상 호출 , 쿼리가 10개면 11번 호출


3. 메인 로그에는 업데이트로 상태를 남겨야 함