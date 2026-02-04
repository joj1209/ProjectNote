CREATE TABLE dataset_name.validation_summary (
  table_name           STRING,      -- 원본 테이블 이름
  stat_dt              DATE,        -- 기준일자 (검증 기준일)
  seq                  INT64,       -- 실행 시퀀스 (버전)
  row_count            INT64,       -- 전체 건수
  filter_stats_json    STRING,      -- 조건 필터 건수 및 합계 (JSON)
  created_at           TIMESTAMP    -- 실행 시간
);
