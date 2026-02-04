DECLARE v_stat_dt DATE DEFAULT DATE '2026-01-01';
DECLARE v_seq INT64 DEFAULT 1;

INSERT INTO dataset_name.validation_summary
WITH
-- 예시 테이블: sales_2023_q1 (계수 컬럼: 상품금액, 구매횟수)
table1_all AS (
  SELECT COUNT(*) AS total_count
  FROM dataset1.sales_2023_q1
),
table1_filtered AS (
  SELECT
    COUNT(*) AS filtered_count,
    SUM(product_amount) AS total_product_amount,
    SUM(purchase_count) AS total_purchase_count
  FROM dataset1.sales_2023_q1
  WHERE stat_dt = v_stat_dt
),
table1_stats AS (
  SELECT
    'sales_2023_q1' AS table_name,
    v_stat_dt AS stat_dt,
    v_seq AS seq,
    total_count AS row_count,
    TO_JSON_STRING(STRUCT(
      'stat_dt' AS filter_type,
      CAST(v_stat_dt AS STRING) AS stat_dt,
      filtered_count,
      STRUCT(
        total_product_amount AS 상품금액,
        total_purchase_count AS 구매횟수
      ) AS metrics
    )) AS filter_stats_json
  FROM table1_all, table1_filtered
)

-- 최종 적재
SELECT *, CURRENT_TIMESTAMP() AS created_at FROM table1_stats;
