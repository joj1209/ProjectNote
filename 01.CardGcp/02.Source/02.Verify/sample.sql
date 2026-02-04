DECLARE v_stat_dt DATE DEFAULT DATE '2026-01-01';
DECLARE v_seq INT64 DEFAULT 1;

INSERT INTO dataset_name.validation_summary
WITH
-- table1: 기준일자(stat_dt) 기준 필터링
table1_all AS (
  SELECT COUNT(*) AS total_count
  FROM dataset1.sales_2023_q1
),
table1_filtered AS (
  SELECT
    COUNT(*) AS filtered_count,
    SUM(sales_amount) AS filtered_sum
  FROM dataset1.sales_2023_q1
  WHERE stat_dt = DATE '2026-01-01'
),
table1_stats AS (
  SELECT
    'sales_2023_q1' AS table_name,
    v_stat_dt AS stat_dt,
    v_seq AS seq,
    total_count AS row_count,
    JSON_OBJECT(
      'filter_type', 'stat_dt',
      'stat_dt', '2026-01-01',
      'filtered_count', filtered_count,
      'filtered_sum', filtered_sum
    ) AS filter_stats_json
  FROM table1_all, table1_filtered
),

-- table2: 기준년월(stat_ym) 기준 필터링
table2_all AS (
  SELECT COUNT(*) AS total_count
  FROM dataset1.orders_2023_q1
),
table2_filtered AS (
  SELECT
    COUNT(*) AS filtered_count,
    SUM(order_total) AS filtered_sum
  FROM dataset1.orders_2023_q1
  WHERE stat_ym = '202601'
),
table2_stats AS (
  SELECT
    'orders_2023_q1' AS table_name,
    v_stat_dt AS stat_dt,
    v_seq AS seq,
    total_count AS row_count,
    JSON_OBJECT(
      'filter_type', 'stat_ym',
      'stat_ym', '202601',
      'filtered_count', filtered_count,
      'filtered_sum', filtered_sum
    ) AS filter_stats_json
  FROM table2_all, table2_filtered
)

-- 최종 적재
SELECT *, CURRENT_TIMESTAMP() AS created_at FROM table1_stats
UNION ALL
SELECT *, CURRENT_TIMESTAMP() AS created_at FROM table2_stats;
