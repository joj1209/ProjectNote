mkdir -p sql/udf doc && cat > sql/udf/substrb_euckr.sql <<'SQL'
-- Oracle SUBSTRB(EUC-KR) compatible(ish) BigQuery UDF
--
-- BigQuery strings are UTF-8. Oracle SUBSTRB uses database character set bytes.
-- For common EUC-KR usage, ASCII is 1 byte and most Korean chars are 2 bytes.
-- This UDF emulates SUBSTRB byte offsets under a *simplified* EUC-KR byte-width model:
--   - code point <= 0x7F  => 1 byte
--   - otherwise           => 2 bytes
--
-- IMPORTANT LIMITATIONS
-- - This does NOT perform real EUC-KR encoding.
-- - Characters not representable in EUC-KR (e.g., emoji) are still treated as 2 bytes here.
-- - To keep returned strings valid UTF-8, it will not cut inside a character.
--
-- Usage (temporary):
--   CREATE TEMP FUNCTION substrb_euckr(s STRING, pos INT64, len INT64) AS (...);
-- Usage (permanent):
--   CREATE OR REPLACE FUNCTION your_dataset.substrb_euckr(s STRING, pos INT64, len INT64) AS (...);

CREATE TEMP FUNCTION byte_length_euckr(s STRING)
RETURNS INT64
AS (
  IF(
    s IS NULL,
    NULL,
    (
      SELECT SUM(IF(cp <= 0x7F, 1, 2))
      FROM UNNEST(TO_CODE_POINTS(s)) AS cp
    )
  )
);

CREATE TEMP FUNCTION substrb_euckr(s STRING, pos INT64, len INT64)
RETURNS STRING
AS (
  (
    WITH
      cps AS (
        SELECT TO_CODE_POINTS(s) AS cps
      ),
      chars AS (
        SELECT
          off AS idx,
          cp,
          CODE_POINTS_TO_STRING([cp]) AS ch,
          IF(cp <= 0x7F, 1, 2) AS b
        FROM cps, UNNEST(cps) AS cp WITH OFFSET off
      ),
      spans AS (
        SELECT
          idx,
          ch,
          b,
          SUM(b) OVER(ORDER BY idx) AS byte_end,
          SUM(b) OVER(ORDER BY idx) - b + 1 AS byte_start
        FROM chars
      ),
      totals AS (
        SELECT IFNULL(MAX(byte_end), 0) AS total_bytes FROM spans
      ),
      normalized AS (
        SELECT
          s AS input_s,
          IF(pos IS NULL, NULL, IF(pos = 0, 1, pos)) AS pos1,
          len AS len1,
          total_bytes
        FROM totals
      ),
      bounds AS (
        SELECT
          input_s,
          total_bytes,
          -- Oracle: pos>0 from start, pos<0 from end
          GREATEST(
            1,
            CASE
              WHEN pos1 IS NULL THEN NULL
              WHEN pos1 > 0 THEN pos1
              ELSE total_bytes + pos1 + 1
            END
          ) AS start_byte,
          CASE
            WHEN len1 IS NULL THEN total_bytes
            WHEN len1 <= 0 THEN 0
            ELSE GREATEST(
              0,
              GREATEST(
                1,
                CASE
                  WHEN pos1 IS NULL THEN NULL
                  WHEN pos1 > 0 THEN pos1
                  ELSE total_bytes + pos1 + 1
                END
              ) + len1 - 1
            )
          END AS end_byte
        FROM normalized
      )
    SELECT
      CASE
        WHEN s IS NULL OR pos IS NULL THEN NULL
        WHEN (SELECT total_bytes FROM bounds) = 0 THEN ''
        WHEN (SELECT start_byte FROM bounds) > (SELECT total_bytes FROM bounds) THEN ''
        WHEN (SELECT end_byte FROM bounds) < (SELECT start_byte FROM bounds) THEN ''
        ELSE (
          SELECT IFNULL(STRING_AGG(ch, '' ORDER BY idx), '')
          FROM spans
          WHERE byte_start >= (SELECT start_byte FROM bounds)
            AND byte_end   <= (SELECT end_byte FROM bounds)
        )
      END
  )
);

-- Quick sanity examples
-- SELECT
--   byte_length_euckr('ABC가나다') AS bl,
--   substrb_euckr('ABC가나다', 1, 3)  AS ex1,  -- 'ABC'
--   substrb_euckr('ABC가나다', 4, 2)  AS ex2,  -- '가'
--   substrb_euckr('ABC가나다', 4, 4)  AS ex3,  -- '가나'
--   substrb_euckr('ABC가나다', -4, 4) AS ex4;  -- '나'
SQL

cat > doc/bigquery_substrb_euckr_udf.md <<'MD'
# Oracle SUBSTRB(EUC-KR) 대응 BigQuery UDF

오라클 `SUBSTRB`는 **DB 캐릭터셋(EUC-KR) 바이트 기준**으로 문자열을 잘라냅니다.
BigQuery는 문자열을 **UTF-8**로 저장/처리하므로, 오라클에서 사용하던 `SUBSTRB`의 `pos`, `len`(바이트 단위) 로직을 그대로 옮기면 결과가 달라집니다.

이 워크스페이스의 [sql/udf/substrb_euckr.sql](../sql/udf/substrb_euckr.sql) 은 아래 가정 하에 `SUBSTRB` 유사 동작을 제공하는 UDF입니다.

## 핵심 아이디어
- EUC-KR에서 흔히 사용되는 바이트 폭 모델을 사용합니다.
  - ASCII(0x00~0x7F): 1 byte
  - 그 외 문자: 2 bytes (대부분의 한글)

## 제공 함수
- `byte_length_euckr(s STRING) -> INT64`
- `substrb_euckr(s STRING, pos INT64, len INT64) -> STRING`
  - `pos`는 오라클처럼 1-base, 음수는 뒤에서부터
  - UTF-8 유효 문자열을 보장하기 위해 **문자 중간 바이트 절단은 하지 않습니다**

## 사용 예
```sql
-- 쿼리 안에서 임시로
-- (파일의 CREATE TEMP FUNCTION 블록을 그대로 붙여넣고 사용)
SELECT
  byte_length_euckr('ABC가나다') AS bl,
  substrb_euckr('ABC가나다', 4, 4) AS s;
```

## 제한사항 (중요)
- 실제 EUC-KR 인코딩 변환을 수행하지 않습니다.
- EUC-KR로 표현 불가능한 문자(예: 이모지)는 오라클과 1:1 바이트 호환이 불가능합니다.
- “정확한 EUC-KR 바이트 기준”이 반드시 필요하면, 별도 변환 파이프라인(사전 인코딩/디코딩) 또는 매핑 테이블 기반 구현이 필요합니다.
MD