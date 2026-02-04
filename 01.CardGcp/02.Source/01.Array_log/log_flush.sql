CREATE OR REPLACE PROCEDURE sp_log_flush_array(
  IN logs ARRAY<log_struct>
)
BEGIN
  -- ===== dm_log (집계 + MERGE) =====
  MERGE dm_log T
  USING (
    SELECT
      영역구분명,
      주제영역명,
      초기변경구분코드,
      프로그램ID,
      스키마,
      테이블명,
      작업기준일자 AS 기준일자,
      작업단계번호,
      작업시작일시,
      MAX(작업단위종료일시) AS 작업종료일시,
      MAX(전체경과시간) AS 작업경과시간,
      차수,
      SUM(INSERT건수) AS INSERT건수,
      SUM(DELETE건수) AS DELETE건수,
      SUM(READ건수) AS READ건수,
      SUM(REJECT건수) AS REJECT건수,
      MAX(작업결과코드) AS 작업결과코드,
      MAX(작업에러문자번호) AS 작업에러문자번호,
      MAX(작업에러내용) AS 작업에러내용
    FROM UNNEST(logs)
    GROUP BY
      영역구분명, 주제영역명, 초기변경구분코드,
      프로그램ID, 스키마, 테이블명,
      작업기준일자, 작업단계번호, 작업시작일시, 차수
  ) S
  ON T.프로그램ID = S.프로그램ID
 AND T.작업시작일시 = S.작업시작일시

  WHEN MATCHED THEN UPDATE SET
    작업종료일시 = S.작업종료일시,
    작업경과시간 = S.작업경과시간,
    INSERT건수 = T.INSERT건수 + S.INSERT건수,
    DELETE건수 = T.DELETE건수 + S.DELETE건수,
    READ건수   = T.READ건수   + S.READ건수,
    REJECT건수 = T.REJECT건수 + S.REJECT건수,
    작업결과코드 = S.작업결과코드,
    작업에러문자번호 = S.작업에러문자번호,
    작업에러내용 = S.작업에러내용,
    작업종료여부='Y'

  WHEN NOT MATCHED THEN INSERT (
    영역구분명, 주제영역명, 초기변경구분코드,
    프로그램ID, 스키마, 테이블명,
    기준일자, 작업단계번호, 작업시작일시, 작업종료일시,
    작업경과시간, 차수,
    INSERT건수, DELETE건수, READ건수, REJECT건수,
    작업종료여부, 작업결과코드, 작업에러문자번호, 작업에러내용
  )
  VALUES (
    S.영역구분명, S.주제영역명, S.초기변경구분코드,
    S.프로그램ID, S.스키마, S.테이블명,
    S.기준일자, S.작업단계번호, S.작업시작일시, S.작업종료일시,
    S.작업경과시간, S.차수,
    S.INSERT건수, S.DELETE건수, S.READ건수, S.REJECT건수,
    'Y', S.작업결과코드, S.작업에러문자번호, S.작업에러내용
  );

  -- ===== dm_log_dtl 일괄 INSERT =====
  INSERT INTO dm_log_dtl (
    영역구분명, 프로그램ID, 단위작업명, 기준일자, 작업단계번호,
    작업시작일시, 작업단위시작일시, 작업단위종료일시,
    작업소요시분초, 작업건수,
    작업결과코드, 작업에러문자번호, 작업에러내용
  )
  SELECT
    영역구분명,
    프로그램ID,
    단위작업명,
    작업기준일자,
    작업단계번호,
    작업시작일시,
    작업단위시작일시,
    작업단위종료일시,
    단위경과시간,
    INSERT건수+DELETE건수+READ건수+REJECT건수,
    작업결과코드,
    작업에러문자번호,
    작업에러내용
  FROM UNNEST(logs);
END;
