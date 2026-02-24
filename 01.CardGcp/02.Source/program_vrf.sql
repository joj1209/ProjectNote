WITH SRC AS (
	SELECT 
		FORMAT_DATE('%E4Y%m%d',CURRENT_DATE('Asia/Seoul')) AS `기준일자`
		,N10.`DWDM구분코드`
		,TRIM(N10.`타겟테이블명`) AS `타겟테이블명`
		,TRIM(N10.`타겟컬럼명	`) AS `타겟컬럼명`
		,'BI메타검증_테이블단위' AS `타겟컬럼명1`
		, N10.`검증구분`
		,'04' AS `검증유형`
		,vs_seq_d AS `차수`
		,N10.`검증내용`
		,'MCSBM_008_01' AS `검증프로그램ID`
		,' ' AS `파라미터값`
		,'1' AS `검증결과구분`
		,TRIM(N10.`검증결과값`) AS `검증결과값`
		,COALESCE(N10.`데이터타입명`,'') AS `데이터타입명`
		,COALESCE(N10.`길이`,'') AS `길이`
		,COALESCE(N10.`소수점길이`,'') AS `소수점길이`
		,ROW_NUMBER() OVER (PARTITION BY N10.`DWDM구분코드`,N10.`타겟테이블명` ORDER BY N10.`타겟테이블명`) AS SORT_KEY
		,COUNT(1) OVER() AS `전체건수`
FROM BMWRK.`프로그램검증현황01_02` N10
LEFT JOIN BMWRK.`프로그램검증현황01_01` N11
ON N11.`DWDM구분코드` = N10.`DWDM구분코드`
AND N11.`테이블명` = N10.`테이블명`
WHERE N11.`테이블명` IS NULL
ORDER BY N10.`DWDM구분코드`
, N10.`타겟테이블명`
, N10.`검증내용`
),
RANKED AS (
	SELECT S.*
		, SUM(CASE WHEN `검증내용` = '2테이블컬럼검증' THEN 1 ELSE O END) OVER (PARTITION BY `DWDM구분코드`,`타겟테이블명`
				ORDER BY SORT_KEY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS JJ
		, SUM(CASE WHEN `검증내용` = '3테이블컬럼TYPE검증' THEN 1 ELSE O END) OVER (PARTITION BY `DWDM구분코드`,`타겟테이블명`
				ORDER BY SORT_KEY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS KK
  FROM SRC S
),
TOKENS AS (
	SELECT `DWDM구분코드`
			,`타겟테이블명`
			,`검증구분`
			,`검증유형`
			,`차수`
			,`검증프로그램ID`
			,`파라미터값`
			,`검증결과구분`
			,CASE WHEN `검증내용`='1테이블검증'
							THEN CONCAT('\n[ 테이블검증 ]',
															'\n  -. TABLE : ',`타겟테이블명`,
															'\n  -.     오류내용 : ',`검증결과값`)
							WHEN `검증내용` = '2테이블컬럼검증' AND JJ BETWEEN 1 AND 3
							THEN CONCAT(
												IF(JJ=1,CONCAT('\n[ 톄이쁠컬럼검증 ]',
																						'\n  -. TABLE : ',`타겟테이블명`),''),
																						'\n     COLUMN : ',`타겟컬럼명`,
																						',  오류내용 : ',`검증결과값`,
													IF(JJ=3,'\n 상위컬럼외 다수 테이블컬럼명 오류. ','')
												)
							WHEN `검증내용` = '3테이블컬럼TYPE검증' AND KK BETWEEN 1 AND 3
							THEN CONCAT(
												IF(JJ=1,CONCAT('\n[ 톄이쁠컬럼TYPE검증 ]',
																						'\n  -. TABLE : ',`타겟테이블명`),''),
																						'\n     COLUMN : ',`타겟컬럼명`,
																						', Type : ',`데이터타입명`,
																						' 컬럼길이 : ',`길이`,
																						' 소수점길이 : ',`소수점길이`,
																						',  오류내용 : ',`검증결과값`,
													IF(JJ=3,'\n 상위컬럼외 다수 테이블컬럼TYPE 오류. ','')
												)
							ELSE NULL
				END AS TOKEN
			,SORT_KEY
		FROM RANKED
),
AGG AS (
	SELECT
		`DWDM구분코드`
		,`타겟테이블명`
		,ARRAY_AGG(`검증구분` ORDER BY SORT_KEY DESC LIMIN 1)[OFFSET(0)] AS `검증구분`
		,ARRAY_AGG(`검증유형` ORDER BY SORT_KEY DESC LIMIN 1)[OFFSET(0)] AS `검증유형`
		,ARRAY_AGG(`차수` ORDER BY SORT_KEY DESC LIMIN 1)[OFFSET(0)] AS `차수`
		,ARRAY_AGG(`검증프로그램ID` ORDER BY SORT_KEY DESC LIMIN 1)[OFFSET(0)] AS `검증프로그램ID`
		,ARRAY_AGG(`파라미터값` ORDER BY SORT_KEY DESC LIMIN 1)[OFFSET(0)] AS `파라미터값`
		,ARRAY_AGG(`검증결과구분` ORDER BY SORT_KEY DESC LIMIN 1)[OFFSET(0)] AS `검증결과구분`
		,STRING_AGG(TOKEN,'' ORDER BY SORT_KEY) AS ERR_CONTENT
		FROM TOKENS
	WHERE TOKEN IS NOT NULL
	GROUP BY `DWDM구분코드`,`타겟테이블명`
)
SELECT
		FORMAT_DATE('%E4Y%m%d',CURRENT_DATE('Asia/Seoul')) AS `기준일자`
	,`DWDM구분코드`
	,`타겟테이블명`
	,`BI` AS `타겟컬럼명`
	,`검증구분`
	,`검증유형`
	,`차수`
	,`TABLE_META` AS `검증내용`
	,`검증프로그램ID`
	,`파라미터값`
	,`검증결과구분`
	,ERR_CONTENT AS `검증결과값`
	,1 AS `검증모수`
	FROM AGG
WHERE ERR_CONTENT IS NOT NULL
		AND ERR_CONTENT != ''