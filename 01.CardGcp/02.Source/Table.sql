BM.프로그램검증현황
기준일자 vachar2(8) PK1
DWDM구분코드 vachar2(10) PK2
타겟테이블명 vachar2(100) PK3
타겟컬럼명 vachar2(120) PK4
검증구분 vachar2(2) PK5
검증유형 vachar2(2) PK6
차수 vachar2(2) PK7
검증내용 vachar2(50)
검증프로그램ID vachar2(30)
파라미터값 vachar2(100)
검증결과구분 number(5)
검증결과값 clob
검증모수 number(19)
검증시작일시 timestamp(6)
적재일시 timestamp(6)


20260101
DM
~
BI메타검증_테이블단위
03
04
01
테이블메타검증
MCSBM_008
{null}
0
BI메타검증결과 테이블 정상매핑됨
1
시간~
시간~


기준일자 20260116
DWDM구분코드 : DM
TRIM(N10.타겟테이블명) : 공공코드
TRIM(N10.타겟컬럼명) : 구간대
타겟컬럼명1 : BI메타검증_테이블단위
검증구분 : 03
검증유형 : 04
차수 : 0
검증내용 : 1테이블검증
검증프로그램ID : MCSBM_008
파라미터값 : {null}
검증결과구분 : 1
TRIM(N10.검증결과값) : 실테이블 존재, BI메타 테이블정보 미 등록오류
NVL(N10.데이터타입명,'') : {null}
NVL(N10.길이,'') : {null}
NVL(N10.소수점길이,'') : {null}
RNO : 1