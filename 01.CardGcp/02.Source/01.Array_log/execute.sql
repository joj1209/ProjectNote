DECLARE logs ARRAY<log_struct> DEFAULT [];

-- 쿼리1 종료
CALL sp_log_array_push(logs, ...);

-- 쿼리2 종료
CALL sp_log_array_push(logs, ...);

-- 쿼리10 종료
CALL sp_log_array_push(logs, ...);

-- 🔥 단 한번만 테이블 반영
CALL sp_log_flush_array(logs);
