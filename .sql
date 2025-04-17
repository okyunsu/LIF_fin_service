-- 모든 회사의 코드와 이름 조회
SELECT DISTINCT corp_code, corp_name
FROM fin_statements
ORDER BY corp_name;


-- 특정 회사 코드의 재무제표 데이터 확인
SELECT DISTINCT corp_code, corp_name, bsns_year, sj_div, sj_nm
FROM fin_statements
WHERE corp_code = '01515323'  -- LG에너지솔루션의 회사 코드
ORDER BY bsns_year DESC, sj_div;


-- 회사 코드와 재무비율 데이터 함께 조회
SELECT 
    s.corp_code,
    s.corp_name,
    r.bsns_year,
    ROUND(r.debt_ratio, 2) as debt_ratio,
    ROUND(r.current_ratio, 2) as current_ratio
FROM fin_statements s
JOIN fin_ratios r ON s.corp_code = r.corp_code
WHERE s.corp_name = 'LG에너지솔루션'
ORDER BY r.bsns_year DESC;