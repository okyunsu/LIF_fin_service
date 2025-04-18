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


CREATE TABLE IF NOT EXISTS fin_data (
    id SERIAL PRIMARY KEY,
    corp_code VARCHAR(20) NOT NULL,
    corp_name VARCHAR(100) NOT NULL,
    stock_code VARCHAR(20),
    rcept_no VARCHAR(20),
    reprt_code VARCHAR(20),
    bsns_year VARCHAR(4) NOT NULL,
    sj_div VARCHAR(10),
    sj_nm VARCHAR(100),
    account_nm VARCHAR(100),
    thstrm_nm VARCHAR(20),
    thstrm_amount NUMERIC,
    frmtrm_nm VARCHAR(20),
    frmtrm_amount NUMERIC,
    bfefrmtrm_nm VARCHAR(20),
    bfefrmtrm_amount NUMERIC,
    ord INTEGER,
    currency VARCHAR(10),
    debt_ratio NUMERIC,
    current_ratio NUMERIC,
    interest_coverage_ratio NUMERIC,
    operating_profit_ratio NUMERIC,
    net_profit_ratio NUMERIC,
    roe NUMERIC,
    roa NUMERIC,
    debt_dependency NUMERIC,
    cash_flow_debt_ratio NUMERIC,
    sales_growth NUMERIC,
    operating_profit_growth NUMERIC,
    eps_growth NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(corp_code, bsns_year, sj_div, account_nm)
); 