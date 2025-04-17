from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import logging

logger = logging.getLogger(__name__)

class FinRepository:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def delete_financial_statements(self, corp_code: str, rcept_no: str) -> None:
        """재무제표 데이터를 삭제합니다."""
        query = text("""
            DELETE FROM fin_statements 
            WHERE corp_code = :corp_code 
            AND rcept_no = :rcept_no
        """)
        await self.db_session.execute(query, {"corp_code": corp_code, "rcept_no": rcept_no})
        await self.db_session.commit()

    async def insert_financial_statement(self, data: dict) -> None:
        """재무제표 데이터를 저장합니다."""
        query = text("""
            INSERT INTO fin_statements (
                corp_code, corp_name, stock_code, rcept_no, reprt_code,
                bsns_year, sj_div, sj_nm, account_nm, thstrm_nm,
                thstrm_amount, frmtrm_nm, frmtrm_amount, bfefrmtrm_nm,
                bfefrmtrm_amount, ord, currency
            ) VALUES (
                :corp_code, :corp_name, :stock_code, :rcept_no, :reprt_code,
                :bsns_year, :sj_div, :sj_nm, :account_nm, :thstrm_nm,
                :thstrm_amount, :frmtrm_nm, :frmtrm_amount, :bfefrmtrm_nm,
                :bfefrmtrm_amount, :ord, :currency
            )
        """)
        await self.db_session.execute(query, data)
        await self.db_session.commit()

    async def get_statement_summary(self) -> list[dict]:
        """회사별 재무제표 종류와 데이터 수를 조회합니다."""
        query = text("""
            SELECT corp_code, corp_name, sj_div, sj_nm, COUNT(*) as count
            FROM fin_statements
            GROUP BY corp_code, corp_name, sj_div, sj_nm
            ORDER BY corp_code, sj_div
        """)
        result = await self.db_session.execute(query)
        return [dict(row) for row in result]

    async def get_key_financial_items(self) -> list[dict]:
        """회사별 주요 재무 항목을 조회합니다."""
        query = text("""
            SELECT corp_code, corp_name, sj_div, account_nm, thstrm_nm, thstrm_amount
            FROM fin_statements
            WHERE account_nm IN (
                '당기순이익', '영업이익', '영업활동현금흐름', 
                '자산총계', '부채총계', '자본총계'
            )
            ORDER BY corp_code, sj_div, account_nm
        """)
        result = await self.db_session.execute(query)
        return [dict(row) for row in result]

    async def get_company_by_name(self, company_name: str) -> dict:
        """회사명으로 회사 정보를 조회합니다."""
        query = text("""
            SELECT DISTINCT corp_code, corp_name, stock_code
            FROM fin_statements
            WHERE corp_name = :company_name
            LIMIT 1
        """)
        result = await self.db_session.execute(query, {"company_name": company_name})
        row = result.fetchone()
        if row:
            # 튜플을 딕셔너리로 변환할 때 컬럼명을 키로 사용
            return {
                "corp_code": row[0],
                "corp_name": row[1],
                "stock_code": row[2]
            }
        return None

    async def get_financial_statements_by_corp_code(self, corp_code: str) -> list[dict]:
        """회사 코드로 재무제표 데이터를 조회합니다."""
        query = text("""
            SELECT *
            FROM fin_statements
            WHERE corp_code = :corp_code
            ORDER BY sj_div, ord
        """)
        result = await self.db_session.execute(query, {"corp_code": corp_code})
        return [dict(row) for row in result]

    async def save_financial_statements(self, statements: list[dict]) -> None:
        """여러 재무제표 데이터를 일괄 저장합니다."""
        try:
            for statement in statements:
                await self.insert_financial_statement(statement)
        except Exception as e:
            logger.error(f"Error saving financial statements: {e}")
            raise

    async def save_financial_ratios(self, ratios: dict) -> None:
        """재무비율을 저장합니다."""
        query = text("""
            INSERT INTO fin_ratios (
                corp_code, corp_name, bsns_year,
                debt_ratio, current_ratio, interest_coverage_ratio,
                operating_profit_ratio, net_profit_ratio, roe, roa,
                debt_dependency, cash_flow_debt_ratio,
                sales_growth, operating_profit_growth, eps_growth
            ) VALUES (
                :corp_code, :corp_name, :bsns_year,
                :debt_ratio, :current_ratio, :interest_coverage_ratio,
                :operating_profit_ratio, :net_profit_ratio, :roe, :roa,
                :debt_dependency, :cash_flow_debt_ratio,
                :sales_growth, :operating_profit_growth, :eps_growth
            )
            ON CONFLICT (corp_code, bsns_year) 
            DO UPDATE SET
                debt_ratio = EXCLUDED.debt_ratio,
                current_ratio = EXCLUDED.current_ratio,
                interest_coverage_ratio = EXCLUDED.interest_coverage_ratio,
                operating_profit_ratio = EXCLUDED.operating_profit_ratio,
                net_profit_ratio = EXCLUDED.net_profit_ratio,
                roe = EXCLUDED.roe,
                roa = EXCLUDED.roa,
                debt_dependency = EXCLUDED.debt_dependency,
                cash_flow_debt_ratio = EXCLUDED.cash_flow_debt_ratio,
                sales_growth = EXCLUDED.sales_growth,
                operating_profit_growth = EXCLUDED.operating_profit_growth,
                eps_growth = EXCLUDED.eps_growth
        """)
        await self.db_session.execute(query, ratios)
        await self.db_session.commit()

    async def get_financial_statements(self, corp_code: str, bsns_year: str) -> list[dict]:
        """회사 코드와 사업연도로 재무제표 데이터를 조회합니다."""
        query = text("""
            SELECT 
                corp_code,
                corp_name,
                stock_code,
                rcept_no,
                reprt_code,
                bsns_year,
                sj_div,
                sj_nm,
                account_nm,
                thstrm_nm,
                thstrm_amount,
                frmtrm_nm,
                frmtrm_amount,
                bfefrmtrm_nm,
                bfefrmtrm_amount,
                ord,
                currency
            FROM fin_statements
            WHERE corp_code = :corp_code
            AND bsns_year = :bsns_year
            ORDER BY sj_div, ord
        """)
        result = await self.db_session.execute(query, {"corp_code": corp_code, "bsns_year": bsns_year})
        rows = result.fetchall()
        return [dict(zip(result.keys(), row)) for row in rows]