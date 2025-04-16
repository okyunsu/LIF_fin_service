import os
from dotenv import load_dotenv
from app.domin.fin.models.entities import FinancialStatement
from app.domin.fin.models.schemas import RawFinancialStatement, CompanyInfo, DartApiResponse
from app.domin.fin.repository.fin_repository import FinRepository
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
import logging
import aiohttp
from collections import defaultdict

logger = logging.getLogger(__name__)

class FinService:
    def __init__(self, db_session: AsyncSession):
        self.repository = FinRepository()
        self.db_session = db_session
        load_dotenv()

    def _convert_amount(self, amount_str: str) -> float:
        """금액 문자열을 숫자로 변환합니다."""
        if not amount_str:
            return 0.0
        return float(amount_str.replace(",", ""))

    def _deduplicate_statements(self, statements: list[RawFinancialStatement]) -> list[RawFinancialStatement]:
        """중복되는 계정과목을 제거하고 가장 최신의 금액만 남깁니다."""
        # 계정과목별로 가장 최신 데이터만 저장
        latest_statements = {}
        for stmt in statements:
            key = (stmt.account_nm, stmt.sj_nm)  # 계정과목명과 재무제표명으로 키 생성
            if key not in latest_statements or int(stmt.ord) < int(latest_statements[key].ord):
                latest_statements[key] = stmt
        return list(latest_statements.values())

    async def fetch_and_save_financial_data(self, company_name: str) -> dict:
        """회사명으로 재무제표 데이터를 조회하고 저장합니다."""
        try:
            # 1. 회사 정보 조회
            company_info = await self.repository.get_company_info(company_name)
            
            # 2. 재무제표 데이터 조회
            statements = await self.repository.get_financial_statements(company_info.corp_code)
            
            # 3. 중복 제거
            statements = self._deduplicate_statements(statements)
            
            # 4. 기존 데이터 삭제
            delete_query = text("""
                DELETE FROM fin_statements 
                WHERE corp_code = :corp_code 
                AND rcept_no = :rcept_no
            """)
            
            # 첫 번째 statement의 rcept_no를 사용하여 해당 회사의 모든 데이터 삭제
            if statements:
                await self.db_session.execute(
                    delete_query, 
                    {
                        "corp_code": company_info.corp_code,
                        "rcept_no": statements[0].rcept_no
                    }
                )
            
            # 5. 새로운 데이터 삽입
            for statement in statements:
                # SQL 쿼리 생성
                insert_query = text("""
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
                
                # 데이터 준비
                data = {
                    "corp_code": company_info.corp_code,
                    "corp_name": company_info.corp_name,
                    "stock_code": company_info.stock_code,
                    "rcept_no": statement.rcept_no,
                    "reprt_code": statement.reprt_code,
                    "bsns_year": statement.bsns_year,
                    "sj_div": statement.sj_div,
                    "sj_nm": statement.sj_nm,
                    "account_nm": statement.account_nm,
                    "thstrm_nm": statement.thstrm_nm,
                    "thstrm_amount": self._convert_amount(statement.thstrm_amount),
                    "frmtrm_nm": statement.frmtrm_nm,
                    "frmtrm_amount": self._convert_amount(statement.frmtrm_amount),
                    "bfefrmtrm_nm": statement.bfefrmtrm_nm,
                    "bfefrmtrm_amount": self._convert_amount(statement.bfefrmtrm_amount),
                    "ord": statement.ord,
                    "currency": statement.currency
                }
                
                # 쿼리 실행
                await self.db_session.execute(insert_query, data)
            
            # 변경사항 저장
            await self.db_session.commit()
            
            return {
                "status": "success",
                "message": f"{company_name}의 재무제표 데이터가 성공적으로 저장되었습니다.",
                "data": {
                    "company": company_info.model_dump(),
                    "statements_count": len(statements)
                }
            }
            
        except Exception as e:
            # 오류 발생 시 롤백
            await self.db_session.rollback()
            logger.error(f"Error in fetch_and_save_financial_data: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }

    async def get_financial_data(self, company_name: str = None) -> dict:
        try:
            # 회사 정보 조회
            company_info = await self.repository.get_company_info(company_name)
            if not company_info:
                return {"error": "회사 정보를 찾을 수 없습니다."}

            # 재무제표 데이터 조회
            financial_statements = await self.repository.get_financial_statements(company_info.corp_code)

            if not financial_statements:
                return {"error": "재무제표 데이터를 찾을 수 없습니다."}

            return {
                "company_info": company_info.model_dump(),
                "financial_statements": [stmt.model_dump() for stmt in financial_statements]
            }

        except Exception as e:
            logger.error(f"Error in get_financial_data: {str(e)}")
            return {"error": str(e)}