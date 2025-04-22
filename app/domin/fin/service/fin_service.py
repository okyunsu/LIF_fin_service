import os
from typing import Optional, List, Dict, Any
from datetime import datetime
import logging
import aiohttp
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO
from dotenv import load_dotenv
from sqlalchemy import text

from app.domin.fin.models.entities import FinancialStatement
from app.domin.fin.models.schemas import RawFinancialStatement, CompanyInfo, DartApiResponse, StockInfo
from app.domin.fin.repository.fin_repository import FinRepository
from app.domin.fin.service.ratio_service import RatioService
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

class FinService:
    def __init__(self, db_session: AsyncSession):
        """서비스 초기화"""
        self.db_session = db_session
        self.repository = FinRepository(db_session)
        self.ratio_service = RatioService(db_session)
        load_dotenv()
        self.api_key = os.getenv("DART_API_KEY")
        if not self.api_key:
            raise ValueError("DART API 키가 필요합니다.")

    def _convert_amount(self, amount_str: Optional[str]) -> float:
        """금액 문자열을 숫자로 변환합니다."""
        if not amount_str:
            return 0.0
        try:
            return float(amount_str.replace(",", ""))
        except (ValueError, AttributeError) as e:
            logger.warning(f"금액 변환 실패: {amount_str}, 에러: {str(e)}")
            return 0.0

    def _deduplicate_statements(self, statements: List[RawFinancialStatement]) -> List[RawFinancialStatement]:
        """중복되는 계정과목을 제거하고 가장 최신의 금액만 남깁니다."""
        latest_statements = {}
        for stmt in statements:
            key = (stmt.account_nm, stmt.sj_nm)
            if key not in latest_statements or int(stmt.ord) < int(latest_statements[key].ord):
                latest_statements[key] = stmt
        return list(latest_statements.values())

    def _prepare_statement_data(self, statement: RawFinancialStatement, company_info: CompanyInfo) -> Dict[str, Any]:
        """재무제표 데이터를 DB 저장 형식으로 변환합니다."""
        return {
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

    async def _fetch_company_info_from_api(self, company_name: str) -> CompanyInfo:
        """DART API에서 회사 정보를 조회합니다."""
        url = "https://opendart.fss.or.kr/api/corpCode.xml"
        params = {"crtfc_key": self.api_key}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    raise Exception(f"API 요청 실패: {response.status}")
                
                content = await response.read()
                with zipfile.ZipFile(BytesIO(content)) as zip_file:
                    with zip_file.open('CORPCODE.xml') as xml_file:
                        tree = ET.parse(xml_file)
                        root = tree.getroot()
                        
                        for company in root.findall('.//list'):
                            if company.findtext('corp_name') == company_name:
                                return CompanyInfo(
                                    corp_code=company.findtext('corp_code'),
                                    corp_name=company_name,
                                    stock_code=company.findtext('stock_code') or "",
                                    modify_date=company.findtext('modify_date')
                                )
                        
                        raise ValueError(f"회사명 '{company_name}'을 찾을 수 없습니다.")

    async def get_company_info(self, company_name: str) -> CompanyInfo:
        """회사 정보를 조회합니다."""
        try:
            # DB에서 먼저 조회
            db_company = await self.repository.get_company_by_name(company_name)
            if db_company:
                # 딕셔너리 키를 CompanyInfo 필드와 일치시킴
                company_data = {
                    "corp_code": db_company.get("corp_code", ""),
                    "corp_name": db_company.get("corp_name", company_name),
                    "stock_code": db_company.get("stock_code", ""),
                    "modify_date": datetime.now().strftime("%Y%m%d")
                }
                try:
                    return CompanyInfo(**company_data)
                except Exception as e:
                    logger.warning(f"DB 데이터로 CompanyInfo 생성 실패: {e}")
            
            # API에서 조회
            return await self._fetch_company_info_from_api(company_name)
            
        except Exception as e:
            logger.error(f"회사 정보 조회 실패: {str(e)}")
            raise

    async def _fetch_financial_statements_from_api(self, corp_code: str) -> List[RawFinancialStatement]:
        """DART API에서 재무제표 데이터를 조회합니다."""
        statements = []
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
        params = {
            "crtfc_key": self.api_key,
            "corp_code": corp_code,
            "bsns_year": "2023",
            "reprt_code": "11011",
            "fs_div": "CFS"
        }
        
        async with aiohttp.ClientSession() as session:
            # 재무상태표와 손익계산서 조회
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    api_response = DartApiResponse(**data)
                    
                    if api_response.status == "000":
                        for item in api_response.list:
                            if item.get("sj_div") in ["BS", "IS"]:
                                item["thstrm_nm"] = f"{int(item['bsns_year'])}년"
                                item["frmtrm_nm"] = f"{int(item['bsns_year'])-1}년"
                                item["bfefrmtrm_nm"] = f"{int(item['bsns_year'])-2}년"
                                statements.append(RawFinancialStatement(**item))
            
            # 현금흐름표 조회
            cf_url = "https://opendart.fss.or.kr/api/fnlttCashFlow.json"
            async with session.get(cf_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    api_response = DartApiResponse(**data)
                    
                    if api_response.status == "000":
                        for item in api_response.list:
                            item["sj_div"] = "CF"
                            item["sj_nm"] = "현금흐름표"
                            item["thstrm_nm"] = f"{int(item['bsns_year'])}년"
                            item["frmtrm_nm"] = f"{int(item['bsns_year'])-1}년"
                            item["bfefrmtrm_nm"] = f"{int(item['bsns_year'])-2}년"
                            statements.append(RawFinancialStatement(**item))
        
        return statements

    async def get_financial_statements(self, company_info: CompanyInfo) -> List[RawFinancialStatement]:
        """재무제표 데이터를 조회합니다."""
        try:
            statements = await self._fetch_financial_statements_from_api(company_info.corp_code)
            logger.info(f"조회된 재무제표 수: {len(statements)}")
            return statements
        except Exception as e:
            logger.error(f"재무제표 조회 실패: {str(e)}")
            raise

    async def fetch_and_save_financial_data(self, company_name: str) -> Dict[str, Any]:
        """회사명으로 재무제표 데이터를 조회하고 저장합니다."""
        try:
            # 이미 데이터가 있는지 확인
            check_query = text("""
                SELECT COUNT(*) FROM fin_data 
                WHERE corp_name = :company_name
            """)
            result = await self.db_session.execute(check_query, {"company_name": company_name})
            count = result.scalar()
            
            if count > 0:
                logging.info(f"회사 '{company_name}'의 재무제표 데이터가 이미 존재합니다. 크롤링을 건너뜁니다.")
                # 기존 데이터 반환
                data_query = text("""
                    SELECT 
                        bsns_year,
                        sj_div,
                        sj_nm,
                        account_nm,
                        thstrm_amount,
                        frmtrm_amount,
                        bfefrmtrm_amount
                    FROM fin_data 
                    WHERE corp_name = :company_name
                    AND sj_div != 'RATIO'
                    ORDER BY bsns_year DESC, sj_div, ord
                """)
                data_result = await self.db_session.execute(data_query, {"company_name": company_name})
                
                # 결과를 딕셔너리로 변환
                data = []
                for row in data_result:
                    row_dict = {}
                    for idx, column in enumerate(data_result.keys()):
                        row_dict[column] = row[idx]
                    data.append(row_dict)
                    
                return {
                    "status": "success",
                    "message": f"{company_name}의 재무제표 데이터가 이미 존재합니다.",
                    "data": data
                }
                
            # 데이터가 없는 경우 크롤링 수행
            logging.info(f"회사 '{company_name}'의 재무제표 데이터를 크롤링합니다.")
            
            # 1. 회사 정보 조회
            company_info = await self.get_company_info(company_name)
            
            # 2. 재무제표 데이터 조회
            statements = await self.get_financial_statements(company_info)
            
            # 3. 중복 제거
            statements = self._deduplicate_statements(statements)
            
            # 4. 기존 데이터 삭제
            if statements:
                await self.repository.delete_financial_statements(company_info.corp_code, statements[0].rcept_no)
            
            # 5. 새로운 데이터 저장
            statement_data = [self._prepare_statement_data(stmt, company_info) for stmt in statements]
            await self.repository.save_financial_statements(statement_data)
            
            # 6. 재무비율 계산 및 저장
            bsns_year = statements[0].bsns_year if statements else None
            if bsns_year:
                ratios = await self.ratio_service.calculate_and_save_ratios(
                    corp_code=company_info.corp_code,
                    corp_name=company_info.corp_name,
                    bsns_year=bsns_year
                )
            
            # 7. 저장된 데이터 조회하여 반환
            data_query = text("""
                SELECT 
                    bsns_year,
                    sj_div,
                    sj_nm,
                    account_nm,
                    thstrm_amount,
                    frmtrm_amount,
                    bfefrmtrm_amount
                FROM fin_data 
                WHERE corp_name = :company_name
                AND sj_div != 'RATIO'
                ORDER BY bsns_year DESC, sj_div, ord
            """)
            data_result = await self.db_session.execute(data_query, {"company_name": company_name})
            
            # 결과를 딕셔너리로 변환
            data = []
            for row in data_result:
                row_dict = {}
                for idx, column in enumerate(data_result.keys()):
                    row_dict[column] = row[idx]
                data.append(row_dict)
            
            return {
                "status": "success",
                "message": f"{company_name}의 재무제표 데이터가 성공적으로 저장되었습니다.",
                "data": data
            }
            
        except Exception as e:
            logger.error(f"재무제표 데이터 저장 실패: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }

