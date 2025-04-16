import aiohttp
import os
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO
from dotenv import load_dotenv
from app.domin.fin.models.schemas import RawFinancialStatement, CompanyInfo, DartApiResponse, StockInfo
from datetime import datetime

load_dotenv()

class FinRepository:
    def __init__(self):
        self.api_key = os.getenv("DART_API_KEY")
        if not self.api_key:
            raise ValueError("DART API 키가 필요합니다.")

    async def get_company_info(self, company_name: str) -> CompanyInfo:
        """회사 정보를 조회합니다."""
        url = "https://opendart.fss.or.kr/api/corpCode.xml"
        params = {
            "crtfc_key": self.api_key
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    # ZIP 파일 다운로드 및 압축 해제
                    content = await response.read()
                    with zipfile.ZipFile(BytesIO(content)) as zip_file:
                        # CORPCODE.xml 파일 읽기
                        with zip_file.open('CORPCODE.xml') as xml_file:
                            tree = ET.parse(xml_file)
                            root = tree.getroot()
                            
                            # 회사명으로 검색
                            for company in root.findall('.//list'):
                                corp_name = company.findtext('corp_name')
                                if corp_name == company_name:
                                    company_data = {
                                        "corp_code": company.findtext('corp_code'),
                                        "corp_name": corp_name,
                                        "stock_code": company.findtext('stock_code') or "",
                                        "modify_date": company.findtext('modify_date')
                                    }
                                    return CompanyInfo(**company_data)
                            
                            raise ValueError(f"회사명 '{company_name}'을 찾을 수 없습니다. 공시회사명을 정확히 입력해주세요.")
                else:
                    raise Exception(f"API 요청 실패: {response.status}")

    async def get_stock_info(self, corp_code: str) -> StockInfo:
        """주식 발행정보를 조회합니다."""
        url = "https://opendart.fss.or.kr/api/stockTotqySttus.json"
        current_year = datetime.now().year
        
        params = {
            "crtfc_key": self.api_key,
            "corp_code": corp_code,
            "bsns_year": str(current_year),
            "reprt_code": "11011"  # 사업보고서
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("status") == "000":
                        stock_data = data.get("list", [])[0] if data.get("list") else None
                        if stock_data:
                            return StockInfo(
                                istc_totqy=int(stock_data.get("istc_totqy", "0").replace(",", "")),  # 발행한 주식의 총수
                                distb_stock_qy=int(stock_data.get("distb_stock_qy", "0").replace(",", "")),  # 유통주식수
                                tesstk_co=int(stock_data.get("tesstk_co", "0").replace(",", "")),  # 자기주식수
                            )
                    raise Exception(f"주식 정보를 찾을 수 없습니다: {data.get('message')}")
                else:
                    raise Exception(f"API 요청 실패: {response.status}")

    async def get_financial_statements(self, corp_code: str) -> list[RawFinancialStatement]:
        """재무제표 데이터를 조회합니다."""
        statements = []
        
        # 1. 재무상태표와 손익계산서 조회
        url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
        params = {
            "crtfc_key": self.api_key,
            "corp_code": corp_code,
            "bsns_year": "2023",
            "reprt_code": "11011",  # 사업보고서
            "fs_div": "CFS"  # 연결재무제표
        }
        
        async with aiohttp.ClientSession() as session:
            # 재무상태표와 손익계산서 조회
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    api_response = DartApiResponse(**data)
                    
                    if api_response.status == "000":
                        print(f"BS/IS API 응답 상태: {api_response.status}")
                        print(f"BS/IS 총 데이터 수: {len(api_response.list) if api_response.list else 0}")
                        
                        for item in api_response.list:
                            if item.get("sj_div") in ["BS", "IS"]:
                                print(f"처리 중인 BS/IS 항목 - 구분: {item.get('sj_div')}, 계정: {item.get('account_nm')}")
                                item["thstrm_nm"] = f"{int(item['bsns_year'])}년"
                                item["frmtrm_nm"] = f"{int(item['bsns_year'])-1}년"
                                item["bfefrmtrm_nm"] = f"{int(item['bsns_year'])-2}년"
                                statements.append(RawFinancialStatement(**item))
                    else:
                        print(f"BS/IS API 경고: {api_response.message}")
            
            # 현금흐름표 조회
            cf_url = "https://opendart.fss.or.kr/api/fnlttCashFlow.json"
            async with session.get(cf_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    api_response = DartApiResponse(**data)
                    
                    if api_response.status == "000":
                        print(f"CF API 응답 상태: {api_response.status}")
                        print(f"CF 총 데이터 수: {len(api_response.list) if api_response.list else 0}")
                        
                        for item in api_response.list:
                            print(f"처리 중인 CF 항목 - 계정: {item.get('account_nm')}")
                            # CF 데이터 형식을 BS/IS와 동일하게 맞춤
                            item["sj_div"] = "CF"
                            item["sj_nm"] = "현금흐름표"
                            item["thstrm_nm"] = f"{int(item['bsns_year'])}년"
                            item["frmtrm_nm"] = f"{int(item['bsns_year'])-1}년"
                            item["bfefrmtrm_nm"] = f"{int(item['bsns_year'])-2}년"
                            statements.append(RawFinancialStatement(**item))
                    else:
                        print(f"CF API 경고: {api_response.message}")
        
        print(f"최종 저장된 재무제표 수: {len(statements)}")
        return statements