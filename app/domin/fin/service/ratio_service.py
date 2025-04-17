from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
import logging
from datetime import datetime

from app.domin.fin.repository.fin_repository import FinRepository

logger = logging.getLogger(__name__)

class RatioService:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session
        self.repository = FinRepository(db_session)

    def _calculate_growth_rate(self, current: float, previous: float) -> float:
        """성장률을 계산합니다."""
        if previous == 0:
            return 0.0
        return ((current - previous) / abs(previous)) * 100

    async def calculate_financial_ratios(self, corp_code: str, bsns_year: str) -> Dict[str, Any]:
        """재무비율을 계산합니다."""
        try:
            # 재무제표 데이터 조회
            statements = await self.repository.get_financial_statements(corp_code, bsns_year)
            logger.info(f"조회된 재무제표 데이터: {statements}")
            
            if not statements:
                logger.warning(f"재무제표 데이터가 없습니다: {corp_code}, {bsns_year}")
                return {}
            
            # 재무비율 계산을 위한 딕셔너리
            ratios = {
                "corp_code": corp_code,
                "bsns_year": bsns_year,
                "debt_ratio": 0,
                "current_ratio": 0,
                "interest_coverage_ratio": 0,
                "operating_profit_ratio": 0,
                "net_profit_ratio": 0,
                "roe": 0,
                "roa": 0,
                "debt_dependency": 0,
                "cash_flow_debt_ratio": 0,
                "sales_growth": 0,
                "operating_profit_growth": 0,
                "eps_growth": 0
            }
            
            # 안정성 지표 계산
            total_assets = next((float(s["thstrm_amount"]) for s in statements if s["sj_div"] == "BS" and s["account_nm"] == "자산총계"), 0)
            total_liabilities = next((float(s["thstrm_amount"]) for s in statements if s["sj_div"] == "BS" and s["account_nm"] == "부채총계"), 0)
            total_equity = next((float(s["thstrm_amount"]) for s in statements if s["sj_div"] == "BS" and s["account_nm"] == "자본총계"), 0)
            current_assets = next((float(s["thstrm_amount"]) for s in statements if s["sj_div"] == "BS" and s["account_nm"] == "유동자산"), 0)
            current_liabilities = next((float(s["thstrm_amount"]) for s in statements if s["sj_div"] == "BS" and s["account_nm"] == "유동부채"), 0)
            
            if total_equity > 0:
                ratios["debt_ratio"] = (total_liabilities / total_equity) * 100
            
            if current_liabilities > 0:
                ratios["current_ratio"] = (current_assets / current_liabilities) * 100
            
            # 수익성 지표 계산
            revenue = next((float(s["thstrm_amount"]) for s in statements if s["sj_div"] == "IS" and s["account_nm"] == "매출액"), 0)
            operating_profit = next((float(s["thstrm_amount"]) for s in statements if s["sj_div"] == "IS" and s["account_nm"] == "영업이익"), 0)
            net_income = next((float(s["thstrm_amount"]) for s in statements if s["sj_div"] == "IS" and s["account_nm"] == "당기순이익"), 0)
            
            if revenue > 0:
                ratios["operating_profit_ratio"] = (operating_profit / revenue) * 100
                ratios["net_profit_ratio"] = (net_income / revenue) * 100
            
            if total_equity > 0:
                ratios["roe"] = (net_income / total_equity) * 100
            
            if total_assets > 0:
                ratios["roa"] = (net_income / total_assets) * 100
            
            # 건전성 지표 계산
            short_term_debt = next((float(s["thstrm_amount"]) for s in statements if s["sj_div"] == "BS" and s["account_nm"] == "유동부채"), 0)
            long_term_debt = next((float(s["thstrm_amount"]) for s in statements if s["sj_div"] == "BS" and s["account_nm"] == "비유동부채"), 0)
            
            if total_liabilities > 0:
                ratios["debt_dependency"] = ((short_term_debt + long_term_debt) / total_liabilities) * 100
            
            # 성장률 지표 계산 - 3개년 데이터 사용
            # 전기 데이터
            prev_revenue = next((float(s["frmtrm_amount"]) for s in statements if s["sj_div"] == "IS" and s["account_nm"] == "매출액"), 0)
            prev_operating_profit = next((float(s["frmtrm_amount"]) for s in statements if s["sj_div"] == "IS" and s["account_nm"] == "영업이익"), 0)
            prev_net_income = next((float(s["frmtrm_amount"]) for s in statements if s["sj_div"] == "IS" and s["account_nm"] == "당기순이익"), 0)
            
            # 전전기 데이터
            prev_prev_revenue = next((float(s["bfefrmtrm_amount"]) for s in statements if s["sj_div"] == "IS" and s["account_nm"] == "매출액"), 0)
            prev_prev_operating_profit = next((float(s["bfefrmtrm_amount"]) for s in statements if s["sj_div"] == "IS" and s["account_nm"] == "영업이익"), 0)
            prev_prev_net_income = next((float(s["bfefrmtrm_amount"]) for s in statements if s["sj_div"] == "IS" and s["account_nm"] == "당기순이익"), 0)
            
            # 성장률 계산
            if prev_revenue > 0 and prev_prev_revenue > 0:
                ratios["sales_growth"] = self._calculate_growth_rate(revenue, prev_revenue)
            
            if prev_operating_profit > 0 and prev_prev_operating_profit > 0:
                ratios["operating_profit_growth"] = self._calculate_growth_rate(operating_profit, prev_operating_profit)
            
            if prev_net_income > 0 and prev_prev_net_income > 0:
                ratios["eps_growth"] = self._calculate_growth_rate(net_income, prev_net_income)
            
            return ratios
            
        except Exception as e:
            logger.error(f"재무비율 계산 중 오류 발생: {str(e)}")
            raise

    async def calculate_and_save_ratios(self, corp_code: str, corp_name: str, bsns_year: str) -> Dict[str, Any]:
        """재무비율을 계산하고 저장합니다."""
        try:
            # 재무비율 계산
            ratios = await self.calculate_financial_ratios(corp_code, bsns_year)
            
            # 회사명 추가
            ratios["corp_name"] = corp_name
            
            # DB에 저장
            await self.repository.save_financial_ratios(ratios)
            
            return ratios
            
        except Exception as e:
            logger.error(f"재무비율 계산 및 저장 중 오류 발생: {str(e)}")
            raise 