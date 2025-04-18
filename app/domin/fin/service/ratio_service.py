from typing import Dict, Any, Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
import logging
from datetime import datetime
from sqlalchemy import text

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
            # 재무제표 데이터 조회
            query = text("""
                SELECT * FROM fin_data 
                WHERE corp_code = :corp_code 
                AND bsns_year = :bsns_year
                AND sj_div IN ('BS', 'IS')
                ORDER BY sj_div, ord
            """)
            result = await self.db_session.execute(query, {
                "corp_code": corp_code,
                "bsns_year": bsns_year
            })
            
            statements = []
            for row in result:
                row_dict = {}
                for idx, column in enumerate(result.keys()):
                    row_dict[column] = row[idx]
                statements.append(row_dict)
            
            if not statements:
                logger.warning(f"재무제표 데이터가 없습니다: {corp_code}, {bsns_year}")
                return {}
            
            # 재무비율 계산
            ratios = self._calculate_ratios(statements)
            
            # 재무비율 저장
            await self._save_ratios(corp_code, corp_name, bsns_year, ratios)
            
            return ratios
            
        except Exception as e:
            logger.error(f"재무비율 계산 및 저장 실패: {str(e)}")
            raise

    def _calculate_ratios(self, statements: List[Dict[str, Any]]) -> Dict[str, float]:
        """재무제표 데이터로부터 재무비율을 계산합니다."""
        ratios = {}
        
        # 재무상태표와 손익계산서 데이터 분리
        bs_data = {item["account_nm"]: item for item in statements if item["sj_div"] == "BS"}
        is_data = {item["account_nm"]: item for item in statements if item["sj_div"] == "IS"}
        
        # 1. 유동비율
        if "유동자산" in bs_data and "유동부채" in bs_data:
            current_assets = bs_data["유동자산"]["thstrm_amount"]
            current_liabilities = bs_data["유동부채"]["thstrm_amount"]
            if current_liabilities != 0:
                ratios["current_ratio"] = (current_assets / current_liabilities) * 100
        
        # 2. 부채비율
        if "부채총계" in bs_data and "자본총계" in bs_data:
            total_debt = bs_data["부채총계"]["thstrm_amount"]
            total_equity = bs_data["자본총계"]["thstrm_amount"]
            if total_equity != 0:
                ratios["debt_ratio"] = (total_debt / total_equity) * 100
        
        # 3. ROE
        if "당기순이익" in is_data and "자본총계" in bs_data:
            net_income = is_data["당기순이익"]["thstrm_amount"]
            total_equity = bs_data["자본총계"]["thstrm_amount"]
            if total_equity != 0:
                ratios["roe"] = (net_income / total_equity) * 100
        
        # 4. ROA
        if "당기순이익" in is_data and "자산총계" in bs_data:
            net_income = is_data["당기순이익"]["thstrm_amount"]
            total_assets = bs_data["자산총계"]["thstrm_amount"]
            if total_assets != 0:
                ratios["roa"] = (net_income / total_assets) * 100
        
        # 5. 영업이익률
        if "영업이익" in is_data and "매출액" in is_data:
            operating_income = is_data["영업이익"]["thstrm_amount"]
            revenue = is_data["매출액"]["thstrm_amount"]
            if revenue != 0:
                ratios["operating_profit_ratio"] = (operating_income / revenue) * 100
        
        # 6. 순이익률
        if "당기순이익" in is_data and "매출액" in is_data:
            net_income = is_data["당기순이익"]["thstrm_amount"]
            revenue = is_data["매출액"]["thstrm_amount"]
            if revenue != 0:
                ratios["net_profit_ratio"] = (net_income / revenue) * 100
        
        # 7. 이자보상배율
        if "영업이익" in is_data and "이자비용" in is_data:
            operating_income = is_data["영업이익"]["thstrm_amount"]
            interest_expense = is_data["이자비용"]["thstrm_amount"]
            if interest_expense != 0:
                ratios["interest_coverage_ratio"] = operating_income / interest_expense
        
        # 8. 부채의존도
        if "부채총계" in bs_data and "차입금" in bs_data:
            total_debt = bs_data["부채총계"]["thstrm_amount"]
            borrowings = bs_data["차입금"]["thstrm_amount"]
            if total_debt != 0:
                ratios["debt_dependency"] = (borrowings / total_debt) * 100
        
        # 9. 성장률 계산
        # 매출액 성장률
        if "매출액" in is_data:
            revenue = is_data["매출액"]["thstrm_amount"]
            prev_revenue = is_data["매출액"]["frmtrm_amount"]
            if prev_revenue != 0:
                ratios["sales_growth"] = self._calculate_growth_rate(revenue, prev_revenue)
        
        # 영업이익 성장률
        if "영업이익" in is_data:
            operating_income = is_data["영업이익"]["thstrm_amount"]
            prev_operating_income = is_data["영업이익"]["frmtrm_amount"]
            if prev_operating_income != 0:
                ratios["operating_profit_growth"] = self._calculate_growth_rate(operating_income, prev_operating_income)
        
        # EPS 성장률 (당기순이익 기준)
        if "당기순이익" in is_data:
            net_income = is_data["당기순이익"]["thstrm_amount"]
            prev_net_income = is_data["당기순이익"]["frmtrm_amount"]
            if prev_net_income != 0:
                ratios["eps_growth"] = self._calculate_growth_rate(net_income, prev_net_income)
        
        # 10. 현금흐름부채비율 (현금흐름표가 있는 경우에만 계산)
        if "영업활동현금흐름" in is_data and "부채총계" in bs_data:
            operating_cash_flow = is_data["영업활동현금흐름"]["thstrm_amount"]
            total_debt = bs_data["부채총계"]["thstrm_amount"]
            if total_debt != 0:
                ratios["cash_flow_debt_ratio"] = (operating_cash_flow / total_debt) * 100
        
        return ratios

    async def _save_ratios(self, corp_code: str, corp_name: str, bsns_year: str, ratios: Dict[str, float]) -> None:
        """계산된 재무비율을 저장합니다."""
        try:
            # 기존 재무비율 데이터 삭제
            delete_query = text("""
                DELETE FROM fin_data 
                WHERE corp_code = :corp_code 
                AND bsns_year = :bsns_year
                AND sj_div = 'RATIO'
            """)
            await self.db_session.execute(delete_query, {
                "corp_code": corp_code,
                "bsns_year": bsns_year
            })
            
            # 새로운 재무비율 데이터 저장
            insert_query = text("""
                INSERT INTO fin_data (
                    corp_code, corp_name, bsns_year, sj_div, sj_nm,
                    debt_ratio, current_ratio, interest_coverage_ratio,
                    operating_profit_ratio, net_profit_ratio, roe, roa,
                    debt_dependency, cash_flow_debt_ratio,
                    sales_growth, operating_profit_growth, eps_growth
                ) VALUES (
                    :corp_code, :corp_name, :bsns_year, 'RATIO', '재무비율',
                    :debt_ratio, :current_ratio, :interest_coverage_ratio,
                    :operating_profit_ratio, :net_profit_ratio, :roe, :roa,
                    :debt_dependency, :cash_flow_debt_ratio,
                    :sales_growth, :operating_profit_growth, :eps_growth
                )
            """)
            
            # 기본값 설정
            ratio_data = {
                "corp_code": corp_code,
                "corp_name": corp_name,
                "bsns_year": bsns_year,
                "debt_ratio": ratios.get("debt_ratio", 0),
                "current_ratio": ratios.get("current_ratio", 0),
                "interest_coverage_ratio": ratios.get("interest_coverage_ratio", 0),
                "operating_profit_ratio": ratios.get("operating_profit_ratio", 0),
                "net_profit_ratio": ratios.get("net_profit_ratio", 0),
                "roe": ratios.get("roe", 0),
                "roa": ratios.get("roa", 0),
                "debt_dependency": ratios.get("debt_dependency", 0),
                "cash_flow_debt_ratio": ratios.get("cash_flow_debt_ratio", 0),
                "sales_growth": ratios.get("sales_growth", 0),
                "operating_profit_growth": ratios.get("operating_profit_growth", 0),
                "eps_growth": ratios.get("eps_growth", 0)
            }
            
            await self.db_session.execute(insert_query, ratio_data)
            await self.db_session.commit()
            
            logger.info(f"재무비율 저장 완료: {corp_code}, {bsns_year}")
            
        except Exception as e:
            logger.error(f"재무비율 저장 중 오류 발생: {str(e)}")
            await self.db_session.rollback()
            raise 