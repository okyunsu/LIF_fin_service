from fastapi import HTTPException
from app.domin.fin.service.fin_service import FinService
from sqlalchemy.ext.asyncio import AsyncSession

class FinController:
    def __init__(self, db_session: AsyncSession):
        self.service = FinService(db_session)

    async def get_financial(self, company_name=None):
        try:
            data = await self.service.fetch_and_save_financial_data(company_name=company_name)
            return {
                "status": "success", 
                "message": "재무정보가 성공적으로 조회되었습니다.",
                "data": data
            }
        except ValueError as e:
            # 회사명 관련 오류
            error_message = str(e)
            raise HTTPException(status_code=400, detail=error_message)
        except Exception as e:
            # 기타 오류
            error_message = str(e)
            raise HTTPException(status_code=500, detail=error_message)

    async def get_financial_data(self, company_name=None):
        """재무제표 데이터를 조회합니다."""
        return await self.service.fetch_and_save_financial_data(company_name)