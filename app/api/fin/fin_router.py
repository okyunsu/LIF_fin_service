from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.domin.fin.controller.fin_controller import FinController
from app.foundation.infra.database.database import get_db_session

router = APIRouter(prefix="/api/fin", tags=["financial"])

@router.get("/ratios/{company_name}")
async def get_financial_ratios(company_name: str, db: AsyncSession = Depends(get_db_session)):
    """회사명으로 재무비율을 조회합니다."""
    controller = FinController(db)
    return await controller.get_financial_ratios(company_name)

@router.get("/financial", summary="재무제표 조회 (기본 회사)")
async def get_financial(db: AsyncSession = Depends(get_db_session)):
    """기본 회사의 재무제표를 조회합니다."""
    controller = FinController(db)
    return await controller.get_financial()

@router.post("/financial", summary="회사명으로 재무제표 조회")
async def get_financial_by_name(company_name: str, db: AsyncSession = Depends(get_db_session)):
    """회사명으로 재무제표를 조회합니다."""
    controller = FinController(db)
    return await controller.get_financial(company_name=company_name)