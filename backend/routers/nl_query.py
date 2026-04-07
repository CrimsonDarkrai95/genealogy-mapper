from fastapi import APIRouter, HTTPException, Request
from backend.models.responses import NLQueryRequest, NLQueryOut
from backend.nl.pipeline import run_nl_pipeline

router = APIRouter()


@router.post("/nl", response_model=NLQueryOut)
async def nl_query(body: NLQueryRequest, request: Request):
    pipeline_result = await run_nl_pipeline(body.query, body.api_key)
    return NLQueryOut(**pipeline_result)