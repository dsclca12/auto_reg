"""
Contribution 贡献功能 API
"""

import os
import time
import json
import logging
from typing import Optional, Dict, Any
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from core.config_store import ConfigStore

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/contribution", tags=["contribution"])


class ContributionConfig(BaseModel):
    """贡献配置"""
    enabled: bool = True
    api_url: str = ""
    api_key: str = ""


class ContributionStats(BaseModel):
    """贡献统计信息"""
    quota_account_count: Optional[int] = None
    quota_total: Optional[float] = None
    quota_used: Optional[float] = None
    quota_remaining: Optional[float] = None
    quota_used_percent: Optional[float] = None
    quota_remaining_percent: Optional[float] = None
    quota_remaining_accounts: Optional[float] = None


class ContributionKeyInfo(BaseModel):
    """API Key 信息"""
    id: Optional[str] = None
    name: Optional[str] = None
    source: Optional[str] = None
    balance_usd: Optional[float] = None
    bound_account_count: Optional[int] = None
    settled_amount_usd: Optional[float] = None
    created_ip: Optional[str] = None
    last_used_ip: Optional[str] = None
    created_at: Optional[str] = None
    last_used_at: Optional[str] = None


class RedeemRequest(BaseModel):
    """兑换请求"""
    amount_usd: Optional[float] = None


class RedeemResponse(BaseModel):
    """兑换响应"""
    code: str = ""
    redeemed_amount_usd: float = 0
    remaining_balance_usd: float = 0


def _request_contribution_api(
    method: str, 
    path: str, 
    api_url: str, 
    api_key: str,
    data: Optional[Dict] = None,
    headers: Optional[Dict] = None
) -> tuple[int, Dict[str, Any]]:
    """请求贡献服务器 API"""
    try:
        from curl_cffi import requests as cffi_requests
        
        url = f"{api_url.rstrip('/')}{path}"
        
        req_headers = {"Accept": "application/json"}
        if api_key:
            req_headers["X-Public-Key"] = api_key
        if headers:
            req_headers.update(headers)
        if data and method.upper() == "POST":
            req_headers["Content-Type"] = "application/json"
        
        response = cffi_requests.request(
            method=method.upper(),
            url=url,
            headers=req_headers,
            json=data,
            timeout=30,
            impersonate="chrome",
        )
        
        try:
            result = response.json()
        except Exception:
            result = {"raw": response.text}
        
        return response.status_code, result
    except Exception as e:
        logger.error(f"Contribution API 请求失败: {e}")
        return 0, {"error": str(e)}


@router.get("/config")
async def get_config():
    """获取贡献配置"""
    config = ConfigStore()
    return {
        "enabled": config.get("contribution_enabled", False),  # 默认关闭
        "api_url": config.get("contribution_api_url", ""),
        "api_key": config.get("contribution_api_key", ""),
    }


@router.post("/config")
async def save_config(cfg: ContributionConfig):
    """保存贡献配置"""
    config = ConfigStore()
    config.set("contribution_enabled", cfg.enabled)
    config.set("contribution_api_url", cfg.api_url)
    config.set("contribution_api_key", cfg.api_key)
    return {"success": True, "message": "配置已保存"}


@router.get("/quota-stats")
async def get_quota_stats():
    """获取配额统计信息"""
    config = ConfigStore()
    api_url = config.get("contribution_api_url", "")
    
    if not api_url:
        raise HTTPException(status_code=400, detail="请先配置服务器地址")
    
    # 尝试多个端点
    endpoints = ["/public/quota-stats", "/api/contribution/quota-stats"]
    
    for endpoint in endpoints:
        status, result = _request_contribution_api(
            "GET", endpoint, api_url, 
            config.get("contribution_api_key", "")
        )
        if status == 200:
            return result
    
    raise HTTPException(status_code=502, detail="无法连接到贡献服务器")


@router.get("/key-info")
async def get_key_info():
    """获取 API Key 信息"""
    config = ConfigStore()
    api_url = config.get("contribution_api_url", "")
    api_key = config.get("contribution_api_key", "")
    
    if not api_url:
        raise HTTPException(status_code=400, detail="请先配置服务器地址")
    if not api_key:
        raise HTTPException(status_code=400, detail="请先配置 API Key")
    
    # 尝试多个端点
    endpoints = ["/public/key-info", "/api/contribution/key-info"]
    
    for endpoint in endpoints:
        status, result = _request_contribution_api("GET", endpoint, api_url, api_key)
        if status == 200:
            return result
    
    raise HTTPException(status_code=502, detail="无法获取 Key 信息")


@router.post("/redeem")
async def redeem(req: RedeemRequest):
    """兑换余额"""
    config = ConfigStore()
    api_url = config.get("contribution_api_url", "")
    api_key = config.get("contribution_api_key", "")
    
    if not api_url:
        raise HTTPException(status_code=400, detail="请先配置服务器地址")
    if not api_key:
        raise HTTPException(status_code=400, detail="请先配置 API Key")
    
    payload = {}
    if req.amount_usd and req.amount_usd > 0:
        payload["amount_usd"] = req.amount_usd
    
    # 尝试多个端点
    endpoints = ["/public/redeem", "/api/contribution/redeem"]
    
    for endpoint in endpoints:
        status, result = _request_contribution_api(
            "POST", endpoint, api_url, api_key, data=payload
        )
        if status == 200 and result.get("code"):
            return result
    
    raise HTTPException(
        status_code=502, 
        detail=f"兑换失败: {result.get('error', '未知错误')}"
    )


@router.post("/generate-key")
async def generate_key(name: str = "public-upload"):
    """生成新的 API Key"""
    config = ConfigStore()
    api_url = config.get("contribution_api_url", "")
    
    if not api_url:
        raise HTTPException(status_code=400, detail="请先配置服务器地址")
    
    # 尝试多个端点
    endpoints = ["/public/generate", "/api/contribution/generate"]
    
    for endpoint in endpoints:
        status, result = _request_contribution_api(
            "POST", endpoint, api_url, "", data={"name": name}
        )
        if status == 200 and result.get("key"):
            # 自动保存新生成的 key
            config.set("contribution_api_key", result["key"])
            return result
    
    raise HTTPException(
        status_code=502, 
        detail=f"生成 Key 失败: {result.get('error', '未知错误')}"
    )
