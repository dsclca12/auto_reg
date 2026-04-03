"""
请求头增强模块
提供更真实的 HTTP 请求头模拟，包括：
- 动态优先级设置
- 缓存控制头
- 资源预加载提示
- 更真实的 Sec-CH-UA 链
- 请求意图模拟
"""

import random
import time
import hashlib
from typing import Dict, Optional, List, Tuple
from urllib.parse import urlparse


class RequestHeaderEnhancer:
    """请求头增强器"""
    
    def __init__(self):
        self._etag_cache: Dict[str, str] = {}
        self._last_modified_cache: Dict[str, str] = {}
        self._request_history: List[Dict] = []
    
    def enhance_headers(
        self,
        url: str,
        method: str = "GET",
        resource_type: str = "document",
        base_headers: Optional[Dict] = None,
        is_first_visit: bool = False,
    ) -> Dict:
        """
        增强请求头，使其更接近真实浏览器行为
        
        Args:
            url: 请求 URL
            method: HTTP 方法
            resource_type: 资源类型 (document, script, stylesheet, image, font, xhr, fetch)
            base_headers: 基础请求头
            is_first_visit: 是否首次访问（影响缓存行为）
        
        Returns:
            增强后的请求头
        """
        headers = (base_headers or {}).copy()
        
        # 添加资源类型特定的头
        headers.update(self._build_resource_headers(resource_type))
        
        # 添加缓存相关头
        if not is_first_visit:
            headers.update(self._build_cache_headers(url))
        
        # 添加优先级
        headers.update(self._build_priority_headers(resource_type))
        
        # 添加 Sec-CH-UA 完整链
        headers.update(self._build_sec_ch_ua_chain())
        
        # 添加请求意图
        headers["Sec-Fetch-Dest"] = self._map_resource_type_to_fetch_dest(resource_type)
        headers["Sec-Fetch-Mode"] = self._map_resource_type_to_fetch_mode(resource_type)
        
        # 更新请求历史
        self._request_history.append({
            "url": url,
            "method": method,
            "resource_type": resource_type,
            "timestamp": time.time(),
        })
        
        # 限制历史记录长度
        if len(self._request_history) > 100:
            self._request_history = self._request_history[-50:]
        
        return headers
    
    def _build_resource_headers(self, resource_type: str) -> Dict:
        """根据资源类型添加特定的请求头"""
        headers = {}
        
        if resource_type == "document":
            headers.update({
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control": "max-age=0",
            })
        
        elif resource_type in ("script", "stylesheet", "image", "font"):
            # 静态资源通常有较长的缓存
            headers["Accept"] = self._get_accept_for_resource(resource_type)
        
        elif resource_type in ("xhr", "fetch"):
            headers.update({
                "Accept": "application/json, text/plain, */*",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            })
        
        return headers
    
    def _build_cache_headers(self, url: str) -> Dict:
        """构建缓存相关头"""
        headers = {}
        
        # 检查是否有缓存的 ETag
        if url in self._etag_cache:
            headers["If-None-Match"] = self._etag_cache[url]
        
        # 检查是否有缓存的 Last-Modified
        if url in self._last_modified_cache:
            headers["If-Modified-Since"] = self._last_modified_cache[url]
        
        # 偶尔不发送缓存头（模拟用户强制刷新）
        if random.random() < 0.05:
            headers["Cache-Control"] = "no-cache"
            headers["Pragma"] = "no-cache"
        
        return headers
    
    def _build_priority_headers(self, resource_type: str) -> Dict:
        """构建优先级头"""
        # Chrome 使用 Priority 头表示资源优先级
        priority_map = {
            "document": "u=0, i",
            "script": "u=1, i",
            "stylesheet": "u=0, i",
            "image": "u=3, i",
            "font": "u=2, i",
            "xhr": "u=1, i",
            "fetch": "u=1, i",
        }
        
        priority = priority_map.get(resource_type, "u=3, i")
        
        # 偶尔调整优先级（模拟浏览器动态调整）
        if random.random() < 0.1:
            priority = f"u={random.randint(0, 3)}, i"
        
        return {"Priority": priority}
    
    def _build_sec_ch_ua_chain(self) -> Dict:
        """构建完整的 Sec-CH-UA 链"""
        # Chrome 136 示例
        sec_ch_ua = '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"'
        sec_ch_ua_full_version_list = (
            '"Chromium";v="136.0.7103.148", '
            '"Google Chrome";v="136.0.7103.148", '
            '"Not.A/Brand";v="99.0.0.0"'
        )
        
        # 随机微调版本号
        patch_version = random.randint(100, 200)
        sec_ch_ua_full_version_list = sec_ch_ua_full_version_list.replace(
            "7103.148", f"7103.{patch_version}"
        )
        
        return {
            "sec-ch-ua": sec_ch_ua,
            "sec-ch-ua-full-version-list": sec_ch_ua_full_version_list,
        }
    
    def _map_resource_type_to_fetch_dest(self, resource_type: str) -> str:
        """映射资源类型到 Sec-Fetch-Dest"""
        mapping = {
            "document": "document",
            "script": "script",
            "stylesheet": "style",
            "image": "image",
            "font": "font",
            "xhr": "empty",
            "fetch": "empty",
        }
        return mapping.get(resource_type, "empty")
    
    def _map_resource_type_to_fetch_mode(self, resource_type: str) -> str:
        """映射资源类型到 Sec-Fetch-Mode"""
        mapping = {
            "document": "navigate",
            "script": "no-cors",
            "stylesheet": "no-cors",
            "image": "no-cors",
            "font": "cors",
            "xhr": "cors",
            "fetch": "cors",
        }
        return mapping.get(resource_type, "cors")
    
    def _get_accept_for_resource(self, resource_type: str) -> str:
        """获取资源类型的 Accept 头"""
        accept_map = {
            "script": "application/javascript, */*;q=0.8",
            "stylesheet": "text/css, */*;q=0.1",
            "image": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            "font": "font/woff2,application/font-woff2;q=0.9,*/*;q=0.8",
        }
        return accept_map.get(resource_type, "*/*")
    
    def update_cache(self, url: str, response_headers: Dict):
        """
        更新缓存信息（从响应头中提取）
        
        Args:
            url: 请求 URL
            response_headers: 响应头字典
        """
        # 提取 ETag
        etag = response_headers.get("ETag") or response_headers.get("etag")
        if etag:
            self._etag_cache[url] = etag
        
        # 提取 Last-Modified
        last_modified = response_headers.get("Last-Modified") or response_headers.get("last-modified")
        if last_modified:
            self._last_modified_cache[url] = last_modified
    
    def get_request_pattern(self) -> Dict:
        """
        分析请求模式（用于调试和分析）
        
        Returns:
            请求模式统计
        """
        if not self._request_history:
            return {}
        
        resource_types = {}
        methods = {}
        
        for req in self._request_history:
            rt = req["resource_type"]
            resource_types[rt] = resource_types.get(rt, 0) + 1
            
            m = req["method"]
            methods[m] = methods.get(m, 0) + 1
        
        return {
            "total_requests": len(self._request_history),
            "resource_types": resource_types,
            "methods": methods,
            "time_span": self._request_history[-1]["timestamp"] - self._request_history[0]["timestamp"],
        }
    
    def reset(self):
        """重置缓存和历史"""
        self._etag_cache.clear()
        self._last_modified_cache.clear()
        self._request_history.clear()


# 预加载资源模拟
class PreloadSimulator:
    """预加载资源模拟器"""
    
    def __init__(self):
        self._preloaded_resources: List[Dict] = []
    
    def simulate_preload(self, url: str, resource_type: str = "script"):
        """
        模拟浏览器预加载资源
        
        Args:
            url: 预加载的 URL
            resource_type: 资源类型
        """
        self._preloaded_resources.append({
            "url": url,
            "resource_type": resource_type,
            "preloaded_at": time.time(),
        })
    
    def get_preloaded_resources(self) -> List[Dict]:
        """获取预加载的资源列表"""
        return self._preloaded_resources.copy()
    
    def clear(self):
        """清空预加载列表"""
        self._preloaded_resources.clear()


# 全局实例
_header_enhancer = RequestHeaderEnhancer()
_preload_simulator = PreloadSimulator()


def enhance_request_headers(
    url: str,
    method: str = "GET",
    resource_type: str = "document",
    base_headers: Optional[Dict] = None,
    is_first_visit: bool = False,
) -> Dict:
    """便捷函数：增强请求头"""
    return _header_enhancer.enhance_headers(
        url, method, resource_type, base_headers, is_first_visit
    )


def update_cache_from_response(url: str, response_headers: Dict):
    """便捷函数：从响应更新缓存"""
    _header_enhancer.update_cache(url, response_headers)


def simulate_resource_preload(url: str, resource_type: str = "script"):
    """便捷函数：模拟资源预加载"""
    _preload_simulator.simulate_preload(url, resource_type)


def get_header_enhancer() -> RequestHeaderEnhancer:
    """获取请求头增强器实例"""
    return _header_enhancer
