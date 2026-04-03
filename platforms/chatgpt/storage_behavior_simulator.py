"""
Cookie 和 Storage 行为模拟模块
提供更真实的浏览器存储行为模拟，包括：
- Cookie 的生命周期管理
- localStorage/sessionStorage 模拟
- 第三方 Cookie 模拟
- Cookie 的读写时序模拟
- 存储数据的序列化和版本管理
"""

import random
import time
import json
import hashlib
from typing import Dict, Optional, List, Any, Tuple
from dataclasses import dataclass, field


@dataclass
class CookieInfo:
    """Cookie 信息"""
    name: str
    value: str
    domain: str
    path: str = "/"
    expires: Optional[float] = None  # Unix timestamp
    max_age: Optional[int] = None
    secure: bool = True
    http_only: bool = True
    same_site: str = "Lax"  # Strict, Lax, None
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0


@dataclass
class StorageEntry:
    """存储条目"""
    key: str
    value: str
    created_at: float = field(default_factory=time.time)
    last_modified: float = field(default_factory=time.time)
    access_count: int = 0
    version: int = 1


class CookieManager:
    """Cookie 管理器"""
    
    def __init__(self):
        self._cookies: Dict[str, CookieInfo] = {}
        self._access_log: List[Dict] = []
    
    def set_cookie(
        self,
        name: str,
        value: str,
        domain: str,
        path: str = "/",
        max_age: Optional[int] = None,
        expires: Optional[float] = None,
        secure: bool = True,
        http_only: bool = True,
        same_site: str = "Lax",
    ):
        """
        设置 Cookie
        
        Args:
            name: Cookie 名称
            value: Cookie 值
            domain: 域名
            path: 路径
            max_age: 最大存活时间（秒）
            expires: 过期时间（Unix timestamp）
            secure: 是否仅 HTTPS
            http_only: 是否仅 HTTP
            same_site: SameSite 属性
        """
        # 计算过期时间
        if max_age is not None:
            expires = time.time() + max_age
        
        cookie = CookieInfo(
            name=name,
            value=value,
            domain=domain,
            path=path,
            expires=expires,
            max_age=max_age,
            secure=secure,
            http_only=http_only,
            same_site=same_site,
        )
        
        key = self._make_cookie_key(name, domain, path)
        self._cookies[key] = cookie
        
        self._log_access("set", cookie)
    
    def get_cookie(self, name: str, domain: str, path: str = "/") -> Optional[str]:
        """
        获取 Cookie 值
        
        Args:
            name: Cookie 名称
            domain: 域名
            path: 路径
        
        Returns:
            Cookie 值，如果不存在或已过期则返回 None
        """
        key = self._make_cookie_key(name, domain, path)
        cookie = self._cookies.get(key)
        
        if cookie is None:
            return None
        
        # 检查是否过期
        if self._is_cookie_expired(cookie):
            self._remove_cookie(key)
            return None
        
        # 更新访问时间
        cookie.last_accessed = time.time()
        cookie.access_count += 1
        
        self._log_access("get", cookie)
        
        return cookie.value
    
    def delete_cookie(self, name: str, domain: str, path: str = "/"):
        """删除 Cookie"""
        key = self._make_cookie_key(name, domain, path)
        self._remove_cookie(key)
    
    def get_all_cookies(self, domain: str, path: str = "/") -> Dict[str, str]:
        """
        获取指定域名和路径下的所有有效 Cookie
        
        Args:
            domain: 域名
            path: 路径
        
        Returns:
            Cookie 字典
        """
        valid_cookies = {}
        
        for key, cookie in list(self._cookies.items()):
            # 检查过期
            if self._is_cookie_expired(cookie):
                self._remove_cookie(key)
                continue
            
            # 检查域名和路径匹配
            if self._cookie_matches(cookie, domain, path):
                valid_cookies[cookie.name] = cookie.value
        
        return valid_cookies
    
    def cleanup_expired(self) -> int:
        """清理过期的 Cookie，返回清理数量"""
        expired_keys = [
            key for key, cookie in self._cookies.items()
            if self._is_cookie_expired(cookie)
        ]
        
        for key in expired_keys:
            self._remove_cookie(key)
        
        return len(expired_keys)
    
    def _make_cookie_key(self, name: str, domain: str, path: str) -> str:
        """生成 Cookie 唯一键"""
        return f"{name}:{domain}:{path}"
    
    def _is_cookie_expired(self, cookie: CookieInfo) -> bool:
        """检查 Cookie 是否过期"""
        if cookie.expires is not None:
            return time.time() > cookie.expires
        if cookie.max_age is not None:
            return time.time() > (cookie.created_at + cookie.max_age)
        return False
    
    def _remove_cookie(self, key: str):
        """删除 Cookie"""
        if key in self._cookies:
            del self._cookies[key]
    
    def _cookie_matches(self, cookie: CookieInfo, domain: str, path: str) -> bool:
        """检查 Cookie 是否匹配域名和路径"""
        # 域名匹配（支持子域名）
        if not (domain == cookie.domain or domain.endswith("." + cookie.domain.lstrip("."))):
            return False
        
        # 路径匹配
        if not path.startswith(cookie.path):
            return False
        
        return True
    
    def _log_access(self, action: str, cookie: CookieInfo):
        """记录访问日志"""
        self._access_log.append({
            "action": action,
            "cookie_name": cookie.name,
            "domain": cookie.domain,
            "timestamp": time.time(),
        })
        
        # 限制日志长度
        if len(self._access_log) > 1000:
            self._access_log = self._access_log[-500:]
    
    def get_access_log(self) -> List[Dict]:
        """获取访问日志"""
        return self._access_log.copy()


class StorageManager:
    """localStorage/sessionStorage 管理器"""
    
    def __init__(self, storage_type: str = "local"):
        """
        初始化存储器
        
        Args:
            storage_type: 存储类型 ("local" 或 "session")
        """
        self.storage_type = storage_type
        self._storage: Dict[str, StorageEntry] = {}
        self._access_log: List[Dict] = []
    
    def set_item(self, key: str, value: Any, version: int = 1):
        """
        设置存储项
        
        Args:
            key: 键
            value: 值（会被序列化为 JSON）
            version: 版本号
        """
        now = time.time()
        
        # 序列化值
        if isinstance(value, str):
            serialized = value
        else:
            serialized = json.dumps(value, ensure_ascii=False)
        
        if key in self._storage:
            # 更新已有项
            entry = self._storage[key]
            entry.value = serialized
            entry.last_modified = now
            entry.version = version
        else:
            # 创建新项
            entry = StorageEntry(
                key=key,
                value=serialized,
                created_at=now,
                last_modified=now,
                version=version,
            )
            self._storage[key] = entry
        
        self._log_access("set", key)
    
    def get_item(self, key: str, parse_json: bool = True) -> Optional[Any]:
        """
        获取存储项
        
        Args:
            key: 键
            parse_json: 是否解析 JSON
        
        Returns:
            存储的值
        """
        entry = self._storage.get(key)
        
        if entry is None:
            return None
        
        entry.last_modified = time.time()
        entry.access_count += 1
        
        self._log_access("get", key)
        
        if parse_json:
            try:
                return json.loads(entry.value)
            except json.JSONDecodeError:
                return entry.value
        
        return entry.value
    
    def remove_item(self, key: str):
        """删除存储项"""
        if key in self._storage:
            del self._storage[key]
            self._log_access("remove", key)
    
    def clear(self):
        """清空存储"""
        self._storage.clear()
        self._log_access("clear", "")
    
    def length(self) -> int:
        """获取存储项数量"""
        return len(self._storage)
    
    def key_at(self, index: int) -> Optional[str]:
        """获取指定索引的键"""
        keys = list(self._storage.keys())
        if 0 <= index < len(keys):
            return keys[index]
        return None
    
    def get_all_items(self) -> Dict[str, Any]:
        """获取所有存储项"""
        return {
            key: entry.value
            for key, entry in self._storage.items()
        }
    
    def _log_access(self, action: str, key: str):
        """记录访问日志"""
        self._access_log.append({
            "action": action,
            "key": key,
            "timestamp": time.time(),
        })
        
        if len(self._access_log) > 1000:
            self._access_log = self._access_log[-500:]


class BrowserStorageSimulator:
    """浏览器存储模拟器"""
    
    def __init__(self, domain: str = "chatgpt.com"):
        self.domain = domain
        self.cookie_manager = CookieManager()
        self.local_storage = StorageManager("local")
        self.session_storage = StorageManager("session")
        
        # 初始化默认的存储数据
        self._init_default_storage()
    
    def _init_default_storage(self):
        """初始化默认的存储数据（模拟真实浏览器的存储）"""
        now = time.time()
        
        # 模拟常见的 localStorage 数据
        self.local_storage.set_item("oai/apps/hasCompletedShare", "true")
        self.local_storage.set_item("oai/apps/capabilities", {
            "gizmo_live": True,
            "plugins": True,
            "sunshine": True,
        })
        self.local_storage.set_item("oai-did", self._generate_device_id())
        self.local_storage.set_item("oai/apps/locale", "en-US")
        self.local_storage.set_item("oai/country", "US")
        
        # 模拟 Google Analytics 数据
        self.local_storage.set_item("_ga", self._generate_ga_cookie())
        self.local_storage.set_item("_gid", self._generate_gid_cookie())
        
        # 模拟 Session 数据
        self.session_storage.set_item("oai/session/started_at", now)
        self.session_storage.set_item("oai/session/page_views", 0)
    
    def simulate_browsing_activity(self, duration: float = 30.0):
        """
        模拟浏览活动（定期更新存储）
        
        Args:
            duration: 模拟持续时间（秒）
        """
        start_time = time.time()
        
        while time.time() - start_time < duration:
            # 随机更新一些数据
            if random.random() < 0.3:
                # 更新页面浏览次数
                views = self.session_storage.get_item("oai/session/page_views") or 0
                self.session_storage.set_item("oai/session/page_views", views + 1)
            
            if random.random() < 0.2:
                # 更新时间戳
                self.local_storage.set_item("oai/apps/last_active", time.time())
            
            # 等待一段时间
            time.sleep(random.uniform(1.0, 5.0))
    
    def simulate_third_party_cookies(self):
        """模拟第三方 Cookie"""
        # Google Analytics
        self.cookie_manager.set_cookie(
            "_ga",
            self._generate_ga_cookie(),
            domain=".chatgpt.com",
            max_age=63072000,  # 2 年
            same_site="None",
        )
        
        self.cookie_manager.set_cookie(
            "_gid",
            self._generate_gid_cookie(),
            domain=".chatgpt.com",
            max_age=86400,  # 24 小时
            same_site="None",
        )
        
        # Google Tag Manager
        self.cookie_manager.set_cookie(
            "_gat",
            "1",
            domain=".chatgpt.com",
            max_age=60,  # 1 分钟
            same_site="None",
        )
    
    def _generate_device_id(self) -> str:
        """生成设备 ID"""
        import uuid
        return str(uuid.uuid4())
    
    def _generate_ga_cookie(self) -> str:
        """生成 Google Analytics Cookie"""
        # 格式：GA1.2.xxxxxxxxxx.xxxxxxxxxx
        client_id1 = random.randint(100000000, 999999999)
        client_id2 = int(time.time()) - random.randint(0, 86400 * 365)
        return f"GA1.2.{client_id1}.{client_id2}"
    
    def _generate_gid_cookie(self) -> str:
        """生成 Google Analytics GID Cookie"""
        # 格式：GA1.2.xxxxxxxxxx.xxxxxxxxxx (每天更新)
        client_id1 = random.randint(100000000, 999999999)
        client_id2 = int(time.time()) % 86400
        return f"GA1.2.{client_id1}.{client_id2}"
    
    def inject_to_session(self, session):
        """
        将 Cookie 注入到 HTTP session
        
        Args:
            session: curl_cffi 的 Session 对象
        """
        cookies = self.cookie_manager.get_all_cookies(self.domain)
        
        for name, value in cookies.items():
            try:
                session.cookies.set(name, value, domain=self.domain)
            except Exception:
                pass


# 全局实例
_storage_simulator: Optional[BrowserStorageSimulator] = None


def get_storage_simulator(domain: str = "chatgpt.com") -> BrowserStorageSimulator:
    """获取存储模拟器实例"""
    global _storage_simulator
    if _storage_simulator is None or _storage_simulator.domain != domain:
        _storage_simulator = BrowserStorageSimulator(domain)
    return _storage_simulator


def reset_storage_simulator():
    """重置存储模拟器"""
    global _storage_simulator
    _storage_simulator = None
