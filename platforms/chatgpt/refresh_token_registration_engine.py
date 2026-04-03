"""
注册流程引擎
从 main.py 中提取并重构的注册流程
"""

import base64
import json
import logging
import random
import secrets
import time
import urllib.parse
from typing import Optional, Dict, Any, Tuple, Callable
from dataclasses import dataclass
from datetime import datetime

from curl_cffi import requests as cffi_requests

from core.task_runtime import TaskInterruption
from .oauth import OAuthManager, OAuthStart
from .http_client import OpenAIHTTPClient
from .sentinel_browser import get_sentinel_token_via_browser
from .sentinel_token import build_sentinel_token
from .utils import (
    generate_datadog_trace,
    generate_device_id,
    generate_random_password,
    normalize_flow_url,
    seed_oai_device_cookie,
)
# from ..services import EmailServiceFactory, BaseEmailService, EmailServiceType  # removed: external dep
# from ..database import crud  # removed: external dep
# from ..database.session import get_db  # removed: external dep
from .constants import (
    OPENAI_API_ENDPOINTS,
    OPENAI_PAGE_TYPES,
    generate_random_user_info,
    OTP_CODE_PATTERN,
    DEFAULT_PASSWORD_LENGTH,
    PASSWORD_CHARSET,
)
# from ..config.settings import get_settings  # removed: external dep


logger = logging.getLogger(__name__)


@dataclass
class RegistrationResult:
    """注册结果"""
    success: bool
    email: str = ""
    password: str = ""  # 注册密码
    account_id: str = ""
    workspace_id: str = ""
    access_token: str = ""
    refresh_token: str = ""
    id_token: str = ""
    session_token: str = ""  # 会话令牌
    error_message: str = ""
    logs: list = None
    metadata: dict = None
    source: str = "register"  # 'register' 或 'login'，区分账号来源

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "success": self.success,
            "email": self.email,
            "password": self.password,
            "account_id": self.account_id,
            "workspace_id": self.workspace_id,
            "access_token": self.access_token[:20] + "..." if self.access_token else "",
            "refresh_token": self.refresh_token[:20] + "..." if self.refresh_token else "",
            "id_token": self.id_token[:20] + "..." if self.id_token else "",
            "session_token": self.session_token[:20] + "..." if self.session_token else "",
            "error_message": self.error_message,
            "logs": self.logs or [],
            "metadata": self.metadata or {},
            "source": self.source,
        }


@dataclass
class SignupFormResult:
    """提交注册表单的结果"""
    success: bool
    page_type: str = ""  # 响应中的 page.type 字段
    is_existing_account: bool = False  # 是否为已注册账号
    response_data: Dict[str, Any] = None  # 完整的响应数据
    error_message: str = ""


class RefreshTokenRegistrationEngine:
    """
    注册引擎
    负责协调邮箱服务、OAuth 流程和 OpenAI API 调用
    """

    def __init__(
        self,
        email_service,
        proxy_url: Optional[str] = None,
        callback_logger: Optional[Callable[[str], None]] = None,
        task_uuid: Optional[str] = None,
        browser_mode: str = "headless",
    ):
        """
        初始化注册引擎

        Args:
            email_service: 邮箱服务实例
            proxy_url: 代理 URL
            callback_logger: 日志回调函数
            task_uuid: 任务 UUID（用于数据库记录）
        """
        self.email_service = email_service
        self.proxy_url = proxy_url
        self.callback_logger = callback_logger or (lambda msg: logger.info(msg))
        self.task_uuid = task_uuid
        self.browser_mode = str(browser_mode or "headless").strip().lower()

        # 创建 HTTP 客户端
        self.http_client = OpenAIHTTPClient(proxy_url=proxy_url)

        # 创建 OAuth 管理器
        from .constants import OAUTH_CLIENT_ID, OAUTH_AUTH_URL, OAUTH_TOKEN_URL, OAUTH_REDIRECT_URI, OAUTH_SCOPE
        self.oauth_manager = OAuthManager(
            client_id=OAUTH_CLIENT_ID,
            auth_url=OAUTH_AUTH_URL,
            token_url=OAUTH_TOKEN_URL,
            redirect_uri=OAUTH_REDIRECT_URI,
            scope=OAUTH_SCOPE,
            proxy_url=proxy_url  # 传递代理配置
        )

        # 状态变量
        self.email: Optional[str] = None
        self.password: Optional[str] = None  # 注册密码
        self.email_info: Optional[Dict[str, Any]] = None
        self.oauth_start: Optional[OAuthStart] = None
        self.session: Optional[cffi_requests.Session] = None
        self.session_token: Optional[str] = None  # 会话令牌
        self.logs: list = []
        self._otp_sent_at: Optional[float] = None  # OTP 发送时间戳
        self._device_id: Optional[str] = None  # 当前注册流程复用的 Device ID
        self._used_verification_codes = set()  # 已取过的验证码，避免二次登录时捞到旧码
        self._is_existing_account: bool = False  # 是否为已注册账号（用于自动登录）
        self._token_acquisition_requires_login: bool = False  # 新注册账号需要二次登录拿 token
        self._post_otp_continue_url: str = ""
        self._post_otp_page_type: str = ""

    def _log(self, message: str, level: str = "info"):
        """记录日志"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_message = f"[{timestamp}] {message}"

        # 添加到日志列表
        self.logs.append(log_message)

        # 调用回调函数
        if self.callback_logger:
            self.callback_logger(log_message)

        # 记录到数据库（如果有关联任务）
        if self.task_uuid:
            try:
                with get_db() as db:
                    crud.append_task_log(db, self.task_uuid, log_message)
            except Exception as e:
                logger.warning(f"记录任务日志失败: {e}")

        # 根据级别记录到日志系统
        if level == "error":
            logger.error(message)
        elif level == "warning":
            logger.warning(message)
        else:
            logger.info(message)

    def _generate_password(self, length: int = DEFAULT_PASSWORD_LENGTH) -> str:
        """生成随机密码"""
        resolved_length = max(int(length or DEFAULT_PASSWORD_LENGTH), 8)
        return generate_random_password(resolved_length)

    def _check_ip_location(self) -> Tuple[bool, Optional[str]]:
        """检查 IP 地理位置"""
        try:
            return self.http_client.check_ip_location()
        except Exception as e:
            self._log(f"检查 IP 地理位置失败: {e}", "error")
            return False, None

    def _create_email(self) -> bool:
        """创建邮箱"""
        try:
            self._log(f"正在创建 {self.email_service.service_type.value} 邮箱...")
            self.email_info = self.email_service.create_email()

            if not self.email_info or "email" not in self.email_info:
                self._log("创建邮箱失败: 返回信息不完整", "error")
                return False

            email_value = str(self.email_info.get("email") or "").strip()
            if not email_value:
                self._log(
                    f"创建邮箱失败: {self.email_service.service_type.value} 返回空邮箱地址",
                    "error",
                )
                return False

            self.email_info["email"] = email_value
            self.email = email_value
            self._log(f"成功创建邮箱: {self.email}")
            return True

        except Exception as e:
            self._log(f"创建邮箱失败: {e}", "error")
            return False

    def _start_oauth(self) -> bool:
        """开始 OAuth 流程"""
        try:
            self._log("开始 OAuth 授权流程...")
            self.oauth_start = self.oauth_manager.start_oauth()
            self._log(f"OAuth URL 已生成: {self.oauth_start.auth_url[:80]}...")
            return True
        except Exception as e:
            self._log(f"生成 OAuth URL 失败: {e}", "error")
            return False

    def _init_session(self) -> bool:
        """初始化会话"""
        try:
            self.session = self.http_client.session
            if self._device_id:
                seed_oai_device_cookie(self.session, self._device_id)
            return True
        except Exception as e:
            self._log(f"初始化会话失败: {e}", "error")
            return False

    def _get_device_id(self) -> Optional[str]:
        """获取并复用 Device ID，同时访问 OAuth URL 建立当前会话。"""
        if not self.oauth_start:
            return None

        if not self._device_id:
            self._device_id = generate_device_id()

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                if not self.session:
                    self.session = self.http_client.session

                seed_oai_device_cookie(self.session, self._device_id)

                response = self.session.get(
                    self.oauth_start.auth_url,
                    timeout=20
                )

                if response.status_code < 400:
                    self._log(f"Device ID: {self._device_id}")
                    return self._device_id

                self._log(
                    f"获取 Device ID 失败: 建立 OAuth 会话返回 HTTP {response.status_code} (第 {attempt}/{max_attempts} 次)",
                    "warning" if attempt < max_attempts else "error"
                )
            except Exception as e:
                self._log(
                    f"获取 Device ID 失败: {e} (第 {attempt}/{max_attempts} 次)",
                    "warning" if attempt < max_attempts else "error"
                )

            if attempt < max_attempts:
                time.sleep(attempt)
                self.http_client.close()
                self.session = self.http_client.session

        return None

    def _default_user_agent(self) -> str:
        try:
            user_agent = str(self.session.headers.get("User-Agent") or "").strip()
            if user_agent:
                return user_agent
        except Exception:
            pass
        return (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        )

    def _build_json_headers(
        self,
        *,
        referer: str,
        include_device_id: bool = False,
        include_datadog: bool = False,
        content_type: str = "application/json",
        accept: str = "application/json",
    ) -> Dict[str, str]:
        headers = {
            "accept": accept,
            "accept-language": "en-US,en;q=0.9",
            "content-type": content_type,
            "origin": "https://auth.openai.com",
            "referer": referer,
            "user-agent": self._default_user_agent(),
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }
        if include_device_id and self._device_id:
            headers["oai-device-id"] = self._device_id
        if include_datadog:
            headers.update(generate_datadog_trace())
        return headers

    def _build_navigation_headers(self, *, referer: str) -> Dict[str, str]:
        return {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "referer": referer,
            "user-agent": self._default_user_agent(),
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
        }

    def _check_sentinel(self, did: str, *, flow: str = "authorize_continue") -> Optional[str]:
        """按参考实现为指定 flow 生成完整 Sentinel token。"""
        try:
            if not self.session:
                self.session = self.http_client.session
            if flow in {"username_password_create", "oauth_create_account"}:
                # 服务器无 XServer，必须强制 headless
                import os
                has_display = bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))
                force_headless = not has_display
                browser_token = get_sentinel_token_via_browser(
                    flow=flow,
                    proxy=self.proxy_url,
                    headless=force_headless,
                    device_id=did,
                    log_fn=lambda msg: self._log(msg),
                )
                if browser_token:
                    self._log(f"Sentinel Browser token 获取成功 ({flow})")
                    return browser_token
            sen_token = build_sentinel_token(self.session, did, flow=flow)
            if sen_token:
                self._log(f"Sentinel token 获取成功 ({flow})")
                return sen_token
            self._log(f"Sentinel 检查失败: 未获取到 token ({flow})", "warning")
            return None
        except Exception as e:
            self._log(f"Sentinel 检查异常 ({flow}): {e}", "warning")
            return None

    def _submit_auth_start(
        self,
        did: str,
        sen_token: Optional[str],
        *,
        screen_hint: str,
        referer: str,
        log_label: str,
        record_existing_account: bool = True,
    ) -> SignupFormResult:
        """
        提交授权入口表单

        Returns:
            SignupFormResult: 提交结果，包含账号状态判断
        """
        try:
            # 先访问注册页面获取 Cloudflare Cookie
            self._log(f"{log_label}: 先访问页面获取 Cloudflare Cookie...")
            try:
                page_url = referer
                nav_headers = self._build_navigation_headers(referer=page_url)
                page_resp = self.session.get(
                    page_url,
                    headers=nav_headers,
                    allow_redirects=True,
                    timeout=15,
                )
                self._log(f"{log_label}: 页面访问状态: {page_resp.status_code}")
                # 短暂等待 Cloudflare 验证完成
                time.sleep(random.uniform(1.0, 2.5))
            except Exception as page_err:
                self._log(f"{log_label}: 页面访问异常（继续尝试）: {page_err}")

            request_body = json.dumps({
                "username": {
                    "value": self.email,
                    "kind": "email",
                },
                "screen_hint": screen_hint,
            })

            headers = self._build_json_headers(
                referer=referer,
                include_device_id=True,
                include_datadog=True,
            )
            headers["oai-device-id"] = did

            if sen_token:
                headers["openai-sentinel-token"] = sen_token

            # 提交请求前添加自然延迟
            time.sleep(random.uniform(0.8, 2.0))

            response = self.session.post(
                OPENAI_API_ENDPOINTS["signup"],
                headers=headers,
                data=request_body,
            )

            self._log(f"{log_label}状态: {response.status_code}")

            if response.status_code != 200:
                return SignupFormResult(
                    success=False,
                    error_message=f"HTTP {response.status_code}: {response.text[:200]}"
                )

            # 解析响应判断账号状态
            try:
                response_data = response.json()
                page_type = response_data.get("page", {}).get("type", "")
                self._log(f"响应页面类型: {page_type}")

                is_existing = page_type == OPENAI_PAGE_TYPES["EMAIL_OTP_VERIFICATION"]

                if is_existing:
                    self._otp_sent_at = time.time()
                    if record_existing_account:
                        self._log(f"检测到已注册账号，将自动切换到登录流程")
                        self._is_existing_account = True
                    else:
                        self._log("登录流程已触发，等待系统发送验证码")

                return SignupFormResult(
                    success=True,
                    page_type=page_type,
                    is_existing_account=is_existing,
                    response_data=response_data
                )

            except Exception as parse_error:
                self._log(f"解析响应失败: {parse_error}", "warning")
                # 无法解析，默认成功
                return SignupFormResult(success=True)

        except Exception as e:
            self._log(f"{log_label}失败: {e}", "error")
            return SignupFormResult(success=False, error_message=str(e))

    def _submit_signup_form(
        self,
        did: str,
        sen_token: Optional[str],
        *,
        record_existing_account: bool = True,
    ) -> SignupFormResult:
        """提交注册入口表单。"""
        return self._submit_auth_start(
            did,
            sen_token,
            screen_hint="signup",
            referer="https://auth.openai.com/create-account",
            log_label="提交注册表单",
            record_existing_account=record_existing_account,
        )

    def _submit_login_start(self, did: str, sen_token: Optional[str]) -> SignupFormResult:
        """提交登录入口表单。"""
        return self._submit_auth_start(
            did,
            sen_token,
            screen_hint="login",
            referer="https://auth.openai.com/log-in",
            log_label="提交登录入口",
            record_existing_account=False,
        )

    def _submit_login_password(self) -> SignupFormResult:
        """提交登录密码，进入邮箱验证码页面。"""
        try:
            headers = self._build_json_headers(
                referer="https://auth.openai.com/log-in/password",
                include_device_id=True,
                include_datadog=True,
            )
            sen_token = self._check_sentinel(self._device_id or "", flow="password_verify")
            if sen_token:
                headers["openai-sentinel-token"] = sen_token

            response = self.session.post(
                OPENAI_API_ENDPOINTS["password_verify"],
                headers=headers,
                data=json.dumps({"password": self.password}),
            )

            self._log(f"提交登录密码状态: {response.status_code}")

            if response.status_code != 200:
                return SignupFormResult(
                    success=False,
                    error_message=f"HTTP {response.status_code}: {response.text[:200]}"
                )

            response_data = response.json()
            page_type = response_data.get("page", {}).get("type", "")
            self._log(f"登录密码响应页面类型: {page_type}")

            is_existing = page_type == OPENAI_PAGE_TYPES["EMAIL_OTP_VERIFICATION"]
            if is_existing:
                self._otp_sent_at = time.time()
                self._log("登录密码校验通过，等待系统发送验证码")

            return SignupFormResult(
                success=True,
                page_type=page_type,
                is_existing_account=is_existing,
                response_data=response_data,
            )

        except Exception as e:
            self._log(f"提交登录密码失败: {e}", "error")
            return SignupFormResult(success=False, error_message=str(e))

    def _reset_auth_flow(self) -> None:
        """重置会话，准备重新发起 OAuth 流程。"""
        self.http_client.close()
        self.session = None
        self.oauth_start = None
        self.session_token = None
        self._otp_sent_at = None
        self._post_otp_continue_url = ""
        self._post_otp_page_type = ""

    def _prepare_authorize_flow(self, label: str) -> Tuple[Optional[str], Optional[str]]:
        """初始化当前阶段的授权流程，返回 device id 和 sentinel token。"""
        self._log(f"{label}: 初始化会话...")
        if not self._init_session():
            return None, None

        self._log(f"{label}: 初始化 OAuth 授权流程...")
        if not self._start_oauth():
            return None, None

        self._log(f"{label}: 获取 Device ID...")
        did = self._get_device_id()
        if not did:
            return None, None

        self._log(f"{label}: 执行 Sentinel POW 验证...")
        sen_token = self._check_sentinel(did)
        if not sen_token:
            return did, None

        self._log(f"{label}: Sentinel 验证通过")
        return did, sen_token

    def _complete_token_exchange(self, result: RegistrationResult) -> bool:
        """在登录态已建立后，继续完成 workspace 和 OAuth token 获取。"""
        self._log("等待登录验证码...")
        code = self._get_verification_code()
        if not code:
            result.error_message = "获取验证码失败"
            return False

        self._log("校验登录验证码...")
        if not self._validate_verification_code(code):
            result.error_message = "验证码校验失败"
            return False

        # 检查是否进入 add_phone 页面（需要手机号验证）
        post_page_type = getattr(self, "_post_otp_page_type", "") or ""
        if post_page_type.lower() == "add_phone":
            self._log("注册被拦截：OpenAI 要求绑定手机号", "warning")
            result.error_message = (
                "注册失败：当前域名或 IP 可能被 OpenAI 风控，触发手机号验证。"
                "建议：1) 更换代理 IP（使用住宅代理）；2) 更换邮箱域名；3) 降低注册频率"
            )
            return False

        self._log("获取 Workspace ID...")
        workspace_id = self._get_workspace_id()
        if not workspace_id:
            result.error_message = "获取 Workspace ID 失败"
            return False

        result.workspace_id = workspace_id

        self._log("选择 Workspace...")
        continue_url = self._select_workspace(workspace_id)
        if not continue_url:
            result.error_message = "选择 Workspace 失败"
            return False

        self._log("跟随重定向链...")
        callback_url = self._follow_redirects(continue_url)
        if not callback_url:
            result.error_message = "跟随重定向链失败"
            return False

        self._log("处理 OAuth 回调并获取 Token...")
        token_info = self._handle_oauth_callback(callback_url)
        if not token_info:
            result.error_message = "处理 OAuth 回调失败"
            return False

        result.account_id = token_info.get("account_id", "")
        result.access_token = token_info.get("access_token", "")
        result.refresh_token = token_info.get("refresh_token", "")
        result.id_token = token_info.get("id_token", "")
        result.password = self.password or ""
        result.source = "login" if self._is_existing_account else "register"

        session_cookie = self.session.cookies.get("__Secure-next-auth.session-token")
        if session_cookie:
            self.session_token = session_cookie
            result.session_token = session_cookie
            self._log("成功获取 Session Token")

        return True

    def _restart_login_flow(self) -> Tuple[bool, str]:
        """新注册账号完成建号后，重新发起一次登录流程拿 token。"""
        self._token_acquisition_requires_login = True
        self._log("注册完成，开始重新登录以获取 Token...")
        self._reset_auth_flow()

        did, sen_token = self._prepare_authorize_flow("重新登录")
        if not did:
            return False, "重新登录时获取 Device ID 失败"
        if not sen_token:
            return False, "重新登录时 Sentinel POW 验证失败"

        login_start_result = self._submit_login_start(did, sen_token)
        if not login_start_result.success:
            return False, f"重新登录提交邮箱失败: {login_start_result.error_message}"
        if login_start_result.page_type != OPENAI_PAGE_TYPES["LOGIN_PASSWORD"]:
            return False, f"重新登录未进入密码页面: {login_start_result.page_type or 'unknown'}"

        password_result = self._submit_login_password()
        if not password_result.success:
            return False, f"重新登录提交密码失败: {password_result.error_message}"
        if not password_result.is_existing_account:
            return False, f"重新登录未进入验证码页面: {password_result.page_type or 'unknown'}"
        return True, ""

    def _register_password(self) -> Tuple[bool, Optional[str]]:
        """注册密码"""
        try:
            # 生成密码
            password = self._generate_password()
            self.password = password  # 保存密码到实例变量
            self._log(f"生成密码: {password}")

            # 先访问注册页面获取 Cloudflare Cookie
            self._log("提交密码前：先访问页面获取 Cloudflare Cookie...")
            try:
                page_url = "https://auth.openai.com/create-account/password"
                nav_headers = self._build_navigation_headers(referer=page_url)
                page_resp = self.session.get(
                    page_url,
                    headers=nav_headers,
                    allow_redirects=True,
                    timeout=15,
                )
                self._log(f"提交密码前：页面访问状态: {page_resp.status_code}")
                time.sleep(random.uniform(1.5, 3.0))
            except Exception as page_err:
                self._log(f"提交密码前：页面访问异常（继续尝试）: {page_err}")

            # 提交密码注册
            register_body = json.dumps({
                "password": password,
                "username": self.email
            })

            headers = self._build_json_headers(
                referer="https://auth.openai.com/create-account/password",
                include_device_id=True,
                include_datadog=True,
            )
            sen_token = self._check_sentinel(
                self._device_id or "",
                flow="username_password_create",
            )
            if sen_token:
                headers["openai-sentinel-token"] = sen_token

            # 提交前添加自然延迟
            time.sleep(random.uniform(1.0, 2.5))

            response = self.session.post(
                OPENAI_API_ENDPOINTS["register"],
                headers=headers,
                data=register_body,
            )

            self._log(f"提交密码状态: {response.status_code}")

            if response.status_code != 200:
                error_text = response.text[:500]
                self._log(f"密码注册失败: {error_text}", "warning")

                # 解析错误信息，判断是否是邮箱已注册
                try:
                    error_json = response.json()
                    error_msg = error_json.get("error", {}).get("message", "")
                    error_code = error_json.get("error", {}).get("code", "")

                    # 检测邮箱已注册的情况
                    if "already" in error_msg.lower() or "exists" in error_msg.lower() or error_code == "user_exists":
                        self._log(f"邮箱 {self.email} 可能已在 OpenAI 注册过", "error")
                        # 标记此邮箱为已注册状态
                        self._mark_email_as_registered()
                except Exception:
                    pass

                return False, None

            return True, password

        except Exception as e:
            self._log(f"密码注册失败: {e}", "error")
            return False, None

    def _mark_email_as_registered(self):
        """标记邮箱为已注册状态（用于防止重复尝试）"""
        try:
            with get_db() as db:
                # 检查是否已存在该邮箱的记录
                existing = crud.get_account_by_email(db, self.email)
                if not existing:
                    # 创建一个失败记录，标记该邮箱已注册过
                    crud.create_account(
                        db,
                        email=self.email,
                        password="",  # 空密码表示未成功注册
                        email_service=self.email_service.service_type.value,
                        email_service_id=self.email_info.get("service_id") if self.email_info else None,
                        status="failed",
                        extra_data={"register_failed_reason": "email_already_registered_on_openai"}
                    )
                    self._log(f"已在数据库中标记邮箱 {self.email} 为已注册状态")
        except Exception as e:
            logger.warning(f"标记邮箱状态失败: {e}")

    def _send_verification_code(self) -> bool:
        """发送验证码"""
        try:
            # 记录发送时间戳
            self._otp_sent_at = time.time()

            response = self.session.get(
                OPENAI_API_ENDPOINTS["send_otp"],
                headers=self._build_navigation_headers(
                    referer="https://auth.openai.com/create-account/password"
                ),
            )

            self._log(f"验证码发送状态: {response.status_code}")
            return response.status_code == 200

        except Exception as e:
            self._log(f"发送验证码失败: {e}", "error")
            return False

    def _get_verification_code(self) -> Optional[str]:
        """获取验证码"""
        try:
            self._log(f"正在等待邮箱 {self.email} 的验证码...")

            email_id = self.email_info.get("service_id") if self.email_info else None
            exclude_codes = {
                str(code).strip()
                for code in self._used_verification_codes
                if str(code or "").strip()
            }
            if exclude_codes:
                self._log(
                    "本轮取件将跳过已取过的验证码: "
                    + ", ".join(sorted(exclude_codes))
                )
            code = self.email_service.get_verification_code(
                email=self.email,
                email_id=email_id,
                timeout=30,
                pattern=OTP_CODE_PATTERN,
                otp_sent_at=self._otp_sent_at,
                exclude_codes=exclude_codes,
            )

            if code:
                self._used_verification_codes.add(str(code).strip())
                self._log(f"成功获取验证码: {code}")
                return code
            else:
                self._log("等待验证码超时", "error")
                return None

        except TaskInterruption:
            raise
        except Exception as e:
            self._log(f"获取验证码失败: {e}", "error")
            return None

    def _validate_verification_code(self, code: str) -> bool:
        """验证验证码"""
        try:
            code_body = f'{{"code":"{code}"}}'
            headers = self._build_json_headers(
                referer="https://auth.openai.com/email-verification",
                include_device_id=True,
                include_datadog=True,
            )
            sen_token = self._check_sentinel(
                self._device_id or "",
                flow="email_otp_validate",
            )
            if sen_token:
                headers["openai-sentinel-token"] = sen_token

            response = self.session.post(
                OPENAI_API_ENDPOINTS["validate_otp"],
                headers=headers,
                data=code_body,
            )

            self._log(f"验证码校验状态: {response.status_code}")
            if response.status_code != 200:
                return False

            try:
                response_data = response.json() or {}
            except Exception:
                response_data = {}

            self._post_otp_continue_url = str(response_data.get("continue_url") or "").strip()
            self._post_otp_page_type = str(
                ((response_data.get("page") or {}).get("type")) or ""
            ).strip()
            if self._post_otp_continue_url:
                self._log(f"验证码校验后 continue_url: {self._post_otp_continue_url}")
            if self._post_otp_page_type:
                self._log(f"验证码校验后页面类型: {self._post_otp_page_type}")
            return True

        except Exception as e:
            self._log(f"验证验证码失败: {e}", "error")
            return False

    def _create_user_account(self) -> bool:
        """创建用户账户"""
        try:
            user_info = generate_random_user_info()
            self._log(f"生成用户信息: {user_info['name']}, 生日: {user_info['birthdate']}")
            create_account_body = json.dumps(user_info)

            headers = self._build_json_headers(
                referer="https://auth.openai.com/about-you",
                include_device_id=True,
                include_datadog=True,
            )
            sen_token = self._check_sentinel(
                self._device_id or "",
                flow="oauth_create_account",
            )
            if sen_token:
                headers["openai-sentinel-token"] = sen_token

            response = self.session.post(
                OPENAI_API_ENDPOINTS["create_account"],
                headers=headers,
                data=create_account_body,
            )

            self._log(f"账户创建状态: {response.status_code}")

            if response.status_code == 200:
                return True

            body_preview = response.text[:200]
            self._log(f"账户创建失败: {body_preview}", "warning")

            should_retry = response.status_code in (400, 403) and (
                "sentinel" in body_preview.lower()
                or "registration_disallowed" in body_preview.lower()
            )
            if not should_retry:
                return False

            self._log("create_account 命中 sentinel 校验，刷新 token 后重试一次...", "warning")
            retry_token = self._check_sentinel(
                self._device_id or "",
                flow="oauth_create_account",
            )
            if retry_token:
                headers["openai-sentinel-token"] = retry_token

            retry_resp = self.session.post(
                OPENAI_API_ENDPOINTS["create_account"],
                headers=headers,
                data=create_account_body,
            )
            self._log(f"账户创建重试状态: {retry_resp.status_code}")
            if retry_resp.status_code == 200:
                return True

            self._log(f"账户创建重试失败: {retry_resp.text[:200]}", "warning")
            return False

        except Exception as e:
            self._log(f"创建账户失败: {e}", "error")
            return False

    @staticmethod
    def _decode_cookie_json_value(raw_value: str) -> Optional[Dict[str, Any]]:
        value = str(raw_value or "").strip()
        if not value:
            return None

        candidates = [value]
        if "." in value:
            parts = value.split(".")
            candidates = [parts[0], value, *parts[:2]]

        for candidate in candidates:
            candidate = str(candidate or "").strip()
            if not candidate:
                continue
            padded = candidate + "=" * (-len(candidate) % 4)
            for decoder in (base64.urlsafe_b64decode, base64.b64decode):
                try:
                    decoded = decoder(padded.encode("ascii")).decode("utf-8")
                    parsed = json.loads(decoded)
                except Exception:
                    continue
                if isinstance(parsed, dict):
                    return parsed
        return None

    def _decode_auth_session_cookie(self) -> Optional[Dict[str, Any]]:
        try:
            auth_cookie = self.session.cookies.get("oai-client-auth-session")
        except Exception:
            auth_cookie = None
        if not auth_cookie:
            return None
        return self._decode_cookie_json_value(auth_cookie)

    def _extract_callback_url_from_candidate(self, candidate: str) -> str:
        normalized = normalize_flow_url(str(candidate or "").strip(), auth_base="https://auth.openai.com")
        if not normalized:
            return ""
        parsed = urllib.parse.urlparse(normalized)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        code = str((query.get("code") or [""])[0] or "").strip()
        state = str((query.get("state") or [""])[0] or "").strip()
        return normalized if code and state else ""

    def _follow_and_extract_callback_url(self, start_url: str, max_depth: int = 10) -> str:
        current_url = normalize_flow_url(start_url, auth_base="https://auth.openai.com")
        referer = "https://auth.openai.com/sign-in-with-chatgpt/codex/consent"

        for hop in range(max_depth):
            if not current_url:
                return ""

            callback_url = self._extract_callback_url_from_candidate(current_url)
            if callback_url:
                return callback_url

            self._log(f"OAuth 跟随重定向 {hop + 1}/{max_depth}: {current_url[:120]}...")

            try:
                response = self.session.get(
                    current_url,
                    headers=self._build_navigation_headers(referer=referer),
                    allow_redirects=False,
                    timeout=15,
                )
            except Exception as e:
                self._log(f"OAuth 跟随重定向失败: {e}", "warning")
                return ""

            referer = current_url
            location = str(response.headers.get("Location") or "").strip()
            if response.status_code in (301, 302, 303, 307, 308) and location:
                next_url = normalize_flow_url(
                    urllib.parse.urljoin(current_url, location),
                    auth_base="https://auth.openai.com",
                )
                callback_url = self._extract_callback_url_from_candidate(next_url)
                if callback_url:
                    return callback_url
                current_url = next_url
                continue

            callback_url = self._extract_callback_url_from_candidate(str(response.url))
            if callback_url:
                return callback_url
            break

        return ""

    def _create_account_during_oauth_if_needed(self) -> str:
        user_info = generate_random_user_info()
        headers = self._build_json_headers(
            referer="https://auth.openai.com/about-you",
            include_device_id=True,
            include_datadog=True,
        )
        sen_token = self._check_sentinel(self._device_id or "", flow="oauth_create_account")
        if sen_token:
            headers["openai-sentinel-token"] = sen_token

        try:
            response = self.session.post(
                OPENAI_API_ENDPOINTS["create_account"],
                headers=headers,
                data=json.dumps(user_info),
            )
        except Exception as e:
            self._log(f"OAuth about-you create_account 失败: {e}", "warning")
            return ""

        if response.status_code == 200:
            try:
                response_data = response.json() or {}
            except Exception:
                response_data = {}
            return normalize_flow_url(
                str(response_data.get("continue_url") or ""),
                auth_base="https://auth.openai.com",
            )

        body_text = response.text[:200]
        if response.status_code == 400 and "already_exists" in body_text.lower():
            return "https://auth.openai.com/sign-in-with-chatgpt/codex/consent"

        self._log(f"OAuth about-you create_account 失败: {response.status_code} {body_text}", "warning")
        return ""

    def _resolve_post_otp_continue_url(self) -> str:
        continue_url = normalize_flow_url(
            self._post_otp_continue_url,
            auth_base="https://auth.openai.com",
        )
        page_type = str(self._post_otp_page_type or "").strip().lower()

        if continue_url and "about-you" in continue_url:
            self._log("OTP 后进入 about-you，按参考 RT 逻辑补齐 consent 跳转...")
            try:
                response = self.session.get(
                    "https://auth.openai.com/about-you",
                    headers=self._build_navigation_headers(
                        referer="https://auth.openai.com/email-verification"
                    ),
                    allow_redirects=True,
                    timeout=30,
                )
                final_url = normalize_flow_url(
                    str(response.url or ""),
                    auth_base="https://auth.openai.com",
                )
                callback_url = self._extract_callback_url_from_candidate(final_url)
                if callback_url:
                    return callback_url
                if "consent" in final_url or "organization" in final_url:
                    return final_url
            except Exception as e:
                self._log(f"GET about-you 失败: {e}", "warning")

            created_continue_url = self._create_account_during_oauth_if_needed()
            if created_continue_url:
                return created_continue_url

        if not continue_url and "consent" in page_type:
            continue_url = "https://auth.openai.com/sign-in-with-chatgpt/codex/consent"

        if continue_url:
            return continue_url

        return "https://auth.openai.com/sign-in-with-chatgpt/codex/consent"

    def _get_workspace_id(self) -> Optional[str]:
        """从 oai-client-auth-session cookie 中解析 workspace_id。"""
        try:
            auth_json = self._decode_auth_session_cookie()
            if not auth_json:
                self._log("未能解码 oai-client-auth-session Cookie", "error")
                return None

            workspaces = auth_json.get("workspaces") or []
            if not workspaces:
                self._log("授权 Cookie 里没有 workspace 信息", "error")
                return None

            workspace_id = str((workspaces[0] or {}).get("id") or "").strip()
            if not workspace_id:
                self._log("无法解析 workspace_id", "error")
                return None

            self._log(f"Workspace ID: {workspace_id}")
            return workspace_id
        except Exception as e:
            self._log(f"获取 Workspace ID 失败: {e}", "error")
            return None

    def _select_workspace(self, workspace_id: str) -> Optional[str]:
        """兼容旧逻辑：仅提交 workspace 并返回 continue_url。"""
        try:
            response = self.session.post(
                OPENAI_API_ENDPOINTS["select_workspace"],
                headers=self._build_json_headers(
                    referer="https://auth.openai.com/sign-in-with-chatgpt/codex/consent",
                    include_device_id=True,
                    include_datadog=True,
                ),
                data=json.dumps({"workspace_id": workspace_id}),
                allow_redirects=False,
                timeout=30,
            )
            if response.status_code != 200:
                self._log(f"选择 workspace 失败: {response.status_code}", "error")
                self._log(f"响应: {response.text[:200]}", "warning")
                return None
            continue_url = str((response.json() or {}).get("continue_url") or "").strip()
            if not continue_url:
                self._log("workspace/select 响应里缺少 continue_url", "error")
                return None
            self._log(f"Continue URL: {continue_url[:100]}...")
            return continue_url
        except Exception as e:
            self._log(f"选择 Workspace 失败: {e}", "error")
            return None

    def _follow_redirects(self, start_url: str) -> Optional[str]:
        """兼容旧逻辑：手动跟随重定向，寻找 OAuth 回调 URL。"""
        callback_url = self._follow_and_extract_callback_url(start_url)
        if callback_url:
            return callback_url
        self._log("未能在重定向链中找到回调 URL", "error")
        return None

    def _resolve_oauth_callback_url(self, start_url: str) -> Tuple[str, str]:
        consent_url = normalize_flow_url(
            start_url or "https://auth.openai.com/sign-in-with-chatgpt/codex/consent",
            auth_base="https://auth.openai.com",
        )
        workspace_id = ""

        callback_url = self._extract_callback_url_from_candidate(consent_url)
        if callback_url:
            return callback_url, workspace_id

        self._log(f"consent URL: {consent_url}")

        try:
            response = self.session.get(
                consent_url,
                headers=self._build_navigation_headers(
                    referer="https://auth.openai.com/email-verification"
                ),
                allow_redirects=False,
                timeout=30,
            )
            if response.status_code in (301, 302, 303, 307, 308):
                location = normalize_flow_url(
                    urllib.parse.urljoin(consent_url, str(response.headers.get("Location") or "")),
                    auth_base="https://auth.openai.com",
                )
                callback_url = self._extract_callback_url_from_candidate(location)
                if callback_url:
                    return callback_url, workspace_id
                callback_url = self._follow_and_extract_callback_url(location)
                if callback_url:
                    return callback_url, workspace_id
        except Exception as e:
            self._log(f"加载 consent 页面异常: {e}", "warning")

        workspace_id = self._get_workspace_id() or ""
        if workspace_id:
            try:
                ws_response = self.session.post(
                    OPENAI_API_ENDPOINTS["select_workspace"],
                    headers=self._build_json_headers(
                        referer=consent_url,
                        include_device_id=True,
                        include_datadog=True,
                    ),
                    data=json.dumps({"workspace_id": workspace_id}),
                    allow_redirects=False,
                    timeout=30,
                )

                self._log(f"workspace/select -> {ws_response.status_code}")

                if ws_response.status_code in (301, 302, 303, 307, 308):
                    location = normalize_flow_url(
                        urllib.parse.urljoin(
                            consent_url, str(ws_response.headers.get("Location") or "")
                        ),
                        auth_base="https://auth.openai.com",
                    )
                    callback_url = self._extract_callback_url_from_candidate(location)
                    if callback_url:
                        return callback_url, workspace_id
                    callback_url = self._follow_and_extract_callback_url(location)
                    if callback_url:
                        return callback_url, workspace_id

                if ws_response.status_code == 200:
                    try:
                        ws_data = ws_response.json() or {}
                    except Exception:
                        ws_data = {}

                    ws_continue_url = normalize_flow_url(
                        str(ws_data.get("continue_url") or ""),
                        auth_base="https://auth.openai.com",
                    )
                    orgs = ((ws_data.get("data") or {}).get("orgs")) or []

                    if orgs:
                        first_org = orgs[0] or {}
                        org_id = str(first_org.get("id") or "").strip()
                        project_id = str(
                            (((first_org.get("projects") or [None])[0]) or {}).get("id") or ""
                        ).strip()
                        if org_id:
                            org_payload = {"org_id": org_id}
                            if project_id:
                                org_payload["project_id"] = project_id

                            org_referer = ws_continue_url or consent_url
                            org_response = self.session.post(
                                OPENAI_API_ENDPOINTS["select_organization"],
                                headers=self._build_json_headers(
                                    referer=org_referer,
                                    include_device_id=True,
                                    include_datadog=True,
                                ),
                                data=json.dumps(org_payload),
                                allow_redirects=False,
                                timeout=30,
                            )

                            self._log(f"organization/select -> {org_response.status_code}")

                            if org_response.status_code in (301, 302, 303, 307, 308):
                                location = normalize_flow_url(
                                    urllib.parse.urljoin(
                                        org_referer,
                                        str(org_response.headers.get("Location") or ""),
                                    ),
                                    auth_base="https://auth.openai.com",
                                )
                                callback_url = self._extract_callback_url_from_candidate(location)
                                if callback_url:
                                    return callback_url, workspace_id
                                callback_url = self._follow_and_extract_callback_url(location)
                                if callback_url:
                                    return callback_url, workspace_id

                            if org_response.status_code == 200:
                                try:
                                    org_data = org_response.json() or {}
                                except Exception:
                                    org_data = {}
                                org_continue_url = normalize_flow_url(
                                    str(org_data.get("continue_url") or ""),
                                    auth_base="https://auth.openai.com",
                                )
                                callback_url = self._extract_callback_url_from_candidate(org_continue_url)
                                if callback_url:
                                    return callback_url, workspace_id
                                if org_continue_url:
                                    callback_url = self._follow_and_extract_callback_url(org_continue_url)
                                    if callback_url:
                                        return callback_url, workspace_id

                    callback_url = self._extract_callback_url_from_candidate(ws_continue_url)
                    if callback_url:
                        return callback_url, workspace_id
                    if ws_continue_url:
                        callback_url = self._follow_and_extract_callback_url(ws_continue_url)
                        if callback_url:
                            return callback_url, workspace_id
            except Exception as e:
                self._log(f"处理 workspace/select 响应异常: {e}", "warning")

        try:
            response = self.session.get(
                consent_url,
                headers=self._build_navigation_headers(
                    referer="https://auth.openai.com/email-verification"
                ),
                allow_redirects=True,
                timeout=30,
            )
            callback_url = self._extract_callback_url_from_candidate(str(response.url or ""))
            if callback_url:
                return callback_url, workspace_id
            for item in getattr(response, "history", []) or []:
                callback_url = self._extract_callback_url_from_candidate(
                    str((item.headers or {}).get("Location") or "")
                )
                if callback_url:
                    return callback_url, workspace_id
        except Exception as e:
            self._log(f"consent fallback 跟随失败: {e}", "warning")

        return "", workspace_id

    def _handle_oauth_callback(self, callback_url: str) -> Optional[Dict[str, Any]]:
        """处理 OAuth 回调"""
        try:
            if not self.oauth_start:
                self._log("OAuth 流程未初始化", "error")
                return None

            self._log("处理 OAuth 回调...")
            token_info = self.oauth_manager.handle_callback(
                callback_url=callback_url,
                expected_state=self.oauth_start.state,
                code_verifier=self.oauth_start.code_verifier
            )

            self._log("OAuth 授权成功")
            return token_info

        except Exception as e:
            self._log(f"处理 OAuth 回调失败: {e}", "error")
            return None

    def run(self) -> RegistrationResult:
        """
        执行完整的注册流程

        支持已注册账号自动登录：
        - 如果检测到邮箱已注册，自动切换到登录流程
        - 已注册账号跳过：设置密码、发送验证码、创建用户账户
        - 共用步骤：获取验证码、验证验证码、Workspace 和 OAuth 回调

        Returns:
            RegistrationResult: 注册结果
        """
        result = RegistrationResult(success=False, logs=self.logs)

        try:
            self._is_existing_account = False
            self._token_acquisition_requires_login = False
            self._otp_sent_at = None
            self._device_id = None
            self._post_otp_continue_url = ""
            self._post_otp_page_type = ""
            self._used_verification_codes.clear()

            self._log("=" * 60)
            self._log("注册流程启动")
            self._log("=" * 60)

            # 1. 检查 IP 地理位置
            self._log("1. 检查 IP 地理位置...")
            ip_ok, location = self._check_ip_location()
            if not ip_ok:
                result.error_message = f"IP 地理位置不支持: {location}"
                self._log(f"IP 检查失败: {location}", "error")
                return result

            self._log(f"IP 位置: {location}")

            # 2. 创建邮箱
            self._log("2. 创建邮箱...")
            if not self._create_email():
                result.error_message = "创建邮箱失败"
                return result

            result.email = self.email

            # 3. 准备首轮授权流程
            did, sen_token = self._prepare_authorize_flow("首次授权")
            if not did:
                result.error_message = "获取 Device ID 失败"
                return result
            if not sen_token:
                result.error_message = "Sentinel POW 验证失败"
                return result

            # 4. 提交注册入口邮箱
            self._log("4. 提交注册邮箱...")
            signup_result = self._submit_signup_form(did, sen_token)
            if not signup_result.success:
                result.error_message = f"提交注册表单失败: {signup_result.error_message}"
                return result

            if self._is_existing_account:
                self._log("检测到该邮箱已注册，切换到登录流程获取 Token")
            else:
                self._log("5. 设置密码...")
                password_ok, _ = self._register_password()
                if not password_ok:
                    result.error_message = "注册密码失败"
                    return result

                self._log("6. 发送注册验证码...")
                if not self._send_verification_code():
                    result.error_message = "发送验证码失败"
                    return result

                self._log("7. 等待注册验证码...")
                code = self._get_verification_code()
                if not code:
                    result.error_message = "获取验证码失败"
                    return result

                self._log("8. 校验注册验证码...")
                if not self._validate_verification_code(code):
                    result.error_message = "验证验证码失败"
                    return result

                self._log("9. 创建用户账户...")
                if not self._create_user_account():
                    result.error_message = "创建用户账户失败"
                    return result

                login_ready, login_error = self._restart_login_flow()
                if not login_ready:
                    result.error_message = login_error
                    return result

            if not self._complete_token_exchange(result):
                return result

            # 10. 完成
            self._log("=" * 60)
            if self._is_existing_account:
                self._log("登录成功")
            else:
                self._log("注册成功")
            self._log(f"邮箱: {result.email}")
            self._log(f"Account ID: {result.account_id}")
            self._log(f"Workspace ID: {result.workspace_id}")
            self._log("=" * 60)

            result.success = True
            result.metadata = {
                "email_service": self.email_service.service_type.value,
                "proxy_used": self.proxy_url,
                "registered_at": datetime.now().isoformat(),
                "is_existing_account": self._is_existing_account,
                "token_acquired_via_relogin": self._token_acquisition_requires_login,
            }

            return result

        except TaskInterruption:
            raise
        except Exception as e:
            self._log(f"注册过程中发生未预期错误: {e}", "error")
            result.error_message = str(e)
            return result

    def save_to_database(self, result: RegistrationResult) -> bool:
        """
        保存注册结果到数据库

        Args:
            result: 注册结果

        Returns:
            是否保存成功
        """
        if not result.success:
            return False

        return True  # 由 account_manager 统一处理存库


# 兼容旧命名，逐步迁移到更见名知意的类名。
RegistrationEngine = RefreshTokenRegistrationEngine
