"""平台插件基类"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum
import os
import time


class AccountStatus(str, Enum):
    REGISTERED   = "registered"
    TRIAL        = "trial"
    SUBSCRIBED   = "subscribed"
    EXPIRED      = "expired"
    INVALID      = "invalid"


@dataclass
class Account:
    platform: str
    email: str
    password: str
    user_id: str = ""
    region: str = ""
    token: str = ""
    status: AccountStatus = AccountStatus.REGISTERED
    trial_end_time: int = 0       # unix timestamp
    extra: dict = field(default_factory=dict)  # 平台自定义字段
    created_at: int = field(default_factory=lambda: int(time.time()))


@dataclass
class RegisterConfig:
    """注册任务配置"""
    executor_type: str = "protocol"   # protocol | headless | headed
    captcha_solver: str = "yescaptcha"  # yescaptcha | 2captcha | manual
    proxy: Optional[str] = None
    extra: dict = field(default_factory=dict)


class BasePlatform(ABC):
    # 子类必须定义
    name: str = ""
    display_name: str = ""
    version: str = "1.0.0"
    # 子类声明支持的执行器类型，未列出的自动降级到 protocol
    supported_executors: list = ["protocol", "headless", "headed"]

    def __init__(self, config: RegisterConfig = None):
        self.config = config or RegisterConfig()
        self._task_control = None
        if self.config.executor_type not in self.supported_executors:
            raise NotImplementedError(
                f"{self.display_name} 暂不支持 '{self.config.executor_type}' 执行器，"
                f"当前支持: {self.supported_executors}"
            )

    @abstractmethod
    def register(self, email: str, password: str = None) -> Account:
        """执行注册流程，返回 Account"""
        ...

    @abstractmethod
    def check_valid(self, account: Account) -> bool:
        """检测账号是否有效"""
        ...

    def get_trial_url(self, account: Account) -> Optional[str]:
        """生成试用激活链接（可选实现）"""
        return None

    def get_platform_actions(self) -> list:
        """
        返回平台支持的额外操作列表，每项格式:
        {"id": str, "label": str, "params": [{"key": str, "label": str, "type": str}]}
        """
        return []

    def execute_action(self, action_id: str, account: Account, params: dict) -> dict:
        """
        执行平台特定操作，返回 {"ok": bool, "data": any, "error": str}
        """
        raise NotImplementedError(f"平台 {self.name} 不支持操作: {action_id}")

    def get_quota(self, account: Account) -> dict:
        """查询账号配额（可选实现）"""
        return {}

    def bind_task_control(self, task_control) -> None:
        """绑定协作式任务控制器，供邮箱等待/人工跳过等场景复用。"""
        self._task_control = task_control
        mailbox = getattr(self, "mailbox", None)
        if mailbox is not None:
            mailbox._task_control = task_control

    def get_mailbox_otp_timeout(self, default: int = 120) -> int:
        """统一解析邮箱 OTP 等待秒数，避免平台内散落魔法值。"""
        extra = getattr(self.config, "extra", {}) or {}
        candidates = (
            extra.get("mailbox_otp_timeout_seconds"),
            extra.get("email_otp_timeout_seconds"),
            extra.get("otp_timeout"),
            default,
        )
        for value in candidates:
            if value in (None, ""):
                continue
            try:
                resolved = int(value)
            except (TypeError, ValueError):
                continue
            if resolved > 0:
                return resolved
        return default

    def _make_executor(self):
        """根据 config 创建执行器"""
        from .executors.protocol import ProtocolExecutor
        t = self.config.executor_type
        if t == "protocol":
            return ProtocolExecutor(proxy=self.config.proxy)
        elif t == "headless":
            from .executors.playwright import PlaywrightExecutor
            return PlaywrightExecutor(proxy=self.config.proxy, headless=True)
        elif t == "headed":
            from .executors.playwright import PlaywrightExecutor
            return PlaywrightExecutor(proxy=self.config.proxy, headless=False)
        raise ValueError(f"未知执行器类型: {t}")

    def _make_captcha(self, **kwargs):
        """根据 config 创建验证码解决器"""
        from .base_captcha import YesCaptcha, ManualCaptcha, LocalSolverCaptcha
        t = self.config.captcha_solver
        if t == "yescaptcha":
            key = kwargs.get("key") or self.config.extra.get("yescaptcha_key", "")
            return YesCaptcha(key)
        elif t == "manual":
            return ManualCaptcha()
        elif t == "local_solver":
            url = (
                self.config.extra.get("solver_url")
                or os.getenv("LOCAL_SOLVER_URL")
                or f"http://127.0.0.1:{os.getenv('SOLVER_PORT', '8889')}"
            )
            return LocalSolverCaptcha(url)
        raise ValueError(f"未知验证码解决器: {t}")
