"""Trae.ai 平台插件"""
from core.base_platform import BasePlatform, Account, AccountStatus, RegisterConfig
from core.base_mailbox import BaseMailbox
from core.registry import register


@register
class TraePlatform(BasePlatform):
    name = "trae"
    display_name = "Trae.ai"
    version = "1.0.0"

    def __init__(self, config: RegisterConfig = None, mailbox: BaseMailbox = None):
        super().__init__(config)
        self.mailbox = mailbox

    def register(self, email: str, password: str = None) -> Account:
        from platforms.trae.core import TraeRegister
        log = getattr(self, '_log_fn', print)

        mail_acct = self.mailbox.get_email() if self.mailbox else None
        email = email or (mail_acct.email if mail_acct else None)
        log(f"邮箱: {email}")
        before_ids = self.mailbox.get_current_ids(mail_acct) if mail_acct else set()
        otp_timeout = self.get_mailbox_otp_timeout()

        def otp_cb():
            log("等待验证码...")
            code = self.mailbox.wait_for_code(
                mail_acct,
                keyword="",
                timeout=otp_timeout,
                before_ids=before_ids,
            )
            if code: log(f"验证码: {code}")
            return code

        with self._make_executor() as ex:
            reg = TraeRegister(executor=ex, log_fn=log)
            result = reg.register(
                email=email,
                password=password,
                otp_callback=otp_cb if self.mailbox else None,
            )

        return Account(
            platform="trae",
            email=result["email"],
            password=result["password"],
            user_id=result["user_id"],
            token=result["token"],
            region=result["region"],
            status=AccountStatus.REGISTERED,
            extra={"cashier_url": result["cashier_url"],
                   "ai_pay_host": result["ai_pay_host"]},
        )

    def check_valid(self, account: Account) -> bool:
        return bool(account.token)

    def get_platform_actions(self) -> list:
        """返回平台支持的操作列表"""
        return [
            {"id": "switch_account", "label": "切换到桌面应用", "params": []},
            {"id": "get_user_info", "label": "获取用户信息", "params": []},
            {"id": "get_cashier_url", "label": "获取升级链接", "params": []},
        ]

    def execute_action(self, action_id: str, account: Account, params: dict) -> dict:
        """执行平台操作"""
        if action_id == "switch_account":
            from platforms.trae.switch import switch_trae_account, restart_trae_ide
            
            token = account.token
            user_id = account.user_id or ""
            email = account.email or ""
            region = account.region or ""
            
            if not token:
                return {"ok": False, "error": "账号缺少 token"}
            
            ok, msg = switch_trae_account(token, user_id, email, region)
            if not ok:
                return {"ok": False, "error": msg}
            
            restart_ok, restart_msg = restart_trae_ide()
            return {
                "ok": True,
                "data": {
                    "message": f"{msg}。{restart_msg}" if restart_ok else msg,
                }
            }
        
        elif action_id == "get_user_info":
            from platforms.trae.switch import get_trae_user_info
            
            token = account.token
            if not token:
                return {"ok": False, "error": "账号缺少 token"}
            
            user_info = get_trae_user_info(token)
            if user_info:
                return {"ok": True, "data": user_info}
            return {"ok": False, "error": "获取用户信息失败"}
        
        elif action_id == "get_cashier_url":
            from platforms.trae.core import TraeRegister
            with self._make_executor() as ex:
                reg = TraeRegister(executor=ex)
                # 重新登录刷新 session，再获取新 token 和 cashier_url
                reg.step4_trae_login()
                token = reg.step5_get_token()
                if not token:
                    token = account.token
                cashier_url = reg.step7_create_order(token)
            if not cashier_url:
                return {"ok": False, "error": "获取升级链接失败，token 可能已过期，请重新注册"}
            return {"ok": True, "data": {"cashier_url": cashier_url, "message": "请在浏览器中打开升级链接完成 Pro 订阅"}}

        raise NotImplementedError(f"未知操作: {action_id}")
