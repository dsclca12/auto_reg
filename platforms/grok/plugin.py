"""Grok (x.ai) 平台插件"""
from core.base_platform import BasePlatform, Account, AccountStatus, RegisterConfig
from core.base_mailbox import BaseMailbox
from core.registry import register


@register
class GrokPlatform(BasePlatform):
    name = "grok"
    display_name = "Grok"
    version = "1.0.0"

    def __init__(self, config: RegisterConfig = None, mailbox: BaseMailbox = None):
        super().__init__(config)
        self.mailbox = mailbox

    def register(self, email: str, password: str = None) -> Account:
        from platforms.grok.core import GrokRegister
        from core.config_store import config_store
        log = getattr(self, '_log_fn', print)

        # 优先从任务配置读取，兜底从全局配置读取
        yescaptcha_key = self.config.extra.get("yescaptcha_key") or config_store.get("yescaptcha_key", "")
        captcha_solver = self._make_captcha(key=yescaptcha_key)
        reg = GrokRegister(captcha_solver=captcha_solver, yescaptcha_key=yescaptcha_key, proxy=self.config.proxy, log_fn=log)
        mailbox_attempts = 1 if email else int(self.config.extra.get("grok_mailbox_attempts", 8))
        otp_timeout = self.get_mailbox_otp_timeout()
        last_error = None

        for attempt in range(1, mailbox_attempts + 1):
            mail_acct = None
            current_email = email
            if self.mailbox and not current_email:
                mail_acct = self.mailbox.get_email()
                current_email = mail_acct.email if mail_acct else None
            log(f"邮箱: {current_email}")
            before_ids = self.mailbox.get_current_ids(mail_acct) if (self.mailbox and mail_acct) else set()

            def otp_cb():
                log("等待验证码...")
                code = self.mailbox.wait_for_code(
                    mail_acct,
                    keyword="",
                    timeout=otp_timeout,
                    before_ids=before_ids,
                    code_pattern=r'[A-Z0-9]{3}-[A-Z0-9]{3}',
                )
                if code:
                    code = code.replace('-', '').replace(' ', '')
                    log(f"验证码: {code}")
                return code

            try:
                result = reg.register(
                    email=current_email,
                    password=password,
                    otp_callback=otp_cb if self.mailbox else None,
                )
                break
            except Exception as e:
                last_error = e
                msg = str(e)
                if attempt < mailbox_attempts and "邮箱域名被拒绝" in msg:
                    log(f"Grok 邮箱域名被拒绝，切换新邮箱重试 {attempt + 1}/{mailbox_attempts}")
                    continue
                raise
        else:
            raise last_error if last_error else RuntimeError("Grok 注册失败")

        return Account(
            platform="grok",
            email=result["email"],
            password=result["password"],
            status=AccountStatus.REGISTERED,
            extra={
                "sso": result["sso"],
                "sso_rw": result["sso_rw"],
                "given_name": result["given_name"],
                "family_name": result["family_name"],
            },
        )

    def check_valid(self, account: Account) -> bool:
        return bool((account.extra or {}).get("sso"))

    def get_platform_actions(self) -> list:
        return [
            {"id": "upload_grok2api", "label": "导入 grok2api", "params": []},
        ]

    def execute_action(self, action_id: str, account: Account, params: dict) -> dict:
        if action_id == "upload_grok2api":
            from platforms.grok.grok2api_upload import upload_to_grok2api

            ok, msg = upload_to_grok2api(account)
            return {"ok": ok, "data": {"message": msg}}
        raise NotImplementedError(f"未知操作: {action_id}")
