"""OpenBlockLabs 平台插件"""
import random, string
from core.base_platform import BasePlatform, Account, AccountStatus, RegisterConfig
from core.base_mailbox import BaseMailbox
from core.registry import register


@register
class OpenBlockLabsPlatform(BasePlatform):
    name = "openblocklabs"
    display_name = "OpenBlockLabs"
    version = "1.0.0"
    supported_executors = ["protocol"]

    def __init__(self, config: RegisterConfig = None, mailbox: BaseMailbox = None):
        super().__init__(config)
        self.mailbox = mailbox

    def register(self, email: str = None, password: str = None) -> Account:
        from platforms.openblocklabs.core import OpenBlockLabsRegister
        log = getattr(self, '_log_fn', print)
        proxy = self.config.proxy

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

        # 随机姓名
        first_name = "".join(random.choices(string.ascii_lowercase, k=5)).capitalize()
        last_name  = "".join(random.choices(string.ascii_lowercase, k=5)).capitalize()

        reg = OpenBlockLabsRegister(proxy=proxy)
        reg.log = lambda msg: log(msg)

        result = reg.register(
            email=email,
            password=password,
            first_name=first_name,
            last_name=last_name,
            otp_callback=otp_cb if self.mailbox else None,
        )

        if not result.get("success"):
            raise RuntimeError(f"注册失败: {result.get('error')}")

        return Account(
            platform="openblocklabs",
            email=result["email"],
            password=result["password"],
            status=AccountStatus.REGISTERED,
            extra={"wos_session": result.get("wos_session", "")},
            token=result.get("wos_session", ""),
        )

    def check_valid(self, account: Account) -> bool:
        return bool(account.extra.get("wos_session"))
