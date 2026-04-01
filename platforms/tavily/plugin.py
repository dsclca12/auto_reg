"""Tavily 平台插件"""
import random, string
from core.base_platform import BasePlatform, Account, AccountStatus, RegisterConfig
from core.base_mailbox import BaseMailbox
from core.registry import register


@register
class TavilyPlatform(BasePlatform):
    name = "tavily"
    display_name = "Tavily"
    version = "1.0.0"
    supported_executors = ["protocol", "headless", "headed"]

    def __init__(self, config: RegisterConfig = None, mailbox: BaseMailbox = None):
        super().__init__(config)
        self.mailbox = mailbox

    def _register_browser(self, email: str, password: str) -> Account:
        import sys, os, importlib, pathlib
        extra = self.config.extra or {}
        os.environ["LOCAL_SOLVER_URL"] = "http://127.0.0.1:8889"
        os.environ["SOLVER_PORT"] = "8889"
        os.environ["REGISTER_HEADLESS"] = "true"
        if extra.get("duckmail_api_key"): os.environ["DUCKMAIL_API_KEY"] = extra["duckmail_api_key"]
        if extra.get("duckmail_api_url"): os.environ["DUCKMAIL_API_URL"] = extra["duckmail_api_url"]
        if extra.get("duckmail_domain"):  os.environ["DUCKMAIL_DOMAIN"]  = extra["duckmail_domain"]
        tavily_gen_path = str(pathlib.Path(__file__).resolve().parents[3] / "tavily-key-generator")
        if tavily_gen_path not in sys.path:
            sys.path.insert(0, tavily_gen_path)
        if "config" in sys.modules: importlib.reload(sys.modules["config"])
        if "tavily_browser_solver" in sys.modules: importlib.reload(sys.modules["tavily_browser_solver"])
        from tavily_browser_solver import register_with_browser_solver
        api_key = register_with_browser_solver(email, password)
        if not api_key:
            raise RuntimeError("浏览器注册失败")
        return Account(platform="tavily", email=email, password=password,
                       status=AccountStatus.REGISTERED, extra={"api_key": api_key})

    def register(self, email: str, password: str = None) -> Account:
        if not password:
            password = "".join(random.choices(string.ascii_letters + string.digits + "!@#", k=14))
        log = getattr(self, '_log_fn', print)

        if (self.config.executor_type or "") in ("headless", "headed"):
            log(f"使用浏览器模式注册: {email}")
            return self._register_browser(email, password)

        mail_acct = self.mailbox.get_email() if self.mailbox else None
        email = email or (mail_acct.email if mail_acct else None)
        log(f"邮箱: {email}")
        before_ids = self.mailbox.get_current_ids(mail_acct) if mail_acct else set()
        otp_timeout = self.get_mailbox_otp_timeout()

        def otp_cb():
            log("等待验证码邮件...")
            code = self.mailbox.wait_for_code(
                mail_acct,
                keyword="",
                timeout=otp_timeout,
                before_ids=before_ids,
            )
            if code: log(f"验证码: {code}")
            return code

        captcha = self._make_captcha(key=self.config.extra.get("yescaptcha_key", ""))

        from platforms.tavily.core import TavilyRegister
        with self._make_executor() as ex:
            reg = TavilyRegister(executor=ex, captcha=captcha, log_fn=log)
            result = reg.register(email=email, password=password,
                                  otp_callback=otp_cb if self.mailbox else None)

        return Account(platform="tavily", email=result["email"], password=result["password"],
                       status=AccountStatus.REGISTERED, extra={"api_key": result["api_key"]})

    def check_valid(self, account: Account) -> bool:
        api_key = account.extra.get("api_key", "")
        if not api_key:
            return False
        import requests
        try:
            r = requests.post("https://api.tavily.com/search",
                              json={"api_key": api_key, "query": "test", "max_results": 1},
                              timeout=10)
            return r.status_code != 401
        except Exception:
            return False
