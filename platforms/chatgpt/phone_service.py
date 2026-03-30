from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable, Optional

from smstome_tool import (
    PhoneEntry,
    get_unused_phone,
    mark_phone_blacklisted,
    parse_country_slugs,
    update_global_phone_list,
    wait_for_otp,
)


def _to_positive_int(value, default: int, *, minimum: int = 1) -> int:
    try:
        parsed = int(str(value).strip())
    except Exception:
        return default
    return parsed if parsed >= minimum else default


def _prefix_hint(phone: str, width: int = 7) -> str:
    value = str(phone or "").strip()
    return value[: min(len(value), width)] if value else ""


class SMSToMePhoneService:
    def __init__(self, config: Optional[dict] = None, log_fn: Optional[Callable[[str], None]] = None):
        self.config = dict(config or {})
        self.log_fn = log_fn or (lambda _msg: None)
        self.cookie_header = str(self.config.get("smstome_cookie", "") or "").strip() or None
        self.country_slugs = parse_country_slugs(self.config.get("smstome_country_slugs"))
        self.global_file = Path(str(self.config.get("smstome_global_file") or "smstome_all_numbers.txt"))
        self.used_numbers_dir = Path(str(self.config.get("smstome_used_numbers_dir") or "smstome_used"))
        self.task_name = str(self.config.get("smstome_task_name") or "chatgpt_add_phone").strip() or "chatgpt_add_phone"
        self.max_attempts = _to_positive_int(self.config.get("smstome_phone_attempts"), 3)
        self.otp_timeout_seconds = _to_positive_int(self.config.get("smstome_otp_timeout_seconds"), 45, minimum=10)
        self.poll_interval_seconds = _to_positive_int(self.config.get("smstome_poll_interval_seconds"), 5, minimum=1)
        self.sync_max_pages_per_country = _to_positive_int(
            self.config.get("smstome_sync_max_pages_per_country"),
            5,
        )

    @property
    def enabled(self) -> bool:
        return self._has_pool_file() or bool(self.cookie_header)

    def prefix_hint(self, phone: str) -> str:
        return _prefix_hint(phone)

    def _has_pool_file(self) -> bool:
        try:
            return self.global_file.exists() and self.global_file.stat().st_size > 0
        except OSError:
            return False

    def ensure_pool_ready(self) -> None:
        if self._has_pool_file():
            return
        if not self.cookie_header:
            raise RuntimeError("未找到 SMSToMe 号码池文件，且未配置 smstome_cookie")

        self.log_fn("SMSToMe 号码池不存在，开始自动同步...")
        count = update_global_phone_list(
            cookie_header=self.cookie_header,
            countries=self.country_slugs or None,
            output_path=self.global_file,
            max_pages_per_country=self.sync_max_pages_per_country,
        )
        if count <= 0:
            raise RuntimeError("SMSToMe 号码池同步后为空")
        self.log_fn(f"SMSToMe 号码池同步完成，共 {count} 个号码")

    def acquire_phone(self, *, exclude_prefixes: Optional[Iterable[str]] = None) -> Optional[PhoneEntry]:
        self.ensure_pool_ready()
        return get_unused_phone(
            self.task_name,
            country_slug=self.country_slugs or None,
            global_file=self.global_file,
            used_numbers_dir=self.used_numbers_dir,
            exclude_prefixes=exclude_prefixes,
        )

    def mark_blacklisted(self, phone: str) -> None:
        mark_phone_blacklisted(self.task_name, phone, used_numbers_dir=self.used_numbers_dir)

    def wait_for_code(self, entry: PhoneEntry, *, timeout: Optional[int] = None) -> Optional[str]:
        wait_seconds = _to_positive_int(timeout, self.otp_timeout_seconds, minimum=10)
        return wait_for_otp(
            entry,
            cookie_header=self.cookie_header,
            timeout=wait_seconds,
            poll_interval=self.poll_interval_seconds,
            trace=lambda message: self.log_fn(f"[SMSToMe] {message}"),
            raise_on_timeout=False,
        )
