#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

from sqlmodel import Session, select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.base_platform import Account, AccountStatus, RegisterConfig
from core.db import AccountModel, engine
from core.registry import get, load_all


def load_accounts(ids: list[int]) -> list[AccountModel]:
    with Session(engine) as session:
        items = []
        for account_id in ids:
            acc = session.get(AccountModel, account_id)
            if acc and acc.platform == "chatgpt":
                items.append(acc)
        return items


def to_account_model(acc: AccountModel) -> Account:
    return Account(
        platform=acc.platform,
        email=acc.email,
        password=acc.password,
        user_id=acc.user_id,
        token=acc.token,
        status=AccountStatus(acc.status),
        extra=acc.get_extra(),
    )


def parse_id_list(raw: str) -> list[int]:
    ids = []
    for part in raw.split(","):
        text = part.strip()
        if not text:
            continue
        ids.append(int(text))
    return ids


def choose_account(items: list[AccountModel]) -> AccountModel:
    print("\n可选 ChatGPT 账号:")
    for idx, acc in enumerate(items, start=1):
        extra = acc.get_extra()
        print(
            f"{idx}. id={acc.id} email={acc.email} "
            f"refresh={'Y' if extra.get('refresh_token') else 'N'} "
            f"session={'Y' if extra.get('session_token') else 'N'} "
            f"cookies={'Y' if extra.get('cookies') else 'N'}"
        )

    while True:
        raw = input("\n选择序号: ").strip()
        try:
            pos = int(raw)
        except ValueError:
            print("请输入数字序号。")
            continue
        if 1 <= pos <= len(items):
            return items[pos - 1]
        print("序号超出范围。")


def main() -> None:
    load_all()
    platform_cls = get("chatgpt")
    instance = platform_cls(config=RegisterConfig())

    raw_ids = input("输入 ChatGPT 账号 id 列表，逗号分隔: ").strip()
    if not raw_ids:
        print("未输入账号 id。")
        return

    try:
        ids = parse_id_list(raw_ids)
    except ValueError:
        print("账号 id 列表格式错误。示例: 22,21,18")
        return

    accounts = load_accounts(ids)
    if not accounts:
        print("没有找到匹配的 ChatGPT 账号。")
        return

    selected = choose_account(accounts)
    plan = input("套餐 [plus/team]，默认 plus: ").strip().lower() or "plus"
    if plan not in {"plus", "team"}:
        print("不支持的套餐。")
        return

    country = input("地区代码，默认 US: ").strip().upper() or "US"

    params: dict[str, object] = {"plan": plan, "country": country}
    if plan == "team":
        workspace_name = input("Workspace 名称，默认 MyTeam: ").strip() or "MyTeam"
        price_interval = input("周期 [month/year]，默认 month: ").strip().lower() or "month"
        seat_quantity_raw = input("席位数，默认 5: ").strip() or "5"
        try:
            seat_quantity = int(seat_quantity_raw)
        except ValueError:
            print("席位数必须是整数。")
            return
        params.update(
            {
                "workspace_name": workspace_name,
                "price_interval": price_interval,
                "seat_quantity": seat_quantity,
            }
        )

    account = to_account_model(selected)
    print(f"\n使用账号 id={selected.id} email={selected.email}")

    refresh_answer = input("先刷新 token? [Y/n]: ").strip().lower()
    if refresh_answer in {"", "y", "yes"}:
        refresh_result = instance.execute_action("refresh_token", account, {})
        print("refresh_result =", refresh_result)
        if not refresh_result.get("ok"):
            return
        data = refresh_result.get("data") or {}
        if data.get("access_token"):
            account.token = data["access_token"]
            account.extra["access_token"] = data["access_token"]
        if data.get("refresh_token"):
            account.extra["refresh_token"] = data["refresh_token"]

    result = instance.execute_action("payment_link", account, params)
    print("payment_result =", result)
    if result.get("ok"):
        url = ((result.get("data") or {}).get("url") or "").strip()
        if url:
            print("\n支付链接:")
            print(url)


if __name__ == "__main__":
    main()
