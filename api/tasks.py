import asyncio
import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlmodel import Session, select

from core.db import ScheduledTaskModel, TaskLog, engine
from core.task_runtime import (
    AttemptOutcome,
    AttemptResult,
    RegisterTaskStore,
    SkipCurrentAttemptRequested,
    StopTaskRequested,
)

router = APIRouter(prefix="/tasks", tags=["tasks"])
logger = logging.getLogger(__name__)

MAX_FINISHED_TASKS = 200
CLEANUP_THRESHOLD = 250

_task_store = RegisterTaskStore(
    max_finished_tasks=MAX_FINISHED_TASKS,
    cleanup_threshold=CLEANUP_THRESHOLD,
)


class RegisterTaskRequest(BaseModel):
    platform: str
    email: Optional[str] = None
    password: Optional[str] = None
    count: int = Field(default=1, ge=1, le=1000)
    concurrency: int = Field(default=1, ge=1, le=10)
    register_delay_seconds: float = Field(default=0, ge=0)
    random_delay_min: Optional[float] = Field(default=None, ge=0)
    random_delay_max: Optional[float] = Field(default=None, ge=0)
    proxy: Optional[str] = None
    executor_type: str = "protocol"
    captcha_solver: str = "yescaptcha"
    extra: dict = Field(default_factory=dict)
    task_id: Optional[str] = None
    interval_type: Optional[str] = None
    interval_value: Optional[int] = None


class TaskLogBatchDeleteRequest(BaseModel):
    ids: list[int]


def _ensure_task_exists(task_id: str) -> None:
    if not _task_store.exists(task_id):
        raise HTTPException(404, "任务不存在")


def _prepare_register_request(req: RegisterTaskRequest) -> RegisterTaskRequest:
    mail_provider = req.extra.get("mail_provider")
    if mail_provider == "luckmail":
        if req.platform in ("tavily", "openblocklabs"):
            raise HTTPException(400, f"LuckMail 渠道暂时不支持 {req.platform} 项目注册")

        mapping = {
            "trae": "trae",
            "cursor": "cursor",
            "grok": "grok",
            "kiro": "kiro",
            "chatgpt": "openai",
        }
        req.extra["luckmail_project_code"] = mapping.get(req.platform, req.platform)
    return req


def _create_task_record(
    task_id: str,
    req: RegisterTaskRequest,
    source: str,
    meta: dict[str, Any] | None,
):
    return _task_store.create(
        task_id,
        platform=req.platform,
        total=req.count,
        source=source,
        meta=meta,
    )


def _log(task_id: str, msg: str):
    ts = time.strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    _task_store.append_log(task_id, entry)
    print(entry)


def _save_task_log(
    platform: str,
    email: str,
    status: str,
    error: str = "",
    detail: dict | None = None,
):
    with Session(engine) as s:
        log = TaskLog(
            platform=platform,
            email=email,
            status=status,
            error=error,
            detail_json=json.dumps(detail or {}, ensure_ascii=False),
        )
        s.add(log)
        s.commit()


def _auto_upload_integrations(task_id: str, account):
    try:
        from services.external_sync import sync_account

        for result in sync_account(account):
            name = result.get("name", "Auto Upload")
            ok = bool(result.get("ok"))
            msg = result.get("msg", "")
            _log(task_id, f"  [{name}] {'[OK] ' + msg if ok else '[FAIL] ' + msg}")
    except Exception as exc:
        _log(task_id, f"  [Auto Upload] 自动导入异常: {exc}")


def _sleep_with_task_control(
    seconds: float,
    *,
    task_control,
    attempt_id: int,
):
    remaining = max(float(seconds or 0), 0.0)
    while remaining > 0:
        task_control.checkpoint(attempt_id=attempt_id)
        chunk = min(0.25, remaining)
        time.sleep(chunk)
        remaining -= chunk


def _build_register_meta(req: RegisterTaskRequest, *, scope: str) -> dict[str, Any]:
    return {
        "scope": scope,
        "count": req.count,
        "concurrency": req.concurrency,
    }


def _run_register(task_id: str, req: RegisterTaskRequest):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from core.base_mailbox import create_mailbox
    from core.base_platform import RegisterConfig
    from core.config_store import config_store
    from core.db import save_account
    from core.proxy_pool import proxy_pool
    from core.registry import get

    _ensure_task_exists(task_id)
    task_control = _task_store.control_for(task_id)
    _task_store.mark_running(task_id)

    success = 0
    skipped = 0
    errors: list[str] = []
    start_gate_lock = threading.Lock()
    next_start_time = time.time()

    try:
        platform_cls = get(req.platform)

        def _build_mailbox(proxy: Optional[str]):
            merged_extra = config_store.get_all().copy()
            merged_extra.update(
                {k: v for k, v in req.extra.items() if v is not None and v != ""}
            )
            return create_mailbox(
                provider=merged_extra.get("mail_provider", "laoudo"),
                extra=merged_extra,
                proxy=proxy,
            )

        def _do_one(i: int) -> AttemptResult:
            nonlocal next_start_time

            attempt_id = task_control.start_attempt()
            _proxy = req.proxy
            try:
                task_control.checkpoint(attempt_id=attempt_id)
                if not _proxy:
                    _proxy = proxy_pool.get_next()

                if req.register_delay_seconds > 0 or (
                    req.random_delay_min is not None
                    and req.random_delay_max is not None
                ):
                    with start_gate_lock:
                        now = time.time()
                        wait_seconds = max(0.0, next_start_time - now)

                        if req.register_delay_seconds > 0 and wait_seconds > 0:
                            _log(task_id, f"第 {i + 1} 个账号启动前延迟 {wait_seconds:g} 秒")
                            _sleep_with_task_control(
                                wait_seconds,
                                task_control=task_control,
                                attempt_id=attempt_id,
                            )
                        next_start_time = time.time() + req.register_delay_seconds

                        if (
                            req.random_delay_min is not None
                            and req.random_delay_max is not None
                        ):
                            import random

                            random_delay = random.uniform(
                                req.random_delay_min,
                                req.random_delay_max,
                            )
                            if random_delay > 0:
                                _log(
                                    task_id,
                                    f"第 {i + 1} 个账号随机延迟 {random_delay:.1f} 秒 "
                                    f"({req.random_delay_min}-{req.random_delay_max}秒)",
                                )
                                _sleep_with_task_control(
                                    random_delay,
                                    task_control=task_control,
                                    attempt_id=attempt_id,
                                )
                            next_start_time = time.time() + random_delay

                merged_extra = config_store.get_all().copy()
                merged_extra.update(
                    {k: v for k, v in req.extra.items() if v is not None and v != ""}
                )
                config = RegisterConfig(
                    executor_type=req.executor_type,
                    captcha_solver=req.captcha_solver,
                    proxy=_proxy,
                    extra=merged_extra,
                )
                mailbox = _build_mailbox(_proxy)
                platform = platform_cls(config=config, mailbox=mailbox)
                platform._log_fn = lambda msg: _log(task_id, msg)
                platform.bind_task_control(task_control)
                if getattr(platform, "mailbox", None) is not None:
                    platform.mailbox._log_fn = platform._log_fn
                    platform.mailbox._task_attempt_token = attempt_id

                _task_store.set_progress(task_id, f"{i + 1}/{req.count}")
                _log(task_id, f"开始注册第 {i + 1}/{req.count} 个账号")
                if _proxy:
                    _log(task_id, f"使用代理: {_proxy}")

                task_control.checkpoint(attempt_id=attempt_id)
                account = platform.register(
                    email=req.email or None,
                    password=req.password,
                )
                task_control.checkpoint(attempt_id=attempt_id)

                if isinstance(account.extra, dict):
                    mail_provider = merged_extra.get("mail_provider", "")
                    if mail_provider:
                        account.extra.setdefault("mail_provider", mail_provider)
                    if mail_provider == "luckmail" and req.platform == "chatgpt":
                        mailbox_token = getattr(mailbox, "_token", "") or ""
                        if mailbox_token:
                            account.extra.setdefault("mailbox_token", mailbox_token)
                        for key in (
                            "luckmail_project_code",
                            "luckmail_email_type",
                            "luckmail_domain",
                            "luckmail_base_url",
                        ):
                            value = merged_extra.get(key)
                            if value:
                                account.extra.setdefault(key, value)

                saved_account = save_account(account)
                if _proxy:
                    proxy_pool.report_success(_proxy)
                _log(task_id, f"[OK] 注册成功: {account.email}")
                _save_task_log(req.platform, account.email, "success")
                _auto_upload_integrations(task_id, saved_account or account)

                cashier_url = (account.extra or {}).get("cashier_url", "")
                if cashier_url:
                    _log(task_id, f"  [升级链接] {cashier_url}")
                    _task_store.add_cashier_url(task_id, cashier_url)
                return AttemptResult.success()
            except SkipCurrentAttemptRequested as exc:
                _log(task_id, str(exc))
                return AttemptResult.skipped(str(exc))
            except StopTaskRequested as exc:
                _log(task_id, str(exc))
                return AttemptResult.stopped(str(exc))
            except Exception as exc:
                if _proxy:
                    proxy_pool.report_fail(_proxy)
                _log(task_id, f"[FAIL] 注册失败: {exc}")
                _save_task_log(req.platform, req.email or "", "failed", error=str(exc))
                return AttemptResult.failed(str(exc))
            finally:
                task_control.finish_attempt(attempt_id)

        max_workers = min(req.concurrency, req.count, 5)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_do_one, i) for i in range(req.count)]
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as exc:
                    _log(task_id, f"[ERROR] 任务线程异常: {exc}")
                    errors.append(str(exc))
                    continue

                if result.outcome == AttemptOutcome.SUCCESS:
                    success += 1
                elif result.outcome == AttemptOutcome.SKIPPED:
                    skipped += 1
                elif result.outcome == AttemptOutcome.FAILED:
                    errors.append(result.message)
                elif result.outcome == AttemptOutcome.STOPPED:
                    pass
    except StopTaskRequested as exc:
        _log(task_id, str(exc))
    except Exception as exc:
        _log(task_id, f"致命错误: {exc}")
        _task_store.finish(
            task_id,
            status="failed",
            success=success,
            skipped=skipped,
            errors=errors,
            error=str(exc),
        )
        _task_store.cleanup()
        return

    final_status = "stopped" if task_control.is_stop_requested() else "done"
    _task_store.finish(
        task_id,
        status=final_status,
        success=success,
        skipped=skipped,
        errors=errors,
    )
    summary = f"完成: 成功 {success} 个, 跳过 {skipped} 个, 失败 {len(errors)} 个"
    if final_status == "stopped":
        summary = f"任务已停止: 成功 {success} 个, 跳过 {skipped} 个, 失败 {len(errors)} 个"
    _log(task_id, summary)
    _task_store.cleanup()


def _run_scheduled_task_and_update_status(task_id: str, req: RegisterTaskRequest):
    from core.scheduler import update_task_run_status

    try:
        _run_register(task_id, req)
        snapshot = _task_store.snapshot(task_id)
        success = snapshot.get("status") == "done" and not snapshot.get("errors")
        update_task_run_status(task_id.split("_", 2)[1] if task_id.startswith("scheduled_") else task_id, success, snapshot.get("error"))
    except Exception as exc:
        update_task_run_status(task_id, False, str(exc))
        raise


@router.post("/register")
def create_register_task(
    req: RegisterTaskRequest,
    background_tasks: BackgroundTasks,
):
    req = _prepare_register_request(req)
    task_id = f"task_{int(time.time() * 1000)}"
    _create_task_record(task_id, req, "manual", _build_register_meta(req, scope="manual"))
    background_tasks.add_task(_run_register, task_id, req)
    return {"task_id": task_id}


@router.post("/{task_id}/skip-current")
def skip_current_task_attempt(task_id: str):
    _ensure_task_exists(task_id)
    control = _task_store.request_skip_current(task_id)
    return {
        "task_id": task_id,
        "control": control,
        "message": "已发送跳过当前账号请求",
    }


@router.post("/{task_id}/stop")
def stop_task(task_id: str):
    _ensure_task_exists(task_id)
    control = _task_store.request_stop(task_id)
    return {
        "task_id": task_id,
        "control": control,
        "message": "已发送停止任务请求",
    }


@router.get("/logs")
def get_logs(platform: str = None, page: int = 1, page_size: int = 50):
    with Session(engine) as s:
        query = select(TaskLog)
        if platform:
            query = query.where(TaskLog.platform == platform)
        query = query.order_by(TaskLog.id.desc())
        total = len(s.exec(query).all())
        items = s.exec(query.offset((page - 1) * page_size).limit(page_size)).all()
    return {"total": total, "items": items}


@router.post("/logs/batch-delete")
def batch_delete_logs(body: TaskLogBatchDeleteRequest):
    if not body.ids:
        raise HTTPException(400, "任务历史 ID 列表不能为空")

    unique_ids = list(dict.fromkeys(body.ids))
    if len(unique_ids) > 1000:
        raise HTTPException(400, "单次最多删除 1000 条任务历史")

    with Session(engine) as s:
        try:
            logs = s.exec(select(TaskLog).where(TaskLog.id.in_(unique_ids))).all()
            found_ids = {log.id for log in logs if log.id is not None}

            for log in logs:
                s.delete(log)

            s.commit()
            deleted_count = len(found_ids)
            not_found_ids = [log_id for log_id in unique_ids if log_id not in found_ids]
            logger.info("批量删除任务历史成功: %s 条", deleted_count)
            return {
                "deleted": deleted_count,
                "not_found": not_found_ids,
                "total_requested": len(unique_ids),
            }
        except Exception as exc:
            s.rollback()
            logger.exception("批量删除任务历史失败")
            raise HTTPException(500, f"批量删除任务历史失败: {exc}")


@router.get("/{task_id}/logs/stream")
async def stream_logs(task_id: str, since: int = 0):
    _ensure_task_exists(task_id)

    async def event_generator():
        sent = since
        while True:
            logs, status = _task_store.log_state(task_id)
            while sent < len(logs):
                yield f"data: {json.dumps({'line': logs[sent]})}\n\n"
                sent += 1
            if status in ("done", "failed", "stopped"):
                yield f"data: {json.dumps({'done': True, 'status': status})}\n\n"
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/schedule/{task_id}/run")
def run_scheduled_task_now(task_id: str, background_tasks: BackgroundTasks):
    from core.scheduler import get_scheduled_register_tasks, update_task_run_status

    tasks = get_scheduled_register_tasks()
    if task_id not in tasks:
        raise HTTPException(404, "任务不存在")

    task_config = tasks[task_id]
    run_task_id = f"manual_{task_id}_{int(time.time())}"
    req = RegisterTaskRequest(**task_config)
    _create_task_record(run_task_id, req, "schedule", {"scheduled_task_id": task_id, "scope": "manual-run"})
    _log(run_task_id, f"开始手动运行定时任务 {task_id}")

    def run_with_status():
        try:
            _run_register(run_task_id, req)
            snapshot = _task_store.snapshot(run_task_id)
            success = snapshot.get("status") == "done" and not snapshot.get("errors")
            update_task_run_status(task_id, success, snapshot.get("error"))
            logger.info("任务 %s 运行完成", run_task_id)
        except Exception as exc:
            update_task_run_status(task_id, False, str(exc))
            logger.exception("任务 %s 运行失败", run_task_id)

    background_tasks.add_task(run_with_status)
    return {"task_id": run_task_id, "status": "running"}


@router.post("/schedule")
def create_scheduled_task(body: RegisterTaskRequest):
    from core.scheduler import add_scheduled_register_task, update_task_run_status

    task_id = f"sched_{uuid.uuid4().hex[:8]}"
    db_task = ScheduledTaskModel(
        task_id=task_id,
        platform=body.platform,
        count=body.count,
        executor_type=body.executor_type,
        captcha_solver=body.captcha_solver,
        extra_json=json.dumps(body.extra, ensure_ascii=False),
        interval_type=body.interval_type or "minutes",
        interval_value=body.interval_value or 30,
        paused=False,
    )
    with Session(engine) as s:
        s.add(db_task)
        s.commit()
        s.refresh(db_task)

    config = body.dict()
    config["task_id"] = task_id
    add_scheduled_register_task(task_id, config)

    def run_now():
        run_task_id = f"scheduled_{task_id}_{int(time.time())}"
        req = RegisterTaskRequest(**config)
        _create_task_record(run_task_id, req, "schedule", {"scheduled_task_id": task_id, "scope": "auto-run"})
        try:
            _run_register(run_task_id, req)
            snapshot = _task_store.snapshot(run_task_id)
            success = snapshot.get("status") == "done" and not snapshot.get("errors")
            update_task_run_status(task_id, success, snapshot.get("error"))
            print(f"[Scheduler] 任务 {task_id} 已执行", flush=True)
        except Exception as exc:
            update_task_run_status(task_id, False, str(exc))
            print(f"[Scheduler] 任务 {task_id} 执行失败：{exc}", flush=True)

    threading.Thread(target=run_now, daemon=True).start()
    print(f"[Scheduler] 任务 {task_id} 已创建并启动", flush=True)
    return {"task_id": task_id, "status": "scheduled", "config": config}


@router.get("/schedule")
def list_scheduled_tasks():
    from core.scheduler import get_all_task_run_status, get_scheduled_register_tasks

    tasks = get_scheduled_register_tasks()
    run_status = get_all_task_run_status()

    result = []
    for task in tasks.values():
        task_data = dict(task)
        task_id = task.get("task_id")
        if task_id and task_id in run_status:
            task_data.update(run_status[task_id])
        else:
            task_data.setdefault("last_run_at", None)
            task_data.setdefault("last_run_success", None)
            task_data.setdefault("last_error", None)
        result.append(task_data)
    return {"tasks": result}


@router.put("/schedule")
def update_scheduled_task(body: RegisterTaskRequest):
    from core.scheduler import add_scheduled_register_task, get_scheduled_register_tasks

    task_id = getattr(body, "task_id", None) or (body.extra and body.extra.get("task_id"))
    if not task_id:
        raise HTTPException(400, "缺少任务 ID")

    tasks = get_scheduled_register_tasks()
    if task_id not in tasks:
        raise HTTPException(404, "任务不存在")

    config = body.dict()
    config["task_id"] = task_id
    add_scheduled_register_task(task_id, config)
    return {"task_id": task_id, "status": "updated", "config": config}


@router.delete("/schedule/{task_id}")
def delete_scheduled_task(task_id: str):
    from core.scheduler import remove_scheduled_register_task

    with Session(engine) as s:
        task = s.get(ScheduledTaskModel, task_id)
        if task:
            s.delete(task)
            s.commit()

    remove_scheduled_register_task(task_id)
    return {"ok": True}


@router.post("/schedule/{task_id}/toggle")
def toggle_scheduled_task(task_id: str):
    from core.scheduler import add_scheduled_register_task, get_scheduled_register_tasks

    with Session(engine) as s:
        task = s.get(ScheduledTaskModel, task_id)
        if not task:
            raise HTTPException(404, "任务不存在")
        task.paused = not task.paused
        task.updated_at = datetime.now(timezone.utc)
        s.add(task)
        s.commit()

    tasks = get_scheduled_register_tasks()
    if task_id in tasks:
        task_config = dict(tasks[task_id])
        task_config["paused"] = task.paused
        add_scheduled_register_task(task_id, task_config)

    return {"task_id": task_id, "paused": task.paused}


@router.get("/{task_id}")
def get_task(task_id: str):
    _ensure_task_exists(task_id)
    return _task_store.snapshot(task_id)


@router.get("")
def list_tasks():
    return _task_store.list_snapshots()

