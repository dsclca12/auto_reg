from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlmodel import Session, select
from typing import Optional
from core.db import TaskLog, engine
import time, json, asyncio, threading, logging

router = APIRouter(prefix="/tasks", tags=["tasks"])
logger = logging.getLogger(__name__)

_tasks: dict = {}
_tasks_lock = threading.Lock()

MAX_FINISHED_TASKS = 200
CLEANUP_THRESHOLD = 250


def _cleanup_old_tasks():
    """Remove oldest finished tasks when the dict grows too large."""
    with _tasks_lock:
        finished = [
            (tid, t) for tid, t in _tasks.items()
            if t.get("status") in ("done", "failed")
        ]
        if len(finished) <= MAX_FINISHED_TASKS:
            return
        finished.sort(key=lambda x: x[0])
        to_remove = finished[: len(finished) - MAX_FINISHED_TASKS]
        for tid, _ in to_remove:
            del _tasks[tid]


class RegisterTaskRequest(BaseModel):
    platform: str
    email: Optional[str] = None
    password: Optional[str] = None
    count: int = Field(default=1, ge=1, le=1000)  # 最大支持 1000 个
    concurrency: int = Field(default=1, ge=1, le=10)  # 最大并发 10
    register_delay_seconds: float = Field(default=0, ge=0)
    random_delay_min: Optional[float] = Field(default=None, ge=0)  # 随机延迟最小值 (秒)
    random_delay_max: Optional[float] = Field(default=None, ge=0)  # 随机延迟最大值 (秒)
    proxy: Optional[str] = None
    executor_type: str = "protocol"
    captcha_solver: str = "yescaptcha"
    extra: dict = Field(default_factory=dict)
    # 定时任务配置
    task_id: Optional[str] = None  # 定时任务 ID（更新时使用）
    interval_type: Optional[str] = None  # minutes | hours
    interval_value: Optional[int] = None  # 间隔值


class TaskLogBatchDeleteRequest(BaseModel):
    ids: list[int]


def _log(task_id: str, msg: str):
    """向任务追加一条日志"""
    ts = time.strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    with _tasks_lock:
        if task_id in _tasks:
            _tasks[task_id].setdefault("logs", []).append(entry)
    print(entry)


def _save_task_log(platform: str, email: str, status: str,
                   error: str = "", detail: dict = None):
    """Write a TaskLog record to the database."""
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
    """注册成功后自动导入外部系统。"""
    try:
        from services.external_sync import sync_account

        for result in sync_account(account):
            name = result.get("name", "Auto Upload")
            ok = bool(result.get("ok"))
            msg = result.get("msg", "")
            _log(task_id, f"  [{name}] {'[OK] ' + msg if ok else '[FAIL] ' + msg}")
    except Exception as e:
        _log(task_id, f"  [Auto Upload] 自动导入异常: {e}")


def _run_register(task_id: str, req: RegisterTaskRequest):
    from core.registry import get
    from core.base_platform import RegisterConfig
    from core.db import save_account
    from core.base_mailbox import create_mailbox

    with _tasks_lock:
        _tasks[task_id]["status"] = "running"
    success = 0
    errors = []
    start_gate_lock = threading.Lock()
    next_start_time = time.time()

    try:
        PlatformCls = get(req.platform)

        def _build_mailbox(proxy: Optional[str]):
            from core.config_store import config_store
            merged_extra = config_store.get_all().copy()
            merged_extra.update({k: v for k, v in req.extra.items() if v is not None and v != ""})
            return create_mailbox(
                provider=merged_extra.get("mail_provider", "laoudo"),
                extra=merged_extra,
                proxy=proxy,
            )

        def _do_one(i: int):
            nonlocal next_start_time
            try:
                from core.proxy_pool import proxy_pool

                _proxy = req.proxy
                if not _proxy:
                    _proxy = proxy_pool.get_next()
                # 延迟控制
                if req.register_delay_seconds > 0 or (req.random_delay_min is not None and req.random_delay_max is not None):
                    with start_gate_lock:
                        now = time.time()
                        wait_seconds = max(0.0, next_start_time - now)
                        
                        # 固定延迟
                        if req.register_delay_seconds > 0 and wait_seconds > 0:
                            _log(task_id, f"第 {i+1} 个账号启动前延迟 {wait_seconds:g} 秒")
                            time.sleep(wait_seconds)
                        next_start_time = time.time() + req.register_delay_seconds
                        
                        # 随机延迟
                        if req.random_delay_min is not None and req.random_delay_max is not None:
                            import random
                            random_delay = random.uniform(req.random_delay_min, req.random_delay_max)
                            if random_delay > 0:
                                _log(task_id, f"第 {i+1} 个账号随机延迟 {random_delay:.1f} 秒 ({req.random_delay_min}-{req.random_delay_max}秒)")
                                time.sleep(random_delay)
                            next_start_time = time.time() + random_delay
                from core.config_store import config_store
                merged_extra = config_store.get_all().copy()
                merged_extra.update({k: v for k, v in req.extra.items() if v is not None and v != ""})
                
                _config = RegisterConfig(
                    executor_type=req.executor_type,
                    captcha_solver=req.captcha_solver,
                    proxy=_proxy,
                    extra=merged_extra,
                )
                _mailbox = _build_mailbox(_proxy)
                _platform = PlatformCls(config=_config, mailbox=_mailbox)
                _platform._log_fn = lambda msg: _log(task_id, msg)
                if getattr(_platform, "mailbox", None) is not None:
                    _platform.mailbox._log_fn = _platform._log_fn
                with _tasks_lock:
                    _tasks[task_id]["progress"] = f"{i+1}/{req.count}"
                _log(task_id, f"开始注册第 {i+1}/{req.count} 个账号")
                if _proxy: _log(task_id, f"使用代理: {_proxy}")
                account = _platform.register(
                    email=req.email or None,
                    password=req.password,
                )
                if isinstance(account.extra, dict):
                    mail_provider = merged_extra.get("mail_provider", "")
                    if mail_provider:
                        account.extra.setdefault("mail_provider", mail_provider)
                    if mail_provider == "luckmail" and req.platform == "chatgpt":
                        mailbox_token = getattr(_mailbox, "_token", "") or ""
                        if mailbox_token:
                            account.extra.setdefault("mailbox_token", mailbox_token)
                        if merged_extra.get("luckmail_project_code"):
                            account.extra.setdefault("luckmail_project_code", merged_extra.get("luckmail_project_code"))
                        if merged_extra.get("luckmail_email_type"):
                            account.extra.setdefault("luckmail_email_type", merged_extra.get("luckmail_email_type"))
                        if merged_extra.get("luckmail_domain"):
                            account.extra.setdefault("luckmail_domain", merged_extra.get("luckmail_domain"))
                        if merged_extra.get("luckmail_base_url"):
                            account.extra.setdefault("luckmail_base_url", merged_extra.get("luckmail_base_url"))
                saved_account = save_account(account)
                if _proxy: proxy_pool.report_success(_proxy)
                _log(task_id, f"[OK] 注册成功: {account.email}")
                _save_task_log(req.platform, account.email, "success")
                _auto_upload_integrations(task_id, saved_account or account)
                cashier_url = (account.extra or {}).get("cashier_url", "")
                if cashier_url:
                    _log(task_id, f"  [升级链接] {cashier_url}")
                    with _tasks_lock:
                        _tasks[task_id].setdefault("cashier_urls", []).append(cashier_url)
                return True
            except Exception as e:
                if _proxy: proxy_pool.report_fail(_proxy)
                _log(task_id, f"[FAIL] 注册失败: {e}")
                _save_task_log(req.platform, req.email or "", "failed", error=str(e))
                return str(e)

        from concurrent.futures import ThreadPoolExecutor, as_completed
        max_workers = min(req.concurrency, req.count, 5)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_do_one, i) for i in range(req.count)]
            for f in as_completed(futures):
                try:
                    result = f.result()
                except Exception as e:
                    _log(task_id, f"[ERROR] 任务线程异常: {e}")
                    errors.append(str(e))
                    continue
                if result is True:
                    success += 1
                else:
                    errors.append(result)
    except Exception as e:
        _log(task_id, f"致命错误: {e}")
        with _tasks_lock:
            _tasks[task_id]["status"] = "failed"
            _tasks[task_id]["error"] = str(e)
        return

    with _tasks_lock:
        _tasks[task_id]["status"] = "done"
        _tasks[task_id]["success"] = success
        _tasks[task_id]["errors"] = errors
    _log(task_id, f"完成: 成功 {success} 个, 失败 {len(errors)} 个")
    _cleanup_old_tasks()


@router.post("/register")
def create_register_task(
    req: RegisterTaskRequest,
    background_tasks: BackgroundTasks,
):
    mail_provider = req.extra.get("mail_provider")
    if mail_provider == "luckmail":
        platform = req.platform
        if platform in ("tavily", "openblocklabs"):
            raise HTTPException(400, f"LuckMail 渠道暂时不支持 {platform} 项目注册")
        
        mapping = {
            "trae": "trae",
            "cursor": "cursor",
            "grok": "grok",
            "kiro": "kiro",
            "chatgpt": "openai"
        }
        req.extra["luckmail_project_code"] = mapping.get(platform, platform)

    task_id = f"task_{int(time.time()*1000)}"
    with _tasks_lock:
        _tasks[task_id] = {"id": task_id, "status": "pending",
                           "progress": f"0/{req.count}", "logs": []}
    background_tasks.add_task(_run_register, task_id, req)
    return {"task_id": task_id}


@router.get("/logs")
def get_logs(platform: str = None, page: int = 1, page_size: int = 50):
    with Session(engine) as s:
        q = select(TaskLog)
        if platform:
            q = q.where(TaskLog.platform == platform)
        q = q.order_by(TaskLog.id.desc())
        total = len(s.exec(q).all())
        items = s.exec(q.offset((page - 1) * page_size).limit(page_size)).all()
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
        except Exception as e:
            s.rollback()
            logger.exception("批量删除任务历史失败")
            raise HTTPException(500, f"批量删除任务历史失败: {str(e)}")


@router.get("/{task_id}/logs/stream")
async def stream_logs(task_id: str, since: int = 0):
    """SSE 实时日志流"""
    with _tasks_lock:
        if task_id not in _tasks:
            raise HTTPException(404, "任务不存在")

    async def event_generator():
        sent = since
        while True:
            with _tasks_lock:
                logs = list(_tasks.get(task_id, {}).get("logs", []))
                status = _tasks.get(task_id, {}).get("status", "")
            while sent < len(logs):
                yield f"data: {json.dumps({'line': logs[sent]})}\n\n"
                sent += 1
            if status in ("done", "failed"):
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



# 定时任务管理 API
@router.post("/schedule/{task_id}/run")
def run_scheduled_task_now(task_id: str, background_tasks: BackgroundTasks):
    """立即执行定时任务"""
    from core.scheduler import get_scheduled_register_tasks, update_task_run_status
    from api.tasks import _run_register, _log
    import logging
    
    logger = logging.getLogger(__name__)
    
    tasks = get_scheduled_register_tasks()
    if task_id not in tasks:
        raise HTTPException(404, "任务不存在")
    
    task_config = tasks[task_id]
    run_task_id = f"manual_{task_id}_{int(time.time())}"
    
    # 创建 RegisterTaskRequest
    req = RegisterTaskRequest(**task_config)
    logger.info(f"准备手动运行任务 {run_task_id}, 配置：{task_config}")
    
    def run_with_status():
        try:
            # 先初始化 _tasks 记录
            with _tasks_lock:
                _tasks[run_task_id] = {"id": run_task_id, "status": "pending", "progress": "0/1", "logs": []}
            # 先记录开始
            _log(run_task_id, f"开始手动运行定时任务 {task_id}")
            _run_register(run_task_id, req)
            # 运行完成后更新状态（延迟一点等待 cleanup 完成）
            import time
            time.sleep(2)
            update_task_run_status(task_id, True, None)
            logger.info(f"任务 {run_task_id} 运行完成")
        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n{traceback.format_exc()}"
            update_task_run_status(task_id, False, error_msg)
            logger.error(f"任务 {run_task_id} 运行失败：{error_msg}")
    
    background_tasks.add_task(run_with_status)
    
    return {"task_id": run_task_id, "status": "running"}

@router.post("/schedule/{task_id}/toggle")
def toggle_scheduled_task(task_id: str):
    """暂停或恢复定时任务"""
    from core.scheduler import get_scheduled_register_tasks, add_scheduled_register_task
    
    tasks = get_scheduled_register_tasks()
    if task_id not in tasks:
        raise HTTPException(404, "任务不存在")
    
    task_config = tasks[task_id]
    task_config['paused'] = not task_config.get('paused', False)
    add_scheduled_register_task(task_id, task_config)
    
    return {"task_id": task_id, "paused": task_config['paused']}

@router.post("/schedule")
def create_scheduled_task(body: RegisterTaskRequest):
    """创建定时注册任务"""
    import uuid
    from core.db import ScheduledTaskModel, Session, engine
    from sqlmodel import select
    
    task_id = f"sched_{uuid.uuid4().hex[:8]}"
    
    # 保存到数据库
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
    
    # 添加到内存
    from core.scheduler import add_scheduled_register_task
    config = body.dict()
    config['task_id'] = task_id
    add_scheduled_register_task(task_id, config)
    
    # 创建后立即在线程中执行一次
    def run_now():
        run_task_id = f"scheduled_{task_id}_{int(time.time())}"
        success = False
        error_msg = None
        try:
            # 先初始化 _tasks 记录
            with _tasks_lock:
                _tasks[run_task_id] = {"id": run_task_id, "status": "pending", "progress": "0/1", "logs": []}
            req = RegisterTaskRequest(**config)
            _run_register(run_task_id, req)
            success = True
            print(f"[Scheduler] 任务 {task_id} 已执行", flush=True)
        except Exception as e:
            error_msg = str(e)
            print(f"[Scheduler] 任务 {task_id} 执行失败：{e}", flush=True)
        finally:
            # 更新任务运行状态
            from core.scheduler import update_task_run_status
            update_task_run_status(task_id, success, error_msg)
    
    threading.Thread(target=run_now, daemon=True).start()
    print(f"[Scheduler] 任务 {task_id} 已创建并启动", flush=True)
    
    return {"task_id": task_id, "status": "scheduled", "config": config}


@router.get("/schedule")
def list_scheduled_tasks():
    """获取所有定时任务"""
    from core.scheduler import get_scheduled_register_tasks, get_all_task_run_status
    tasks = get_scheduled_register_tasks()
    run_status = get_all_task_run_status()
    
    # 合并运行状态
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


@router.delete("/schedule/{task_id}")
def delete_scheduled_task(task_id: str):
    """删除定时任务"""
    from core.scheduler import remove_scheduled_register_task
    remove_scheduled_register_task(task_id)
    return {"ok": True}

@router.get("/{task_id}")
def get_task(task_id: str):
    with _tasks_lock:
        if task_id not in _tasks:
            raise HTTPException(404, "任务不存在")
        return _tasks[task_id]


@router.get("")
def list_tasks():
    with _tasks_lock:
        return list(_tasks.values())



# 定时任务管理 API - 更新任务
@router.put("/schedule")
def update_scheduled_task(body: RegisterTaskRequest):
    """更新定时任务配置"""
    from core.scheduler import get_scheduled_register_tasks, add_scheduled_register_task, remove_scheduled_register_task
    
    # 从根级别或 extra 中获取 task_id
    task_id = getattr(body, 'task_id', None) or (body.extra and body.extra.get('task_id'))
    if not task_id:
        raise HTTPException(400, "缺少任务 ID")
    
    tasks = get_scheduled_register_tasks()
    if task_id not in tasks:
        raise HTTPException(404, "任务不存在")
    
    config = body.dict()
    config['task_id'] = task_id
    add_scheduled_register_task(task_id, config)
    
    return {"task_id": task_id, "status": "updated", "config": config}


# 手动运行定时任务



# 暂停/恢复定时任务



@router.delete("/schedule/{task_id}")
def delete_scheduled_task(task_id: str):
    """删除定时任务"""
    from core.db import ScheduledTaskModel, Session, engine
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
    """暂停或恢复定时任务"""
    from core.db import ScheduledTaskModel, Session, engine
    from core.scheduler import add_scheduled_register_task, get_scheduled_register_tasks
    
    with Session(engine) as s:
        task = s.get(ScheduledTaskModel, task_id)
        if not task:
            raise HTTPException(404, "任务不存在")
        task.paused = not task.paused
        task.updated_at = datetime.now(timezone.utc)
        s.add(task)
        s.commit()
        
        # 更新内存中的任务
        tasks = get_scheduled_register_tasks()
        if task_id in tasks:
            tasks[task_id]['paused'] = task.paused
            add_scheduled_register_task(task_id, tasks[task_id])
    
    return {"task_id": task_id, "paused": task.paused}
