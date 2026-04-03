"""
增强版 ChatGPT 注册客户端
集成人类行为模拟、浏览器指纹、请求头增强、存储模拟和智能重试
提供更接近真实浏览器人类操作行为的注册流程
"""

import random
import time
import uuid
from typing import Optional, Tuple, Dict, Any
from urllib.parse import urlparse

from .chatgpt_client import ChatGPTClient
from .human_behavior_simulator import (
    HumanBehaviorSimulator,
    HumanBehaviorConfig,
    human_delay,
    human_think,
    human_observe_page,
    human_type,
    human_scroll,
)
from .browser_fingerprint_enhancer import (
    BrowserFingerprintGenerator,
    BrowserFingerprintConfig,
    get_browser_fingerprint,
)
from .request_header_enhancer import (
    RequestHeaderEnhancer,
    enhance_request_headers,
    update_cache_from_response,
    simulate_resource_preload,
)
from .storage_behavior_simulator import (
    BrowserStorageSimulator,
    get_storage_simulator,
)
from .smart_retry_handler import (
    RetryManager,
    RetryConfig,
    ErrorClassifier,
    ErrorCategory,
    RetryStrategy,
    CircuitBreaker,
)


class EnhancedChatGPTClient:
    """增强版 ChatGPT 注册客户端"""
    
    def __init__(
        self,
        proxy=None,
        verbose=True,
        browser_mode="protocol",
        enable_human_behavior=True,
        enable_fingerprint=True,
        enable_storage_sim=True,
    ):
        """
        初始化增强版客户端
        
        Args:
            proxy: 代理地址
            verbose: 是否输出详细日志
            browser_mode: protocol | headless | headed
            enable_human_behavior: 是否启用人类行为模拟
            enable_fingerprint: 是否启用浏览器指纹
            enable_storage_sim: 是否启用存储模拟
        """
        # 创建基础客户端
        self.base_client = ChatGPTClient(
            proxy=proxy,
            verbose=verbose,
            browser_mode=browser_mode,
        )
        
        # 配置
        self.proxy = proxy
        self.verbose = verbose
        self.browser_mode = browser_mode
        self.enable_human_behavior = enable_human_behavior
        self.enable_fingerprint = enable_fingerprint
        self.enable_storage_sim = enable_storage_sim
        
        # 初始化增强模块
        if enable_human_behavior:
            behavior_config = HumanBehaviorConfig(
                headed_mode=(browser_mode == "headed"),
            )
            self.behavior_sim = HumanBehaviorSimulator(behavior_config)
        else:
            self.behavior_sim = None
        
        if enable_fingerprint:
            self.fingerprint_gen = BrowserFingerprintGenerator()
            self.browser_fingerprint = self.fingerprint_gen.generate()
        else:
            self.fingerprint_gen = None
            self.browser_fingerprint = None
        
        self.header_enhancer = RequestHeaderEnhancer()
        
        if enable_storage_sim:
            self.storage_sim = get_storage_simulator()
        else:
            self.storage_sim = None
        
        # 智能重试
        self.retry_manager = RetryManager(RetryConfig(
            max_retries=3,
            base_delay=1.0,
            max_delay=30.0,
            strategy=RetryStrategy.JITTERED,
        ))
        
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=120.0,
        )
        
        # 重写日志函数
        self.base_client._log = self._log
    
    def _log(self, msg):
        """输出日志"""
        if self.verbose:
            print(f"  [Enhanced] {msg}")
    
    def visit_homepage_enhanced(self) -> bool:
        """
        增强版访问首页
        包含人类行为模拟和更真实的请求模式
        """
        self._log("访问 ChatGPT 首页（增强版）...")
        
        # 页面加载前的自然延迟
        if self.enable_human_behavior:
            self.behavior_sim.natural_delay(0.5, 2.0)
        
        # 模拟资源预加载
        if self.enable_fingerprint:
            simulate_resource_preload("https://chatgpt.com/", "document")
            simulate_resource_preload("https://chatgpt.com/main.js", "script")
            simulate_resource_preload("https://chatgpt.com/styles.css", "stylesheet")
        
        # 访问首页
        success = self.base_client.visit_homepage()
        
        if success:
            # 页面加载后的观察时间
            if self.enable_human_behavior:
                human_observe_page()
            
            # 模拟滚动浏览
            if self.enable_human_behavior and random.random() < 0.6:
                human_scroll()
            
            # 更新存储
            if self.enable_storage_sim:
                self.storage_sim.local_storage.set_item(
                    "oai/apps/last_visit", time.time()
                )
                page_views = (
                    self.storage_sim.session_storage.get_item("oai/session/page_views") or 0
                )
                self.storage_sim.session_storage.set_item(
                    "oai/session/page_views", page_views + 1
                )
            
            self._log("首页访问完成")
        else:
            self._log("首页访问失败")
        
        return success
    
    def get_csrf_token_enhanced(self) -> Optional[str]:
        """
        增强版获取 CSRF token
        包含更自然的请求延迟
        """
        self._log("获取 CSRF token（增强版）...")
        
        # 自然的请求间隔
        if self.enable_human_behavior:
            self.behavior_sim.natural_delay(0.3, 1.0)
        
        return self.base_client.get_csrf_token()
    
    def signin_enhanced(self, email: str, csrf_token: str) -> Optional[str]:
        """
        增强版提交邮箱
        模拟用户输入邮箱的行为
        """
        self._log(f"提交邮箱: {email}（增强版）")
        
        # 模拟输入邮箱的节奏
        if self.enable_human_behavior:
            # 思考时间（用户准备输入）
            human_think()
            
            # 模拟打字输入邮箱
            human_type(email, per_char=True)
            
            # 输入完成后的短暂停顿
            self.behavior_sim.natural_delay(0.2, 0.6)
        
        # 提交
        auth_url = self.base_client.signin(email, csrf_token)
        
        if auth_url:
            self._log("邮箱提交成功")
        else:
            self._log("邮箱提交失败")
        
        return auth_url
    
    def authorize_enhanced(self, url: str, max_retries: int = 3) -> str:
        """
        增强版访问 authorize URL
        包含智能重试和更自然的等待
        """
        self._log(f"访问 authorize URL（增强版）...")
        
        # 点击授权按钮前的思考时间
        if self.enable_human_behavior:
            human_think()
        
        # 使用智能重试执行授权
        result = ""
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    self._log(f"授权重试 {attempt + 1}/{max_retries}")
                
                result = self.base_client.authorize(url, max_retries=1)
                
                if result:
                    # 授权成功后等待（模拟页面加载）
                    if self.enable_human_behavior:
                        human_observe_page()
                    
                    # 更新存储
                    if self.enable_storage_sim:
                        self.storage_sim.session_storage.set_item(
                            "oai/session/authorized", True
                        )
                    
                    self._log(f"授权成功: {result}")
                    return result
                
            except Exception as e:
                error_info = ErrorClassifier.classify_error(e)
                self._log(f"授权异常: {error_info.category} - {e}")
                
                if error_info.retryable and attempt < max_retries - 1:
                    delay = random.uniform(1.0, 3.0) * (attempt + 1)
                    self._log(f"等待 {delay:.2f}s 后重试...")
                    time.sleep(delay)
                else:
                    break
        
        return result
    
    def register_user_enhanced(self, email: str, password: str) -> Tuple[bool, str]:
        """
        增强版注册用户
        模拟用户输入密码的行为
        """
        self._log(f"注册用户（增强版）...")
        
        # 模拟输入密码的行为
        if self.enable_human_behavior:
            # 密码输入前的思考时间
            human_think()
            
            # 模拟打字输入密码（逐字符）
            human_type(password, per_char=True)
            
            # 输入完成后的停顿（用户检查密码）
            self.behavior_sim.natural_delay(0.5, 1.5)
        
        # 提交注册
        success, message = self.base_client.register_user(email, password)
        
        if success:
            self._log("密码注册成功")
            
            # 更新存储
            if self.enable_storage_sim:
                self.storage_sim.local_storage.set_item(
                    "oai/apps/registration_completed", time.time()
                )
        else:
            self._log(f"密码注册失败: {message}")
        
        return success, message
    
    def send_email_otp_enhanced(self) -> bool:
        """
        增强版发送验证码
        """
        self._log("触发发送验证码（增强版）...")
        
        # 点击发送按钮前的延迟
        if self.enable_human_behavior:
            self.behavior_sim.natural_delay(0.3, 0.8)
        
        return self.base_client.send_email_otp()
    
    def verify_email_otp_enhanced(self, otp_code: str) -> Tuple[bool, str]:
        """
        增强版验证 OTP 码
        模拟用户输入验证码的行为
        """
        self._log(f"验证 OTP 码: {otp_code}（增强版）")
        
        # 模拟输入验证码
        if self.enable_human_behavior:
            # 验证码通常是逐位输入的
            human_type(otp_code, per_char=True)
            
            # 输入完成后的思考时间
            human_think()
        
        # 提交验证
        success, result = self.base_client.verify_email_otp(otp_code, return_state=True)
        
        if success:
            self._log("验证码验证成功")
            
            # 更新存储
            if self.enable_storage_sim:
                self.storage_sim.local_storage.set_item(
                    "oai/apps/email_verified", time.time()
                )
        else:
            self._log(f"验证码验证失败: {result}")
        
        return success, result
    
    def create_account_enhanced(
        self,
        first_name: str,
        last_name: str,
        birthdate: str
    ) -> Tuple[bool, str]:
        """
        增强版创建账号
        模拟用户填写个人信息的行为
        """
        name = f"{first_name} {last_name}"
        self._log(f"完成账号创建: {name}（增强版）")
        
        # 模拟填写表单的完整流程
        if self.enable_human_behavior:
            fields = [
                ("first_name", first_name),
                ("last_name", last_name),
                ("birthdate", birthdate),
            ]
            
            # 使用表单填写序列
            self.behavior_sim.form_filling_sequence(
                fields,
                submit_callback=None  # 提交由 base 客户端处理
            )
        
        # 提交账号创建
        success, result = self.base_client.create_account(
            first_name, last_name, birthdate, return_state=True
        )
        
        if success:
            self._log("账号创建成功")
            
            # 更新存储
            if self.enable_storage_sim:
                self.storage_sim.local_storage.set_item("oai/apps/account_created", time.time())
                self.storage_sim.local_storage.set_item("oai/apps/profile_name", name)
        else:
            self._log(f"账号创建失败: {result}")
        
        return success, result
    
    def register_complete_flow_enhanced(
        self,
        email: str,
        password: str,
        first_name: str,
        last_name: str,
        birthdate: str,
        skymail_client
    ) -> Tuple[bool, str]:
        """
        增强版完整注册流程
        集成所有优化模块
        """
        self._log("=" * 60)
        self._log("开始增强版注册流程")
        self._log("=" * 60)
        
        max_auth_attempts = 3
        final_url = ""
        final_path = ""
        
        # 阶段 1: 预授权（带智能重试）
        for auth_attempt in range(max_auth_attempts):
            if auth_attempt > 0:
                self._log(f"预授权阶段重试 {auth_attempt + 1}/{max_auth_attempts}...")
                self.base_client._reset_session()
                
                # 重置会话后等待
                if self.enable_human_behavior:
                    self.behavior_sim.natural_delay(1.0, 3.0)
            
            # 1. 访问首页
            if not self.visit_homepage_enhanced():
                if auth_attempt < max_auth_attempts - 1:
                    continue
                return False, "访问首页失败"
            
            # 2. 获取 CSRF token
            csrf_token = self.get_csrf_token_enhanced()
            if not csrf_token:
                if auth_attempt < max_auth_attempts - 1:
                    continue
                return False, "获取 CSRF token 失败"
            
            # 3. 提交邮箱
            auth_url = self.signin_enhanced(email, csrf_token)
            if not auth_url:
                if auth_attempt < max_auth_attempts - 1:
                    continue
                return False, "提交邮箱失败"
            
            # 4. 访问 authorize URL
            final_url = self.authorize_enhanced(auth_url)
            if not final_url:
                if auth_attempt < max_auth_attempts - 1:
                    continue
                return False, "Authorize 失败"
            
            final_path = urlparse(final_url).path
            self._log(f"Authorize → {final_path}")
            
            # 检测 Cloudflare 中间页
            if "api/accounts/authorize" in final_path or final_path == "/error":
                self._log(f"检测到 Cloudflare/SPA 中间页: {final_url[:160]}...")
                if auth_attempt < max_auth_attempts - 1:
                    continue
                return False, f"预授权被拦截: {final_path}"
            
            break
        
        # 阶段 2: 注册状态机（增强版）
        state = self.base_client._state_from_url(final_url)
        self._log(f"注册状态起点: {self.base_client._describe_flow_state(state)}")
        
        register_submitted = False
        otp_verified = False
        account_created = False
        seen_states = {}
        
        for step in range(12):
            signature = self.base_client._state_signature(state)
            seen_states[signature] = seen_states.get(signature, 0) + 1
            
            if seen_states[signature] > 2:
                return False, f"注册状态卡住: {self.base_client._describe_flow_state(state)}"
            
            # 检查注册是否完成
            if self.base_client._is_registration_complete_state(state):
                self.base_client.last_registration_state = state
                self._log("✅ 增强版注册流程完成")
                return True, "注册成功"
            
            # 密码注册阶段
            if self.base_client._state_is_password_registration(state):
                self._log("全新注册流程")
                if register_submitted:
                    return False, "注册密码阶段重复进入"
                
                success, msg = self.register_user_enhanced(email, password)
                if not success:
                    return False, f"注册失败: {msg}"
                
                register_submitted = True
                
                if not self.send_email_otp_enhanced():
                    self._log("发送验证码接口返回失败，继续等待邮箱中的验证码...")
                
                state = self.base_client._state_from_url(
                    f"{self.base_client.AUTH}/email-verification"
                )
                continue
            
            # 邮箱验证码阶段
            if self.base_client._state_is_email_otp(state):
                self._log("等待邮箱验证码（增强版）...")
                
                # 等待验证码（模拟用户查看邮箱的行为）
                if self.enable_human_behavior:
                    self._log("模拟用户切换到邮箱应用...")
                    self.behavior_sim.natural_delay(3.0, 8.0)
                    self._log("用户返回浏览器")
                
                otp_code = skymail_client.wait_for_verification_code(email, timeout=90)
                if not otp_code:
                    return False, "未收到验证码"
                
                success, next_state = self.verify_email_otp_enhanced(otp_code)
                if not success:
                    return False, f"验证码失败: {next_state}"
                
                otp_verified = True
                state = next_state
                self.base_client.last_registration_state = state
                continue
            
            # 填写个人信息阶段
            if self.base_client._state_is_about_you(state):
                if account_created:
                    return False, "填写信息阶段重复进入"
                
                success, next_state = self.create_account_enhanced(
                    first_name, last_name, birthdate
                )
                if not success:
                    return False, f"创建账号失败: {next_state}"
                
                account_created = True
                state = next_state
                self.base_client.last_registration_state = state
                continue
            
            # 需要导航的阶段
            if self.base_client._state_requires_navigation(state):
                success, next_state = self.base_client._follow_flow_state(
                    state,
                    referer=state.current_url or f"{self.base_client.AUTH}/about-you",
                )
                if not success:
                    return False, f"跳转失败: {next_state}"
                
                state = next_state
                self.base_client.last_registration_state = state
                
                # 跟随导航后观察页面
                if self.enable_human_behavior:
                    human_observe_page()
                
                continue
            
            # 回退到全新注册流程
            if (not register_submitted) and (not otp_verified) and (not account_created):
                self._log(
                    f"未知起始状态，回退为全新注册流程: "
                    f"{self.base_client._describe_flow_state(state)}"
                )
                state = self.base_client._state_from_url(
                    f"{self.base_client.AUTH}/create-account/password"
                )
                continue
            
            return False, f"未支持的注册状态: {self.base_client._describe_flow_state(state)}"
        
        return False, "注册状态机超出最大步数"
    
    def get_session_and_tokens(self) -> Tuple[bool, Any]:
        """
        增强版获取 Session 和 Tokens
        包含更自然的等待和重试
        """
        self._log("复用注册会话获取 Token（增强版）...")
        
        # 获取前的等待（模拟用户等待页面加载）
        if self.enable_human_behavior:
            human_observe_page()
        
        # 使用重试机制获取 session
        success, result = self.base_client.reuse_session_and_get_tokens()
        
        if success:
            self._log("Token 提取成功")
            
            # 成功后更新存储
            if self.enable_storage_sim:
                self.storage_sim.local_storage.set_item(
                    "oai/apps/session_established", time.time()
                )
        else:
            self._log(f"Token 提取失败: {result}")
        
        return success, result
    
    def get_enhanced_stats(self) -> Dict:
        """获取增强版统计信息"""
        stats = {
            "human_behavior": {},
            "retry_stats": self.retry_manager.get_error_statistics(),
            "circuit_breaker_state": self.circuit_breaker.state,
        }
        
        if self.behavior_sim:
            stats["human_behavior"]["action_log_length"] = len(
                self.behavior_sim.get_action_log()
            )
        
        if self.storage_sim:
            stats["storage"] = {
                "local_storage_items": self.storage_sim.local_storage.length(),
                "session_storage_items": self.storage_sim.session_storage.length(),
            }
        
        return stats


# 便捷函数：创建增强版客户端
def create_enhanced_client(
    proxy=None,
    verbose=True,
    browser_mode="protocol",
    **kwargs
) -> EnhancedChatGPTClient:
    """
    创建增强版 ChatGPT 客户端
    
    Args:
        proxy: 代理地址
        verbose: 是否输出详细日志
        browser_mode: protocol | headless | headed
        **kwargs: 其他配置参数
    
    Returns:
        EnhancedChatGPTClient 实例
    """
    return EnhancedChatGPTClient(
        proxy=proxy,
        verbose=verbose,
        browser_mode=browser_mode,
        **kwargs
    )
