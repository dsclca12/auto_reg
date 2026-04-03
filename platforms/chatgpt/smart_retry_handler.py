"""
智能重试和错误处理模块
提供更智能的错误恢复策略，包括：
- 指数退避重试
- 错误分类和处理策略
- 自适应重试间隔
- 故障转移机制
- 错误统计和分析
"""

import random
import time
import logging
from typing import (
    Callable, Optional, Any, Dict, List, Tuple, Type, Union
)
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps

logger = logging.getLogger(__name__)


class ErrorCategory(str, Enum):
    """错误分类"""
    NETWORK_ERROR = "network_error"
    TLS_ERROR = "tls_error"
    RATE_LIMIT = "rate_limit"
    CAPTCHA = "captcha"
    VALIDATION_ERROR = "validation_error"
    AUTH_ERROR = "auth_error"
    SERVER_ERROR = "server_error"
    TIMEOUT = "timeout"
    CLIENT_ERROR = "client_error"
    UNKNOWN = "unknown"


class RetryStrategy(str, Enum):
    """重试策略"""
    EXPONENTIAL = "exponential"  # 指数退避
    LINEAR = "linear"            # 线性增长
    FIXED = "fixed"              # 固定间隔
    JITTERED = "jittered"        # 抖动策略（推荐）


@dataclass
class RetryConfig:
    """重试配置"""
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.JITTERED
    retryable_errors: List[ErrorCategory] = field(default_factory=lambda: [
        ErrorCategory.NETWORK_ERROR,
        ErrorCategory.TLS_ERROR,
        ErrorCategory.TIMEOUT,
        ErrorCategory.RATE_LIMIT,
        ErrorCategory.SERVER_ERROR,
    ])
    backoff_factor: float = 2.0
    jitter_range: Tuple[float, float] = (0.5, 1.5)


@dataclass
class ErrorInfo:
    """错误信息"""
    category: ErrorCategory
    message: str
    status_code: Optional[int] = None
    retryable: bool = True
    retry_after: Optional[float] = None
    timestamp: float = field(default_factory=time.time)
    attempt: int = 1


class ErrorClassifier:
    """错误分类器"""
    
    @staticmethod
    def classify_error(error: Exception, status_code: Optional[int] = None) -> ErrorInfo:
        """
        分类错误
        
        Args:
            error: 异常对象
            status_code: HTTP 状态码（如果有）
        
        Returns:
            错误信息
        """
        error_msg = str(error).lower()
        
        # TLS/SSL 错误
        if any(keyword in error_msg for keyword in [
            "tls", "ssl", "curl: (35)", "handshake", "certificate"
        ]):
            return ErrorInfo(
                category=ErrorCategory.TLS_ERROR,
                message=str(error),
                status_code=status_code,
                retryable=True,
            )
        
        # 网络错误
        if any(keyword in error_msg for keyword in [
            "connection", "timeout", "refused", "unreachable",
            "network", "dns", "socket"
        ]):
            return ErrorInfo(
                category=ErrorCategory.NETWORK_ERROR,
                message=str(error),
                status_code=status_code,
                retryable=True,
            )
        
        # 超时
        if any(keyword in error_msg for keyword in [
            "timeout", "timed out"
        ]):
            return ErrorInfo(
                category=ErrorCategory.TIMEOUT,
                message=str(error),
                status_code=status_code,
                retryable=True,
            )
        
        # 根据 HTTP 状态码分类
        if status_code is not None:
            return ErrorClassifier._classify_by_status_code(status_code, str(error))
        
        # 默认未知错误
        return ErrorInfo(
            category=ErrorCategory.UNKNOWN,
            message=str(error),
            retryable=False,
        )
    
    @staticmethod
    def _classify_by_status_code(status_code: int, error_msg: str) -> ErrorInfo:
        """根据 HTTP 状态码分类"""
        
        # 4xx 客户端错误
        if 400 <= status_code < 500:
            if status_code == 429:
                # 速率限制
                return ErrorInfo(
                    category=ErrorCategory.RATE_LIMIT,
                    message=error_msg,
                    status_code=status_code,
                    retryable=True,
                    retry_after=ErrorClassifier._extract_retry_after(error_msg),
                )
            elif status_code == 403:
                # 可能是验证码或封禁
                if any(keyword in error_msg.lower() for keyword in [
                    "captcha", "verify", "challenge", "cloudflare"
                ]):
                    return ErrorInfo(
                        category=ErrorCategory.CAPTCHA,
                        message=error_msg,
                        status_code=status_code,
                        retryable=True,
                    )
                return ErrorInfo(
                    category=ErrorCategory.AUTH_ERROR,
                    message=error_msg,
                    status_code=status_code,
                    retryable=False,
                )
            elif status_code == 401:
                return ErrorInfo(
                    category=ErrorCategory.AUTH_ERROR,
                    message=error_msg,
                    status_code=status_code,
                    retryable=False,
                )
            elif status_code == 400:
                return ErrorInfo(
                    category=ErrorCategory.VALIDATION_ERROR,
                    message=error_msg,
                    status_code=status_code,
                    retryable=False,
                )
            else:
                return ErrorInfo(
                    category=ErrorCategory.CLIENT_ERROR,
                    message=error_msg,
                    status_code=status_code,
                    retryable=False,
                )
        
        # 5xx 服务器错误
        elif 500 <= status_code < 600:
            return ErrorInfo(
                category=ErrorCategory.SERVER_ERROR,
                message=error_msg,
                status_code=status_code,
                retryable=True,
            )
        
        # 其他状态码
        return ErrorInfo(
            category=ErrorCategory.UNKNOWN,
            message=error_msg,
            status_code=status_code,
            retryable=False,
        )
    
    @staticmethod
    def _extract_retry_after(error_msg: str) -> Optional[float]:
        """从错误消息中提取重试间隔"""
        import re
        
        # 尝试匹配 "retry after X seconds"
        match = re.search(r'retry after\s+(\d+)', error_msg, re.IGNORECASE)
        if match:
            return float(match.group(1))
        
        # 尝试匹配 "X seconds"
        match = re.search(r'(\d+)\s+seconds?', error_msg, re.IGNORECASE)
        if match:
            return float(match.group(1))
        
        return None


class RetryManager:
    """重试管理器"""
    
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self._error_history: List[ErrorInfo] = []
        self._retry_stats: Dict[str, int] = {
            "total_retries": 0,
            "successful_retries": 0,
            "failed_retries": 0,
        }
    
    def execute_with_retry(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """
        执行函数并重试
        
        Args:
            func: 要执行的函数
            *args: 位置参数
            **kwargs: 关键字参数
        
        Returns:
            函数执行结果
        
        Raises:
            最后一次重试失败后抛出的异常
        """
        last_error = None
        
        for attempt in range(1, self.config.max_retries + 1):
            try:
                result = func(*args, **kwargs)
                
                # 成功后记录统计
                if attempt > 1:
                    self._retry_stats["successful_retries"] += 1
                    logger.info(f"重试成功 (尝试 {attempt}/{self.config.max_retries})")
                
                return result
            
            except Exception as e:
                last_error = e
                
                # 分类错误
                status_code = kwargs.pop("status_code", None)
                error_info = ErrorClassifier.classify_error(e, status_code)
                error_info.attempt = attempt
                
                # 记录错误历史
                self._error_history.append(error_info)
                
                # 检查是否可重试
                if not error_info.retryable:
                    logger.error(f"不可重试的错误: {error_info.category} - {e}")
                    raise
                
                # 检查是否在可重试错误列表中
                if error_info.category not in self.config.retryable_errors:
                    logger.warning(f"错误 {error_info.category} 不在重试列表中")
                    raise
                
                # 检查是否还有重试机会
                if attempt >= self.config.max_retries:
                    logger.error(f"达到最大重试次数 ({self.config.max_retries})")
                    self._retry_stats["failed_retries"] += 1
                    raise
                
                # 计算等待时间
                delay = self._calculate_delay(attempt, error_info)
                
                logger.warning(
                    f"重试 {attempt}/{self.config.max_retries} - "
                    f"错误: {error_info.category} - "
                    f"等待 {delay:.2f}s"
                )
                
                self._retry_stats["total_retries"] += 1
                time.sleep(delay)
        
        # 不应该到这里，但为了安全
        if last_error:
            raise last_error
    
    def _calculate_delay(self, attempt: int, error_info: ErrorInfo) -> float:
        """
        计算重试延迟
        
        Args:
            attempt: 当前尝试次数
            error_info: 错误信息
        
        Returns:
            延迟时间（秒）
        """
        # 如果有明确的重试间隔
        if error_info.retry_after is not None:
            return min(error_info.retry_after, self.config.max_delay)
        
        # 根据策略计算
        if self.config.strategy == RetryStrategy.EXPONENTIAL:
            delay = self.config.base_delay * (self.config.backoff_factor ** (attempt - 1))
        
        elif self.config.strategy == RetryStrategy.LINEAR:
            delay = self.config.base_delay * attempt
        
        elif self.config.strategy == RetryStrategy.FIXED:
            delay = self.config.base_delay
        
        elif self.config.strategy == RetryStrategy.JITTERED:
            # 指数退避 + 随机抖动（推荐）
            exp_delay = self.config.base_delay * (self.config.backoff_factor ** (attempt - 1))
            jitter = random.uniform(*self.config.jitter_range)
            delay = exp_delay * jitter
        
        else:
            delay = self.config.base_delay
        
        # 限制最大延迟
        delay = min(delay, self.config.max_delay)
        
        # 添加随机扰动
        jitter = delay * random.uniform(-0.2, 0.2)
        delay = max(0.1, delay + jitter)
        
        return delay
    
    def get_error_statistics(self) -> Dict:
        """获取错误统计信息"""
        category_counts = {}
        for error in self._error_history:
            cat = error.category.value
            category_counts[cat] = category_counts.get(cat, 0) + 1
        
        return {
            "total_retries": self._retry_stats["total_retries"],
            "successful_retries": self._retry_stats["successful_retries"],
            "failed_retries": self._retry_stats["failed_retries"],
            "success_rate": (
                self._retry_stats["successful_retries"] / max(1, self._retry_stats["total_retries"])
            ) if self._retry_stats["total_retries"] > 0 else 0.0,
            "error_categories": category_counts,
            "total_errors": len(self._error_history),
        }
    
    def reset_stats(self):
        """重置统计信息"""
        self._error_history.clear()
        self._retry_stats = {
            "total_retries": 0,
            "successful_retries": 0,
            "failed_retries": 0,
        }


class CircuitBreaker:
    """熔断器（防止连续失败）"""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 3,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        
        self._failure_count = 0
        self._state = "closed"  # closed, open, half_open
        self._last_failure_time = 0.0
        self._half_open_calls = 0
    
    def can_execute(self) -> bool:
        """检查是否可以执行"""
        if self._state == "closed":
            return True
        
        if self._state == "open":
            # 检查是否过了恢复时间
            if time.time() - self._last_failure_time > self.recovery_timeout:
                self._state = "half_open"
                self._half_open_calls = 0
                return True
            return False
        
        # half_open 状态
        return self._half_open_calls < self.half_open_max_calls
    
    def record_success(self):
        """记录成功"""
        if self._state == "half_open":
            self._half_open_calls += 1
            if self._half_open_calls >= self.half_open_max_calls:
                self._state = "closed"
                self._failure_count = 0
        else:
            self._failure_count = 0
    
    def record_failure(self):
        """记录失败"""
        self._failure_count += 1
        self._last_failure_time = time.time()
        
        if self._state == "half_open":
            # 半开状态下失败，重新打开
            self._state = "open"
        elif self._failure_count >= self.failure_threshold:
            self._state = "open"
    
    @property
    def state(self) -> str:
        """获取当前状态"""
        return self._state
    
    def reset(self):
        """重置熔断器"""
        self._failure_count = 0
        self._state = "closed"
        self._last_failure_time = 0.0
        self._half_open_calls = 0


# 装饰器
def retry_on_failure(config: Optional[RetryConfig] = None):
    """
    重试装饰器
    
    Usage:
        @retry_on_failure(RetryConfig(max_retries=3))
        def my_function():
            pass
    """
    manager = RetryManager(config)
    
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return manager.execute_with_retry(func, *args, **kwargs)
        return wrapper
    return decorator


# 全局实例
_retry_manager = RetryManager()
_circuit_breaker = CircuitBreaker()


def get_retry_manager() -> RetryManager:
    """获取重试管理器实例"""
    return _retry_manager


def get_circuit_breaker() -> CircuitBreaker:
    """获取熔断器实例"""
    return _circuit_breaker
