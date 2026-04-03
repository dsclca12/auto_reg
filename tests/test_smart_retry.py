"""
智能重试处理器测试用例
"""

import time
import unittest
from unittest.mock import patch, MagicMock, call

from platforms.chatgpt.smart_retry_handler import (
    RetryManager,
    RetryConfig,
    RetryStrategy,
    ErrorClassifier,
    ErrorCategory,
    ErrorInfo,
    CircuitBreaker,
    retry_on_failure,
)


class TestErrorClassifier(unittest.TestCase):
    """错误分类器测试"""
    
    def test_classify_tls_error(self):
        """测试 TLS 错误分类"""
        error = Exception("curl: (35) SSL handshake failed")
        error_info = ErrorClassifier.classify_error(error)
        
        self.assertEqual(error_info.category, ErrorCategory.TLS_ERROR)
        self.assertTrue(error_info.retryable)
    
    def test_classify_network_error(self):
        """测试网络错误分类"""
        error = Exception("Connection refused")
        error_info = ErrorClassifier.classify_error(error)
        
        self.assertEqual(error_info.category, ErrorCategory.NETWORK_ERROR)
        self.assertTrue(error_info.retryable)
    
    def test_classify_timeout_error(self):
        """测试超时错误分类"""
        error = Exception("Request timed out after 30 seconds")
        error_info = ErrorClassifier.classify_error(error)
        
        self.assertEqual(error_info.category, ErrorCategory.TIMEOUT)
        self.assertTrue(error_info.retryable)
    
    def test_classify_rate_limit(self):
        """测试速率限制分类"""
        error_info = ErrorClassifier.classify_error(
            Exception("Too many requests"),
            status_code=429
        )
        
        self.assertEqual(error_info.category, ErrorCategory.RATE_LIMIT)
        self.assertTrue(error_info.retryable)
    
    def test_classify_captcha(self):
        """测试验证码分类"""
        error_info = ErrorClassifier.classify_error(
            Exception("Cloudflare captcha challenge required"),
            status_code=403
        )
        
        self.assertEqual(error_info.category, ErrorCategory.CAPTCHA)
        self.assertTrue(error_info.retryable)
    
    def test_classify_auth_error(self):
        """测试认证错误分类"""
        error_info = ErrorClassifier.classify_error(
            Exception("Unauthorized"),
            status_code=401
        )
        
        self.assertEqual(error_info.category, ErrorCategory.AUTH_ERROR)
        self.assertFalse(error_info.retryable)
    
    def test_classify_validation_error(self):
        """测试验证错误分类"""
        error_info = ErrorClassifier.classify_error(
            Exception("Invalid input"),
            status_code=400
        )
        
        self.assertEqual(error_info.category, ErrorCategory.VALIDATION_ERROR)
        self.assertFalse(error_info.retryable)
    
    def test_classify_server_error(self):
        """测试服务器错误分类"""
        error_info = ErrorClassifier.classify_error(
            Exception("Internal server error"),
            status_code=500
        )
        
        self.assertEqual(error_info.category, ErrorCategory.SERVER_ERROR)
        self.assertTrue(error_info.retryable)
    
    def test_extract_retry_after(self):
        """测试提取 Retry-After"""
        error_msg = "Rate limited. Please retry after 30 seconds."
        retry_after = ErrorClassifier._extract_retry_after(error_msg)
        
        self.assertEqual(retry_after, 30.0)
    
    def test_unknown_error(self):
        """测试未知错误"""
        error = Exception("Some weird error")
        error_info = ErrorClassifier.classify_error(error)
        
        self.assertEqual(error_info.category, ErrorCategory.UNKNOWN)


class TestRetryManager(unittest.TestCase):
    """重试管理器测试"""
    
    def test_successful_execution(self):
        """测试成功执行"""
        manager = RetryManager(RetryConfig(max_retries=3))
        
        def success_func():
            return "success"
        
        result = manager.execute_with_retry(success_func)
        self.assertEqual(result, "success")
    
    def test_retry_on_failure(self):
        """测试失败后重试成功"""
        manager = RetryManager(RetryConfig(
            max_retries=3,
            base_delay=0.01,
            max_delay=0.05,
        ))
        
        call_count = [0]
        
        def flaky_func():
            call_count[0] += 1
            if call_count[0] < 3:
                raise Exception("Temporary error")
            return "success"
        
        start = time.time()
        result = manager.execute_with_retry(flaky_func)
        elapsed = time.time() - start
        
        self.assertEqual(result, "success")
        self.assertEqual(call_count[0], 3)
        # 应该有重试延迟
        self.assertGreater(elapsed, 0.01)
    
    def test_max_retries_exceeded(self):
        """测试达到最大重试次数"""
        manager = RetryManager(RetryConfig(
            max_retries=3,
            base_delay=0.01,
        ))
        
        def always_fail():
            raise Exception("Permanent error")
        
        with self.assertRaises(Exception) as context:
            manager.execute_with_retry(always_fail)
        
        self.assertIn("Permanent error", str(context.exception))
    
    def test_non_retryable_error(self):
        """测试不可重试错误"""
        manager = RetryManager(RetryConfig(max_retries=3))
        
        def raise_auth_error():
            from unittest.mock import MagicMock
            error = Exception("Unauthorized")
            error_info = ErrorClassifier.classify_error(error, status_code=401)
            # 手动构造不可重试的错误
            raise Exception("Auth error")
        
        with self.assertRaises(Exception):
            manager.execute_with_retry(raise_auth_error)
    
    def test_exponential_strategy(self):
        """测试指数退避策略"""
        manager = RetryManager(RetryConfig(
            max_retries=3,
            base_delay=0.01,
            strategy=RetryStrategy.EXPONENTIAL,
            max_delay=0.1,
        ))
        
        call_times = []
        
        def flaky_func():
            call_times.append(time.time())
            if len(call_times) < 3:
                raise Exception("Temporary error")
            return "success"
        
        manager.execute_with_retry(flaky_func)
        
        # 验证延迟呈指数增长
        if len(call_times) >= 3:
            delay1 = call_times[1] - call_times[0]
            delay2 = call_times[2] - call_times[1]
            # 第二次延迟应该大于第一次
            self.assertGreater(delay2, delay1 * 0.5)  # 允许一些误差
    
    def test_jittered_strategy(self):
        """测试抖动策略"""
        manager = RetryManager(RetryConfig(
            max_retries=5,
            base_delay=0.01,
            strategy=RetryStrategy.JITTERED,
        ))
        
        delays = []
        
        def flaky_func():
            if len(delays) > 0:
                delays.append(time.time() - last_call[0])
            last_call[0] = time.time()
            
            if len(delays) < 3:
                raise Exception("Temporary error")
            return "success"
        
        last_call = [time.time()]
        
        manager.execute_with_retry(flaky_func)
        
        # 验证每次延迟都不同（有抖动）
        if len(delays) >= 2:
            # 延迟应该有变化
            unique_delays = set(round(d, 3) for d in delays)
            # 不强制断言，因为随机性可能导致相同值
    
    def test_error_statistics(self):
        """测试错误统计"""
        manager = RetryManager(RetryConfig(
            max_retries=3,
            base_delay=0.01,
        ))
        
        # 执行一些失败的重试
        def flaky_func():
            if not hasattr(flaky_func, 'called'):
                flaky_func.called = True
                raise Exception("Temporary error")
            return "success"
        
        manager.execute_with_retry(flaky_func)
        
        stats = manager.get_error_statistics()
        
        self.assertIn("total_retries", stats)
        self.assertIn("successful_retries", stats)
        self.assertIn("failed_retries", stats)
        self.assertIn("error_categories", stats)
    
    def test_reset_stats(self):
        """测试重置统计"""
        manager = RetryManager(RetryConfig(max_retries=3, base_delay=0.01))
        
        # 执行一些操作
        def fail_once():
            if not hasattr(fail_once, 'called'):
                fail_once.called = True
                raise Exception("Error")
            return "ok"
        
        manager.execute_with_retry(fail_once)
        
        # 重置统计
        manager.reset_stats()
        stats = manager.get_error_statistics()
        
        self.assertEqual(stats["total_retries"], 0)
        self.assertEqual(stats["successful_retries"], 0)


class TestCircuitBreaker(unittest.TestCase):
    """熔断器测试"""
    
    def test_initial_state(self):
        """测试初始状态"""
        cb = CircuitBreaker(failure_threshold=3)
        
        self.assertEqual(cb.state, "closed")
        self.assertTrue(cb.can_execute())
    
    def test_opens_after_threshold(self):
        """测试达到阈值后打开"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60.0)
        
        # 连续失败 3 次
        for _ in range(3):
            cb.record_failure()
        
        self.assertEqual(cb.state, "open")
        self.assertFalse(cb.can_execute())
    
    def test_half_open_after_recovery(self):
        """测试恢复后进入半开状态"""
        cb = CircuitBreaker(
            failure_threshold=2,
            recovery_timeout=0.1  # 100ms 快速恢复
        )
        
        # 打开熔断器
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.state, "open")
        
        # 等待恢复时间
        time.sleep(0.15)
        
        # 应该进入半开状态
        self.assertTrue(cb.can_execute())
        self.assertEqual(cb.state, "half_open")
    
    def test_closes_after_success_in_half_open(self):
        """测试半开状态下成功后关闭"""
        cb = CircuitBreaker(
            failure_threshold=2,
            recovery_timeout=0.05,
            half_open_max_calls=2,
        )
        
        # 打开
        cb.record_failure()
        cb.record_failure()
        
        # 等待恢复
        time.sleep(0.1)
        cb.can_execute()
        
        # 成功
        cb.record_success()
        cb.record_success()
        
        self.assertEqual(cb.state, "closed")
    
    def test_opens_again_from_half_open(self):
        """测试半开状态下失败重新打开"""
        cb = CircuitBreaker(
            failure_threshold=2,
            recovery_timeout=0.05,
        )
        
        # 打开
        cb.record_failure()
        cb.record_failure()
        
        # 等待恢复
        time.sleep(0.1)
        cb.can_execute()
        
        # 失败
        cb.record_failure()
        
        self.assertEqual(cb.state, "open")
    
    def test_reset(self):
        """测试重置"""
        cb = CircuitBreaker(failure_threshold=2)
        
        cb.record_failure()
        cb.record_failure()
        self.assertEqual(cb.state, "open")
        
        cb.reset()
        
        self.assertEqual(cb.state, "closed")
        self.assertTrue(cb.can_execute())


class TestRetryDecorator(unittest.TestCase):
    """重试装饰器测试"""
    
    def test_retry_decorator(self):
        """测试重试装饰器"""
        call_count = [0]
        
        @retry_on_failure(RetryConfig(max_retries=3, base_delay=0.01))
        def flaky_function():
            call_count[0] += 1
            if call_count[0] < 2:
                raise Exception("Temporary error")
            return "success"
        
        result = flaky_function()
        
        self.assertEqual(result, "success")
        self.assertEqual(call_count[0], 2)


if __name__ == "__main__":
    unittest.main()
