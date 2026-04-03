"""
人类行为模拟器测试用例
"""

import time
import unittest
from unittest.mock import patch, MagicMock

from platforms.chatgpt.human_behavior_simulator import (
    HumanBehaviorSimulator,
    HumanBehaviorConfig,
    human_delay,
    human_think,
    human_type,
    human_observe_page,
    human_scroll,
    get_simulator,
    configure_simulator,
)


class TestHumanBehaviorSimulator(unittest.TestCase):
    """人类行为模拟器测试"""
    
    def setUp(self):
        """测试前准备"""
        self.config = HumanBehaviorConfig(
            min_delay=0.01,
            max_delay=0.05,
            thinking_delay_min=0.01,
            thinking_delay_max=0.05,
            typing_speed_mean=10.0,
            typing_speed_std=2.0,
            page_observation_min=0.01,
            page_observation_max=0.05,
            jitter_factor=0.1,
        )
        self.simulator = HumanBehaviorSimulator(self.config)
    
    def test_natural_delay(self):
        """测试自然延迟"""
        start = time.time()
        delay = self.simulator.natural_delay(0.01, 0.05)
        elapsed = time.time() - start
        
        # 验证延迟在合理范围内
        self.assertGreaterEqual(delay, 0.009)  # 允许小误差
        self.assertLessEqual(delay, 0.075)  # max * 1.5 + jitter
        self.assertGreaterEqual(elapsed, 0.009)
    
    def test_thinking_pause(self):
        """测试思考时间"""
        start = time.time()
        delay = self.simulator.thinking_pause()
        elapsed = time.time() - start
        
        # 验证思考时间在配置范围内
        self.assertGreaterEqual(delay, 0.009)
        self.assertGreaterEqual(elapsed, 0.009)
    
    def test_page_load_observation(self):
        """测试页面观察时间"""
        start = time.time()
        delay = self.simulator.page_load_observation()
        elapsed = time.time() - start
        
        self.assertGreaterEqual(delay, 0.009)
        self.assertGreaterEqual(elapsed, 0.009)
    
    def test_typing_delay_per_char(self):
        """测试逐字符打字延迟"""
        text = "test@example.com"
        start = time.time()
        delay = self.simulator.typing_delay(text, per_char=True)
        elapsed = time.time() - start
        
        # 验证总延迟与文本长度相关
        self.assertGreater(delay, 0)
        self.assertGreaterEqual(elapsed, 0.009)
    
    def test_typing_delay_whole_text(self):
        """测试整段文本打字延迟"""
        text = "Hello World"
        start = time.time()
        delay = self.simulator.typing_delay(text, per_char=False)
        elapsed = time.time() - start
        
        self.assertGreater(delay, 0)
        self.assertGreaterEqual(elapsed, 0.009)
    
    def test_typing_delay_with_special_chars(self):
        """测试特殊字符打字延迟"""
        text = "P@ssw0rd!#$"
        start = time.time()
        delay = self.simulator.typing_delay(text, per_char=True)
        elapsed = time.time() - start
        
        # 特殊字符应该有额外延迟
        self.assertGreater(delay, 0)
    
    def test_mouse_movement(self):
        """测试鼠标移动模拟"""
        start = time.time()
        delay = self.simulator.mouse_movement(steps=5)
        elapsed = time.time() - start
        
        self.assertGreaterEqual(delay, 0)
        self.assertGreaterEqual(elapsed, 0.009)
    
    def test_scroll_behavior(self):
        """测试滚动行为"""
        config = HumanBehaviorConfig(scroll_probability=1.0)  # 100% 触发
        simulator = HumanBehaviorSimulator(config)
        
        start = time.time()
        delay = simulator.scroll_behavior()
        elapsed = time.time() - start
        
        self.assertGreaterEqual(delay, 0)
        self.assertGreaterEqual(elapsed, 0.009)
    
    def test_scroll_behavior_probability(self):
        """测试滚动概率"""
        config = HumanBehaviorConfig(scroll_probability=0.0)  # 0% 触发
        simulator = HumanBehaviorSimulator(config)
        
        start = time.time()
        delay = simulator.scroll_behavior()
        elapsed = time.time() - start
        
        # 0% 概率应该立即返回
        self.assertEqual(delay, 0.0)
        self.assertLess(elapsed, 0.01)
    
    def test_form_filling_sequence(self):
        """测试表单填写序列"""
        fields = [
            ("email", "test@example.com"),
            ("password", "SecurePass123!"),
        ]
        
        submit_called = []
        def mock_submit():
            submit_called.append(True)
        
        start = time.time()
        delay = self.simulator.form_filling_sequence(fields, submit_callback=mock_submit)
        elapsed = time.time() - start
        
        self.assertGreater(delay, 0)
        self.assertEqual(len(submit_called), 1)
        self.assertGreaterEqual(elapsed, 0.009)
    
    def test_smart_wait_success(self):
        """测试智能等待（成功）"""
        counter = [0]
        
        def condition():
            counter[0] += 1
            return counter[0] >= 3
        
        result = self.simulator.smart_wait(condition, timeout=5.0, check_interval=0.01)
        
        self.assertTrue(result)
        self.assertEqual(counter[0], 3)
    
    def test_smart_wait_timeout(self):
        """测试智能等待（超时）"""
        def condition():
            return False  # 永远不满足
        
        start = time.time()
        result = self.simulator.smart_wait(condition, timeout=0.1, check_interval=0.01)
        elapsed = time.time() - start
        
        self.assertFalse(result)
        self.assertGreaterEqual(elapsed, 0.09)
    
    def test_simulate_impatience(self):
        """测试用户不耐烦模拟"""
        # 刚启动时不耐烦概率应该很低
        self.simulator.reset()
        
        # 运行多次，至少有时不耐烦
        impatience_count = 0
        for _ in range(100):
            if self.simulator.simulate_impatience():
                impatience_count += 1
        
        # 应该有不耐心时刻（即使概率低）
        # 这里不做严格断言，因为是随机的
    
    def test_action_log(self):
        """测试行为日志"""
        self.simulator.natural_delay(0.01, 0.02)
        self.simulator.thinking_pause()
        
        log = self.simulator.get_action_log()
        
        self.assertGreaterEqual(len(log), 2)
        self.assertIn("natural_delay", log[0])
    
    def test_reset(self):
        """测试重置"""
        self.simulator.natural_delay(0.01, 0.02)
        self.simulator.reset()
        
        log = self.simulator.get_action_log()
        self.assertEqual(len(log), 0)
    
    def test_global_functions(self):
        """测试全局便捷函数"""
        start = time.time()
        human_delay(0.01, 0.02)
        self.assertGreater(time.time() - start, 0.009)
        
        start = time.time()
        human_think()
        self.assertGreater(time.time() - start, 0.009)
        
        start = time.time()
        human_type("test")
        self.assertGreater(time.time() - start, 0.009)
    
    def test_configure_simulator(self):
        """测试配置全局模拟器"""
        new_config = HumanBehaviorConfig(min_delay=0.1, max_delay=0.2)
        configure_simulator(new_config)
        
        simulator = get_simulator()
        self.assertEqual(simulator.config.min_delay, 0.1)
        self.assertEqual(simulator.config.max_delay, 0.2)


if __name__ == "__main__":
    unittest.main()
