"""
人类行为模拟增强模块
提供更真实的浏览器操作行为模拟，包括：
- 自然的时间延迟和节奏变化
- 鼠标移动、滚动、点击轨迹
- 打字节奏和输入行为
- 页面停留和浏览轨迹
- 更智能的等待和重试策略
"""

import random
import time
import math
from typing import Optional, Callable, List, Tuple
from dataclasses import dataclass, field


@dataclass
class HumanBehaviorConfig:
    """人类行为模拟配置"""
    
    # 基础延迟范围（秒）
    min_delay: float = 0.1
    max_delay: float = 0.8
    
    # 思考时间范围（秒）- 用户在关键操作前的停顿
    thinking_delay_min: float = 1.0
    thinking_delay_max: float = 5.0
    
    # 打字速度（每秒字符数，符合正态分布）
    typing_speed_mean: float = 5.0  # 平均每秒 5 个字符
    typing_speed_std: float = 1.5   # 标准差
    
    # 鼠标移动延迟（毫秒）
    mouse_move_delay_min: float = 0.01
    mouse_move_delay_max: float = 0.05
    
    # 页面加载后的观察时间（秒）
    page_observation_min: float = 1.5
    page_observation_max: float = 4.0
    
    # 滚动行为
    scroll_probability: float = 0.7  # 70% 概率会滚动
    scroll_steps_min: int = 2
    scroll_steps_max: int = 8
    
    # 错误和重试
    error_simulation_probability: float = 0.05  # 5% 概率模拟输入错误
    retry_delay_base: float = 2.0
    retry_delay_max: float = 10.0
    
    # 随机扰动因子（让行为更自然）
    jitter_factor: float = 0.3
    
    # 是否启用 headed 模式增强
    headed_mode: bool = False


class HumanBehaviorSimulator:
    """人类行为模拟器"""
    
    def __init__(self, config: Optional[HumanBehaviorConfig] = None):
        self.config = config or HumanBehaviorConfig()
        self._action_log: List[str] = []
        self._session_start = time.time()
    
    def _log_action(self, action: str):
        """记录行为日志"""
        elapsed = time.time() - self._session_start
        self._action_log.append(f"[{elapsed:.2f}s] {action}")
    
    def natural_delay(self, low: Optional[float] = None, high: Optional[float] = None) -> float:
        """
        生成自然的随机延迟
        使用指数分布 + 均匀分布的混合，模拟人类操作节奏
        """
        low = low if low is not None else self.config.min_delay
        high = high if high is not None else self.config.max_delay
        
        # 70% 使用均匀分布，30% 使用指数分布（模拟偶然的长时间停顿）
        if random.random() < 0.7:
            delay = random.uniform(low, high)
        else:
            # 指数分布，模拟"思考时间"
            scale = (high - low) / 2
            delay = low + random.expovariate(1.0 / scale)
            delay = min(delay, high * 1.5)  # 设置上限
        
        # 添加微小扰动
        jitter = delay * random.uniform(-self.config.jitter_factor, self.config.jitter_factor)
        delay = max(low, delay + jitter)
        
        self._log_action(f"natural_delay: {delay:.3f}s")
        time.sleep(delay)
        return delay
    
    def thinking_pause(self) -> float:
        """
        模拟用户思考时间
        在关键操作前（如点击提交、填写重要字段）使用
        """
        delay = random.uniform(
            self.config.thinking_delay_min,
            self.config.thinking_delay_max
        )
        
        # 添加偶尔的长时间停顿（用户可能去忙别的了）
        if random.random() < 0.15:
            delay *= random.uniform(1.5, 3.0)
        
        self._log_action(f"thinking_pause: {delay:.3f}s")
        time.sleep(delay)
        return delay
    
    def page_load_observation(self) -> float:
        """
        页面加载后的观察时间
        模拟用户浏览页面、等待加载完成的行为
        """
        delay = random.uniform(
            self.config.page_observation_min,
            self.config.page_observation_max
        )
        
        # 模拟用户可能会快速扫视或仔细阅读
        if random.random() < 0.3:
            delay *= 0.5  # 快速浏览
        elif random.random() < 0.2:
            delay *= 2.0  # 仔细阅读
        
        self._log_action(f"page_load_observation: {delay:.3f}s")
        time.sleep(delay)
        return delay
    
    def typing_delay(self, text: str, per_char: bool = True) -> float:
        """
        模拟打字节奏
        
        Args:
            text: 要输入的文本
            per_char: 是否逐字符延迟（True）或整段延迟（False）
        
        Returns:
            总延迟时间
        """
        if not text:
            return 0.0
        
        total_delay = 0.0
        
        if per_char:
            for i, char in enumerate(text):
                # 打字速度符合正态分布
                speed = max(1.0, random.gauss(
                    self.config.typing_speed_mean,
                    self.config.typing_speed_std
                ))
                
                # 单个字符延迟
                char_delay = 1.0 / speed
                
                # 特殊字符可能需要更长时间
                if char in '!@#$%^&*()_+-=[]{}|;:,.<>?':
                    char_delay *= random.uniform(1.5, 2.5)
                
                # 单词之间的停顿（空格后）
                if char == ' ' and i > 0:
                    char_delay += random.uniform(0.2, 0.6)
                
                # 大写字母可能需要 shift 键
                if char.isupper() and (i == 0 or text[i-1] == ' '):
                    char_delay *= random.uniform(1.2, 1.8)
                
                # 模拟偶尔的打字错误和修正
                if random.random() < self.config.error_simulation_probability:
                    # 删除并重输
                    error_delay = random.uniform(0.3, 0.8)
                    total_delay += error_delay
                    self._log_action(f"typing error correction: {error_delay:.3f}s")
                
                total_delay += char_delay
                time.sleep(char_delay)
        else:
            # 整段文本一次性延迟
            avg_speed = random.gauss(
                self.config.typing_speed_mean,
                self.config.typing_speed_std
            )
            total_delay = len(text) / max(1.0, avg_speed)
            time.sleep(total_delay)
        
        self._log_action(f"typing_delay: '{text[:20]}...' {total_delay:.3f}s")
        return total_delay
    
    def mouse_movement(self, steps: int = 5) -> float:
        """
        模拟鼠标移动轨迹
        
        Args:
            steps: 移动步数
        
        Returns:
            总延迟时间
        """
        total_delay = 0.0
        
        for _ in range(steps):
            delay = random.uniform(
                self.config.mouse_move_delay_min,
                self.config.mouse_move_delay_max
            )
            total_delay += delay
            time.sleep(delay)
        
        self._log_action(f"mouse_movement: {steps} steps, {total_delay:.3f}s")
        return total_delay
    
    def scroll_behavior(self) -> float:
        """
        模拟页面滚动行为
        
        Returns:
            总延迟时间
        """
        if random.random() > self.config.scroll_probability:
            return 0.0
        
        steps = random.randint(
            self.config.scroll_steps_min,
            self.config.scroll_steps_max
        )
        
        total_delay = 0.0
        
        for _ in range(steps):
            # 每次滚动的延迟
            delay = random.uniform(0.05, 0.2)
            total_delay += delay
            time.sleep(delay)
            
            # 偶尔停顿（用户在阅读内容）
            if random.random() < 0.3:
                pause = random.uniform(0.5, 2.0)
                total_delay += pause
                time.sleep(pause)
        
        self._log_action(f"scroll_behavior: {steps} steps, {total_delay:.3f}s")
        return total_delay
    
    def form_filling_sequence(
        self,
        fields: List[Tuple[str, str]],
        submit_callback: Optional[Callable] = None
    ) -> float:
        """
        模拟完整的表单填写流程
        
        Args:
            fields: 列表，每项为 (字段名, 字段值)
            submit_callback: 提交表单的回调函数
        
        Returns:
            总延迟时间
        """
        total_delay = 0.0
        
        # 页面加载后先观察
        total_delay += self.page_load_observation()
        
        # 偶尔先滚动页面再填写
        if random.random() < 0.4:
            total_delay += self.scroll_behavior()
            total_delay += self.natural_delay(0.5, 1.5)
        
        for field_name, field_value in fields:
            # 聚焦到字段前的延迟
            total_delay += self.natural_delay(0.2, 0.8)
            
            # 模拟鼠标点击输入框
            total_delay += self.mouse_movement(random.randint(2, 4))
            
            # 打字
            total_delay += self.typing_delay(field_value)
            
            # 字段之间的停顿
            total_delay += self.natural_delay(0.3, 1.2)
            
            self._log_action(f"filled field: {field_name}")
        
        # 提交前的思考时间
        total_delay += self.thinking_pause()
        
        # 执行提交
        if submit_callback:
            submit_callback()
            self._log_action("form submitted")
        
        return total_delay
    
    def smart_wait(
        self,
        condition: Callable[[], bool],
        timeout: float = 30.0,
        check_interval: float = 0.5
    ) -> bool:
        """
        智能等待，模拟用户的耐心程度
        
        Args:
            condition: 等待条件
            timeout: 超时时间（秒）
            check_interval: 检查间隔
        
        Returns:
            是否在超时前满足条件
        """
        start_time = time.time()
        check_count = 0
        
        while time.time() - start_time < timeout:
            if condition():
                elapsed = time.time() - start_time
                self._log_action(f"smart_wait: condition met in {elapsed:.2f}s")
                return True
            
            check_count += 1
            
            # 逐渐增加检查间隔（模拟用户越来越不耐烦）
            progress = (time.time() - start_time) / timeout
            adaptive_interval = check_interval * (1 + progress * 2)
            
            # 添加随机扰动
            adaptive_interval *= random.uniform(0.8, 1.2)
            
            time.sleep(adaptive_interval)
        
        self._log_action(f"smart_wait: timeout after {timeout:.2f}s ({check_count} checks)")
        return False
    
    def simulate_impatience(self) -> bool:
        """
        模拟用户不耐烦行为
        返回是否应该重新加载页面或重试
        
        Returns:
            True 表示用户不耐烦，需要重试
        """
        # 随着时间推移，不耐烦的概率增加
        elapsed = time.time() - self._session_start
        impatience_probability = min(0.3, elapsed / 300.0)  # 5 分钟后最高 30%
        
        if random.random() < impatience_probability:
            self._log_action("user_imp impatience detected")
            return True
        
        return False
    
    def get_action_log(self) -> List[str]:
        """获取行为日志"""
        return self._action_log.copy()
    
    def reset(self):
        """重置模拟器状态"""
        self._action_log.clear()
        self._session_start = time.time()


# 便捷函数，用于在现有代码中快速添加人类行为模拟

def human_delay(low: float = 0.1, high: float = 0.8) -> float:
    """便捷函数：自然延迟"""
    return _global_simulator.natural_delay(low, high)


def human_think() -> float:
    """便捷函数：思考时间"""
    return _global_simulator.thinking_pause()


def human_type(text: str, per_char: bool = True) -> float:
    """便捷函数：打字延迟"""
    return _global_simulator.typing_delay(text, per_char)


def human_observe_page() -> float:
    """便捷函数：页面观察时间"""
    return _global_simulator.page_load_observation()


def human_scroll() -> float:
    """便捷函数：滚动行为"""
    return _global_simulator.scroll_behavior()


# 全局模拟器实例
_global_simulator = HumanBehaviorSimulator()


def get_simulator() -> HumanBehaviorSimulator:
    """获取全局模拟器实例"""
    return _global_simulator


def configure_simulator(config: HumanBehaviorConfig):
    """配置全局模拟器"""
    global _global_simulator
    _global_simulator = HumanBehaviorSimulator(config)
