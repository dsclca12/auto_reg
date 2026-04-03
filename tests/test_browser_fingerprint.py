"""
浏览器指纹增强器测试用例
"""

import unittest
from unittest.mock import patch, MagicMock

from platforms.chatgpt.browser_fingerprint_enhancer import (
    BrowserFingerprintGenerator,
    BrowserFingerprintConfig,
    get_browser_fingerprint,
    inject_fingerprint_to_session,
)


class TestBrowserFingerprintGenerator(unittest.TestCase):
    """浏览器指纹生成器测试"""
    
    def setUp(self):
        """测试前准备"""
        self.generator = BrowserFingerprintGenerator()
    
    def test_generate_fingerprint(self):
        """测试生成指纹"""
        fingerprint = self.generator.generate()
        
        # 验证包含所有必要的指纹信息
        self.assertIn("screen", fingerprint)
        self.assertIn("locale", fingerprint)
        self.assertIn("hardware", fingerprint)
        self.assertIn("canvas", fingerprint)
        self.assertIn("webgl", fingerprint)
        self.assertIn("fonts", fingerprint)
        self.assertIn("audio", fingerprint)
        self.assertIn("webrtc", fingerprint)
        self.assertIn("third_party", fingerprint)
    
    def test_screen_info(self):
        """测试屏幕信息生成"""
        fingerprint = self.generator.generate()
        screen = fingerprint["screen"]
        
        self.assertIn("width", screen)
        self.assertIn("height", screen)
        self.assertIn("avail_width", screen)
        self.assertIn("avail_height", screen)
        self.assertIn("color_depth", screen)
        self.assertIn("device_pixel_ratio", screen)
        
        # 验证分辨率在预设列表中
        valid_resolutions = [
            (1920, 1080), (1366, 768), (1536, 864), (1440, 900),
            (1680, 1050), (1280, 720), (1600, 900), (1920, 1200),
            (2560, 1440), (3840, 2160),
        ]
        self.assertIn((screen["width"], screen["height"]), valid_resolutions)
    
    def test_locale_info(self):
        """测试时区和语言信息"""
        fingerprint = self.generator.generate()
        locale = fingerprint["locale"]
        
        self.assertIn("timezone", locale)
        self.assertIn("languages", locale)
        self.assertIn("language", locale)
        self.assertIn("utc_offset", locale)
        
        # 验证时区在预设列表中
        self.assertIn(locale["timezone"], self.generator.config.timezones)
        
        # 验证语言是列表
        self.assertIsInstance(locale["languages"], list)
        self.assertGreater(len(locale["languages"]), 0)
    
    def test_hardware_info(self):
        """测试硬件信息"""
        fingerprint = self.generator.generate()
        hardware = fingerprint["hardware"]
        
        self.assertIn("hardware_concurrency", hardware)
        self.assertIn("device_memory", hardware)
        self.assertIn("platform", hardware)
        self.assertIn("connection", hardware)
        
        # 验证 CPU 核心数在预设列表中
        self.assertIn(
            hardware["hardware_concurrency"],
            self.generator.config.hardware_concurrency
        )
    
    def test_canvas_fingerprint(self):
        """测试 Canvas 指纹"""
        fingerprint = self.generator.generate()
        canvas = fingerprint["canvas"]
        
        self.assertIn("hash", canvas)
        self.assertIn("data_url", canvas)
        self.assertIn("rendered_text", canvas)
        
        # 验证哈希长度
        self.assertEqual(len(canvas["hash"]), 32)  # SHA256 前 32 位
    
    def test_webgl_fingerprint(self):
        """测试 WebGL 指纹"""
        fingerprint = self.generator.generate()
        webgl = fingerprint["webgl"]
        
        self.assertIn("renderer", webgl)
        self.assertIn("vendor", webgl)
        self.assertIn("hash", webgl)
        self.assertIn("parameters", webgl)
        self.assertIn("extensions", webgl)
        
        # 验证渲染器在预设列表中
        self.assertIn(
            webgl["renderer"],
            self.generator.config.webgl_renderers
        )
        
        # 验证扩展是列表
        self.assertIsInstance(webgl["extensions"], list)
        self.assertGreater(len(webgl["extensions"]), 0)
    
    def test_fonts_detection(self):
        """测试字体检测"""
        fingerprint = self.generator.generate()
        fonts = fingerprint["fonts"]
        
        self.assertIn("installed_fonts", fonts)
        self.assertIn("font_count", fonts)
        self.assertIn("default_font", fonts)
        
        # 验证字体数量与列表长度匹配
        self.assertEqual(fonts["font_count"], len(fonts["installed_fonts"]))
        
        # 验证字体在预设列表中
        for font in fonts["installed_fonts"]:
            self.assertIn(font, self.generator.config.system_fonts)
    
    def test_audio_fingerprint(self):
        """测试音频指纹"""
        fingerprint = self.generator.generate()
        audio = fingerprint["audio"]
        
        self.assertIn("sample_rate", audio)
        self.assertIn("channel_count", audio)
        self.assertIn("hash", audio)
        self.assertIn("oscillator", audio)
        
        # 验证采样率是常见值
        self.assertIn(audio["sample_rate"], [44100, 48000])
    
    def test_webrtc_info(self):
        """测试 WebRTC 信息"""
        fingerprint = self.generator.generate()
        webrtc = fingerprint["webrtc"]
        
        self.assertIn("local_ip_addresses", webrtc)
        self.assertIn("has_webrtc", webrtc)
        self.assertIn("ice_servers", webrtc)
        
        # 验证有本地 IP
        self.assertIsInstance(webrtc["local_ip_addresses"], list)
        self.assertGreater(len(webrtc["local_ip_addresses"]), 0)
    
    def test_third_party_scripts(self):
        """测试第三方脚本"""
        fingerprint = self.generator.generate()
        third_party = fingerprint["third_party"]
        
        self.assertIn("google_analytics", third_party)
        self.assertIn("google_tag_manager", third_party)
        self.assertIn("facebook_pixel", third_party)
        
        # 验证 Google Analytics 结构
        ga = third_party["google_analytics"]
        self.assertIn("present", ga)
        self.assertIn("tracking_id", ga)
    
    def test_fingerprint_caching(self):
        """测试指纹缓存"""
        fingerprint1 = self.generator.generate()
        fingerprint2 = self.generator.generate()
        
        # 短时间内应该返回相同的指纹
        self.assertEqual(fingerprint1, fingerprint2)
    
    def test_force_regenerate(self):
        """测试强制重新生成"""
        fingerprint1 = self.generator.generate()
        fingerprint2 = self.generator.generate(force=True)
        
        # 强制重新生成应该返回不同的指纹
        self.assertNotEqual(fingerprint1, fingerprint2)
    
    def test_inject_to_session(self):
        """测试注入到 session"""
        # 创建 mock session
        mock_session = MagicMock()
        mock_session.headers = {}
        
        self.generator.inject_to_session(mock_session)
        
        # 验证 headers 被更新
        self.assertGreater(len(mock_session.headers), 0)
        self.assertIn("Sec-CH-UA-Platform", mock_session.headers)
    
    def test_custom_config(self):
        """测试自定义配置"""
        custom_config = BrowserFingerprintConfig(
            screen_resolutions=[(1920, 1080)],
            timezones=["America/New_York"],
        )
        generator = BrowserFingerprintGenerator(custom_config)
        
        fingerprint = generator.generate()
        
        # 验证使用自定义配置
        self.assertEqual(fingerprint["screen"]["width"], 1920)
        self.assertEqual(fingerprint["screen"]["height"], 1080)
        self.assertEqual(fingerprint["locale"]["timezone"], "America/New_York")
    
    def test_global_functions(self):
        """测试全局便捷函数"""
        fingerprint = get_browser_fingerprint()
        
        self.assertIsInstance(fingerprint, dict)
        self.assertIn("screen", fingerprint)
    
    def test_reset(self):
        """测试重置"""
        self.generator.generate()
        self.generator.reset()
        
        # 重置后缓存应该被清空
        self.assertIsNone(self.generator._fingerprint)
        self.assertEqual(self.generator._generated_at, 0.0)


if __name__ == "__main__":
    unittest.main()
