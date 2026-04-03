"""
浏览器指纹增强模块
提供更真实的浏览器环境模拟，包括：
- Canvas/WebGL 指纹
- 字体检测
- 屏幕分辨率和色彩深度
- 时区和语言设置
- 硬件并发数和设备内存
- WebRTC 泄露模拟
- 第三方脚本模拟（Google Analytics 等）
"""

import random
import time
import hashlib
import base64
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field


@dataclass
class BrowserFingerprintConfig:
    """浏览器指纹配置"""
    
    # 屏幕分辨率（常见 Windows 分辨率）
    screen_resolutions: List[Tuple[int, int]] = field(default_factory=lambda: [
        (1920, 1080), (1366, 768), (1536, 864), (1440, 900),
        (1680, 1050), (1280, 720), (1600, 900), (1920, 1200),
        (2560, 1440), (3840, 2160),  # 4K
    ])
    
    # 色彩深度
    color_depths: List[int] = field(default_factory=lambda: [24, 30, 32])
    
    # 像素比
    device_pixel_ratios: List[float] = field(default_factory=lambda: [1.0, 1.25, 1.5, 1.75, 2.0])
    
    # 时区（常见英语时区）
    timezones: List[str] = field(default_factory=lambda: [
        "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles",
        "Europe/London", "Europe/Berlin", "Europe/Paris",
        "Asia/Tokyo", "Asia/Shanghai", "Asia/Hong_Kong",
        "Australia/Sydney", "Pacific/Auckland",
    ])
    
    # 语言设置
    languages: List[List[str]] = field(default_factory=lambda: [
        ["en-US", "en"],
        ["en-US", "en", "zh-CN"],
        ["en", "en-US"],
        ["en-GB", "en"],
        ["en-CA", "en", "fr-CA"],
        ["en-AU", "en"],
    ])
    
    # 硬件并发数（CPU 核心数）
    hardware_concurrency: List[int] = field(default_factory=lambda: [2, 4, 6, 8, 12, 16])
    
    # 设备内存（GB）
    device_memory: List[float] = field(default_factory=lambda: [2, 4, 6, 8, 16])
    
    # 字体列表（常见 Windows 字体）
    system_fonts: List[str] = field(default_factory=lambda: [
        "Arial", "Arial Black", "Calibri", "Cambria", "Comic Sans MS",
        "Consolas", "Courier New", "Georgia", "Impact", "Lucida Console",
        "Microsoft Sans Serif", "Microsoft YaHei", "Segoe UI", "Tahoma",
        "Times New Roman", "Trebuchet MS", "Verdana",
    ])
    
    # WebGL 渲染器（常见显卡）
    webgl_renderers: List[str] = field(default_factory=lambda: [
        "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0)",
        "ANGLE (NVIDIA, NVIDIA GeForce RTX 3070 Direct3D11 vs_5_0 ps_5_0)",
        "ANGLE (NVIDIA, NVIDIA GeForce RTX 4060 Direct3D11 vs_5_0 ps_5_0)",
        "ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0)",
        "ANGLE (Intel, Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0)",
        "ANGLE (AMD, AMD Radeon RX 6600 XT Direct3D11 vs_5_0 ps_5_0)",
        "ANGLE (AMD, AMD Radeon RX 6700 XT Direct3D11 vs_5_0 ps_5_0)",
        "ANGLE (Microsoft Basic Render Driver Direct3D11 vs_5_0 ps_5_0)",
    ])
    
    # WebGL 供应商
    webgl_vendors: List[str] = field(default_factory=lambda: [
        "Google Inc. (NVIDIA)",
        "Google Inc. (Intel)",
        "Google Inc. (AMD)",
        "Microsoft Corporation",
    ])
    
    # 平台
    platform: str = "Win32"
    
    # 架构
    architectures: List[str] = field(default_factory=lambda: ["x86", "x86_64"])
    
    # 是否启用触摸
    max_touch_points: int = 0  # 桌面端通常为 0
    
    # 连接类型
    connection_types: List[str] = field(default_factory=lambda: ["4g", "wifi"])
    
    # 有效带宽（kbps）
    effective_bandwidths: List[float] = field(default_factory=lambda: [
        10000.0, 50000.0, 100000.0, 500000.0, 1000000.0
    ])


class BrowserFingerprintGenerator:
    """浏览器指纹生成器"""
    
    def __init__(self, config: Optional[BrowserFingerprintConfig] = None):
        self.config = config or BrowserFingerprintConfig()
        self._fingerprint: Optional[Dict] = None
        self._generated_at: float = 0.0
    
    def generate(self, force: bool = False) -> Dict:
        """
        生成完整的浏览器指纹
        
        Args:
            force: 是否强制重新生成
        
        Returns:
            浏览器指纹字典
        """
        # 缓存指纹（5 分钟内不重复生成）
        if (not force and self._fingerprint and 
            time.time() - self._generated_at < 300):
            return self._fingerprint.copy()
        
        fingerprint = {
            # 屏幕信息
            "screen": self._generate_screen_info(),
            
            # 时区和语言
            "locale": self._generate_locale(),
            
            # 硬件信息
            "hardware": self._generate_hardware(),
            
            # Canvas 指纹
            "canvas": self._generate_canvas_fingerprint(),
            
            # WebGL 指纹
            "webgl": self._generate_webgl_fingerprint(),
            
            # 字体检测
            "fonts": self._generate_fonts(),
            
            # 音频指纹
            "audio": self._generate_audio_fingerprint(),
            
            # WebRTC 信息
            "webrtc": self._generate_webrtc_info(),
            
            # 第三方脚本标识
            "third_party": self._generate_third_party_scripts(),
        }
        
        self._fingerprint = fingerprint
        self._generated_at = time.time()
        
        return fingerprint.copy()
    
    def _generate_screen_info(self) -> Dict:
        """生成屏幕信息"""
        width, height = random.choice(self.config.screen_resolutions)
        color_depth = random.choice(self.config.color_depths)
        pixel_ratio = random.choice(self.config.device_pixel_ratios)
        
        return {
            "width": width,
            "height": height,
            "avail_width": width - random.randint(0, 20),  # 任务栏占用
            "avail_height": height - random.randint(40, 100),
            "color_depth": color_depth,
            "pixel_depth": color_depth,
            "device_pixel_ratio": pixel_ratio,
            "orientation": "landscape-primary" if width > height else "portrait-primary",
        }
    
    def _generate_locale(self) -> Dict:
        """生成时区和语言信息"""
        timezone = random.choice(self.config.timezones)
        languages = random.choice(self.config.languages)
        
        # 根据时区计算 UTC 偏移
        utc_offset = self._calculate_utc_offset(timezone)
        
        return {
            "timezone": timezone,
            "utc_offset": utc_offset,
            "languages": languages,
            "language": languages[0],
        }
    
    def _calculate_utc_offset(self, timezone: str) -> str:
        """计算 UTC 偏移（简化版，不依赖 pytz）"""
        # 常见时区的 UTC 偏移映射
        timezone_offsets = {
            "America/New_York": "-05:00",
            "America/Chicago": "-06:00",
            "America/Denver": "-07:00",
            "America/Los_Angeles": "-08:00",
            "Europe/London": "+00:00",
            "Europe/Berlin": "+01:00",
            "Europe/Paris": "+01:00",
            "Asia/Tokyo": "+09:00",
            "Asia/Shanghai": "+08:00",
            "Asia/Hong_Kong": "+08:00",
            "Australia/Sydney": "+11:00",
            "Pacific/Auckland": "+13:00",
        }
        
        return timezone_offsets.get(timezone, "+00:00")
    
    def _generate_hardware(self) -> Dict:
        """生成硬件信息"""
        return {
            "platform": self.config.platform,
            "hardware_concurrency": random.choice(self.config.hardware_concurrency),
            "device_memory": random.choice(self.config.device_memory),
            "max_touch_points": self.config.max_touch_points,
            "architecture": random.choice(self.config.architectures),
            "connection": {
                "type": random.choice(self.config.connection_types),
                "effective_type": "4g",
                "downlink": random.choice(self.config.effective_bandwidths) / 1000,
                "rtt": random.randint(20, 100),
            }
        }
    
    def _generate_canvas_fingerprint(self) -> Dict:
        """
        生成 Canvas 指纹
        模拟真实 Canvas 渲染的哈希值
        """
        # 模拟 Canvas 渲染内容
        canvas_data = (
            f"canvas_{random.randint(100000, 999999)}"
            f"_{time.time()}"
            f"_{random.random()}"
        )
        
        # 生成哈希
        canvas_hash = hashlib.sha256(canvas_data.encode()).hexdigest()[:32]
        
        return {
            "hash": canvas_hash,
            "data_url": f"data:image/png;base64,{base64.b64encode(canvas_data.encode()).decode()[:50]}...",
            "rendered_text": self._generate_canvas_text_rendering(),
        }
    
    def _generate_canvas_text_rendering(self) -> Dict:
        """模拟 Canvas 文本渲染信息"""
        return {
            "text_metrics": {
                "width": round(random.uniform(100.0, 200.0), 2),
                "height": round(random.uniform(10.0, 20.0), 2),
                "actual_bounding_box": {
                    "width": round(random.uniform(100.0, 200.0), 2),
                    "height": round(random.uniform(12.0, 18.0), 2),
                }
            },
            "anti_aliasing": "subpixel",
            "font_smoothing": True,
        }
    
    def _generate_webgl_fingerprint(self) -> Dict:
        """生成 WebGL 指纹"""
        renderer = random.choice(self.config.webgl_renderers)
        vendor = random.choice(self.config.webgl_vendors)
        
        # 生成唯一的 WebGL 哈希
        webgl_data = f"{renderer}_{vendor}_{time.time()}"
        webgl_hash = hashlib.sha256(webgl_data.encode()).hexdigest()[:32]
        
        return {
            "renderer": renderer,
            "vendor": vendor,
            "hash": webgl_hash,
            "parameters": self._generate_webgl_parameters(),
            "extensions": self._generate_webgl_extensions(),
        }
    
    def _generate_webgl_parameters(self) -> Dict:
        """生成 WebGL 参数"""
        return {
            "ALIASED_LINE_WIDTH_RANGE": [1, 1024],
            "ALIASED_POINT_SIZE_RANGE": [1, 1024],
            "ALPHA_BITS": 8,
            "BLUE_BITS": 8,
            "DEPTH_BITS": 24,
            "GREEN_BITS": 8,
            "MAX_COMBINED_TEXTURE_IMAGE_UNITS": 32,
            "MAX_CUBE_MAP_TEXTURE_SIZE": 16384,
            "MAX_FRAGMENT_UNIFORM_VECTORS": 1024,
            "MAX_RENDERBUFFER_SIZE": 16384,
            "MAX_TEXTURE_IMAGE_UNITS": 16,
            "MAX_TEXTURE_SIZE": 16384,
            "MAX_VARYING_VECTORS": 30,
            "MAX_VERTEX_ATTRIBS": 16,
            "MAX_VERTEX_TEXTURE_IMAGE_UNITS": 16,
            "MAX_VERTEX_UNIFORM_VECTORS": 4096,
            "MAX_VIEWPORT_DIMS": [32767, 32767],
            "RED_BITS": 8,
            "RENDERER": "WebGL",
            "SHADING_LANGUAGE_VERSION": "WebGL GLSL ES 3.0",
            "STENCIL_BITS": 0,
            "VERSION": "WebGL 2.0",
        }
    
    def _generate_webgl_extensions(self) -> List[str]:
        """生成 WebGL 扩展列表"""
        common_extensions = [
            "ANGLE_instanced_arrays",
            "EXT_blend_minmax",
            "EXT_color_buffer_half_float",
            "EXT_disjoint_timer_query",
            "EXT_float_blend",
            "EXT_frag_depth",
            "EXT_shader_texture_lod",
            "EXT_texture_compression_bptc",
            "EXT_texture_compression_rgtc",
            "EXT_texture_filter_anisotropic",
            "KHR_parallel_shader_compile",
            "OES_element_index_uint",
            "OES_standard_derivatives",
            "OES_texture_float",
            "OES_texture_float_linear",
            "WEBGL_color_buffer_float",
            "WEBGL_compressed_texture_astc",
            "WEBGL_compressed_texture_etc",
            "WEBGL_compressed_texture_s3tc",
            "WEBGL_debug_renderer_info",
            "WEBGL_depth_texture",
            "WEBGL_draw_buffers",
            "WEBGL_lose_context",
        ]
        
        # 随机选择 70%-90% 的扩展
        count = int(len(common_extensions) * random.uniform(0.7, 0.9))
        return random.sample(common_extensions, count)
    
    def _generate_fonts(self) -> Dict:
        """生成字体检测信息"""
        # 随机选择系统字体（通常是全部）
        installed_fonts = random.sample(
            self.config.system_fonts,
            k=random.randint(
                int(len(self.config.system_fonts) * 0.8),
                len(self.config.system_fonts)
            )
        )
        
        return {
            "installed_fonts": sorted(installed_fonts),
            "font_count": len(installed_fonts),
            "default_font": random.choice(["Arial", "Segoe UI", "Calibri"]),
        }
    
    def _generate_audio_fingerprint(self) -> Dict:
        """
        生成音频指纹
        模拟 AudioContext 的细微差异
        """
        # 模拟音频处理的微小差异
        sample_rate = random.choice([44100, 48000])
        
        return {
            "sample_rate": sample_rate,
            "channel_count": 2,
            "hash": hashlib.md5(
                f"audio_{sample_rate}_{time.time()}".encode()
            ).hexdigest()[:16],
            "oscillator": {
                "frequency": 440.0,
                "amplitude": round(random.uniform(0.999, 1.001), 6),
            }
        }
    
    def _generate_webrtc_info(self) -> Dict:
        """生成 WebRTC 信息"""
        return {
            "local_ip_addresses": [
                f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}"
            ],
            "has_webrtc": True,
            "ice_servers": [
                "stun:stun.l.google.com:19302",
                "stun:stun1.l.google.com:19302",
            ],
        }
    
    def _generate_third_party_scripts(self) -> Dict:
        """模拟第三方脚本标识"""
        return {
            "google_analytics": {
                "present": random.random() < 0.7,  # 70% 概率加载
                "tracking_id": f"UA-{random.randint(100000000, 999999999)}-1" if random.random() < 0.7 else None,
            },
            "google_tag_manager": {
                "present": random.random() < 0.5,
            },
            "facebook_pixel": {
                "present": random.random() < 0.3,
            },
        }
    
    def inject_to_session(self, session, fingerprint: Optional[Dict] = None):
        """
        将浏览器指纹注入到 HTTP session 的 headers 中
        
        Args:
            session: curl_cffi 的 Session 对象
            fingerprint: 可选的指纹字典，如果为 None 则自动生成
        """
        if fingerprint is None:
            fingerprint = self.generate()
        
        # 注意：这里只注入可以通过 HTTP headers 传递的信息
        # 实际的浏览器指纹需要在 JavaScript 环境中检测
        
        # 可以在这里添加自定义 headers
        extra_headers = {
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-CH-UA-Platform-Version": f'"{random.randint(10, 15)}.0.0"',
            "Sec-CH-UA-Mobile": "?0",
        }
        
        # 更新 session headers
        if hasattr(session, 'headers'):
            session.headers.update(extra_headers)
    
    def reset(self):
        """重置指纹缓存，强制下次重新生成"""
        self._fingerprint = None
        self._generated_at = 0.0


# 便捷函数
_global_fingerprint_gen = BrowserFingerprintGenerator()


def get_browser_fingerprint(force: bool = False) -> Dict:
    """获取浏览器指纹"""
    return _global_fingerprint_gen.generate(force)


def inject_fingerprint_to_session(session, fingerprint: Optional[Dict] = None):
    """将指纹注入到 session"""
    _global_fingerprint_gen.inject_to_session(session, fingerprint)


def get_fingerprint_generator() -> BrowserFingerprintGenerator:
    """获取指纹生成器实例"""
    return _global_fingerprint_gen
