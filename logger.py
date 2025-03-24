import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(name, log_path, level=logging.DEBUG):
    """设置logger"""
    # 确保日志目录存在
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 清除已有的处理器
    if logger.handlers:
        logger.handlers = []

    # 设置日志轮转
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding="utf-8",  # 明确指定UTF-8编码
    )

    # 设置日志格式 - 修正这里的错误
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)

    # 添加控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
