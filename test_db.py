import sqlite3
import os
from typing import List, Tuple
import random
from logger import setup_logger
from config import DB_NAME, LOG_PATH

# 设置日志
LOG_FILE = os.path.join(os.path.dirname(LOG_PATH), "test_db.log")
logger = setup_logger("test_db", LOG_FILE)


def init_database(db_name):
    """初始化数据库和表结构"""
    try:
        # 确保数据库目录存在
        os.makedirs(os.path.dirname(db_name), exist_ok=True)

        logger.info(f"初始化数据库: {db_name}")
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()

            # 创建书籍信息表
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category TEXT,
                    book_name TEXT,
                    book_url TEXT UNIQUE,
                    latest_chapter TEXT,
                    latest_chapter_url TEXT,
                    author TEXT,
                    author_url TEXT,
                    word_count TEXT,
                    update_time TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )
            conn.commit()
        logger.info(f"数据库初始化成功: {db_name}")
        return True
    except Exception as e:
        logger.error(f"初始化数据库失败: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        return False


def save_books(db_name, books_data: List[Tuple]):
    """保存书籍信息到数据库"""
    if not books_data:
        logger.warning("没有数据需要保存")
        return 0

    try:
        logger.info(f"正在保存 {len(books_data)} 条数据到 {db_name}")

        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()

            # 先查询现有记录数
            cursor.execute("SELECT COUNT(*) FROM books")
            before_count = cursor.fetchone()[0]
            logger.info(f"保存前数据库中有 {before_count} 条记录")

            # 打印数据示例
            if books_data:
                logger.info(f"数据示例：{books_data[0]}")

            # 插入数据
            cursor.executemany(
                """
                INSERT OR REPLACE INTO books (
                    category, book_name, book_url, latest_chapter,
                    latest_chapter_url, author, author_url,
                    word_count, update_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                books_data,
            )

            # 显式提交事务
            conn.commit()

            # 查询插入后的记录数
            cursor.execute("SELECT COUNT(*) FROM books")
            after_count = cursor.fetchone()[0]

            # 计算实际新增的记录数
            new_records = after_count - before_count

            logger.info(
                f"成功保存数据: 总共处理 {len(books_data)} 条，新增 {new_records} 条记录"
            )
            return new_records
    except Exception as e:
        logger.error(f"保存数据失败: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        return 0


def get_all_books(db_name):
    """获取所有书籍信息"""
    try:
        with sqlite3.connect(db_name) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM books")
            books = cursor.fetchall()
            logger.info(f"从数据库获取了 {len(books)} 条记录")
            return books
    except Exception as e:
        logger.error(f"获取数据失败: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        return []


def test_database():
    """测试数据库功能"""
    # 使用绝对路径确保能找到数据库文件
    db_name = DB_NAME
    logger.info(f"数据库路径: {db_name}")

    # 初始化数据库
    if not init_database(db_name):
        logger.error("数据库初始化失败，测试终止")
        return

    # 生成测试数据
    test_data = []
    for i in range(10):
        test_data.append(
            (
                f"测试分类{i}",  # category
                f"测试书名{i}",  # book_name
                f"https://test.com/book/{random.randint(10000, 99999)}",  # book_url (唯一)
                f"第{i}章",  # latest_chapter
                f"https://test.com/chapter/{i}",  # latest_chapter_url
                f"作者{i}",  # author
                f"https://test.com/author/{i}",  # author_url
                f"{random.randint(10, 100)}万字",  # word_count
                f"2025-03-{random.randint(1, 24)}",  # update_time
            )
        )

    # 获取当前记录数
    before_count = len(get_all_books(db_name))
    logger.info(f"测试前数据库中有 {before_count} 条记录")

    # 保存测试数据
    saved_count = save_books(db_name, test_data)
    logger.info(f"保存测试数据结果: 新增 {saved_count} 条记录")

    # 获取保存后的记录数
    after_count = len(get_all_books(db_name))
    logger.info(f"测试后数据库中有 {after_count} 条记录")

    # 验证记录是否增加
    if after_count > before_count:
        logger.info("✅ 测试通过: 成功将数据保存到数据库")
    else:
        logger.error("❌ 测试失败: 数据库记录数未增加")


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("开始测试数据库功能")
    test_database()
    logger.info("测试完成")
    logger.info("=" * 50)
