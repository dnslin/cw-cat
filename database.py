import sqlite3
import os
from typing import List, Tuple
from logger import setup_logger
from config import DB_NAME, LOG_PATH
import traceback
import json

logger = setup_logger("database", LOG_PATH)


class Database:
    def __init__(self, db_name: str = DB_NAME):
        self.db_name = db_name
        # 确保路径存在
        os.makedirs(os.path.dirname(self.db_name), exist_ok=True)
        logger.info(f"数据库路径: {self.db_name}")
        self.init_database()

    def init_database(self):
        """初始化数据库和表结构"""
        try:
            logger.info(f"初始化数据库: {self.db_name}")
            with sqlite3.connect(self.db_name) as conn:
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
                        detail_crawled INTEGER DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
                )

                # 创建书籍详情表 - 使用正确的扁平化结构
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS book_details (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        book_id INTEGER,
                        book_url TEXT UNIQUE,
                        title TEXT,
                        author TEXT,
                        author_id TEXT,
                        description TEXT,
                        last_update TEXT,
                        status TEXT,
                        tags TEXT,
                        
                        -- 统计信息字段
                        total_hits INTEGER,
                        total_favor INTEGER,
                        total_word INTEGER,
                        
                        -- 详细信息字段
                        total_recommend INTEGER,
                        week_hits INTEGER,
                        mouth_hits INTEGER,
                        week_recommend INTEGER,
                        mouth_recommend INTEGER,
                        book_type TEXT,
                        word_count TEXT,
                        chapter_count INTEGER,
                        first_publish_status TEXT,
                        
                        is_crawled INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (book_id) REFERENCES books (id)
                    )
                """
                )

                conn.commit()
            logger.info("数据库初始化成功")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                logger.info("列已存在，跳过添加")
            else:
                logger.error(f"初始化数据库失败: {str(e)}")
                logger.error(traceback.format_exc())
                raise
        except Exception as e:
            logger.error(f"初始化数据库失败: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def save_books(self, books_data: List[Tuple]):
        """保存书籍信息到数据库"""
        if not books_data:
            logger.warning("没有数据需要保存")
            return 0

        try:
            logger.info(f"准备保存 {len(books_data)} 条数据")
            if books_data:
                logger.debug(f"第一条数据: {books_data[0]}")

            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()

                # 先查询现有记录数
                cursor.execute("SELECT COUNT(*) FROM books")
                before_count = cursor.fetchone()[0]
                logger.debug(f"保存前数据库记录数: {before_count}")

                # 执行插入操作
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

                # 确保提交事务
                conn.commit()
                logger.debug("事务已提交")

                # 查询插入后的记录数
                cursor.execute("SELECT COUNT(*) FROM books")
                after_count = cursor.fetchone()[0]
                logger.debug(f"保存后数据库记录数: {after_count}")

                # 计算实际新增的记录数
                new_records = after_count - before_count

                logger.info(
                    f"成功保存 {len(books_data)} 条书籍信息，新增 {new_records} 条记录"
                )
                return new_records

        except Exception as e:
            logger.error(f"保存数据失败: {str(e)}")
            logger.error(traceback.format_exc())
            return 0

    def get_all_books(self):
        """获取所有书籍信息"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM books")
                books = cursor.fetchall()
                logger.debug(f"获取到 {len(books)} 条书籍信息")
                return books
        except Exception as e:
            logger.error(f"获取数据失败: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def get_all_book_names(self):
        """获取所有书名，用于去重"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT book_name FROM books")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"获取书名失败: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def get_uncrawled_books(self, limit=100):
        """获取未爬取详情的书籍"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT id, book_url FROM books 
                    WHERE detail_crawled = 0 
                    LIMIT ?
                    """,
                    (limit,),
                )
                books = cursor.fetchall()
                logger.debug(f"获取到 {len(books)} 条未爬取详情的书籍")
                return books
        except Exception as e:
            logger.error(f"获取未爬取书籍失败: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def save_book_detail(self, book_id, book_url, detail_data):
        """保存书籍详情信息，使用扁平化字段结构"""
        try:
            # 从detail_data中提取各个字段
            tags_json = json.dumps(detail_data.get("tags", []), ensure_ascii=False)

            # 提取stats字段的数据
            stats = detail_data.get("stats", {})
            total_hits = self._extract_number(stats.get("总点击", "0"))
            total_favor = self._extract_number(stats.get("总收藏", "0"))
            total_word = self._extract_number(stats.get("总字数", "0"))

            # 提取detail_stats字段的数据
            detail_stats = detail_data.get("detail_stats", {})
            total_recommend = self._extract_number(detail_stats.get("总推荐", "0"))
            week_hits = self._extract_number(detail_stats.get("周点击", "0"))
            mouth_hits = self._extract_number(detail_stats.get("月点击", "0"))
            week_recommend = self._extract_number(detail_stats.get("周推荐", "0"))
            mouth_recommend = self._extract_number(detail_stats.get("月点击", "0"))
            book_type = detail_stats.get("小说类别", "")
            word_count = detail_stats.get("完成字数", "")
            chapter_count = self._extract_number(detail_stats.get("章节", "0"))
            first_publish_status = detail_stats.get("首发状态", "")

            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()

                # 插入详情数据 - 修正字段顺序和数量
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO book_details (
                        book_id, book_url, title, author, author_id,
                        description, last_update, status, tags,
                        total_hits, total_favor, total_word,
                        total_recommend, week_hits, mouth_hits, 
                        week_recommend, mouth_recommend,
                        book_type, word_count, chapter_count, 
                        first_publish_status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        book_id,
                        book_url,
                        detail_data.get("title", ""),
                        detail_data.get("author", ""),
                        detail_data.get("author_id", ""),
                        detail_data.get("description", ""),
                        detail_data.get("last_update", ""),
                        detail_data.get("status", ""),
                        tags_json,
                        total_hits,
                        total_favor,
                        total_word,
                        total_recommend,
                        week_hits,
                        mouth_hits,
                        week_recommend,
                        mouth_recommend,
                        book_type,
                        word_count,
                        chapter_count,
                        first_publish_status,
                    ),
                )

                # 更新书籍表中的爬取状态
                cursor.execute(
                    """
                    UPDATE books SET detail_crawled = 1
                    WHERE id = ?
                    """,
                    (book_id,),
                )

                conn.commit()
                logger.info(f"成功保存书籍 {book_url} 的详情")
                return True
        except Exception as e:
            logger.error(f"保存书籍详情失败 {book_url}: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def _extract_number(self, text):
        """从字符串中提取数字"""
        if not text:
            return 0

        try:
            # 移除非数字字符，保留小数点
            text = "".join(c for c in text if c.isdigit() or c == ".")
            if text:
                if "." in text:
                    return float(text)
                return int(text)
            return 0
        except:
            return 0

    def is_detail_exists(self, book_url):
        """检查书籍详情是否已存在"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM book_details WHERE book_url = ?", (book_url,)
                )
                count = cursor.fetchone()[0]
                return count > 0
        except Exception as e:
            logger.error(f"检查书籍详情是否存在失败 {book_url}: {str(e)}")
            logger.error(traceback.format_exc())
            return False
