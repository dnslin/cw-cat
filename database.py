import sqlite3
from typing import List, Tuple
import logging
from logger import setup_logger
from config import DB_NAME, LOG_PATH

logger = setup_logger('database', LOG_PATH)

class Database:
    def __init__(self, db_name: str = DB_NAME):
        self.db_name = db_name
        self.init_database()

    def init_database(self):
        """初始化数据库和表结构"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                
                # 创建书籍信息表
                cursor.execute('''
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
                ''')
                conn.commit()
            logger.info("数据库初始化成功")
        except Exception as e:
            logger.error(f"初始化数据库失败: {str(e)}")
            raise

    def save_books(self, books_data: List[Tuple]):
        """保存书籍信息到数据库"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                
                cursor.executemany('''
                    INSERT OR REPLACE INTO books (
                        category, book_name, book_url, latest_chapter,
                        latest_chapter_url, author, author_url,
                        word_count, update_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', books_data)
                
                conn.commit()
                logger.info(f"成功保存 {len(books_data)} 条书籍信息")
                return cursor.rowcount
        except Exception as e:
            logger.error(f"保存数据失败: {str(e)}")
            raise

    def get_all_books(self):
        """获取所有书籍信息"""
        try:
            with sqlite3.connect(self.db_name) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM books')
                logger.info("成功获取所有书籍信息")
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"获取数据失败: {str(e)}")
            raise 