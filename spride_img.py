import sqlite3
import requests
from concurrent.futures import ThreadPoolExecutor
import random
import time
from typing import List, Dict
import logging
import re
from config import DB_NAME, PROXIES

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ProxyPool:
    def __init__(self, proxies=None):
        self.proxies = proxies or [
            # 默认代理列表，将被config中的代理覆盖
            "http://proxy1.example.com:8080",
            "http://proxy2.example.com:8080",
        ]
        self.current_index = 0

    def get_proxy(self) -> Dict[str, str]:
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        return {"http": proxy, "https": proxy}

class BookImageCrawler:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.proxy_pool = ProxyPool(PROXIES)
        self.session = requests.Session()
        
        # 设置请求头，模拟浏览器
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
        })

    def get_books_without_image(self) -> List[Dict]:
        """获取没有封面的图书信息"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, book_url, book_name 
            FROM books 
            WHERE book_image IS NULL OR book_image = ''
        """)
        books = [{"id": row[0], "book_url": row[1], "book_name": row[2]} 
                for row in cursor.fetchall()]
        conn.close()
        return books

    def extract_image_url(self, html_content: str, book_url: str) -> str:
        """从HTML中提取图片URL"""
        # 根据用户提供的实际HTML结构调整正则表达式
        img_patterns = [
            # 用户提供的特定格式
            r'<div class="cover ly-fl">\s*<img src="(.*?)"',
            # 通用模式作为备选
            r'<img.*?src="(.*?)".*?alt="[^"]*(?:' + re.escape(book_url.split('/')[-1]) + '|[^"]*)"',
            r'<img.*?src="(.*?)".*?>',
            r'<meta property="og:image" content="(.*?)"'
        ]
        
        for pattern in img_patterns:
            matches = re.findall(pattern, html_content)
            if matches:
                img_url = matches[0]
                # 处理相对URL
                if img_url.startswith('//'):
                    img_url = 'https:' + img_url
                elif not img_url.startswith(('http://', 'https://')):
                    # 获取book_url的域名部分
                    domain = '/'.join(book_url.split('/')[:3])
                    img_url = domain + ('/' if not img_url.startswith('/') else '') + img_url
                return img_url
        return None

    def get_image_url(self, book_url: str) -> str:
        """获取图书封面链接"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                proxy = self.proxy_pool.get_proxy()
                response = self.session.get(book_url, proxies=proxy, timeout=10)
                response.raise_for_status()
                
                # 提取图片URL
                img_url = self.extract_image_url(response.text, book_url)
                if img_url:
                    return img_url
                logging.warning(f"在页面中未找到图片链接: {book_url}")
                
            except Exception as e:
                logging.error(f"获取页面失败 {book_url}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(1, 3))
                continue
        return None

    def update_book_image(self, book_id: int, image_url: str):
        """更新数据库中的图片URL"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE books 
            SET book_image = ? 
            WHERE id = ?
        """, (image_url, book_id))
        conn.commit()
        conn.close()

    def process_book(self, book: Dict):
        """处理单本图书"""
        try:
            logging.info(f"开始处理图书: {book['book_name']}")
            image_url = self.get_image_url(book['book_url'])
            if image_url:
                self.update_book_image(book['id'], image_url)
                logging.info(f"成功更新图书封面链接: {book['book_name']} -> {image_url}")
            else:
                logging.error(f"无法获取图书封面链接: {book['book_name']}")
        except Exception as e:
            logging.error(f"处理图书失败 {book['book_name']}: {str(e)}")

    def run(self, max_workers: int = 5):
        """运行爬虫"""
        books = self.get_books_without_image()
        logging.info(f"找到 {len(books)} 本需要处理封面的图书")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(self.process_book, books)

if __name__ == "__main__":
    # 使用示例
    crawler = BookImageCrawler(
        db_path=DB_NAME,  # 使用config中的数据库路径
    )
    crawler.run(max_workers=15)  # 使用5个线程并发爬取
