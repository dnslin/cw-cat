import time
import random
import traceback
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import json

from database import Database
from details import get_book_data
from logger import setup_logger
from config import PROXIES, LOG_PATH
import requests

logger = setup_logger("detail_crawler", LOG_PATH)


def get_valid_proxy(proxies_list):
    """获取有效代理"""
    if not proxies_list:
        logger.warning("没有可用代理")
        return None

    while proxies_list:
        proxy = random.choice(proxies_list)
        proxies = {"http": proxy, "https": proxy} if proxy else None
        try:
            response = requests.get(
                "https://www.ciweimao.com", proxies=proxies, timeout=5
            )
            if response.status_code == 200:
                logger.debug(f"找到有效代理: {proxy}")
                return proxy
            logger.warning(f"代理 {proxy} 响应状态码: {response.status_code}")
        except Exception as e:
            logger.warning(f"代理 {proxy} 测试失败: {str(e)}")
            proxies_list.remove(proxy)
    return None


def crawl_book_detail(book_id, book_url, db, retries=3):
    """爬取单本书籍详情"""
    # 检查详情是否已爬取，避免重复爬取
    if db.is_detail_exists(book_url):
        logger.info(f"书籍 {book_url} 详情已存在，跳过")
        # 更新爬取状态
        with sqlite3.connect(db.db_name) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE books SET detail_crawled = 1 WHERE id = ?", (book_id,)
            )
            conn.commit()
        return True

    # 获取有效代理
    proxy_list = PROXIES.copy()
    proxy = get_valid_proxy(proxy_list)
    proxies = {"http": proxy, "https": proxy} if proxy else None

    for attempt in range(retries):
        try:
            # 增加请求头模拟浏览器
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Referer": "https://www.ciweimao.com/book_list",
            }

            # 使用get_book_data函数爬取详情，传入代理
            # 修改get_book_data函数接收代理参数
            book_data = get_book_data(book_url, proxies=proxies, headers=headers)

            # 保存到数据库
            if db.save_book_detail(book_id, book_url, book_data):
                logger.info(f"成功爬取并保存书籍 {book_url} 的详情")
                return True

        except Exception as e:
            if attempt == retries - 1:  # 最后一次重试
                logger.error(f"爬取书籍 {book_url} 详情失败: {str(e)}")
                logger.error(traceback.format_exc())
                return False

            logger.warning(
                f"爬取书籍 {book_url} 详情失败，重试中... (尝试 {attempt + 1}/{retries})"
            )
            # 更换代理重试
            proxy = get_valid_proxy(proxy_list)
            proxies = {"http": proxy, "https": proxy} if proxy else None
            time.sleep(random.uniform(1, 3))  # 随机延迟

    return False


def crawl_details_multi_thread(max_books=1000, max_workers=5):
    """多线程爬取书籍详情"""
    db = Database()
    logger.info(f"准备爬取书籍详情，最大数量: {max_books}，线程数: {max_workers}")

    # 获取待爬取的书籍
    books = db.get_uncrawled_books(limit=max_books)
    if not books:
        logger.info("没有需要爬取详情的书籍")
        return

    logger.info(f"找到 {len(books)} 本需要爬取详情的书籍")

    success_count = 0
    fail_count = 0

    # 使用线程池处理爬取任务
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 创建任务列表
        future_to_book = {
            executor.submit(crawl_book_detail, book["id"], book["book_url"], db): book[
                "book_url"
            ]
            for book in books
        }

        # 使用tqdm显示进度
        with tqdm(total=len(books), desc="爬取书籍详情") as pbar:
            for future in future_to_book:
                book_url = future_to_book[future]
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                    else:
                        fail_count += 1
                    pbar.set_description(f"爬取: {success_count}成功/{fail_count}失败")
                except Exception as e:
                    logger.error(f"处理书籍 {book_url} 时出现异常: {str(e)}")
                    fail_count += 1
                finally:
                    pbar.update(1)
                    # 随机延迟，避免请求过快
                    time.sleep(random.uniform(0.5, 1.5))

    logger.info(f"爬取完成! 成功: {success_count}, 失败: {fail_count}")


if __name__ == "__main__":
    import sqlite3

    try:
        # 可以根据需要调整参数
        crawl_details_multi_thread(max_books=5000, max_workers=5)
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        logger.error(traceback.format_exc())
