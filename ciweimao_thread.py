# https://www.ciweimao.com/book_list

import requests
from lxml import html
import random
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple
from fake_useragent import UserAgent
from database import Database
from tqdm import tqdm
from logger import setup_logger
from config import PROXIES, LOG_PATH
import os

logger = setup_logger("ciweimao_thread", LOG_PATH)


def get_valid_proxy(proxies_list: List[str]) -> str:
    """获取有效代理"""
    while proxies_list:
        proxy = random.choice(proxies_list)
        proxies = {"http": proxy, "https": proxy} if proxy else None
        try:
            response = requests.get(
                "https://www.ciweimao.com", proxies=proxies, timeout=5
            )
            if response.status_code == 200:
                return proxy
            logger.warning(f"代理 {proxy} 响应状态码: {response.status_code}")
        except Exception:
            proxies_list.remove(proxy)
    return None


def get_page_data(page: int, retries: int = 3) -> Tuple[List[str], ...]:
    """获取单页数据"""
    url = f"https://www.ciweimao.com/book_list/0-0-0-0-0-0/quanbu/{page}"

    # 获取有效代理
    proxy = get_valid_proxy(PROXIES.copy())
    proxies = {"http": proxy, "https": proxy} if proxy else None
    logger.info(f"使用代理: {proxies}")
    for attempt in range(retries):
        try:
            response = requests.get(url, proxies=proxies, timeout=10)
            if response.status_code == 200:
                tree = html.fromstring(response.text)

                # 提取各项信息
                categories = tree.xpath(
                    "//div[5]/div/div[2]/div[1]/table//tr/td[1]/p/text()"
                )
                book_names = tree.xpath(
                    "//div[5]/div/div[2]/div[1]/table//tr/td[2]/p/a/text()"
                )
                book_urls = tree.xpath(
                    "//div[5]/div/div[2]/div[1]/table//tr/td[2]/p/a/@href"
                )
                latest_chapters = tree.xpath(
                    "//div[5]/div/div[2]/div[1]/table//tr/td[3]/p/a/text()"
                )
                latest_chapter_urls = tree.xpath(
                    "//div[5]/div/div[2]/div[1]/table//tr/td[3]/p/a/@href"
                )
                authors = tree.xpath(
                    "//div[5]/div/div[2]/div[1]/table//tr/td[4]/p/a/text()"
                )
                author_urls = tree.xpath(
                    "//div[5]/div/div[2]/div[1]/table//tr/td[4]/p/a/@href"
                )
                word_counts = tree.xpath(
                    "//div[5]/div/div[2]/div[1]/table//tr/td[5]/p/text()"
                )
                update_times = tree.xpath(
                    "//div[5]/div/div[2]/div[1]/table//tr/td[6]/p/text()"
                )

                logger.info(f"成功获取页面 {page} 的数据")
                # 判断是否获取到数据
                if not book_names:
                    logger.warning(f"页面 {page} 没有获取到数据")
                    # 把html 写入文件
                    with open(f"page_{page}.html", "w", encoding="utf-8") as f:
                        f.write(response.text)
                    return ([],) * 9

                return (
                    categories,
                    book_names,
                    book_urls,
                    latest_chapters,
                    latest_chapter_urls,
                    authors,
                    author_urls,
                    word_counts,
                    update_times,
                )
            else:
                logger.warning(f"页面 {page} 响应状态码: {response.status_code}")
        except Exception as e:
            if attempt == retries - 1:  # 最后一次重试
                logger.error(f"获取页面 {page} 失败: {str(e)}")
                raise
            logger.warning(
                f"获取页面 {page} 失败，重试中... (尝试 {attempt + 1}/{retries})"
            )
            # 如果失败则更换代理重试
            proxy = get_valid_proxy(PROXIES.copy())
            proxies = {"http": proxy, "https": proxy} if proxy else None
            time.sleep(random.uniform(1, 3))  # 随机延迟

    # 所有重试都失败
    raise Exception(f"获取页面 {page} 失败，已重试 {retries} 次")


def process_and_save_page(page: int, db: Database) -> int:
    """处理并保存单页数据"""
    try:
        # 获取页面数据
        result = get_page_data(page)

        # 解包数据
        (
            categories,
            book_names,
            book_urls,
            latest_chapters,
            latest_chapter_urls,
            authors,
            author_urls,
            word_counts,
            update_times,
        ) = result

        # 准备数据库插入数据
        books_data = list(
            zip(
                categories,
                book_names,
                book_urls,
                latest_chapters,
                latest_chapter_urls,
                authors,
                author_urls,
                word_counts,
                update_times,
            )
        )

        # 添加调试信息
        logger.info(f"第 {page} 页获取到 {len(books_data)} 条数据")
        if books_data:
            logger.debug(f"第一条数据: {books_data[0]}")
        else:
            logger.warning(f"第 {page} 页没有获取到数据")
            return 0

        # 获取现有书名
        existing_books = db.get_all_book_names()
        # 过滤掉已存在的书籍
        new_books_data = [book for book in books_data if book[1] not in existing_books]

        skipped_count = len(books_data) - len(new_books_data)
        if skipped_count > 0:
            logger.info(f"第 {page} 页跳过 {skipped_count} 条重复记录")

        # 保存到数据库
        logger.info(f"保存第 {page} 页数据到数据库")
        saved_count = db.save_books(new_books_data)
        logger.info(f"第 {page} 页成功保存 {saved_count} 条记录")

        return saved_count
    except Exception as e:
        logger.error(f"处理第 {page} 页数据时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return 0


def process_pages(start_page: int = 1, end_page: int = 1050, max_workers: int = 10):
    """使用多线程处理多个页面"""
    db = Database()
    logger.info(f"使用数据库: {db.db_name}")

    # 确认数据库文件
    if os.path.exists(db.db_name):
        logger.info(f"数据库文件存在: {db.db_name}")
    else:
        logger.warning(f"数据库文件不存在，将创建: {db.db_name}")

    # 获取初始记录数
    initial_count = len(db.get_all_books())
    logger.info(f"爬取前数据库共有 {initial_count} 条记录")

    pages = list(range(start_page, end_page + 1))
    total_pages = len(pages)
    processed_pages = 0
    saved_records = 0

    # 使用线程池处理页面
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_page = {
            executor.submit(process_and_save_page, page, db): page for page in pages
        }

        # 使用tqdm显示进度
        with tqdm(total=total_pages, desc="处理页面") as pbar:
            for future in tqdm(future_to_page):
                page = future_to_page[future]
                try:
                    saved_count = future.result()
                    processed_pages += 1
                    saved_records += saved_count
                    pbar.set_description(f"第 {page} 页成功保存 {saved_count} 条记录")
                except Exception as e:
                    logger.error(f"处理第 {page} 页时出现异常: {str(e)}")
                finally:
                    pbar.update(1)

    # 获取最终记录数
    final_count = len(db.get_all_books())
    logger.info(f"爬取完成! 处理了 {processed_pages} 页，保存了 {saved_records} 条记录")
    logger.info(
        f"数据库记录数: {initial_count} -> {final_count}, 新增 {final_count - initial_count} 条"
    )


if __name__ == "__main__":
    # 先测试数据库连接
    try:
        db = Database()
        process_pages(start_page=1, end_page=6548, max_workers=5)
    except Exception as e:
        logger.error(f"程序启动时出错: {str(e)}")
        logger.error(traceback.format_exc())
