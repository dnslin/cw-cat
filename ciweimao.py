# https://www.ciweimao.com/book_list

import aiohttp
import asyncio
from lxml import html
import random
from typing import Tuple, List
from fake_useragent import UserAgent
from database import Database
from tqdm import tqdm
from logger import setup_logger
from config import PROXIES, LOG_PATH

logger = setup_logger("ciweimao", LOG_PATH)


async def get_page_data(
    session: aiohttp.ClientSession, page: int, retries: int = 3
) -> Tuple[List[str], ...]:
    url = f"https://www.ciweimao.com/book_list/0-0-0-0-0-0/quanbu/{page}"
    ua = UserAgent()
    headers = {"User-Agent": ua.random}

    # 随机选择代理
    proxy = random.choice(PROXIES)

    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, proxy=proxy) as response:
                if response.status == 200:
                    text = await response.text()
                    tree = html.fromstring(text)

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

        except Exception as e:
            if attempt == retries - 1:  # 最后一次重试
                logger.error(f"获取页面 {page} 失败: {str(e)}")
                raise
            logger.warning(
                f"获取页面 {page} 失败，重试中... (尝试 {attempt + 1}/{retries})"
            )
            # 如果失败则更换代理重试
            proxy = random.choice(PROXIES)
            await asyncio.sleep(random.uniform(1, 3))  # 随机延迟


async def process_pages(
    start_page: int = 1, end_page: int = 1050, max_concurrent_requests: int = 10
):
    db = Database()
    semaphore = asyncio.Semaphore(max_concurrent_requests)

    async with aiohttp.ClientSession() as session:
        tasks = []
        with tqdm(total=end_page - start_page + 1, desc="创建任务") as pbar:
            for page in range(start_page, end_page + 1):
                await asyncio.sleep(random.uniform(0.5, 1.5))
                task = asyncio.create_task(
                    fetch_with_semaphore(semaphore, session, page)
                )
                tasks.append(task)
                pbar.update(1)
                pbar.set_description(f"已创建第 {page} 页的任务")

        # 使用tqdm显示处理进度
        with tqdm(total=len(tasks), desc="处理页面") as pbar:
            results = []
            for task in asyncio.as_completed(tasks):
                result = await task
                results.append(result)
                pbar.update(1)

        # 使用tqdm显示数据保存进度
        with tqdm(total=len(results), desc="保存数据") as pbar:
            for page, result in enumerate(results, start=start_page):
                if isinstance(result, Exception):
                    logger.error(f"页面 {page} 处理失败: {str(result)}")
                    continue

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
                    continue

                try:
                    logger.info(f"保存第 {page} 页数据到数据库")
                    # 保存到数据库
                    saved_count = db.save_books(books_data)

                    # 验证是否真的保存成功
                    total_count = len(db.get_all_books())
                    logger.info(f"数据库当前总记录数: {total_count}")

                    pbar.set_description(f"第 {page} 页成功保存 {saved_count} 条记录")
                    pbar.update(1)
                    logger.info(f"第 {page} 页成功保存 {saved_count} 条记录")
                except Exception as e:
                    logger.error(f"保存第 {page} 页数据时出错: {str(e)}")
                    import traceback

                    logger.error(traceback.format_exc())

    logger.info("所有页面处理完成")


async def fetch_with_semaphore(semaphore, session, page):
    async with semaphore:
        return await get_page_data(session, page)


if __name__ == "__main__":
    asyncio.run(process_pages())
