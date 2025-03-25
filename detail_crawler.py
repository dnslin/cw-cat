import time
import random
import traceback
import argparse
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import sqlite3

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

    proxy_list = proxies_list.copy()
    while proxy_list:
        proxy = random.choice(proxy_list)
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
            proxy_list.remove(proxy)
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
    proxy = get_valid_proxy(PROXIES)
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

            # 爬取详情
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
            proxy = get_valid_proxy(PROXIES)
            proxies = {"http": proxy, "https": proxy} if proxy else None
            time.sleep(random.uniform(1, 3))  # 随机延迟

    return False


def crawl_details_multi_thread(
    batch_size=1000, max_workers=5, continuous=True, rest_time=60
):
    """多线程爬取书籍详情

    参数:
        batch_size: 每批爬取的数量
        max_workers: 线程数
        continuous: 是否持续爬取直到全部完成
        rest_time: 每批爬取后的休息时间(秒)
    """
    db = Database()
    total_success = 0
    total_fail = 0
    batch_count = 0

    while True:
        batch_count += 1
        # 获取待爬取的书籍
        books = db.get_uncrawled_books(limit=batch_size)
        if not books:
            logger.info("没有需要爬取详情的书籍，爬取完成")
            break

        logger.info(f"第 {batch_count} 批: 找到 {len(books)} 本需要爬取详情的书籍")

        success_count = 0
        fail_count = 0

        # 使用线程池处理爬取任务
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 创建任务列表
            future_to_book = {
                executor.submit(
                    crawl_book_detail, book["id"], book["book_url"], db
                ): book["book_url"]
                for book in books
            }

            # 使用tqdm显示进度
            with tqdm(total=len(books), desc=f"批次 {batch_count} 爬取进度") as pbar:
                for future in future_to_book:
                    book_url = future_to_book[future]
                    try:
                        result = future.result()
                        if result:
                            success_count += 1
                        else:
                            fail_count += 1
                        pbar.set_description(
                            f"批次 {batch_count}: {success_count}成功/{fail_count}失败"
                        )
                    except Exception as e:
                        logger.error(f"处理书籍 {book_url} 时出现异常: {str(e)}")
                        fail_count += 1
                    finally:
                        pbar.update(1)
                        # 随机延迟，避免请求过快
                        time.sleep(random.uniform(0.5, 1.5))

        total_success += success_count
        total_fail += fail_count

        logger.info(
            f"第 {batch_count} 批爬取完成! 成功: {success_count}, 失败: {fail_count}"
        )
        logger.info(f"累计爬取: 成功 {total_success}, 失败 {total_fail}")

        # 如果不是持续爬取模式，退出循环
        if not continuous:
            break

        # 如果还有更多书籍要爬取，休息一段时间再继续
        if books and rest_time > 0:
            logger.info(f"休息 {rest_time} 秒后继续下一批爬取...")
            time.sleep(rest_time)

    logger.info(f"全部爬取任务结束! 总成功: {total_success}, 总失败: {total_fail}")


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="爬取书籍详情")
    parser.add_argument("--batch-size", type=int, default=1000, help="每批爬取的数量")
    parser.add_argument("--workers", type=int, default=5, help="线程数")
    parser.add_argument(
        "--no-continuous", action="store_true", help="不持续爬取，只爬取一批"
    )
    parser.add_argument("--rest", type=int, default=60, help="每批爬取后的休息时间(秒)")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        args = parse_args()

        logger.info(
            f"开始爬取详情，配置: 批量={args.batch_size}, 线程={args.workers}, "
            f"持续爬取={not args.no_continuous}, 休息时间={args.rest}秒"
        )

        # 开始爬取详情
        crawl_details_multi_thread(
            batch_size=args.batch_size,
            max_workers=args.workers,
            continuous=not args.no_continuous,
            rest_time=args.rest,
        )

    except KeyboardInterrupt:
        logger.info("用户中断，程序结束")
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        logger.error(traceback.format_exc())
