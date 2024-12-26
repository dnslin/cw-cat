# https://www.ciweimao.com/reader/8747661

import requests
from lxml import html
import logging

# 传入参数 作者主页链接
def get_author_data(author_url):
    response = requests.get(author_url)
    # 打印响应内容 写入文件
    tree = html.fromstring(response.text)
    return tree

# 获取作者信息
def get_author_info(tree):
    # 使用xpath获取各项数据
    try:
        book_amount = tree.xpath('//*[@id="J_BookAmount"]/text()')[0]
        footprint = tree.xpath('/html/body/div[3]/div/ul/li[2]/b/text()')[0]
        fans = tree.xpath('/html/body/div[3]/div/ul/li[3]/b/text()')[0]
        following = tree.xpath('/html/body/div[3]/div/ul/li[4]/b/text()')[0]
        avatar = tree.xpath('//*[@id="userAvatar"]/@data-original')[0]
        
        # 获取作品数据
        books = []
        book_items = tree.xpath('//ul[contains(@class, "book-list")]/li')
        
        for item in book_items:
            book = {
                "book_id": item.get('data-book-id', ''),
                "cover": item.xpath('.//img/@data-original | .//img/@src')[0] if item.xpath('.//img/@data-original | .//img/@src') else '',
                "name": item.xpath('.//h3[@class="title"]/a/text()')[0].strip() if item.xpath('.//h3[@class="title"]/a/text()') else '',
                "url": item.xpath('.//h3[@class="title"]/a/@href')[0] if item.xpath('.//h3[@class="title"]/a/@href') else '',
                "clicks": item.xpath('.//p[@class="intro"]/span[1]/text()')[0].strip() if item.xpath('.//p[@class="intro"]/span[1]/text()') else '',
                "category": item.xpath('.//p[@class="intro"]/span[2]/text()')[0].strip() if item.xpath('.//p[@class="intro"]/span[2]/text()') else '',
                "update_to": item.xpath('.//div[@class="info"]/p[2]/text()')[0].strip() if item.xpath('.//div[@class="info"]/p[2]/text()') else '',
                "update_time": item.xpath('.//div[@class="info"]/p[4]/text()')[0].strip() if item.xpath('.//div[@class="info"]/p[4]/text()') else ''
            }
            books.append(book)

        # 返回包含所有信息的字典
        return {
            "book_amount": book_amount,
            "footprint": footprint,
            "fans": fans,
            "following": following,
            "avatar": avatar,
            "books": books
        }
    except IndexError as e:
        logging.error(f"获取作者信息失败: {str(e)}")
        return None

tree = get_author_data("https://www.ciweimao.com/reader/229453")
author_info = get_author_info(tree)
if author_info:
    print(f"书架数量: {author_info['book_amount']}")
    print(f"足迹数: {author_info['footprint']}")
    print(f"粉丝数: {author_info['fans']}")
    print(f"关注数: {author_info['following']}")
    print(f"头像: {author_info['avatar']}")
    print(f"作品: {author_info['books']}")
