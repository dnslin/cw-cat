import requests
from lxml import html
from typing import List, Dict
from fake_useragent import UserAgent
from config import PROXIES
from logger import setup_logger
import json

logger = setup_logger('chapters', 'logs/chapters.log')

def get_chapter_list(book_id: int) -> List[Dict]:
    """
    获取小说章节列表,按卷组织
    
    Args:
        book_id: 书籍ID
    
    Returns:
        卷信息列表，每个卷包含标题和章节列表
    """
    url = "https://www.ciweimao.com/chapter/get_chapter_list_in_chapter_detail"
    ua = UserAgent()
    headers = {
        'User-Agent': ua.random,
        'Content-Type': 'application/x-www-form-urlencoded',
        'Referer': f'https://www.ciweimao.com/book/{book_id}'
    }
    
    data = {
        'book_id': str(book_id),
        'chapter_id': '0',
        'orderby': '0'
    }
    
    req = requests.post(url, headers=headers, data=data).text
    tree = html.fromstring(req)
    volumes = []
    
    # 遍历每个卷
    for volume_elem in tree.xpath('//div[@class="book-chapter-box"]'):
        # 获取卷标题
        volume_title = volume_elem.xpath('.//h4[@class="sub-tit"]/text()')[0].strip()
        chapters = []
        
        # 获取该卷下的所有章节
        for chapter_elem in volume_elem.xpath('.//ul[@class="book-chapter-list"]/li/a'):
            chapter = {
                'title': chapter_elem.xpath('text()')[-1].strip(),
                'url': chapter_elem.get('href', ''),
                'is_locked': bool(chapter_elem.xpath('.//i[@class="icon-lock"]'))
            }
            chapters.append(chapter)
        
        volume = {
            'title': volume_title,
            'chapters': chapters
        }
        volumes.append(volume)
    
    return volumes

# 示例用法
book_data = get_chapter_list(100420810)
# 转为 json

print(json.dumps(book_data, ensure_ascii=False, indent=4))