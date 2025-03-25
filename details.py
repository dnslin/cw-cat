# 获取作品详情
import requests
from lxml import etree
import json

# https://www.ciweimao.com/book/100420810


def get_book_data(url, proxies=None, headers=None):
    """获取书籍详情数据，支持代理和自定义请求头"""
    # 使用默认请求头如果没有提供
    if headers is None:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

    response = requests.get(url, proxies=proxies, headers=headers, timeout=10)
    tree = etree.HTML(response.text)

    # 提取书籍标题
    title = tree.xpath("//h1[@class='title']/text()")[0]

    # 提取书籍作者
    author = tree.xpath('//meta[@property="og:novel:author"]/@content')[0]

    # 提取作者 ID
    author_id = (
        tree.xpath("//div[@class='container']//div[@class='author-info']/a")[0]
        .attrib["href"]
        .split("/")[-1]
    )

    # 提取书籍描述
    description = tree.xpath('//meta[@property="og:description"]/@content')[0]

    # 最后更新
    update_text = tree.xpath('//p[@class="update-time"]/text()')[0]
    last_update = update_text.split("：")[1].split("[")[-1].strip("]").strip()
    # 是否连载
    status = tree.xpath('//p[@class="update-state"]/text()')[0].split("·")[0].strip()

    # 获取 tag
    tags = tree.xpath(
        '//p[@class="label-box"]//span[@class="label label-warning J_jubao_tag"]//text()'
    )
    # 清理标签列表
    tags = [tag.strip() for tag in tags if tag.strip() and "举报" not in tag]

    # 获取统计数据
    stats = {}
    stats_text = tree.xpath(
        '//p[@class="book-grade"]/text() | //p[@class="book-grade"]/b/text()'
    )
    stats_text = [t.strip() for t in stats_text if t.strip()]
    for i in range(0, len(stats_text), 2):
        key = stats_text[i].strip("：")
        value = stats_text[i + 1]
        stats[key] = value

    # 获取详细统计
    detail_stats = {}
    property_items = tree.xpath('//div[@class="book-property clearfix"]/span')
    for item in property_items:
        # 获取所有文本内容，包括 span 和 i 标签中的内容
        texts = item.xpath(".//text()")
        # 过滤空白字符并连接
        texts = [t.strip() for t in texts if t.strip()]
        if len(texts) >= 2:  # 确保有键和值
            key = texts[0].rstrip("：")  # 移除冒号
            value = texts[1]  # i 标签中的值
            detail_stats[key] = value

    # 检查是否为首发
    is_first = bool(
        tree.xpath('//span[@class="theme-color" and contains(text(), "本站首发")]')
    )
    if is_first:
        detail_stats["首发状态"] = "本站首发"

    return {
        "title": title,
        "author": author,
        "author_id": author_id,
        "description": description,
        "last_update": last_update,
        "status": status,
        "tags": tags,
        "stats": stats,
        "detail_stats": detail_stats,
    }


# 只在直接运行文件时执行示例
if __name__ == "__main__":
    # 示例用法
    url = "https://www.ciweimao.com/book/100430532"
    book_data = get_book_data(url)
    # 转为 json
    print(json.dumps(book_data, ensure_ascii=False, indent=4))
