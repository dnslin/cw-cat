import os

# 获取项目根目录的绝对路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 设置数据库绝对路径
DB_NAME = os.path.join(BASE_DIR, "ciweimao.db")

# 其他配置保持不变
PROXIES = [
    "http://dnslin:ReZTDC2Pn5kNur@cn-wuhan-1.wjy.me:10288",
    # 添加更多代理
    "http://dnslin:ReZTDC2Pn5kNur@8.217.224.217:10288",
    "http://dnslin:ReZTDC2Pn5kNur@27.106.105.4:10288",
    "http://dnslin:ReZTDC2Pn5kNur@121.91.168.90:10288",
    "http://dnslin:ReZTDC2Pn5kNur@v4.dnslv.com:10288",
    "http://dnslin:ReZTDC2Pn5kNur@v4.447654.xyz:10288",
    "http://dnslin:ReZTDC2Pn5kNur@20.2.2.72:10288",
    "http://dnslin:ReZTDC2Pn5kNur@20.189.96.242:10288",
]

LOG_PATH = os.path.join(BASE_DIR, "logs", "ciweimao.log")

# 确保日志目录存在
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
