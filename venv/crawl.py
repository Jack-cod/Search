import requests
import pymysql
from bs4 import BeautifulSoup
from collections import deque
import time
import re

# 初始 URL 集合
urls = deque(['https://www.csdn.net/'])
# 已访问的 URL 集合
visited = set()

# 连接数据库
conn = pymysql.connect(host='localhost', user='crawl', password='huojin314', db='crawl', charset='utf8')
if conn:
    print("成功连接至数据库")
else:
    print("连接数据库时出错")
# 创建游标
cursor = conn.cursor()

while len(urls)>0:
    # 取出队列中的第一个 URL
    url = urls.popleft()
    # 如果该 URL 已被访问过，则跳过
    if url in visited:
        continue
    # 设置浏览器标识
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    # 发送 HTTP 请求并获取响应
    try:
        response = requests.get(url, headers=headers, timeout=5)
        # 标记为已访问
        visited.add(url)
    except requests.exceptions.RequestException as e:
        print(e)
        continue


    #解析HTML网页
    soup = BeautifulSoup(response.text, 'html.parser')
    # 获取 h1 标签中的文本
    if soup.title:
        title_text = soup.find('title').text
    else:
        title_text = "Title not found"

    # 获取所有 p 标签中的文本
    p_texts = [p.string for p in soup.find_all('p')]
    #过滤
    p_texts = [x for x in p_texts if x is not None]
    # 使用空格作为分隔符
    text = ' '.join(p_texts)

    # 提取所有链接
    links = soup.find_all('a')
    links = [str(link) for link in links]
    # 使用正则表达式提取href属性值
    pattern = r'href="(.+?)"'
    links = [re.search(pattern, x).group(1) for x in links if re.search(pattern, x)]

    # 遍历所有链接
    for link in links:
        # 获取链接地址
        href = link
        # 如果链接未被访问过且在初始 URL 集合中
        if href not in visited:
            # 将链接加入队列尾部
            urls.append(href)
            last_url = urls[0]

    # 将网页存储到数据库中
    try:
        links = ' '.join(links)
        if text and title_text:
            sql = 'INSERT INTO main (url, html, title, from_url, to_url) VALUES (%s, %s, %s, %s, %s)'
            cursor.execute(sql, (url, text, title_text, last_url, links))
    except Exception as e:
        print(e)
        continue

    print(url,title_text)
    # 提交事务
    conn.commit()
    # 设置爬取间隔
    time.sleep(2)

#关闭游标
cursor.close()
#关闭数据库链接
conn.close()
