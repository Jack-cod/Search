import pymysql
import time

search = '%' + input('请输入想要搜索的内容') + '%'

time1 = time.time()

# 连接数据库
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='huojin314',
                             db='crawl',
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)

schrelt = {}


with connection.cursor() as cursor:
    # 选择表格
    sql = "SELECT * FROM main"
    cursor.execute(sql)
    # 检索search
    sql = "SELECT url, title, pagerank FROM main WHERE html LIKE %s;"
    cursor.execute(sql, (search))
    results = cursor.fetchall()
    for result in results:
        url = result['url']
        title = result['title']
        pagerank = result['pagerank']
        schrelt[url] = {'title': title, 'pagerank': pagerank}
    # 根据 "pagerank" 值排序字典
    sorted_schrelt = sorted(schrelt.items(), key=lambda x: x[1]['pagerank'], reverse=True)
    # print结果
    for key,value in sorted_schrelt:
        print(key,value,end='\n')

