import numpy as np
import pymysql
from tqdm import tqdm, trange

# 连接数据库
connection = pymysql.connect(host='localhost',
                             user='crawl',
                             password='huojin314',
                             db='crawl',
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)

graph = {}

def pagerank(graph, d=0.85, num_iterations=100):
    # 建立矩阵
    matrix_size = len(graph) + 2
    A = np.zeros((matrix_size, matrix_size))
    for i in graph:
        for j in graph[i]:
            if len(graph[i]) > 0:
                A[i][j] = 1.0 / len(graph[i])
            else:
                A[i][j] = 0
    # 初始化 PageRank 值
    ranks = np.ones((matrix_size, 1)) / matrix_size
    # 迭代计算 PageRank 值
    for _ in trange(num_iterations):
        ranks = (1 - d) + d * A.T.dot(ranks)
    return {i: ranks[i][0] for i in range(matrix_size)}



with connection.cursor() as cursor:
    # 选择表格
    sql = "SELECT * FROM main"
    cursor.execute(sql)

    # 每次读一个
    row = cursor.fetchone()
    for row in tqdm(cursor):
        id = row['id']
        from_url = row['from_url']
        to_url = row['to_url']
        to_urls = to_url.split(' ')
        to_id = []
        for to_url in to_urls:
            try:
                with connection.cursor() as cursor:
                    # 检索to_url
                    sql = "SELECT id FROM main WHERE url like %s"
                    cursor.execute(sql, (to_url))
                    results = cursor.fetchall()
                    for result in results:
                        if result is not None:
                            to_id.append(result.get('id'))
            except:
                continue
            graph[id] = to_id

pr = pagerank(graph)
print(pr)

with connection.cursor() as cursor:
    for key,value in pr.items():
        # 数据库写入数据库
        query = "UPDATE main SET pagerank = %s WHERE id = %s"
        values = (value,key)
        cursor.execute(query, values)
        connection.commit()
connection.close()
