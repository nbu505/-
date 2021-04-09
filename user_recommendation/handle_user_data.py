import pymysql
#import handle_article_data
import pandas as pd
import numpy as np
#import ast
#import argparse
import time
from sklearn.metrics.pairwise import cosine_similarity


#from flask import Flask, request
import random


def get_history_article(user_id):
    start = time.time()
    conn = pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           passwd='nbu505',
                           db='xstt_local_testdb')
    cursor = conn.cursor()
    # 获取所有文章的标签和关键词
    # --------------
    # cursor.execute("select id, label, keyword from journals_cleaned")
    cursor.execute("select cid, openid, downed from gzh_viewlog where openid = %s", user_id)

    rows = cursor.fetchall()

    art_list = []

    for row in rows:
        art_list.append(int(row[0]))

    conn.commit()
    cursor.close()
    conn.close()
    return art_list


def get_journals_history(art_list):
    start = time.time()
    conn = pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           passwd='nbu505',
                           db='xstt_local_testdb')
    cursor = conn.cursor()
    # 获取所有文章的标签和关键词
    # --------------
    # cursor.execute("select id, label, keyword from journals_cleaned")
    sql_str = "select t_id from article_integration where id in (%s) and tablename = 'journals_cleaned'" % ','.join(['%s'] * len(art_list))
    cursor.execute(sql_str, art_list)
    rows = cursor.fetchall()
    art_list = []

    for row in rows:
        art_list.append(int(row[0]))

    conn.commit()
    cursor.close()
    conn.close()
    return art_list





def recommend(user_id, n):
    article_list = get_history_article(user_id)
    journals_list = get_journals_history(article_list)
    if len(journals_list) == 0:
        return 'user_id: %s 暂无journal的浏览记录' % user_id
    user_vec = np.zeros(100)
    start = time.time()

    for l in journals_list:
        article_index = dit[l]
        user_vec += data[article_index, 1:]
    # print(user_vec)
    #print('for:' + str(time.time() - start) + 's')
    vectors_ = data[:, 1:]
    similar_vec = cosine_similarity(vectors_, np.array(user_vec).reshape(1, -1))[:, 0]
    # similar_vec = np.dot(vectors_, vec.T)
    similar_index = np.argsort(similar_vec)[::-1][0:int(n)]
    return data[similar_index, 0].tolist()
    # return handle_article_data.get_similar_article_by_vec(data, np.array(user_vec), n)


def get_user_openid():
    start = time.time()
    conn = pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           passwd='nbu505',
                           db='xstt_local_testdb')
    cursor = conn.cursor()
    cursor.execute("select DISTINCT(openid) from gzh_viewlog order by openid")
    rows = cursor.fetchall()
    user_list = []

    for row in rows:
        user_list.append(str(row[0]))

    conn.commit()
    cursor.close()
    conn.close()
    print('get_user_openid:' + str(time.time() - start) +'s')

    return user_list



print('loading-----------------')
data = np.loadtxt(open(r'E:\自动化数据处理\article_similar\df.csv', 'rb'), delimiter=',', skiprows=0)
print('start successfully---------------')
keys = [int(data[i, 0]) for i in range(data.shape[0])]
values = [i for i in range(data.shape[0])]
dit = dict(zip(keys, values))

user_list = get_user_openid()
dict = {}
for i in range(len(user_list)):
    rec = recommend(user_list[i], 10)
    if type(rec) == type([]):
        dict[user_list[i]] = str(rec)

print(dict)
#
# app = Flask(__name__)
#
#
# @app.route('/')
# def index():
#     return "hello, World"
#
#
# @app.route('/get', methods=['GET'])
# def getToken():
#     user_id = request.args.get("user")
#     num = request.args.get("num")
#     print('user_id:' + str(user_id))
#     print('num:' + str(num))
#     return recommend(user_id, num)
#
#
# if __name__ == '__main__':
#     print('loading-----------------')
#     data = np.loadtxt(open('df.csv', 'rb'), delimiter=',')
#     print('start successfully---------------')
#     app.run()
#



