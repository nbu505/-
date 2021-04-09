import word2vec
import numpy as np
#import faiss
import re
import pandas as pd
import pymysql

#df=pd.read_csv(r'E:\自动化数据处理\article_similar\journals_cleaned.csv',delimiter=',',header=None)
#----
#lable=df[7].values
#keywords=df[16].values
#----
#print(lable[1])

#-------------数据库连接-----------------------
conn = pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           passwd='nbu505',
                           db='xstt_local_testdb')
cursor0 = conn.cursor()
cursor1 = conn.cursor()
    # 获取所有文章的标签和关键词
    # --------------
cursor0.execute("select id, label, keyword from journals_cleaned")
cursor1.execute("select id from journals_cleaned order by id desc limit 1")
    #cursor.execute("select cid, openid, downed from gzh_viewlog where openid = %s", user_id)

rows = cursor0.fetchall()
maxid = cursor1.fetchall()
lable=[]
keywords=[]

for row in rows:
    lable.append(row[1])
    keywords.append(row[2])
#---------------------------------------------

art_list=[]
for i in range(len(rows)):
    tem_list=[]
    row_str=''
    tem_list.append(str(i))
    #row_str += " ".join(str(lable[i]).split(';'))
    row_str += " ".join(re.split(r'[;,s]s*',str(lable[i])))
    #row_str += " ".join(str(keywords[i]).split(';'))
    row_str += " ".join(re.split(r'[;,s]s*',str(keywords[i])))
    tem_list.append(row_str)
    art_list.append(tem_list)

title=['id','val']
dff=pd.DataFrame(columns=title, data=art_list)
dff.to_csv('article_map.csv', encoding='utf-8')
