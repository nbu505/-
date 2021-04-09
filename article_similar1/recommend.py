import numpy as np
#import faiss
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

df=np.loadtxt(open('df.csv','rb'),delimiter=',',skiprows=0)

conn = pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           passwd='nbu505',
                           db='xstt_local_testdb')
cursor = conn.cursor()
    # 生成相似文章表
cursor.execute("select cid, openid, downed from gzh_viewlog where openid = %s", user_id)

#print(df)

#df=np.delete(df,0,1)
#print(df.shape)

# #faiss
# df=df.astype('float32')
# index = faiss.IndexFlatL2(100)  # 构建index
# print(index.is_trained)  # False时需要train
# index.add(df)  #添加数据
# print(index.ntotal)  #index中向量的个数
#
# query_list=df[:999]
# dis, ind = index.search(query_list, 5)
# print(ind)
#
vector_=df[:,1:]

#余弦相似度
similar_vac=np.dot(vector_,vector_.T)
# print(len(similar_vac))

for i in range(len(similar_vac)):
    sim = cosine_similarity(vector_, vector_[i].reshape(1,-1))[:, 0]
    similar_index=np.argsort(sim)[::-1][1:int(6)]
    print('文章：',i,' 的相似文章为：',similar_index)

# #欧式距离
# for i in range(len(vector_)):
#     similar_vac = []
#     for j in range(len(vector_)):
#         similar_vac.append(np.linalg.norm(vector_[i] - vector_[j]))
#     similar_index=np.argsort(similar_vac)[1:int(6)]
#     print('文章：',i,' 的相似文章为：',similar_index)

#数据库
