import word2vec
import numpy as np
import pandas as pd

#训练word2vec
#word2vec.word2phrase('cn_data.txt','cn_data-phrase.txt',verbose=True)
#word2vec.word2vec('cn_data-phrase.txt','cn_data.bin',size=100, binary=True, verbose=True)
#word2vec.word2clusters('cn_data.txt','cn_data-clusters.txt', 100, verbose=True)
#计算向量
model=word2vec.load(r'E:\自动化数据处理\article_similar\cn_data.bin')
# print(model['</s>'])
#print(model.vocab) n
# print(model.vectors.shape)
# print(model['学生'])
df=pd.read_csv(r'E:\自动化数据处理\article_similar\article_map.csv')
art_vct=[]
for index,row in df.iterrows():
    vec = np.zeros(101)
    str_list = []
    if type(row['val']) == type('asd'):
        str_list = row['val'].replace('"', '').split(' ')
    i = 0
    vec[0] = row['id']
    for s in str_list:
        try:
            i += 1
            vec[1:] = model[s]
        except:
            i -= 1
            continue

    if i != 0:
        vec[1:] /= i
    else:
        vec[1:] = np.random.random(100)
    art_vct.append(vec.tolist())
art_vct=np.array(art_vct)
np.savetxt('df.csv', art_vct, delimiter=',')