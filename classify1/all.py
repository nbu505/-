# -*- coding: utf-8 -*-
import operator
import jieba
import jieba.analyse
import re
import math
import operator
import pymysql
import warnings
warnings.filterwarnings('ignore')

#----------------------------------Keyword---------------------------------------
class keyword(object):

    def __init__(self):
        self.stopwords = []
        self.keywords = []
        self.tf_idf_data = {}
        self.pre_data_map = {}
        self.num = 7000000#总文档数量
        self.tfidf = {}

    def daoru(self):
        with open('E:\自动化数据处理\classify\StopWordTable.txt',encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                line = line.replace('\n','')
                self.stopwords.append(line)

        with open(r'E:\自动化数据处理\classify\t_idf1.txt',encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                line = line.replace('\n','')
                line = line.split(',')
                self.tf_idf_data[line[1].strip("\"")]=int(line[2].strip("\""))

    def yuchuli(self,paper):
        r1 = '[a-zA-Z0-9’!"#$%&\'()*+,-./:;<=>?@，。?★、…【】《》？“”‘’！[\\]^_`{|}~]+'
        paper = re.sub(r1,'',paper)
        seg_list = jieba.lcut(paper)
        for word in seg_list:
            if len(word) > 1:
                if word not in self.stopwords:#去停用词
                    if word in self.pre_data_map.keys():#对每个长度大于等于2的词记录并计数
                        self.pre_data_map[word] += 1
                    else:
                        self.pre_data_map[word] = 1

    def getTF(self,str):
        M = 0
        for s in self.pre_data_map:
            M = M + self.pre_data_map.get(s)
        return float(self.pre_data_map.get(str)/M)

    def getIDF(self,str):
        wendangNum = self.tf_idf_data.get(str)
        if wendangNum == None:
            wendangNum = 0
        res = float(math.log(float(self.num)/(wendangNum+1)))
        return res

    def getTFIDF(self,paper):
        self.daoru()
        self.yuchuli(paper)
        for str in self.pre_data_map.keys():
            self.tfidf[str] = self.getTF(str)*self.getIDF(str)
        self.tfidf=sorted(self.tfidf.items(),key=operator.itemgetter(1),reverse=True)
        num = int(len(self.tfidf)*0.22)
        # self.keywords = self.tfidf[:num]
        temp = self.tfidf[:num]
        for t in temp:
            self.keywords.append(t[0])#('高能', 0.3810204035224749), ('亿颗', 0.3101682504706382)


# ------------------------------from zuclassify import zhClassify--------------------------------------
class zhClassify(object):

    def __init__(self):
        self.data_map = {}

    def daoru(self):
        with open('E:\自动化数据处理\classify\ztxkb4.txt',encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                line = line.replace('\n','')
                line = line.split(',')
                self.data_map[line[0].strip("\"")] = line[1].strip("\"")

    def data_process(self,ztNumber):
        # pattern = "[`~!@#$%^&*()--+=|{}':;',//[//]<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]"
        # ztNumber = re.sub(pattern,'',ztNumber)
        # self.daoru()

        zt = ztNumber.split(';')

        res = ''

        for sn in zt:
            sn_fir = sn
            if len(sn) > 2:
                if sn[0:3] == "S78" or sn[0:3] == "S88" or sn[0:3] == "S89" or sn[0:3]=="U67" :
                    sn = sn_fir
                elif sn[0] == "E":
                    sn = "E"
                elif sn[0:3]=="G25":
                    sn = "G250"
                elif sn[0:5]=="TS941" or sn[0:5]=="TS942" or sn[0:4]=="TS94" or sn[0:3]=="TS1":
                    sn = "TS1"
                elif sn[0:3]=="F23" or sn[0:3]=="F206" or sn[0:3]=="F203" or sn[0:4]=="F719" or sn[0:4]=="F718"or sn[0:4]=="F717"or sn[0:4]=="F716"or sn[0:4]=="F715"or sn[0:4]=="F714"or sn[0:4]=="F593" or sn[0:4]=="F715" or sn[0:5]=="F590."or sn[0:4]=="F592"or sn[0:4]=="F594" or sn[0:4]=="F595" or sn[0:4]=="F596" or sn[0:4]=="F597":
                    sn = "F72"
                else:
                    sn = sn[0:3]
            else:
                if sn[0:2] == "R1" or sn[0:2] == "S3" or sn[0:2] == "S9":
                    sn = sn_fir
                else:
                    sn = sn[0:2]

            try:
                res_ch = self.data_map[sn]
            except KeyError:
                res_ch=''
            if sn_fir != zt[-1] and res_ch!='':
                res = res + res_ch +';'
            else:
                res = res + res_ch
        return res

#-------------------------------------keyword_extract__________________________________________
class Extract(object):
    def keyword_extract(self,input):
        # regul = 'a-zA-Z0-9'
        regul = '0-9'
        jieba.analyse.set_stop_words('E:\自动化数据处理\classify\StopWordTable.txt')
        content = re.sub(regul, '', input)
        keywords = jieba.analyse.extract_tags(content, topK=4, withWeight=False, allowPOS=())
        return keywords

#---------------------------------from BayesClassify import Classify---------------------------------
class Classify(object):

    def __init__(self):
        # sw = keyword()
        # sw.yuchuli('正中国科学院2017年11月29日举行新闻发布会,宣布中科院空间科学战略性先导专项首发星——暗物质粒子探测卫星“悟空”(DAMPE)取得首批重大科学成果。利用DAMPE采集到的数据获得了迄今最精确的高能电子宇宙线能谱。相关成果于2017年11月30日正式在Nature在线发表。DAMPE于2015年12月17日发射成功,在轨运行前530天共采集约28亿颗高能宇宙射线,其中包')
        # sw.getTFIDF('法国电影作为电影界的元老,以其深厚的文化底蕴与独特的艺术智慧,质朴而犀利地展现出人与自然之间、人与人之间、人与自我之间的矛盾,折射出人类原欲的情愫,并循循善诱予观影人以启迪。本文通过对法国电影《蝴蝶》的剖析,旨在分析法国电影的精神欲望构建以及其对促进建设和谐人类精神生态的影响,肯定了法国电影的重要地位,并借以传达出电影在予人消遣的同时,应不忘艺术所承载的社会使命的责任。')
        self.M = 14892.0
        self.Num = 1278586 #分类器训练语料数量
        # self.wordList = sw.keywords
        self.wordList = []
        self.bayes1 = {}
        self.bayes2 = {}
        self.hs = {}

    def daorudata(self):
        with open(r'E:\自动化数据处理\classify\t_bayespro1.txt',encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                line = line.replace('\n', '')
                line = line.split(',')
                self.bayes1[line[2].strip("\"")] = int(line[3].strip("\""))

        with open(r'E:\自动化数据处理\classify\t_bayespro22.txt',encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines:
                line = line.replace('\n','')
                line = line.split(',')
                self.bayes2[(line[1].strip("\""))+'-_-'+(line[2].strip("\""))] = int(line[3].strip("\""))


    def calculatePc(self,cj):  #计算p（cj）
        a = float(self.bayes1.get(cj))
        return float(a/self.Num)

    def getTrainingNumOfClassification(self,cj): #得到cj的语料数量
        return float(self.bayes1.get(cj))

    def getCountContainKeyOfClassification(self,cj,ss):#得到num（xi|cj）
        ret = 0.0
        try:
            ret = float(self.bayes2.get(ss+'-_-'+cj))
        except:
            pass
        return ret

    def pxc(self,ss,cj): #计算p（xi|cj）
        Nxc = self.getCountContainKeyOfClassification(cj,ss)
        Nc = self.getTrainingNumOfClassification(cj)
        ret = float((Nxc+1)/(Nc+self.M))
        return ret

    def calcProd(self,cj):#计算𝑝(𝑥_1,𝑥_2,…,𝑥_𝑛│𝑐_𝑗 )
        ret = 1.0
        for ss in self.wordList:
            ret = ret * self.pxc(ss,cj) * 1000
        ret = ret * self.calculatePc(cj)
        # print(ret)
        return ret

    def classfy(self,inputContent):
        # sw = keyword()
        # # sw.yuchuli('正中国科学院2017年11月29日举行新闻发布会,宣布中科院空间科学战略性先导专项首发星——暗物质粒子探测卫星“悟空”(DAMPE)取得首批重大科学成果。利用DAMPE采集到的数据获得了迄今最精确的高能电子宇宙线能谱。相关成果于2017年11月30日正式在Nature在线发表。DAMPE于2015年12月17日发射成功,在轨运行前530天共采集约28亿颗高能宇宙射线,其中包')
        # sw.getTFIDF(inputContent)
        # self.wordList = sw.keywords

        extract = Extract()
        self.wordList=extract.keyword_extract(inputContent)

        self.daorudata()
        probility = 0.0
        for bc1 in self.bayes1.keys():
            probility = self.calcProd(bc1)
            self.hs[bc1] = probility
        sh = sorted(self.hs.items(),key = operator.itemgetter(1),reverse=True)
        return sh[0][0]



#----------------------------------process------------------------------------
cn_map_id = {}
with open('E:\自动化数据处理\classify\dictionaries_category.csv',encoding='utf-8') as f:
    lines = f.readlines()
    for line in lines:
        line = line.replace('\n', '')
        line = line.split(',')
        cn_map_id[line[1]]=line[0]


def processing(ztNumber,content):

    ztfenlei = zhClassify()
    ztfenlei.daoru()
    beiyesi = Classify()

    res = ''
    if ztNumber is not None:
        res = ztfenlei.data_process(ztNumber)
        if res !='':
            # print(res)
            print("***********中图号分类************")
        else:
            res = beiyesi.classfy(content)


    res_final_id = ''
    resl = res.split(';')
    length = len(resl)
    tem = resl[0]
    for i in range(length):
        # print(i)
        res_id = cn_map_id[resl[i]]
        if i>=1:
            if tem == resl[i]:
                res_final_id =  res_id
                break

        if i == length-1:
            res_final_id = res_final_id + res_id
        else:
            res_final_id = res_final_id + res_id +';'

    return res,res_final_id

#-------------------------------connectMysql-------------------------------------
db = pymysql.connect("localhost","root","nbu505","xstt_local_testdb")
cursor = db.cursor()
cursor2 = db.cursor()

sql = 'select `content`,`title`,`category`,`id`  from journals_cleaned where id >0'
sql2 = "update journals_cleaned set label = ('%s'),category_id = ('%s'),category_cn = ('%s') where id = ('%d')"
cursor.execute(sql)

db.commit()

# print(cursor.fetchmany(10))
result = 1
# list1 = []
# list2 = []
# list3 = []
# s =10
extract = Extract()
while result:
    # s=s-1
    result = cursor.fetchone()
    if not result:
        break
    content = ''.join(result[0])
    #print(content)
    label_str = ''
    label = extract.keyword_extract(content)
    for l in label[:-1]:
        label_str = label_str + l +','
    label_str += label[-1]
    cate,cate_id  = processing(result[2],result[0]+result[1])
    sql_id = int(result[3])

    data = (label_str,cate_id,cate,sql_id)
    print(data)
    cursor2.execute(sql2%data)
    db.commit()

cursor.close()
cursor2.close()
db.close()