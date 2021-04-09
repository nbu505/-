import pymysql
import chardet
db = pymysql.connect("localhost","root","nbu505","nbu_xstj_spider_new")
#db = pymysql.connect("39.104.182.57","root505","123126","nbu_xstj_spider")

cursor1 = db.cursor()
cursor2 = db.cursor()
cursor3 = db.cursor()
cursor4 = db.cursor()
cursor5 = db.cursor()
cursor6 = db.cursor()
cursor7 = db.cursor()

# sql1 = 'select `id`,`title`,`content`,`category`,`author`,`languages`,`journal_no`,`author_department`,`journal_name`,' \
#       '`t_id`,`type`,`source`,`create_time`,`label`,`keyword`,`fund`,`url`,`tablename`,`zlfid` from article_journals where id>%s'
# sql2 = 'select `school` from school'
# sql3 = 'select `id`,`qikan`,`level` from qikan_info'
# sql4 = 'insert into journals_cleaned (id,title,content,category,author,journal_no,author_department,' \
#        'journal_name,article_journals_id,type,source,create_time,label,keyword,fund,url,tablename,zlfid,language,' \
#        'category_id,publication_time,author_id,author_first,department_first,category_cn,journal_level)' \
#        'values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
# sql5 = 'select max(id) id from article_journals group by source'
# sql6 = 'select max(id) id from journals_cleaned group by source'

sql1 = 'select `id`,`title`,`content`,`category`,`author`,`languages`,`journal_no`,`author_department`,`journal_name`,' \
      '`t_id`,`type`,`source`,`create_time`,`label`,`keyword`,`fund`,`url`,`tablename`,`zlfid` from article_journals where id>%s'
sql2 = 'select `school` from school'
sql3 = 'select `id`,`qikan`,`level` from qikan_info'
sql4 = 'insert into journals_cleaned (id,title,content,category,author,journal_no,author_department,' \
       'journal_name,article_journals_id,type,source,create_time,label,keyword,fund,url,tablename,zlfid,language,' \
       'category_id,publication_time,author_id,author_first,department_first,category_cn,journal_level)' \
       'values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
# sql5 = 'select max(id) id from article_journals_copy group by source'
# sql6 = 'select max(id) id from journals_cleaned group by source'
sql5 = 'select id from article_journals order by id desc limit 1'
sql6 = 'select id from journals_cleaned order by id desc limit 1' #4999
sql7 = 'select article_journals_id from journals_cleaned order by article_journals_id desc limit 1'

cursor2.execute(sql2)
school = cursor2.fetchall()


cursor3.execute(sql3)
journal_level = cursor3.fetchall()


cursor5.execute(sql5)
maxid1 = cursor5.fetchall()
print('maxid1=',maxid1)


cursor6.execute(sql6)
maxid2 = cursor6.fetchall()
if maxid2 == ():
    maxid2 = ((0,),)
print('maxid2=',maxid2)


cursor7.execute(sql7)
maxid3 = cursor7.fetchall()
print('maxid3=',maxid3)

print('1111111111111111111111')
if maxid1>maxid2:
    cursor1.execute(sql1,maxid2)
    journals = cursor1.fetchall()
print('2222222222222222222222222222')

exlist = []

jishu=0

print("------------------start---------------------")
for i in journals:
    if i == None:
        break
    i = list(i)
    #筛选无效数据
    if i[1] == '' or len(i[2]) < 10 or chardet.detect(i[2].encode())['encoding'] != 'utf-8':
        i[0] = -1

    #作者字段去除其他信息
    author = i[4]
    #print('author:', author)
    author = author.replace("[^\u4E00-\u9FA5;]", "");
    # while len(author) > 0 and author[0, 1].equals(";"):
    #     author = author[1]
    #     if len(author) < 1:
    #         author = ""

    #语言
    languages = i[5]
    #print('语言', languages)
    if languages == 'ZH':
        language = 1
    if languages == "EN":
        language = 2
    else:
        language = 3
    del i[5]
    i.append(language)

    #category_id
    category_id = ''
    i.append(category_id)

    #出版日期
    journal_no = i[5]
    #print('期刊号',journal_no)
    publication_time = journal_no[:4]
    i.append(publication_time)

    #第一作者id
    author_id = None
    i.append(author_id)

    # 第一作者
    if author == None or len(author) == 0:
        firstAuthor = "未知"
    else:
        aList = author.split(";")
        if len(aList) == 0:
            firstAuthor = "未知"
        else:
            firstAuthor = aList[0]
    i.append(firstAuthor)

    # 第一机构
    author_department = i[6]
    #print('机构',author_department)
    firstdepartment = ''
    for j in school:
        if str(j) in author_department:
            firstdepartment = str(j)
        break
    i.append(firstdepartment)

    #category_cn
    category_cn = ''
    i.append(category_cn)

    #期刊分类
    journal_name = i[7].replace(';', '')
    #print('期刊名',journal_name)
    for j in journal_level:
        j = list(j)
        if journal_name == j[0]:
            journal_level = j[0]
        else:
            journal_level = 'D'
    i.append(journal_level)
    if i[0] != -1:
        exlist.append(i)

    jishu=jishu+1
    print('已经清洗数据条数：',jishu)

j = 1
for i in exlist:
    i[0] = maxid2[0][0]+j
    i[8] = maxid3[0][0]+j
    j=j+1

#print(exlist)
for i in exlist:
    k = 0
    for j in i:
        k = k + 1
        #print(k, j)


cursor4.executemany(sql4, exlist)



db.commit()
cursor1.close()
cursor2.close()
cursor3.close()
cursor4.close()
db.close()