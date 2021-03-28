# -*- coding: utf-8 -*-
# @Time    : 2021/3/28 16:51
# @Author  : Bruce
# @Email   : daishaobing@outlook.com
# @File    : fenci.py
# @Software: PyCharm



# ---- 文件读取 ----
# 一个迭代器，逐篇输出文章

import re
import pymongo
from tqdm import tqdm
import hashlib


# 数据放在mongo里的
db = pymongo.MongoClient().weixin.text_articles
# 用来去重
md5 = lambda s: hashlib.md5(s).hexdigest()

def texts():
    texts_set = set()
    for a in tqdm(db.find(no_cursor_timeout=True).limit(3000000)):
        if md5(a['text'].encode('utf-8')) in texts_set:
            continue
        else:
            texts_set.add(md5(a['text'].encode('utf-8')))
            for t in re.split(u'[^\u4e00-\u9fa50-9a-zA-Z]+', a['text']): # 去除无意义字符
                if t:
                    yield t


# ---- N-grams计数 ----

from collections import defaultdict
import numpy as np

# 最大n-gram
n = 4
# 最小频率
min_count = 128
ngrams = defaultdict(int)

# 遍历所有文本，对长度1-4的词组进行频率统计
for t in texts():
    for i in range(len(t)):
        for j in range(1, n + 1):
            if i + j <= len(t):
                ngrams[t[i:i+j]] += 1

ngrams = {i:j for i, j in ngrams.items() if j >= min_count}
total = 1. * sum([j for i, j in ngrams.items() if len(i) == 1])


# ---- 凝固度筛选 ----

# 通常阈值成5倍等比数列比较好，也要看数据大小
min_proba = {2:5, 3:25, 4:125}

def is_keep(s, min_proba):
    if len(s) >= 2:
        score = min([total*ngrams[s]/(ngrams[s[:i+1]]*ngrams[i+1:]) for i in range(len(s) - 1)])
        if score > min_proba[len(s)]:
            return True
    else:
        return False

ngrams_ = set(i for i, j in ngrams.items() if is_keep(i, min_proba))

# ---- 切分统计 ----

def cut(s):
    r = np.array([0] * (len(s) - 1))
    for i in range(len(s) - 1):
        for j in range(2, n + 1):
            if s[i:i+j] in ngrams_:
                r[i:i+j-1] += 1
    w = [s[0]]
    for i in range(1, len(s)):
        if r[i-1] > 0:
            w[-1] += s[i]
        else:
            w.append(s[i])
    return w

words = defaultdict(int)
for t in texts():
    for i in cut(t):
        words[i] += 1

words = {i:j for i,j in words.items() if j >= min_count}


# ---- 回溯 ----

def is_real(s):
    if len(s) >= 3:
        for i in range(3, n+1):
            for j in range(len(s)-i+1):
                if s[j:j+i] not in ngrams_:
                    return False
        return True
    else:
        return True

w = {i:j for i, j in words.items() if is_real(i)}
