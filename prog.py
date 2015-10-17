__author__ = 'IrinaPavlova'

import re
from lxml import etree
import sqlite3


punctuation = set('!"#$%&()*+,-./:;<=>?@[\]^_`{|}~—–\'«»0123456789')


def prepare_file(file):
    prepare = open(file, 'r', encoding='utf-8')
    res = prepare.read()
    res = re.sub('<mediawiki .*>', '<mediawiki>', res)
    prepare.close()
    prepare = open(file, 'w', encoding='utf-8')
    prepare.write(res)
    prepare.close()


def parse(file):
    prepare_file(file)
    tree = etree.parse(file)
    return tree


def get_page(file):
    page = parse(file).xpath('.//page')
    return page


def get_title(file):
    for e in get_page(file):
        title = e.xpath('.//title')
        for el in title:
            title = el.text
            yield title


def get_text(file):
    for ele in get_page(file):
        text = ele.xpath('.//revision//text')
        for elem in text:
            text = elem.text
            if text is not None:
                yield text


def get_len_text(file):
    for eleme in get_text(file):
        eleme = eleme.split()
        if eleme is not None:
            yield len(eleme)
        else:
            yield 0


def count_links(file):
    q = re.compile('\[\[[^:]*?\]\]')
    for elemen in get_text(file):
        if elemen is not None:
            elemen = len(q.findall(elemen))
            yield elemen
        else:
            yield 0


def combine_values(file):
    allv = []
    for q, w, e in zip(get_title(file), get_len_text(file), count_links(file)):
        allv.append((q, w, e))
    return allv


def frd(file, obj):
    for h in get_text(file):
        h = ''.join(z for z in h if z not in punctuation)
        h = h.lower()
        h = h.split()
        for word in h:
            if len(obj.f(word).fetchall()) > 0:
                obj.upd(word)
            else:
                obj.insert(word)


class DBArticles():
    def __init__(self, dbname):
        self.connection = sqlite3.connect(dbname)
        self.cursor = self.connection.cursor()
        self.table = 'CREATE TABLE info(title TEXT, length INTEGER, count of links INTEGER)'
        self.cursor.execute(self.table)

    def insert(self, ti, length, links):
        inp = 'INSERT into info VALUES ("{}", {}, {})'.format(ti, length, links)
        self.cursor.execute(inp)
        self.connection.commit()

    def query(self, q):
        query = q.format(q)
        res = self.cursor.execute(query)
        return res


class DBFreq():
    def __init__(self, dbname):
        self.connection = sqlite3.connect(dbname)
        self.cursor = self.connection.cursor()
        self.table = 'CREATE TABLE freq(word TEXT, count INTEGER)'
        self.cursor.execute(self.table)

    def insert(self, w):
        inp = 'INSERT into freq VALUES ("{}", {})'.format(w, 1)
        self.cursor.execute(inp)
        self.connection.commit()

    def upd(self, wor):
        inp = 'UPDATE freq SET count = count + 1 WHERE word = "{}"'.format(wor)
        self.cursor.execute(inp)
        self.connection.commit()

    def f(self, ww):
        query = 'SELECT * FROM freq WHERE word = "{}"'.format(ww)
        query = query.format(query)
        res = self.cursor.execute(query)
        return res


dump = 'test.xml'

database = DBArticles('Articles Info')
for m in combine_values(dump):
    database.insert(m[0], m[1], m[2])

db = DBFreq('Frequency')
frd(dump, db)
