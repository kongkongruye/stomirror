# encoding: utf-8
# 适合群晖DS218j 上使用

import shutil

import os
import re
import sys
import httplib2
import csv

is_removecsv = False  # 是否删掉CSV文件
report_type = {'利润表': {'url': 'http://quotes.money.163.com/service/lrb_%s.html'},
               '主要财务指标': {'url': 'http://quotes.money.163.com/service/zycwzb_%s.html'},
               '资产负债表': {'url': 'http://quotes.money.163.com/service/zcfzb_%s.html'},
               '财务报表摘要': {'url': 'http://quotes.money.163.com/service/cwbbzy_%s.html'},
               '盈利能力': {'url': 'http://quotes.money.163.com/service/zycwzb_%s.html?part=ylnl'},
               '偿还能力': {'url': 'http://quotes.money.163.com/service/zycwzb_%s.html?part=chnl'},
               '成长能力': {'url': 'http://quotes.money.163.com/service/zycwzb_%s.html?part=cznl'},
               '营运能力': {'url': 'http://quotes.money.163.com/service/zycwzb_%s.html?part=yynl'},
               '现金流量表': {'url': 'http://quotes.money.163.com/service/xjllb_%s.html'}}

h = httplib2.Http('.cache')

# 矩阵转置
def rotate(matrix):
    return map(list, zip(*matrix))

# 下载csv文件，symbol是股票代码比如600690，report_name是表名
def download(symbol, report_name):
    # 下载文件
    resp, content = h.request(report_type[report_name]['url'] % symbol)

    filename = 'csv/%s/%s.csv' % (symbol, report_name)
    report_file = open(filename, 'w')

    content = content.decode('gbk')
    content = content.strip()

    rows = [r for r in content.split('\n')]
    tab = [row.strip().strip(',').split(',') for row in rows]
    tab = rotate(tab)
    for r in tab:
        report_file.write(','.join(r).encode('utf-8'))
        report_file.write('\n')

    report_file.close()


import pymysql


class DBInvestment:
    conn = pymysql.connect(host='127.0.0.1', unix_socket='/run/mysqld/mysqld10.sock',
                           user='root', passwd='',
                           db='investment',
                           charset='utf8')  # 数据库连接
    cur = conn.cursor()

    field_name2id = {}

    insert_data = "INSERT INTO t_163_data VALUES (%s,%s,%s,%s) "

    #
    def load_fields(self):
        # 类目载入
        self.cur.execute('SELECT * FROM t_163_item')
        for row in self.cur:
            self.field_name2id[row[1] + '.' + row[2]] = row[0]
            # print(self.field_name2id)

    def fieldname2id(self, report, name):
        return self.field_name2id[report + '.' + name]

    def import_csv(self, symbol, report_name):
        file_name = 'csv/%s/%s.csv' % (symbol, report_name)
        with open(file_name) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                for k, v in row.items():
                    try:
                        print(','.join([symbol, row['报告日期'], k, v]))
                        print(self.fieldname2id(report_name, k))
                    except KeyError:
                        print(','.join([symbol, row['报告期'], k, v]))


def main():
    joozy = input('下载网易股票财报，股票代码:')

    pattern = re.compile(r'(\d{6})')
    match = pattern.findall(str(joozy))

    if len(match) == 0:
        pass
    else:
        for symbol in match:
            print('# --------------------------------------------')
            if not os.path.exists('csv/' + symbol):
                os.makedirs('csv/' + symbol)

            #下载各种报表
            db = DBInvestment()
            db.load_fields()

            for r in report_type:
                print('download %s %s' % (symbol, r))
                download(symbol, r)
                # DBInvestment().import_csv(symbol, r)
                db.import_csv(symbol, r)

        if is_removecsv:
            shutil.rmtree('csv/' + symbol)


if __name__ == '__main__':
    sys.exit(main())