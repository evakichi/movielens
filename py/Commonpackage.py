import sys
import os
from datetime import datetime
import math
import boto3
from elasticsearch import Elasticsearch
from multiprocessing import Process
import logging
import hashlib
from pymongo import MongoClient
import pandas as pd 
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as pl

#####
# 共用ルーチンをパッケージしたものです
#####



def get_hash(core):
    """文字列を小文字化してそれをSHA256でハッシュ化したものです。
    
    :param core: 変換前の文字列
    
    :rtype: SHA256でハッシュ化された文字列 str
    """
    return hashlib.sha256(core.lower().replace('&lt;','<').replace('&gt;','>').strip().encode('utf-8')).hexdigest()

def get_date_fstring():
    """現時刻を赤色表示するルーティンです。
    :rtype: 現時刻を赤色表示できる文字列 str
    """
    now = datetime.now()
    return f'\033[31m{now}\033[0m'


def get_jsonline(*keys,jsonline=None):
    """メタデータのJSONから目的のデータを抽出するルーティンです。

    アルゴリズム
    1. メタデータのキーの種類だけループを回します
      1. メタデータにキーのもののデータがあればそれをデータとして追記します。
      2. ない場合は何もしない。もしくは特殊なケースとしてauthorが無ければダミーを入れます

    :param *keys: メタデータのキーの集合
    :param jsonfile: JSONファイルの中身

    :rtype: 抽出したデータの列 list
    """
    newline = {}
    if 'status' in jsonline:
        parsed = jsonline['message']
    else:
        parsed = jsonline
    for key in keys:
        if key in parsed:
            newline[key]=parsed[key]
        else:
            if 'author' == key:
                newline['author'] = [{"given": "Noname", "family": "Nanashi", "sequence": "first", "affiliation": []}]
            elif 'subject' == key:
                newline['subject'] = ['No subject']
            elif 'title' == key:
                newline['title'] = ['No title']
            elif 'published' == key:
                newline['published'] = [{"date-parts":[[9999,12,31]]}]
            else:
                return None
    return newline
  
def get_args(*args):
    """引数処理をするルーティンです。

    アルゴリズム
    1. 期待している引数と実際の引数の数が違う倍は標準エラー出力に期待している引数は何かということを示します。
    2. 数が一致している場合はその引数をタプルで返します

    :param *args: 引数データ

    :rtype: 抽出した引数データのタプル tuple
    """
    count=len(args)
    ARGV = sys.argv
    if (count+1) != len(ARGV):
        print ('Usage: python3 ',ARGV[0],file=sys.stderr,end=' ')
        for arg in args:
            print (arg,file=sys.stderr,end=' ')
        print(file=sys.stderr)
        sys.exit(255)
    else:
        res = (i for i in ARGV)
        return res
    
def get_time_fstring(time):
    """整形された時間を緑色で表示するルーティンです。

    :param time: 時間

    :rtype: 時間(秒)を日・時・分・秒に変換してそれを緑の文字列として返却します。 str
    """
    return f'\033[32m{math.floor(time/(3600*24)):,} day {(math.floor(time/3600))%24:,} hour {math.floor(time/60)%60:0>2} min {time%60:.3f} sec({time:,.3f} sec)\033[0m'
        
def get_counter(v,divisor):
    """並列処理に必要な全体の処理数/スレッド数で大まかな繰りかえし数を取得します

    :param v: 全処理数
    :param divisor: スレッド数

    :rtype: 繰り返さなければいけない数 int
    """
    count = len(v)
    iter = math.ceil(count/divisor)
    return count,iter

def remain(current,iter,whole,divisor):
    """主に並列処理に必要な繰りかえし最後の時の端数を計算します。繰りかえし途中の場合はスレッド数を返します。

    :param current: 現在のくりかえし回数
    :param iter: 全繰りかえし回数
    :param whole: 全処理数
    :param divisor: スレッド数

    :rtype: 繰り返さなければいけない数 int
    """
    if current != iter-1 or whole % divisor == 0:
        return divisor
    return (whole % divisor)

def get_environment_val(env):
    """環境変数を取得します、ない場合はエラーを返します。

    :param env: 知りたい環境変数名

    :rtype: 環境変数 str
    """
    if os.environ.get(env) == None:
        print('\033[31mError: No Environment val "'+env+'"\033[0m',file=sys.stderr)
        sys.exit(230)
    else:
        return os.environ.get(env)

def connect_minio():
    """MinIOに接続する際の情報を作成します。

    :rtype: MinIO接続情報 boto3.client
    """
    return boto3.client(
        "s3",
        endpoint_url = get_environment_val('MINIO_ENDPOINT_URL')   ,
        aws_access_key_id = get_environment_val('MINIO_ACCESS_KEY_ID'),
        aws_secret_access_key = get_environment_val('MINIO_SECRET_ACCESS_KEY')
    )

def connect_elasticsearch():
    """Elasticsearchに接続する際の情報を作成します。

    :rtype: Elasticsearch接続情報 Elasticsearch
    """
    return Elasticsearch(
        get_environment_val('ELASTIC_SERVER_URL'),
        ssl_assert_fingerprint=get_environment_val('ELASTIC_SSL_FINGERPRINT'),
        basic_auth=(get_environment_val('ELASTIC_USER'),get_environment_val('ELASTIC_PASSWORD'))
    )

def connect_mongodb():
    """MongoDBに接続する際の情報を作成します。

    :rtype: MongoDB接続情報 mongodb.connection
    """
    mongo_client = MongoClient(get_environment_val('MONGODB_URL'))
    try:
        mongo_client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        return mongo_client
    except Exception as e:
        print(e)
        exit()

def get_current():
    """現時刻のタイムスタンプを返します。

    :rtype: 現時刻のタイムスタンプ float
    """
    return datetime.timestamp(datetime.now())

def get_elapsed_time(func):
    """関数全体の実行時間を計測するデコレータ
    """
    def wrapper(*args,**kwargs):
        start = get_current()
        res = func(*args,**kwargs)
        end = get_current()
        print(func.__name__+' Elapsed time is '+get_time_fstring(end-start))
        return res
    return wrapper

@get_elapsed_time
def get_all_list(s3:boto3.client,bucket:str ,prefix:str):
    """バケットbucket内の特定のディレクトリprefix内のデータ一覧を取得するルーチン(非再帰版)です。
    boto3ではファイルが1,000個以上になったときにはその1,000個に達したところで次の1,000個をもう一度検索する、
    と言うことが必要になります。

    :param s3: 検索したい対象のMinIO接続情報
    :param bucket: 検索したい対象のバケット
    :param prefix: バケット内のディレクトリ(絶対パス) 

    :rtype: 全ファイルを格納したlist
    """
    keys=[]        
    results = s3.list_objects(Bucket=bucket,Prefix=prefix,Marker='')
    counter = 0
    while True:
        if 'Contents' in results:
            keys.extend(content['Key'] for content in results['Contents'])
            if 'IsTruncated' in results:
                results = s3.list_objects(Bucket=bucket,Prefix=prefix,Marker=keys[-1])
            else:    
                break
        else:
            break
    return keys

def exist_object(s3:boto3.client,bucket:str ,prefix:str):
    """MinIOにデータがあるか調べるルーティンです。

    :param s3: MinIO接続情報
    :param bucket: 検索したい対象のバケット
    :param prefix: 検索したい対象のファイル 

    :rtype: 存在の有無 boolan
    """
    response=s3.list_objects(Bucket=bucket,Prefix=prefix)
    if 'Contents' in response:
        return True
    return False

def download_file(s3,bucket,key,file):
    """MinIOからデータをダウンロードします。

    :param s3: MinIO接続情報
    :param bucket: ダウンロードしたい対象のバケット
    :param key: ダウンロードしたい対象のファイル 
    :param file: 保存先名 

    :rtype: 存在の有無 boolan
    """
#    print (bucket,key,file)
    start_time = get_current()
    if exist_object(s3,bucket,key):
        s3.download_file(bucket,key,file)
        string = f'file "{file}" was downloaded.'
    else:
        string = f'file "{file}" does not exist.'
    end_time = get_current()
    print(string,' ('+get_time_fstring(end_time-start_time)+')')

def mkemptydir(path):
    """空のフォルダを作成します、フォルダが存在する場合はエラーを返します。

    :param path: 作成したいフォルダ名 

    """
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        print(f'directory "{path}" exists!!')
        sys.exit(255)

@get_elapsed_time
def get_hashlist_from_es(es,index,q):
    """Elasticsearchへクエリーを投げてその結果を返すルーティンです。

    :param es: Elasticsearch接続情報
    :param index: 検索したい対象のインデックス
    :param q: クエリ(JSON) 

    :rtype: ソートされた結果(DOIのハッシュ値) list
    """
    data = list()
    hash_list = list()
    result = es.search(index=index,scroll='2m',query=q,size=10000)
    while True:
        if len(result['hits']['hits']) == 0 :
            break
        else:
            sid = result['_scroll_id']
            for hit in result['hits']['hits']:
                data.append(hit)
            result = es.scroll(scroll_id = sid,scroll='2m')
    for hit in result['hits']['hits']:
        data.append(hit)
    counter = 1
    for hit in data:
        print(f"count = {counter}\t",f"DOI = {hit['_source']['DOI']}\t",f"HASH = {hit['_source']['HASH']} ")
        hash_list.append(hit['_source']['HASH'])
        counter += 1
    return sorted(hash_list)