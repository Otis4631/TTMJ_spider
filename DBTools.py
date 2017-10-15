import pymongo
import pymysql
import logging

class Log(object):
    def __init__(self):
        self.logger = logging.getLogger('logger')
        self.logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler('log')
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('[%(asctime)s][%(levelname)s]:  %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

class Mongo(object):
    def __enter__(self):
        return self
    def __init__(self):
        self.client = pymongo.MongoClient(host="127.0.0.1", port=27017)
        self.db = self.client['ttmj']
        self.ttmj = self.db.ttmj
        self.visited_urls = self.db.visited_urls
        self.options = pymongo


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()


class Mysql(object):
    def __enter__(self):
        return  self
    def __init__(self):
        self.conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='lizheng1997', db='ttmj')
        self.cursor = self.conn.cursor()
        init_sql = """
            CREATE TABLE  IF NOT EXISTS ttmj.data(
              id int PRIMARY KEY auto_increment NOT NULL,
              name VARCHAR(100),
              url VARCHAR (100),
              is_new int DEFAULT 1,
              source VARCHAR(10),
              k VARCHAR (20)
            )
            """
        self.cursor.execute((init_sql))
        self.conn.commit()
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

log = Log()