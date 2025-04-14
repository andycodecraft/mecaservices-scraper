#pip install mysql-connector-python
import mysql.connector,re,json
from mysql.connector import Error
from crawldata.functions import *
from datetime import datetime
class CrawldataPipeline:
    def open_spider(self,spider):
        self.DATABASE_NAME='PARTAO_Product_Data'
        self.HOST='partaodb.cpy6cs0k6e9n.eu-central-1.rds.amazonaws.com'
        self.username='admin'
        self.password='dC3EiEq9QO3G65Ztm3XPsIS3bRb0m5n1Yx295t3Qb'
        self.TABLE='A10_rolmax'
        # ALTER TABLE A4_techniekwebshop AUTO_INCREMENT = 1
        try:
            self.conn = mysql.connector.connect(host=self.HOST,database=self.DATABASE_NAME,user=self.username,password=self.password,charset='utf8')
            if self.conn.is_connected():
                print('Connected to DB')
                db_Info = self.conn.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
            else:
                print('Not connect to DB')
        except Error as e:
            print("Error while connecting to MySQL", e)
            self.conn=None
    def close_spider(self,spider):
        if self.conn.is_connected():
            self.conn.close()
    def process_item(self, item, spider):
        sql="SELECT * FROM `"+self.TABLE+"` WHERE original_id='"+item['original_id']+"'"
        try:
            CHK=get_data_db(self.conn,sql)
        except:
            self.conn = mysql.connector.connect(host=self.HOST,database=self.DATABASE_NAME,user=self.username,password=self.password,charset='utf8')
            CHK=get_data_db(self.conn,sql)
        if len(CHK)==0:
            FIELDS=['created_at']
            VALUES=["'"+spider.DATE_CRAWL+"'"]
            for k,v in item.items():
                FIELDS.append(k)
                if k in ('additional_images','tech_spec','reviews'):
                    VALUES.append("'"+str(json.dumps(item[k])).replace("'","''").replace("\\n","\\\\n")+"'")
                else:
                    VALUES.append("'"+str(v).replace("'","''")+"'")
            sql="INSERT INTO `"+self.TABLE+"` ("+(",".join(FIELDS))+") VALUES("+(",".join(VALUES))+")"
            try:
                RUNSQL(self.conn,sql)
            except:
                self.conn = mysql.connector.connect(host=self.HOST,database=self.DATABASE_NAME,user=self.username,password=self.password,charset='utf8')
                RUNSQL(self.conn,sql)
        else:
            UPDATE=[]
            for k,v in item.items():
                if not k in ('original_id'):
                    if k in ('additional_images','tech_spec','reviews'):
                        UPDATE.append(k+"='"+json.dumps(item[k]).replace("'","''").replace("\\n","\\\\n")+"'")
                    else:
                        UPDATE.append(k+"='"+str(v).replace("'","''")+"'")
            sql="UPDATE `"+self.TABLE+"` SET updated_at='"+spider.DATE_CRAWL+"',"+(",".join(UPDATE))+" WHERE original_id='"+item['original_id']+"'"
            try:
                RUNSQL(self.conn,sql)
            except:
                self.conn = mysql.connector.connect(host=self.HOST,database=self.DATABASE_NAME,user=self.username,password=self.password,charset='utf8')
                RUNSQL(self.conn,sql)
            pass
        return item