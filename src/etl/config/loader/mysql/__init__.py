import MySQLdb
import MySQLdb.cursors
from etl.config.loadconfig import COMPANY_CONFIG

BATCHQUERYSIZE=10

db = {}

#db['DEVELOPMENT'] = {"adapter": "mysql","host": "localhost","database": "company_service_development2","username": "company","password": "1qaz2wsx0okm9ijn"}
db['DEVELOPMENT']=COMPANY_CONFIG['etl']['DEVELOPMENT']['datastore']

db['PRODUCTION'] = COMPANY_CONFIG['etl']['PRODUCTION']['datastore']


db['TEST'] = COMPANY_CONFIG['etl']['TEST']['datastore']

class Connection(object):
    _instance = None
    
    def __new__(self, *args, **kwargs):
        if self._instance == None:
            
            database = db.get(kwargs['environment'])
            self.mysql_connection = MySQLdb.connect(
                       host=database['host'], user=database['username'],
                       passwd=database['password'], db=database['database'], use_unicode=True, charset="utf8", init_command='SET NAMES UTF8')
            self._instance = "data"
        return self
