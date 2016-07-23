#'''
#Created on Oct 28, 2011
#
#@author: Mudassar Ali
#'''
#from etl.loaders.sqlite import SQLLiteLoader
#import cloud
#from etl.loaders.dumps import make_file_name, get_dump_location
#import os
#import sqlite3
#
#
#
#class SQLlitePiCloudloader(SQLLiteLoader):
#    
#    
#    def writedata(self, **kwargs):
#        path=SQLLiteLoader.writedata(self, **kwargs)
#        filename = make_file_name(**kwargs)
#        cloud.files.put(path, filename)
#    
#    
#    def loaddumps(self):
#        dumplocation = get_dump_location()
#        listing = os.listdir(dumplocation)
#        categories = []
#        items = []
#        itemsdetail = []
#        itemsbestmatch = []
#        for file in listing:
#            conn=sqlite3.connect(os.path.join(dumplocation, file))
#            cursor=conn.cursor()
#            if file.find('categories') > 0:
#                cursor.execute("select * from categories")
#                categories += cursor.fetchall()
#            if file.find('_item.') > 0:
#                cursor.execute("select * from items")
#                items += cursor.fetchall()
#            if file.find('itemdetail') > 0:
#                cursor.execute("select * from items")
#                itemsdetail += cursor.fetchall()
#            if file.find('bestmatch') > 0:
#                cursor.execute("select * from items")
#                itemsbestmatch += cursor.fetchall()
#        
#        return [categories, items, itemsdetail, itemsbestmatch]
#        
