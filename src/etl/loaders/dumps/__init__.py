'''
Created on Oct 12, 2011

@author: Mudassar Ali
'''
import cloud
import pickle
import StringIO
import os
from etl.config import logger, ETLDUMPLOCATION
import zlib
import zipfile
import datetime

def get_dump_location():
    #return "/mnt/etl-tmp"
    #return os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
    return ETLDUMPLOCATION

def make_file_name(**kwargs):
    filename = "file"
    filename = filename + "_" + kwargs.get('sellerid', '')
    filename = filename + "_" + kwargs.get('locale', '')
    filename = filename + "_" + str(kwargs.get('categoryid', ''))
    itemid = ""
    if type(kwargs.get('itemid')) is list:
        itemid = kwargs.get('itemid')[0]
    else:
        itemid = kwargs.get('itemid', '')
    filename = filename + "_" + str(itemid)
    filename = filename + "_" + str(kwargs.get('limit', ''))
    filename = filename + "_" + str(kwargs.get('offset', ''))
    filename = filename + "_" + str(kwargs.get('datatype', ''))
    filename = filename + ".txt"
    return filename
    
class DumpLoader():

    
    def loaddumps(self):
        dumplocation = get_dump_location()
        listing = os.listdir(dumplocation)
        #print listing
        categories = []
        items = []
        itemsdetail = []
        itemsbestmatch = []
        for file in listing:
            f = open(os.path.join(dumplocation, file), 'r')
            data = f.read()
            data = pickle.loads(data)
            if file.find('categories') > 0:
                categories += data
            if file.find('_item.') > 0:
                items += data
            if file.find('itemdetail') > 0:
                itemsdetail += data
            if file.find('bestmatch') > 0:
                itemsbestmatch += data
        
        return [categories, items, itemsdetail, itemsbestmatch]
    
    def loaddumps_and_zip(self, filenamelist):
        try:
            dumplocation = get_dump_location()
            listing = os.listdir(dumplocation)
            if len(filenamelist) > 0:
                listing = filenamelist
            categories = []
            items = []
            itemsdetail = []
            itemsbestmatch = []
            os.chdir(dumplocation)
            datatt = "data" + datetime.datetime.now().strftime("%Y-%m-%d@%H%M") + ".zip"
            fl = zipfile.ZipFile(datatt, 'w')
            #print listing
            for file in listing:
                try:
                    if file.find('.txt') < 0:continue
                    filepath = file
                    f = open(filepath, 'r')
                    data = f.read()
                    data = pickle.loads(data)
                    if file.find('categories') > 0:
                        categories += data
                    if file.find('_item.') > 0:
                        items += data
                    if file.find('itemdetail') > 0:
                        itemsdetail += data
                    if file.find('bestmatch') > 0:
                        itemsbestmatch += data
                    fl.write(filepath)
                except Exception, e:
                    logger.exception(str(e))
        except Exception, e:
            logger.exception(str(e))
        finally:
            fl.close()
            return [categories, items, itemsdetail, itemsbestmatch]
    
    def delete_files(self, filenamelist):
        try:
            dumplocation = get_dump_location()
            os.chdir(dumplocation)
            listing = os.listdir(".")
            if len(filenamelist) > 0:
                listing = filenamelist
            for filename in listing:
                try:
                    print filename
                    os.remove(filename)
                except Exception, e:
                    pass
                    #logger.exception(str(e))
        except Exception, e:
            logger.exception(str(e))
            
            
    def serialize(self, listdata):
        try:
            output = StringIO.StringIO()
            temp = pickle.dumps(listdata)
            output.write(temp)
            return output
        except Exception, e:
            logger.exception(str(e))
            
    def write(self, listdict, filename):
        try:
            dumplocation = get_dump_location()
            output = self.serialize(listdict)
            #temp=zlib.compress(output.getvalue())
            with open(os.path.join(dumplocation, filename), 'w') as f:
                f.write(output.getvalue())
        except Exception, e:
            print e
        finally:
            output.close()
    
    def writedata(self, **kwargs):
        filename = make_file_name(**kwargs)        
        data = kwargs.get('data')
        self.write(data, filename)
    
if __name__ == '__main__':
    pass
    t=DumpLoader()
    t.loaddumps()
    
