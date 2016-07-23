import os
import json
from datetime import date
from etl.config.loadconfig import COMPANY_CONFIG


def get_dump_location(date="", type=""):
    if date!="":
        if type:
            return os.path.join(COMPANY_CONFIG['etl']['dumps'], date, type)
        else:
            return os.path.join(COMPANY_CONFIG['etl']['dumps'], date)
    else:
        return COMPANY_CONFIG['etl']['dumps']
    


def readsummary(date="", type=None):
    dumplocation = get_dump_location(date, type)
    data = ""
    with open(os.path.join(dumplocation, "summary.json"), 'r') as f:
        data = f.read()
    return json.loads(data)

def readfiltersummary(date="", type=None):
    dumplocation = get_dump_location(date, type)
    data = ""
    with open(os.path.join(dumplocation, "filtersummary.json"), 'r') as f:
        data = f.read()
    return json.loads(data)
            
def create_new_date_directory():
    dumppath = get_dump_location()
    os.chdir(dumppath)
    dirname = date.today().strftime("%d-%m-%Y")
    os.mkdir(dirname, 0777)
    os.chdir(os.path.join(dumppath, dirname))
    os.mkdir("data", 0777)
    os.mkdir("notification", 0777)
    
def copyfile(data, date, type, filename,newfilename):
    try:
        dumplocation = get_dump_location(date, type)
        os.system("cp "+os.path.join(dumplocation, filename)+" "+os.path.join(dumplocation, newfilename))
    except Exception, e:
        print e
    finally:
        pass
    
def writedump(data, date, type, filename):
    try:
        dumplocation = get_dump_location(date, type)
        with open(os.path.join(dumplocation, filename), 'w') as f:
            f.write(data)
    except Exception, e:
        print e
    finally:
        pass
    

if __name__ == "__main__":
    #create_new_date_directory()
    #print readsummary(date="24-04-2012",type="notifications")
    writedump(data="asdasdasdsadsa", date="24-04-2012", type="data", filename="item.xml")
