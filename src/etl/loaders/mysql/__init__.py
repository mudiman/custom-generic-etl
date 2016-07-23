import MySQLdb.cursors
from etl.config.loader.mysql import Connection, BATCHQUERYSIZE
from etl.config import logger, config
import time


def map_record_to_row(headers, record):
    tempdata = []
    for header in headers:
        temp = record.get(header, None)
        if header == "secondarycategoryid" and not temp:
            temp = 0
        if temp == []:
            temp = None
        tempdata.append(temp)
    return tempdata
 

def execute_multiple_query(query, querylist):
#    print query
#    print querylist[0]
#    print querylist[1]
#    print querylist[0]
    fibonic = [1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89,144]
    conn = Connection(environment=config.ENVIRONMENT)
    cursor = MySQLdb.cursors.DictCursor(conn.mysql_connection)
    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
    subquerylist = [querylist[i:i + BATCHQUERYSIZE] for i in range(0, len(querylist), BATCHQUERYSIZE)]
    for lst in subquerylist: 
        try:
            for i in range(0, len(fibonic)):
                try: 
                    cursor.executemany(query, (lst))
                    break
                except Exception, e:
                    print "Retrying "+str(e)
                    time.sleep(fibonic[i])
        except Exception, e:
            logger.exception(str(e))
    conn.mysql_connection.commit()
        
def generic_insert_import(table, headers, records, fieldmap):
    try:
        insertquery = "insert into %s " % table
        newheaders = []
        for field in fieldmap:
            if headers.count(field) > 0:
                newheaders.append(field)

        keys = " ("
        for header in newheaders:
            keys += "`%s`," % fieldmap[header]
        keys = keys[0:len(keys) - 1] + ") "
        
        values = " values ("
        for header in newheaders:
            values += "%s,"
        values = values[0:len(values) - 1] + ") "
        
        
        onduplicatekeyquery = " on duplicate key update "
        for header in newheaders:
            onduplicatekeyquery += "`{0}`=values(`{0}`),".format(fieldmap[header])
        onduplicatekeyquery = onduplicatekeyquery[0:len(onduplicatekeyquery) - 1]
               
        
        data = []
        for record in records:
            tempdata = []
            #[tempdata.append(record.get(header, None)) for header in newheaders]
            tempdata = map_record_to_row(newheaders, record)
            data.append(tuple(tempdata))
        
        insertquery += keys + values + onduplicatekeyquery
        
        return [insertquery, data]
    except Exception, e:
        print str(e)




class MySqlLoader():


    def writedata(self, **kwargs):
        from etl.loaders.mysql.categories import import_categories
        from etl.loaders.mysql.items import import_items
        loadintodict = dict(
                  categories=import_categories,
                  item=import_items,
                  itemdetail=import_items,
                  bestmatch=import_items
                  )
        try:
            datatype = kwargs.get('datatype', '')
            data = kwargs.get('data')
            if len(data) > 0:
                loadintodict[datatype](data)    
        except Exception, e:    
            logger.exception(str(e))

    

    
