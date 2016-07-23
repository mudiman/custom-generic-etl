'''
Created on Apr 2, 2012

@author: mudassar
'''

import os
import sys




try:
    import etl
except ImportError:
    
    
    # Get the absolute file path
    p = os.path.abspath(__file__)
    
    # Move 4 steps above. FIXME: This would change in case of relocation.
    for i in range(0, 3):
        p = os.path.split(p)[0]
    # Append to PYTHONPATH
    sys.path.append(p)
    
from etl.system.system import initialize
from etl.system.getstatistics import save_snapshot
from etl.config.loader.mysql import Connection

if __name__ == '__main__':
    Environment = sys.argv[1]
    initialize(Environment)
    save_snapshot(environment=Environment)
    Connection(Environment).mysql_connection.close()
