'''
Created on Nov 10, 2011

@author: Mudassar Ali
'''
import os

ETL_WATSON_DEPLOYMENT_PATH = os.environ['WORKSPACE']
ETL_STAGGING_DEPLOYMENT_PATH = os.environ['ETL_STAGGING_DEPLOYMENT_PATH']

def run_system_command(cmd, prn=True):
    if prn == True:
        print cmd
    os.system(cmd)


def deploy_in_staging():
    cmd = """
        rsync -Hrvae 'ssh -i /ssh/company-new.pem' %s root@staging.company.com:%s --exclude '.*'
    """ % (ETL_WATSON_DEPLOYMENT_PATH, ETL_STAGGING_DEPLOYMENT_PATH)
    run_system_command(cmd)
    
    
def update_deployed_service():
    """
        company service pull
    """
    os.chdir(ETL_WATSON_DEPLOYMENT_PATH)
    run_system_command("git stash", True)
    run_system_command("git pull", True)


if __name__ == '__main__':
    update_deployed_service()
    run_system_command("chmod 777 -R %s/src/etl/config/*.log" % ETL_WATSON_DEPLOYMENT_PATH)
    run_system_command("chmod 777 -R %s/src/etl/loaders" % ETL_WATSON_DEPLOYMENT_PATH)   
    if os.environ['staging'] == "True":
        deploy_in_staging()
    
