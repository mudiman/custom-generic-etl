from etl.config.loadconfig import COMPANY_CONFIG



service = {}

service['DEVELOPMENT'] = {
      'SERVICE_BASE' : str("http://beta.boostbi.com/newsis"),
      'SCORING_SERVICE_BASE' : str("http://beta.boostbi.com/newsss"),
      'METADATA_SERVICE_BASE' : str(COMPANY_CONFIG['sms']['url']),
      'ROOT_KEY' : str(COMPANY_CONFIG['rootkey']),
      'SOLR_BASE_URL' : str(COMPANY_CONFIG['solr']['DEVELOPMENT']['url'])
      }

service['PRODUCTION'] = {
      'SERVICE_BASE' : str(COMPANY_CONFIG['sis']['url']),
      'SCORING_SERVICE_BASE' : str(COMPANY_CONFIG['sss']['url']),
      'METADATA_SERVICE_BASE' : str(COMPANY_CONFIG['sms']['url']),
      'ROOT_KEY' : str(COMPANY_CONFIG['rootkey']),
      'SOLR_BASE_URL' : str(COMPANY_CONFIG['solr']['PRODUCTION']['url'])
      }


service['TEST'] = {
      'SERVICE_BASE' : str(COMPANY_CONFIG['sis']['url']),
      'SCORING_SERVICE_BASE' : str(COMPANY_CONFIG['sss']['url']),
      'METADATA_SERVICE_BASE' : str(COMPANY_CONFIG['sms']['url']),
      'ROOT_KEY' : str(COMPANY_CONFIG['rootkey']),
      'SOLR_BASE_URL' : str(COMPANY_CONFIG['solr']['TEST']['url'])
      }
