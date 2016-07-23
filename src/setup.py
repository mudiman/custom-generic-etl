'''
Created on Oct 6, 2011

@author: Mudassar Ali
'''

from setuptools import setup, find_packages
import pkg_resources 
from pkg_resources import require, DistributionNotFound, VersionConflict
import sys


version = pkg_resources.safe_version("0.1")


#try:
#    require('ConflictingDistribution')
#
#    print 'You have ConflictingDistribution installed.'
#    print 'You need to remove ConflictingDistribution from your site-packages'
#    print 'before installing this software, or conflicts may result.'
#    sys.exit()
#    
#except (DistributionNotFound, VersionConflict):
#    pass


setup(name='Custom-etl',
      version=version,
      #namespace_packages=['company'],
      install_requires=["MySQL-python",
                         "elementtree"],
#       dependency_links = [
#        "http://www.company.com/ourpackage"
#      ],
      packages=find_packages(),
      zip_safe=False,
      entry_points={
        'console_scripts': [
            'Sovoia-etl = etl',
            ],
        },
		  # metadata for upload to PyPI
		author="Mudassar",
		author_email="mail@company.com",
		description="This is an etl  Package",
		keywords="Custom ETL service",
		url="http://www.company.com/", # project home page, if any
        
      )
