'''
Created on Jul 2, 2012

@author: mudassar
'''

import orange
import sys

from mining.titlemining import get_category_recommended_item_specifics
from string import lower
from mining.titlecreatefixture import make_features, generate_csv, \
    generate_indentities_from_title

locale = "GB"



def convert_title_and_load_instance(title, leafcategoryid=None):
    titletokenidentities = {}
    brand = None
    if leafcategoryid:
        itemspecifics = get_category_recommended_item_specifics(leafcategoryid, locale)
        titletokenidentities = generate_indentities_from_title(title, itemspecifics)
    else:
        tokens = title.split(" ")
        for token in tokens:
            titletokenidentities[token] = {}
    temp = make_features(title, titletokenidentities, brand)
    generate_csv(temp, "test.tab")
    test = orange.ExampleTable("test")
    return test
        
def test_accuracy():
    correct = 0.0
    for ex in data:
        if classifier(ex) == ex.getclass():
            correct += 1
    print "Classification accuracy:", correct / len(data)
        
        
def classify_testdata(test, classifier):
    temp = []
    for i in range(len(test)):
        c = classifier(test[i])
        temp.append(c.value)
        print "original", test[i].getclass(), "classified as", c
    return temp
    
    

def tag_brand_from_title(title, brandlst):
    tempset = set(brandlst)
    tempset = list(tempset)
    maxname = ""
    maxfrequency = 0
    res = {}
    for item in tempset:
        res[item] = brandlst.count(item)
        if brandlst.count(item) > maxfrequency and lower(title).find(lower(item)) > -1:
            maxname = item
            maxfrequency = brandlst.count(item)
                
    print "'%s' has Brand '%s'" % (title, maxname)
    with open('result', 'w') as f:
        f.write(maxname)
         
    
if __name__ == "__main__":
    from etl.system.system import initialize
    Environment = "DEVELOPMENT"
    initialize(Environment)
    
    data = orange.ExampleTable("features.csv")

    filename = "/home/mudassar/workspace/srs/src/company/business/classifiers/baysen_orange_classifier.pck"
    import cPickle
    classifier = cPickle.load(open(filename, "rb"))
        
#    title=sys.argv[1]
#    leafcategoryid=None
#    if len(sys.argv)>2:
#        leafcategoryid=sys.argv[2]
#
#    test=convert_title_and_load_instance(title,leafcategoryid)
#    
#    classifier = orange.BayesLearner(data)
    test_accuracy()
#    brandlst=classify_testdata(test, classifier)
#    tag_brand_from_title(title, brandlst)
