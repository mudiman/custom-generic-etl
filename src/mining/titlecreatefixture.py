# coding: utf-8
'''
Created on Jun 27, 2012

@author: mudassar
'''
import sys, re, os


try:
    import etl
except ImportError:

    
    # Get the absolute file path
    p = os.path.abspath(__file__)
    
    # Move 4 steps above. FIXME: This would change in case of relocation.
    for i in range(0, 2):
        p = os.path.split(p)[0]
    # Append to PYTHONPATH
    sys.path.append(p)
    
from etl.dal import get_leaf_categories, get_all_leafcategories, \
    get_all_items_with_brand, get_all_leafcategories_categoryidonly
from mining.titlemining import get_category_recommended_item_specifics, \
    scan_title_for_single_item, seller_items, comparisonalgo, ngram_tokenizer
from mining.fingerprint import ngramfilter
import csv
import StringIO
from collections import OrderedDict
from Queue import Queue
from etl.lib.ThreadPool.ThreadPool import ThreadPool
from etl.miscfunctions import checkbrand


featureheaders = ['positionfromstart', 'positionfromend', 'indentityword', 'hasdigit', 'onlydigit', 'capitalized', \
            'firstcapital', '&', '\xc2\xa3', 'ngram_current_4', 'ngram_current_5', 'ngram_current_6', \
             'twowordsbeforetitle', \
             'twowordsaftertitle', 'ngram_beforeneighbour_4', 'ngram_beforeneighbour_5', 'ngram_beforeneighbour_6', \
              'ngram_afterneighbour_4', 'ngram_afterneighbour_5', 'ngram_afterneighbour_6', 'memberofbrand', \
               'memberofgarmenttype', 'memberofsizes', 'memberofcolors', 'Brand']
#, 'title'
headertag = OrderedDict({'positionfromstart':['c', 0], 'positionfromend':['c', 0], 'indentityword':['d', 0], 'hasdigit':['d', 0], 'onlydigit':['d', 0], 'capitalized':['d', 0], \
            'firstcapital':['d', 0], '&':['d', 0], '\xc2\xa3':['d', 0], 'ngram_current_4':['d', 0], 'ngram_current_5':['d', 0], 'ngram_current_6':['d', 0], \
             'twowordsbeforetitle':['d', 0], \
             'twowordsaftertitle':['d', 0], 'ngram_beforeneighbour_4':['d', 0], 'ngram_beforeneighbour_5':['d', 0], 'ngram_beforeneighbour_6':['d', 0], \
              'ngram_afterneighbour_4':['d', 0], 'ngram_afterneighbour_5':['d', 0], 'ngram_afterneighbour_6':['d', 0], 'memberofbrand':['d', 0], \
               'memberofgarmenttype':['d', 0], 'memberofsizes':['d', 0], 'memberofcolors':['d', 0], 'Brand':['d', 1]})
#, 'title':['i', 0]

maxngram = 3
algo = "levenstien_distance"
Brandspecific="Brand"

finalq = Queue()
threadpool = ThreadPool(50)

def findtoken_indentity(token, algo, itemspecifics):
    for key, value in itemspecifics.items():
        for val in value:
            if comparisonalgo[algo](token=token, match=val):
                return key, val
    return None, None


def convert_title_into_feature(title, itemspecifics, tag):
    itemattr = generate_indentities_from_title(title, itemspecifics)
    return make_features(title, itemattr, tag)

def calbackfunc(data):
    finalq.put(data)
    
def threadjob(**kwargs):
    title = kwargs.get("title")
    itemspecifics = kwargs.get("itemspecifics")
    brand = kwargs.get('brand')
    itemattr = generate_indentities_from_title(title, itemspecifics)
    data = make_features(title, itemattr, brand)
    return data

def get_all_items_features(sellerid=None, locale=None, Environment=None, csvoutput=None, trainoncategoryid=None):
    totalitems = 0
    leafcategories = []
    categories = get_all_leafcategories_categoryidonly(sellerid=sellerid)
    categoriesnamelist = []
    itemgender = ['Mens', 'Womens', 'Men', 'Women']
    for category in categories:
            categoriesnamelist.append(category['name'])
            leafcategories.append(category['categoryid'])
    
    leafcategories = list(set(leafcategories))
    for category in categories:
        try:
            categoryid = category['categoryid']
            if trainoncategoryid:
                if long(trainoncategoryid) != categoryid:
                    continue
            itemspecifics = get_category_recommended_item_specifics(categoryid, locale)
            itemspecifics['Garment Type'] = categoriesnamelist
            itemspecifics['Gender'] = itemgender
            items = get_all_items_with_brand(categoryid=categoryid, sellerid=sellerid)
            print "fetching data for categoryid %s with items %s" % (categoryid, len(items))
            totalitems += len(items)
            for item in items:
                try:
                    threadpool.add_task(threadjob, title=item['title'], itemspecifics=itemspecifics,brand=item['brand'],callback=calbackfunc)
        #            itemattr = generate_indentities_from_title(item['title'], itemspecifics)
                    #features = make_features(item['title'], itemattr, item['brand'])
#                    for afeature in features:
#                        writeheader(afeature, featureheaders)
                except Exception, e:
                    print e
        except Exception, e:
            print e
    threadpool.wait_completion()
    
    while finalq.unfinished_tasks!=0:
        features=finalq.get()
        for afeature in features:
            writeheader(afeature, featureheaders)
        finalq.task_done()
            
    print "total items %d" % totalitems


def comparision_with_token(tokens, match, algo):
    for gram in range(0, maxngram):
        for token in tokens[gram]:
            res = comparisonalgo[algo](token=token, match=match)
            if res:
                return token
    return None


def find_replace_foundtoken(titletokenidentities, newtoken, data):
    if newtoken in titletokenidentities.keys():
        titletokenidentities[newtoken] = data
        return titletokenidentities
    else:
        newdict = OrderedDict()
        keys = newtoken.split(" ")
        found = False
        for key in titletokenidentities:
            if not key in keys:
                newdict[key] = titletokenidentities[key]
            else:
                if not found:
                    newdict[newtoken] = data
                found = True
        return newdict
    
def generate_indentities_from_title(title, itemspecifics):
    final = OrderedDict()
    temptokens = title.split(" ")
    for ttt in temptokens:
        final[ttt] = {}
    tokens = ngram_tokenizer(title, maxngram)
    for key, value in itemspecifics.items():
        for val in value:
            newtoken = comparision_with_token(tokens, val, algo)
            if newtoken:
                final = find_replace_foundtoken(final, newtoken, {key:val})
    return final
    
    

def make_features(title, titletokenidentities, brand):
    tokens = titletokenidentities.keys()
    rows = []
    for token in tokens:
        try:
            temp = OrderedDict()
            #postion features
            temp['positionfromstart'] = title.find(token)
            temp['positionfromend'] = len(title) - title.find(token)
            #orthographic features
            if len(titletokenidentities.get(token, {}).keys()) > 0:
                temp['indentityword'] = checkbrand(titletokenidentities.get(token, {}).keys()[0])
            else:
                temp['indentityword'] = None
            if re.match('\d', str(token)) > 0:
                temp['hasdigit'] = 1
            else:
                temp['hasdigit'] = 0
            if re.match('\D', str(token)) > 0:
                temp['onlydigit'] = 0
            else:
                temp['onlydigit'] = 1
            if token == token.upper():
                temp['capitalized'] = 1
            else:
                temp['capitalized'] = 0
            if token == token.title():
                temp['firstcapital'] = 1
            else:
                temp['firstcapital'] = 0
            if token == "&":
                temp['&'] = 1
            else:
                temp['&'] = 0
            if str(token) == str("£"):
                temp['£'] = 1
            else:
                temp['£'] = 0
                
            temp['ngram_current_4'] = ngramfilter(token, 4)
            temp['ngram_current_5'] = ngramfilter(token, 5)
            temp['ngram_current_6'] = ngramfilter(token, 6)
            
            
            #contextfeatures
            
            dummy = title[temp['positionfromend'] + 1:len(title)]
            dummy = dummy.split(" ")
            if len(dummy) >= 2:
                temp["twowordsbeforetitle"] = dummy[0] + " " + dummy[1]
            else:
                temp["twowordsbeforetitle"] = dummy[0]
            
            dummy = title[0:temp['positionfromstart'] - 1]
            dummy = dummy.split(" ")
            if len(dummy) >= 2:
                temp["twowordsaftertitle"] = dummy[len(dummy) - 1] + " " + dummy[len(dummy) - 2]
            else:
                temp["twowordsaftertitle"] = dummy[len(dummy) - 1]
            
            beforeneighbour = temp["twowordsbeforetitle"].split(" ")
            temp['ngram_beforeneighbour_4'] = ngramfilter(beforeneighbour[len(beforeneighbour) - 1], 4)
            temp['ngram_beforeneighbour_5'] = ngramfilter(beforeneighbour[len(beforeneighbour) - 1], 5)
            temp['ngram_beforeneighbour_6'] = ngramfilter(beforeneighbour[len(beforeneighbour) - 1], 6)
            
            afterneighbour = temp["twowordsaftertitle"].split(" ")
            temp['ngram_afterneighbour_4'] = ngramfilter(afterneighbour[0], 4)
            temp['ngram_afterneighbour_5'] = ngramfilter(afterneighbour[0], 5)
            temp['ngram_afterneighbour_6'] = ngramfilter(afterneighbour[0], 6)
            
            #dictionary feactures
            
            for key, value in titletokenidentities.items():
                try:
                    count = 0
                    temp["memberofbrand"] = 0
                    temp["memberofgarmenttype"] = 0
                    temp["memberofsizes"] = 0
                    temp["memberofcolors"] = 0
                    
                    if value == "Brand":
                        temp["memberofbrand"] = 1
                        count += 1
                    if value == "Color":
                        temp["memberofcolors"] = 1
                        count += 1
                    if value == "Garment Type":
                        temp["memberofgarmenttype"] = 1
                        count += 1
                    if value == "Size":
                        temp["memberofsizes"] = 1
                        count += 1
                    if count > 1:
                        temp["memberofbrand"] = 0
                        temp["memberofgarmenttype"] = 0
                        temp["memberofsizes"] = 0
                        temp["memberofcolors"] = 0
                except Exception,e:
                    print e
            
            # label
            if brand in (None,""," "):
                continue
            temp['Brand'] = brand
            temp['title'] = title
            
            rows.append(temp)
            
        except Exception,e:
            print e
    return rows


def writeheader(item, headers):
    temp = []
    for header in headers:
        try:
            tt=item.get(header,"")
            tt=str(tt).replace(",","")
            tt=str(tt).replace("\"","")
            tt=str(tt).replace("\'","")
            tt=re.sub('\t', " ", tt)
            temp.append(tt)
        except Exception,e:
            print e
    csvoutput.writerow(temp)
    
def generate_csv(name=None):
    if not name:
        name = "features.csv"
    else:
        pass
        name = name + ".csv"
    f = open(name, 'w')

    print "writing csv"
    csvoutput = csv.writer(f, delimiter="\t", escapechar='`', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    headers = featureheaders
    datatypes = []
    classfield = []
    for header in headers:
        datatypes.append(headertag.get(header,[""])[0])
        if headertag.get(header,["",""])[1] == 1:
            classfield.append("class")
        else:
            classfield.append("")
        
    csvoutput.writerow(headers)
    csvoutput.writerow(datatypes)
    csvoutput.writerow(classfield)

    return f, csvoutput
    
if __name__ == '__main__':
    from etl.system.system import initialize
    Environment = "DEVELOPMENT"
    initialize(Environment)
    
    algo = "levenstien_distance"
    maxngram = 3
    specifickey = None
    ngram = 2
    sellerid = None
    
    locale = sys.argv[1]
    if len(sys.argv) >= 3:
        sellerid = sys.argv[2]  

    if len(sys.argv) >= 4:
        path = sys.argv[3]
        
    trainoncategoryid = None
    if len(sys.argv) >= 5:
        trainoncategoryid = sys.argv[4]
    
    
    if locale=="DE":
        Brandspecific=u"Marke"
    fil, csvoutput = generate_csv(path+sellerid+"_features")
    final = get_all_items_features(sellerid=sellerid, locale=locale, Environment=Environment, csvoutput=csvoutput, trainoncategoryid=trainoncategoryid)
    fil.close()
    cmd="sudo rm -R /tmp/"+sellerid+"*_test.csv"
    os.system(cmd)
    
#    threadpool.wait_completion()
#    final=[]
#    while not finalq.empty():
#        final+=finalq.get()
        
    
