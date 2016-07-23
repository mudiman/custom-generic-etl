# encoding: utf-8
import os
from datetime import date
from elementtree.ElementTree import fromstring
import codecs
import json
from esync.dumps import get_dump_location




def deducting_item(date="", **args):
    
    p_rel = get_dump_location()
    date_str = date
    path = p_rel + "/%s/notifications/" % (str(date_str))
    listing = os.listdir(path)
    files_allowed = ["ItemRevised", "ItemSold", "ItemListed", "ItemClosed"]
    i = 1
    excp = 0
    summary = {}
    temp_list = []
    item_count = 1
    temporary_dict = {"ItemOther":[], "ItemClosed":[]}
    no_of_files = len(listing)
    if "summary.json" in listing:
        no_of_files = no_of_files - 1
    if "inbox_progress.txt" in listing:
        no_of_files = no_of_files - 1
    if "summary_progress.txt" in listing:
        no_of_files = no_of_files - 1
    for infile in listing:
        filename = infile.split("_", 1)[0]            
        if filename in files_allowed:
            with codecs.open(path + "%s" % (infile), "r") as f:
                temp = f.read()
                temp = temp.replace("", "=")
                try:
                    rootElement = fromstring(temp)
                except Exception, e:
                    print " ia m here"
                    print e
                
                itemElement_root = rootElement.find("{http://schemas.xmlsoap.org/soap/envelope/}Body")
                itemElement = itemElement_root.find("{urn:ebay:apis:eBLBaseComponents}GetItemResponse")
                itemElement_it = itemElement.find("{urn:ebay:apis:eBLBaseComponents}Item")
                itemElement = itemElement_it.find("{urn:ebay:apis:eBLBaseComponents}Seller")
                seller_id = itemElement.find("{urn:ebay:apis:eBLBaseComponents}UserID").text
                cat_id = itemElement_it.find("{urn:ebay:apis:eBLBaseComponents}PrimaryCategory/{urn:ebay:apis:eBLBaseComponents}CategoryID").text
                item_id = itemElement_it.find("{urn:ebay:apis:eBLBaseComponents}ItemID").text
                locale = itemElement_it.find("{urn:ebay:apis:eBLBaseComponents}Country").text
                c_seller_id = seller_id + "@" + locale
                if filename == "ItemClosed":
                    temporary_dict[filename].append(item_id)
                else:
                    temporary_dict["ItemOther"].append(item_id)

    for infile in listing:
        try:
            filename = infile.split("_", 1)[0]            
            if filename in files_allowed:
                print str(i) + ":" + infile
                with codecs.open(path + "%s" % (infile), "r") as f:
                    temp = f.read()
                    temp = temp.replace("", "=")
                    rootElement = fromstring(temp)
                    
                    itemElement_root = rootElement.find("{http://schemas.xmlsoap.org/soap/envelope/}Body")
                    itemElement = itemElement_root.find("{urn:ebay:apis:eBLBaseComponents}GetItemResponse")
                    itemElement_it = itemElement.find("{urn:ebay:apis:eBLBaseComponents}Item")
                    itemElement = itemElement_it.find("{urn:ebay:apis:eBLBaseComponents}Seller")
                    seller_id = itemElement.find("{urn:ebay:apis:eBLBaseComponents}UserID").text
                    cat_id = itemElement_it.find("{urn:ebay:apis:eBLBaseComponents}PrimaryCategory/{urn:ebay:apis:eBLBaseComponents}CategoryID").text
                    item_id = itemElement_it.find("{urn:ebay:apis:eBLBaseComponents}ItemID").text
                    locale = itemElement_it.find("{urn:ebay:apis:eBLBaseComponents}Country").text
                    c_seller_id = seller_id + "@" + locale
#                    if filename=="ItemClosed":
#                        temporary_dict[filename].append(item_id)
#                    else:
#                        temporary_dict["ItemOther"].append(item_id)
                    if c_seller_id not in summary.keys():
                        summary[c_seller_id] = {}
                    if cat_id not in summary[c_seller_id].keys():
                        summary[c_seller_id][cat_id] = {}
                    if filename not in summary[c_seller_id][cat_id].keys():
                        if filename != "ItemRevised":
                            summary[c_seller_id][cat_id][filename] = []
#                            summary[c_seller_id][cat_id][filename].append(item_id)
                            if filename == "ItemClosed":
                                if item_id not in temporary_dict["ItemOther"]:
                                    summary[c_seller_id][cat_id][filename].append(item_id) 
                                else:
#                                    if item_id in summary[c_seller_id][cat_id]["ItemRevised"]:
#                                        del summary[c_seller_id][cat_id]["ItemRevised"][item_id]
                                    if item_id not in summary[c_seller_id][cat_id]["ItemClosed"]:
                                        summary[c_seller_id][cat_id]["ItemClosed"].append(item_id)
                                     
                            else:
                                if item_id not in temporary_dict["ItemClosed"]:
                                    summary[c_seller_id][cat_id][filename].append(item_id)
                                else:
                                    pass
                                
                        else:
                            summary[c_seller_id][cat_id][filename] = {}
                            if item_id not in temp_list:
                                if item_id not in temporary_dict["ItemClosed"]:
                                    summary[c_seller_id][cat_id][filename][item_id] = item_count
                                else:
                                    pass
                            else:
                                if item_id not in temporary_dict["ItemClosed"]:
                                    summary[c_seller_id][cat_id][filename][item_id] = temp_list.count(item_id) + 1
                            temp_list.append(item_id)
                            
                    else:
                        if filename != "ItemRevised":
                            if filename == "ItemClosed":
                                if item_id not in temporary_dict["ItemOther"]:
                                    summary[c_seller_id][cat_id][filename].append(item_id) 
                                else:
#                                    if item_id in summary[c_seller_id][cat_id]["ItemRevised"]:
#                                        del summary[c_seller_id][cat_id]["ItemRevised"][item_id]
                                        if item_id not in summary[c_seller_id][cat_id]["ItemClosed"]:
                                            summary[c_seller_id][cat_id]["ItemClosed"].append(item_id)
                            else:
                                if item_id not in temporary_dict["ItemClosed"]:
                                    summary[c_seller_id][cat_id][filename].append(item_id)
                        else:
                            if item_id in temp_list:
                                if item_id not in temporary_dict["ItemClosed"]:
                                    summary[c_seller_id][cat_id][filename][item_id] = temp_list.count(item_id) + 1
                            else:
                                    summary[c_seller_id][cat_id][filename][item_id] = item_count
                            temp_list.append(item_id)
                progress_of_json_processing(args, no_of_files, i, p_rel, date_str)
                i += 1
    
        except Exception, e:
            excp += 1
            print e
            
    if "ItemClosed" in summary[c_seller_id][cat_id].keys(): 
        items_closed = list(set(summary[c_seller_id][cat_id]["ItemClosed"]))     
        for i in items_closed:
            if "ItemRevised" in summary[c_seller_id][cat_id].keys():
                if i in summary[c_seller_id][cat_id]["ItemRevised"]:
                    del summary[c_seller_id][cat_id]["ItemRevised"][i]
            if "ItemSold" in summary[c_seller_id][cat_id].keys():
                if i in summary[c_seller_id][cat_id]["ItemSold"]:
                    summary[c_seller_id][cat_id]["ItemSold"].remove(i)
            if "ItemListed" in summary[c_seller_id][cat_id].keys():
                if i in summary[c_seller_id][cat_id]["ItemListed"]:
                    summary[c_seller_id][cat_id]["ItemListed"].remove(i)
    
    
    with open(path + "/%s.json" % ("summary"), "w") as f:
        f.write(json.dumps(summary))
    

        
    
    print "Total Number of infected files are:%d" % excp    
    print "Summary has updated."
    
    
    
def progress_of_json_processing(callback, total_no_of_files, count, p_rel, date_str):
    if callback == None:
        pass
    else:
        percentage = float((float(count) / float(total_no_of_files)) * 100)
#        print "File Writing in Progress,%s out of %s has completed,%d percent. " % (count,total_no_of_files,round(percentage,2))
        if count == 1:
            with open(p_rel + "/%s/notifications/summary_progress.txt" % (str(date_str)), "w") as f:
                                f.write("File Writing in Progress,%s out of %s has completed,%d percent.\n" % (count, total_no_of_files, round(percentage, 2)))
        else:
            with open(p_rel + "/%s/notifications/summary_progress.txt" % (str(date_str)), "a") as f:
                                f.write("File Writing in Progress,%s out of %s has completed,%d percent.\n" % (count, total_no_of_files, round(percentage, 2)))
if __name__ == "__main__":
    deducting_item("test", callback="f")
