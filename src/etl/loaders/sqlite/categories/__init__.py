#from etl.loaders.sqlite import generic_insert_import, execute_multiple_query
#
#
# 
#
#def import_categories(cursor,records):
#    cursor.execute("DROP TABLE IF EXISTS `categories`;")
#    query="""
#          CREATE TABLE `categories` (
#    `categoryid` INTEGER,
#    `sellers_sellerid` text,
#    `name` text,
#    `count` INTEGER ,
#    `categories_categoryid` real,
#    `aspects` text 
#    )""";
#    
#    cursor.execute(query)
#    fieldmap = dict(
#      count="count",
#      parentcategoryid="categories_categoryid",
#      categoryName="name",
#      categoryid="categoryid",
#      sellerid="sellers_sellerid",
#      aspect="aspects"
#      )
#    headers = records[0].keys()
#    res = generic_insert_import("categories", headers, records, fieldmap)
#    execute_multiple_query(cursor,res[0], res[1])
#    
#
#    
