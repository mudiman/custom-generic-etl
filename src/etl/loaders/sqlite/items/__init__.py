#from etl.loaders.sqlite import generic_insert_import, execute_multiple_query
#
#
#
#def import_items(cursor,records):
#    cursor.execute("DROP TABLE IF EXISTS `items`;")
#    query="""
#    CREATE TABLE `items` (
#  `itemid` integer NOT NULL primary key,
#  `secondarycategoryid` integer DEFAULT NULL,
#  `title` text DEFAULT NULL,
#  `thumbUrl` text DEFAULT NULL,
#  `viewItemUrl` text DEFAULT NULL,
#  `galleryPlusPictureURL` text DEFAULT NULL,
#  `itemrank` integer DEFAULT '0',
#  `salescount` integer DEFAULT '0',
#  `viewitemcount` integer DEFAULT '0',
#  `salesperimpression` real DEFAULT '0.000000000000000000000000000000',
#  `viewitemperimpression` real DEFAULT '0.000000000000000000000000000000',
#  `impressioncount` integer DEFAULT '0',
#  `quantityavailable` integer DEFAULT NULL,
#  `quantitySold` integer DEFAULT NULL,
#  `currentPrice` integer DEFAULT NULL,
#  `convertedCurrentPrice` float(5,2) DEFAULT NULL,
#  `categories_categoryid` integer DEFAULT NULL,
#  `categories_sellers_sellerid` varchar(100)  DEFAULT NULL,
#  `primarycategoryname` varchar(500)  DEFAULT NULL,
#  `properties` text ,
#  `variations` text ,
#  `storename` text  DEFAULT NULL,
#  `secondarycategoryname` text  DEFAULT NULL,
#  `galleryurl` text  DEFAULT NULL,
#  `photodisplay` text  DEFAULT NULL,
#  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
#);""";
#    cursor.execute(query)
#    fieldmap = dict(
#        itemId="itemid",
#        title="title",
#        thumbUrl="thumbUrl",
#        galleryPlusPictureURL="galleryPlusPictureURL",
#        currentPrice="currentPrice",
#        categories_categoryid="categories_categoryid",
#        primarycategoryname="primarycategoryname",
#        secondarycategoryid="secondarycategoryid",
#        secondarycategoryname="secondarycategoryname",
#        convertedCurrentPrice="convertedCurrentPrice",
#        storename="storename",
#        viewItemURL="viewItemUrl",
#        sellerid="categories_sellers_sellerid",
#        itemRank="itemrank",
#        salesCount="salescount",
#        viewItemCount="viewitemcount",
#        salesPerImpression="salesperimpression",
#        viewItemPerImpression="viewitemperimpression",
#        impressionCount="impressioncount",
#        galleryurl="galleryurl",
#        photodisplay="photodisplay",
#        itemspecifics="properties",
#        variation="variations"
#    )
#    headers = records[0].keys()
#      
#    res = generic_insert_import("items", headers, records, fieldmap)
#    execute_multiple_query(cursor,res[0], res[1])
