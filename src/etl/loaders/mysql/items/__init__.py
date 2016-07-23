from etl.loaders.mysql import generic_insert_import, execute_multiple_query


def import_items(records):

    fieldmap = dict(
        itemId="itemid",
        title="title",
        thumbUrl="thumbUrl",
        galleryPlusPictureURL="galleryPlusPictureURL",
        currentPrice="currentPrice",
        categories_categoryid="categories_categoryid",
        primarycategoryname="primarycategoryname",
        secondarycategoryid="secondarycategoryid",
        secondarycategoryname="secondarycategoryname",
        convertedCurrentPrice="convertedCurrentPrice",
        storename="storename",
        viewItemURL="viewItemUrl",
        sellerid="categories_sellers_sellerid",
        itemRank="itemrank",
        salesCount="salescount",
        viewItemCount="viewitemcount",
        salesPerImpression="salesperimpression",
        viewItemPerImpression="viewitemperimpression",
        impressionCount="impressioncount",
        galleryurl="galleryurl",
        photodisplay="photodisplay",
        itemspecifics="properties",
        variation="variations",
        inventory_value='inventory_value',
        sales='sales',
        buy_now_price='buy_now_price',
        quantity='quantity',
        itemsku='itemsku',
        quantitySold='quantitySold',
        conversionrate='conversionrate',
        variationcount='variationcount'
    )
    headers = records[0].keys()
      
    res = generic_insert_import("items", headers, records, fieldmap)
    execute_multiple_query(res[0], res[1])
