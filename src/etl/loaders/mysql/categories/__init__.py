from etl.loaders.mysql import execute_multiple_query, generic_insert_import


def import_categories(records):
    
    fieldmap = dict(
        count="count",
        parentcategoryid="categories_categoryid",
        categoryName="name",
        categoryid="categoryid",
        sellerid="sellers_sellerid",
        aspect="aspects",
        inventory_value='inventoryValue',
        sales='sales',
        isleaf='isleaf'
    )
    headers = records[0].keys()
    res = generic_insert_import("categories", headers, records, fieldmap)
    execute_multiple_query(res[0], res[1])
    

    
