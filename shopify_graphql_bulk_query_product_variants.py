#
#   Written by:  Mark W Kiehl
#   http://mechatronicsolutionsllc.com/
#   http://www.savvysolutions.info/savvycodesolutions/
#

# Define the script version in terms of Semantic Versioning (SemVer)
# when Git or other versioning systems are not employed.
__version__ = "0.0.0"
from pathlib import Path
print("'" + Path(__file__).stem + ".py'  v" + __version__)


# Define your Shopify store's API key and password
API_TOKEN = 'shpat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'       # This goes in the header:  'X-Shopify-Access-Token': 'shpat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

# The store name is the portion of the URL for the store admin page between "https://admin.shopify.com/store/" and "/settings".
# Ex:  https://admin.shopify.com/store/my-store-name/settings/apps/development/123456789012/overview  -> "my-store-name"
STORE_NAME = "your-store-name"

# Define the endpoint version formatted as YYYY-MM  See versions at: https://shopify.dev/docs/api/release-notes#available-release-notes  or https://shopify.dev/docs/api/usage/versioning#release-schedule
GRAPHQL_VER = "2024-10"

# Define the Shopify store's GraphQL endpoint from the STORE_NAME and the GRAPHQL_VER.
#GRAPHQL_ENDPOINT = 'https://plate-bevel-machines.myshopify.com/admin/api/2024-04/graphql.json'
GRAPHQL_ENDPOINT = "https://" + STORE_NAME + ".myshopify.com/admin/api/" + GRAPHQL_VER + "/graphql.json"



def shopify_graphql_bulk_query(query:str=None, verbose:bool=False):
    """
    Execute the bulk query specified by query and return the bulk operation id
    Requires global variables:  API_TOKEN and GRAPHQL_ENDPOINT 

    bulk_op_id = shopify_graphql_bulk_query(query=query)
    """
    # https://shopify.dev/docs/api/admin-graphql/2024-10/objects/BulkOperation

    import json

    # pip install requests
    import requests

    if not isinstance(API_TOKEN, str): raise Exception("API_TOKEN is not defined")
    if not isinstance(GRAPHQL_ENDPOINT, str): raise Exception("GRAPHQL_ENDPOINT is not defined")

    # Define the headers for the GraphQL HTTP POST
    headers = {
        'Content-Type': 'application/graphql',      # Must be: 'application/graphql'    NOT: 'application/json'
        'X-Shopify-Access-Token': f'{API_TOKEN}',
    }


    response = requests.post(GRAPHQL_ENDPOINT, data=query, headers=headers)
    data = response.json()

    #print("type(data):", type(data))    # <class 'dict'>
    #print("type(json.dumps(data, indent=2)):", type(json.dumps(data, indent=2)))    # <class 'str'>

    #print(json.dumps(data, indent=2))
    """

    {
    "errors": "Not Found"
    }
    
    {
    "data": {
        "bulkOperationRunQuery": {
        "bulkOperation": {
            "id": "gid://shopify/BulkOperation/4142422163590",
            "status": "CREATED"
        },
        "userErrors": []
        }
    },
    "extensions": {
        "cost": {
        "requestedQueryCost": 10,
        "actualQueryCost": 10,
        "throttleStatus": {
            "maximumAvailable": 2000.0,
            "currentlyAvailable": 1990,
            "restoreRate": 100.0
        }
        }
    }
    }
    """

    # Check for errors
    if "errors" in data:
        print(f"ERROR: {data['errors']}")
        raise Exception(f"ERROR: {data['errors']}")

    # Extract the bulk operation id
    bulk_op_id = None
    if "bulkOperationRunQuery" in data['data']:
        errors = data['data']['bulkOperationRunQuery']['userErrors']
        bulk_operation =  data['data']['bulkOperationRunQuery']['bulkOperation']
        if not bulk_operation is None: 
            bulk_op_id = bulk_operation['id']
            bulk_op_id = str(bulk_op_id).rsplit(sep="/", maxsplit=1)[1]
            if verbose: print("bulk_op_id:", bulk_op_id)
    elif "productVariantsBulkUpdate" in data['data']:
        pass
        bulk_operation =  data['data']['productVariantsBulkUpdate']
        errors = data['data']['productVariantsBulkUpdate']['userErrors']
    else:
        print("*** Unexpected bulk operation ***")
        print(json.dumps(data, indent=2))
        raise Exception("Unexpected bulk operation")
    if bulk_operation is None: 
        print("ERRORS:")
        for error in errors:
            #print(error)
            print(str(error['message']))
            print()        
        raise Exception("The bulk query is invalid.  ")

    return bulk_op_id


def shopify_graphql_bulk_poll(bulk_op_id:str=None, wait_s:int=10, verbose:bool=False):
    """
    Poll Shopify by the bulk_op_id waiting wait_s between each poll until the bulk results are ready.
    Returns the actual cost, object count, and the URL.  URL will be None if an error occurs.

    bulk_op_id = '1234567890123'
    cost, obj_count, url = shopify_graphql_bulk_poll(bulk_op_id=bulk_op_id, wait_s=10, verbose=False)
    if url is None: raise Exception(f"shopify_graphql_bulk_poll() error for bulk_op_id {bulk_op_id}")
    """
    # https://shopify.dev/docs/api/usage/bulk-operations/queries#option-b-poll-a-running-bulk-operation

    from time import sleep
    import json

    # pip install requests
    import requests

    if not isinstance(API_TOKEN, str): raise Exception("API_TOKEN is not defined")
    if not isinstance(GRAPHQL_ENDPOINT, str): raise Exception("GRAPHQL_ENDPOINT is not defined")

    # Define the headers for the GraphQL HTTP POST
    headers = {
        'Content-Type': 'application/graphql',      # Must be: 'application/graphql'    NOT: 'application/json'
        'X-Shopify-Access-Token': f'{API_TOKEN}',
    }

    # Create the query JSON string
    query = 'query { node(id: "gid://shopify/BulkOperation/' + str(bulk_op_id) + '") {... on BulkOperation {status\nerrorCode\nobjectCount\nurl}}}'

    if verbose: print("Polling GraphQL for bulk query results by bulk operation id " + str(bulk_op_id) + "..")
    url = None
    # Query (poll) Shopify until the bulk data is available
    while url is None:
        response = requests.post(GRAPHQL_ENDPOINT, data=query, headers=headers)
        data = response.json()
        #print(json.dumps(data, indent=2))

        """
        BulkOperation
        {
        "data": {
            "node": {
            "status": "COMPLETED",
            "errorCode": null,
            "objectCount": "47",
            "url": "https://storage.googleapis.com/shopify-tiers-assets-prod-us-east1/bulk-operation-outputs/..jsonl"
            }
        },
        "extensions": {
            "cost": {
            "requestedQueryCost": 1,
            "actualQueryCost": 1,
            "throttleStatus": {
                "maximumAvailable": 2000.0,
                "currentlyAvailable": 1999,
                "restoreRate": 100.0
            }
            }
        }
        }

        productVariantsBulkUpdate
        {
        "data": {
            "node": null
        },
        "extensions": {
            "cost": {
            "requestedQueryCost": 1,
            "actualQueryCost": 1,
            "throttleStatus": {
                "maximumAvailable": 2000.0,
                "currentlyAvailable": 1999,
                "restoreRate": 100.0
            }
            }
        }
        }

        """
        
        error_code = None
        status = None
        obj_count = None

        if not data['data']['node'] is None and 'status' in data['data']['node']:
            # Extract the bulk query status
            # https://shopify.dev/docs/api/admin-graphql/2024-10/enums/bulkoperationstatus
            # CANCELED, CANCELING, COMPLETED, CREATED, EXPIIRED, FAILED, RUNNING
            status = data['data']['node']['status']

            # The errorCode will be Null if no error
            error_code = data['data']['node']['errorCode']

            # objectCount is the number of items that will be returned by the bulk query.
            obj_count = data['data']['node']['objectCount']

        cost = data['extensions']['cost']

        if verbose:
            print(f"status: {status}")
            if not error_code is None: print(f"error_code: {error_code}")
            print(f"obj_count: {obj_count}")

        # If an error occurs, print out the details.
        if not error_code is None:
            print(json.dumps(data, indent=2))
            print(f"status: {status}")
            print(f"error_code: {error_code}")
            print(f"obj_count: {obj_count}")
            return cost['actualQueryCost'], obj_count, None

        url = None
        if not data['data']['node'] is None and 'url' in data['data']['node']:
            url = data['data']['node']['url']
            # url will be None until the bulk query is complete

        if url is None:
            # Wait to give the server a chance to process the query
            if not status is None: print(f"status: {status} \t waiting {wait_s} s ..")
            sleep(wait_s)

        if url is None and status is None: 
            # productVariantsBulkUpdate
            #print(json.dumps(data, indent=2))
            return cost['actualQueryCost'], obj_count, url

    if verbose: 
        print("actualQueryCost:", cost['actualQueryCost'])
        print("url:", url)

    return cost['actualQueryCost'], obj_count, url


def shopify_graphql_bulk_dl_to_file(url:str=None, path_file:str=None, verbose:bool=False):
    """
    Download the bulk query results from url to the local file path_file.

    path_file = shopify_graphql_bulk_dl_to_file(url=url, path_file=path_file, verbose=False)
    """

    def download_from_url_to_file(url):
        """
        Download the file specified by url.
        """
        import requests
        get_response = requests.get(url,stream=True)
        with open(path_file, 'wb') as f:
            for chunk in get_response.iter_content(chunk_size=1024):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)


    # Download the JSON file specified by a URL to a local file path_file.

    # Delete the file if it already exists
    if path_file.is_file(): 
        print(f"Deleting file that already exists {path_file}")
        path_file.unlink()  

    # Download the JSON file specified by url and write it to path_file
    # url:  https://storage.googleapis.com/shopify-tiers-assets-prod-us-east1/bulk-operation-outputs/l32j8ouqfxzi7rkq6hmnxzzzj1lj-final?GoogleAccessId=assets-us-prod%40shopify-tiers.iam.gserviceaccount.com&Expires=1732187275&Signature=TStPV3pLAzM3sFYezQLmH91%2F%2FwJwp55MxfJE9uNUY79xWzD9xIm8WKa1FkWnCG6UwlHHFp26EkmAAoBTjQCuRjSHKq9DI2VC5qUlCDkoTCdfHnf7Y5sq%2BRnz93DDgLxeH0NEiV3%2BYHHsklzt1TKuM%2Bh%2Bnp1eD3EPOriox0Q4UF4%2BmlijQUstRv7kvkVwJbEFMIL7S%2B3MuwdkNVCwd2Mivj%2BJE%2Bsz4gFHlPCJUkdbmI8Q8R5%2Fx9Y7KvJfwQ89uAY%2FwhOjAuOmZiiWE9EstBtPjvmj1UwBkJO1nKRmisAFXY%2BjTdL0PsbfzvdBG64Ovah6KOJlbzlKq5YwI0F%2BQcuOKQ%3D%3D&response-content-disposition=attachment%3B+filename%3D%22bulk-4142422163590.jsonl%22%3B+filename%2A%3DUTF-8%27%27bulk-4142422163590.jsonl&response-content-type=application%2Fjson
    download_from_url_to_file(url)

    if not path_file.is_file(): raise Exception("Download from '" + url + "' to file '" + str(path_file) + "' FAILED")

    if verbose: print("Downloaded JSON file location:", path_file)

    return path_file


# Download the bulk query results (json lines) from the URL to memory (not file)
def shopify_graphql_bulk_dl_to_ram(url:str=None, verbose:bool=False):
    """
    Download the bulk query results from url to memory (ram).

    This is a template to be used to create a custom function.

    """
    
    import requests

    #pip install jsonlines
    import jsonlines

    response = requests.get(url,stream=True)
    response.raise_for_status()
    if not response.status_code == 200:
        raise Exception(f"ERROR: {response.status_code}")

    # Process response content with jsonlines
    with jsonlines.Reader(response.iter_lines()) as reader:
        for chunk in reader.iter():
            if chunk: # filter out keep-alive new chunks
                #print(type(chunk))  # <class 'dict'>
                print(chunk)

    """
    {'id': 'gid://shopify/Product/1629753868406'}
    {'id': 'gid://shopify/ProductVariant/19047055687798', 'title': 'fixed / 120 VAC 1-Phase 60 Hz / < 8 mm', 'sku': 'A900ST120CSTCFTLT8', '__parentId': 'gid://shopify/Product/1629753868406'}
    {'id': 'gid://shopify/ProductVariant/19047055556726', 'title': 'fixed / 120 VAC 1-Phase 60 Hz / 8 mm to 50 mm', 'sku': 'A900ST120CSTCFTSTD', '__parentId': 'gid://shopify/Product/1629753868406'}
    """



if __name__ == '__main__':
    pass


    # ---------------------------------------------------------------------------
    # Execute a bulk query to get all product variants from a Shopify store and 
    # then download them to a variable (memory).

    """
    query = "{products {edges {node {id variants {edges {node {id title sku}}}}}}}"  
    bulk_query = '''mutation {bulkOperationRunQuery(query: \"\"\"''' + query + '''\"\"\") {bulkOperation {id status} userErrors {field message}}}'''
    bulk_op_id = shopify_graphql_bulk_query(query=bulk_query, verbose=False)

    # Poll Shopify via GraphQL until the bulk query is complete.  Return the URL for download. 
    cost, obj_count, url = shopify_graphql_bulk_poll(bulk_op_id=bulk_op_id, wait_s=10, verbose=False)
    print(f"Bulk query actual cost was {cost} for {obj_count} items.")
    if not url is None: print(f"Bulk query results download url: {url}")

    # Download the bulk query results to a local variable (memory). 
    shopify_graphql_bulk_dl_to_ram(url=url)
    """


    # ---------------------------------------------------------------------------
    # Execute a bulk query to get all product variants from a Shopify store and 
    # then download them to a local file using shopify_graphql_bulk_dl_to_file().

    """
    from pathlib import Path

    #pip install jsonlines
    import jsonlines

    # Build a bulk query to get all products and their nested variants:
    query = "{products {edges {node {id variants {edges {node {id title sku price}}}}}}}"  
    bulk_query = '''mutation {bulkOperationRunQuery(query: \"\"\"''' + query + '''\"\"\") {bulkOperation {id status} userErrors {field message}}}'''
    bulk_op_id = shopify_graphql_bulk_query(query=bulk_query, verbose=False)
    print(f"bulk_op_id: {bulk_op_id}")

    # Poll Shopify via GraphQL until the bulk query is complete.  Return the URL for download. 
    cost, obj_count, url = shopify_graphql_bulk_poll(bulk_op_id=bulk_op_id, wait_s=10, verbose=False)
    if not url is None: 
        print(url)

    path_file = Path(Path.cwd()).joinpath("bulk_dl.jsonl")
    path_file = shopify_graphql_bulk_dl_to_file(url=url, path_file=path_file, verbose=False)
    if not path_file.is_file(): raise Exception(f"File not found {path_file}")

    # Use jsonlines to import the data because it executes without errors, unlike using csv reader. 
    with jsonlines.open(path_file) as reader:
        for line in reader:
            print(line)
            break
    """
    # ---------------------------------------------------------------------------