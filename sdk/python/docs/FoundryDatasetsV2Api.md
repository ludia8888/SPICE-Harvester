# spice_harvester_sdk.FoundryDatasetsV2Api

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**abort_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_abort_post**](FoundryDatasetsV2Api.md#abort_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_abort_post) | **POST** /api/v2/datasets/{datasetRid}/transactions/{transactionRid}/abort | Abort Transaction V2
[**commit_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_commit_post**](FoundryDatasetsV2Api.md#commit_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_commit_post) | **POST** /api/v2/datasets/{datasetRid}/transactions/{transactionRid}/commit | Commit Transaction V2
[**create_branch_v2_api_v2_datasets_dataset_rid_branches_post**](FoundryDatasetsV2Api.md#create_branch_v2_api_v2_datasets_dataset_rid_branches_post) | **POST** /api/v2/datasets/{datasetRid}/branches | Create Branch V2
[**create_dataset_v2_api_v2_datasets_post**](FoundryDatasetsV2Api.md#create_dataset_v2_api_v2_datasets_post) | **POST** /api/v2/datasets | Create Dataset V2
[**create_transaction_v2_api_v2_datasets_dataset_rid_transactions_post**](FoundryDatasetsV2Api.md#create_transaction_v2_api_v2_datasets_dataset_rid_transactions_post) | **POST** /api/v2/datasets/{datasetRid}/transactions | Create Transaction V2
[**delete_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_delete**](FoundryDatasetsV2Api.md#delete_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_delete) | **DELETE** /api/v2/datasets/{datasetRid}/branches/{branchName} | Delete Branch V2
[**get_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_get**](FoundryDatasetsV2Api.md#get_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_get) | **GET** /api/v2/datasets/{datasetRid}/branches/{branchName} | Get Branch V2
[**get_dataset_schema_v2_api_v2_datasets_dataset_rid_get_schema_get**](FoundryDatasetsV2Api.md#get_dataset_schema_v2_api_v2_datasets_dataset_rid_get_schema_get) | **GET** /api/v2/datasets/{datasetRid}/getSchema | Get Dataset Schema V2
[**get_dataset_v2_api_v2_datasets_dataset_rid_get**](FoundryDatasetsV2Api.md#get_dataset_v2_api_v2_datasets_dataset_rid_get) | **GET** /api/v2/datasets/{datasetRid} | Get Dataset V2
[**get_file_content_v2_api_v2_datasets_dataset_rid_files_file_path_content_get**](FoundryDatasetsV2Api.md#get_file_content_v2_api_v2_datasets_dataset_rid_files_file_path_content_get) | **GET** /api/v2/datasets/{datasetRid}/files/{filePath}/content | Get File Content V2
[**get_file_v2_api_v2_datasets_dataset_rid_files_file_path_get**](FoundryDatasetsV2Api.md#get_file_v2_api_v2_datasets_dataset_rid_files_file_path_get) | **GET** /api/v2/datasets/{datasetRid}/files/{filePath} | Get File V2
[**get_schema_batch_v2_api_v2_datasets_get_schema_batch_post**](FoundryDatasetsV2Api.md#get_schema_batch_v2_api_v2_datasets_get_schema_batch_post) | **POST** /api/v2/datasets/getSchemaBatch | Get Schema Batch V2
[**get_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_get**](FoundryDatasetsV2Api.md#get_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_get) | **GET** /api/v2/datasets/{datasetRid}/transactions/{transactionRid} | Get Transaction V2
[**list_branches_v2_api_v2_datasets_dataset_rid_branches_get**](FoundryDatasetsV2Api.md#list_branches_v2_api_v2_datasets_dataset_rid_branches_get) | **GET** /api/v2/datasets/{datasetRid}/branches | List Branches V2
[**list_files_v2_api_v2_datasets_dataset_rid_files_get**](FoundryDatasetsV2Api.md#list_files_v2_api_v2_datasets_dataset_rid_files_get) | **GET** /api/v2/datasets/{datasetRid}/files | List Files V2
[**list_transactions_v2_api_v2_datasets_dataset_rid_transactions_get**](FoundryDatasetsV2Api.md#list_transactions_v2_api_v2_datasets_dataset_rid_transactions_get) | **GET** /api/v2/datasets/{datasetRid}/transactions | List Transactions V2
[**put_dataset_schema_v2_api_v2_datasets_dataset_rid_put_schema_put**](FoundryDatasetsV2Api.md#put_dataset_schema_v2_api_v2_datasets_dataset_rid_put_schema_put) | **PUT** /api/v2/datasets/{datasetRid}/putSchema | Put Dataset Schema V2
[**read_table_v2_api_v2_datasets_dataset_rid_read_table_get**](FoundryDatasetsV2Api.md#read_table_v2_api_v2_datasets_dataset_rid_read_table_get) | **GET** /api/v2/datasets/{datasetRid}/readTable | Read Table V2
[**upload_file_v2_api_v2_datasets_dataset_rid_files_file_path_upload_post**](FoundryDatasetsV2Api.md#upload_file_v2_api_v2_datasets_dataset_rid_files_file_path_upload_post) | **POST** /api/v2/datasets/{datasetRid}/files/{filePath}/upload | Upload File V2


# **abort_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_abort_post**
> object abort_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_abort_post(dataset_rid, transaction_rid)

Abort Transaction V2

POST /v2/datasets/{datasetRid}/transactions/{transactionRid}/abort.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    transaction_rid = 'transaction_rid_example' # str | 

    try:
        # Abort Transaction V2
        api_response = api_instance.abort_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_abort_post(dataset_rid, transaction_rid)
        print("The response of FoundryDatasetsV2Api->abort_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_abort_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->abort_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_abort_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **transaction_rid** | **str**|  | 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **commit_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_commit_post**
> object commit_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_commit_post(dataset_rid, transaction_rid)

Commit Transaction V2

POST /v2/datasets/{datasetRid}/transactions/{transactionRid}/commit.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    transaction_rid = 'transaction_rid_example' # str | 

    try:
        # Commit Transaction V2
        api_response = api_instance.commit_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_commit_post(dataset_rid, transaction_rid)
        print("The response of FoundryDatasetsV2Api->commit_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_commit_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->commit_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_commit_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **transaction_rid** | **str**|  | 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_branch_v2_api_v2_datasets_dataset_rid_branches_post**
> object create_branch_v2_api_v2_datasets_dataset_rid_branches_post(dataset_rid)

Create Branch V2

POST /v2/datasets/{datasetRid}/branches — Create branch.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 

    try:
        # Create Branch V2
        api_response = api_instance.create_branch_v2_api_v2_datasets_dataset_rid_branches_post(dataset_rid)
        print("The response of FoundryDatasetsV2Api->create_branch_v2_api_v2_datasets_dataset_rid_branches_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->create_branch_v2_api_v2_datasets_dataset_rid_branches_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_dataset_v2_api_v2_datasets_post**
> object create_dataset_v2_api_v2_datasets_post()

Create Dataset V2

POST /v2/datasets — Create a new dataset.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)

    try:
        # Create Dataset V2
        api_response = api_instance.create_dataset_v2_api_v2_datasets_post()
        print("The response of FoundryDatasetsV2Api->create_dataset_v2_api_v2_datasets_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->create_dataset_v2_api_v2_datasets_post: %s\n" % e)
```



### Parameters

This endpoint does not need any parameter.

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_transaction_v2_api_v2_datasets_dataset_rid_transactions_post**
> object create_transaction_v2_api_v2_datasets_dataset_rid_transactions_post(dataset_rid, branch_name=branch_name)

Create Transaction V2

POST /v2/datasets/{datasetRid}/transactions — Create a transaction.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    branch_name = 'master' # str |  (optional) (default to 'master')

    try:
        # Create Transaction V2
        api_response = api_instance.create_transaction_v2_api_v2_datasets_dataset_rid_transactions_post(dataset_rid, branch_name=branch_name)
        print("The response of FoundryDatasetsV2Api->create_transaction_v2_api_v2_datasets_dataset_rid_transactions_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->create_transaction_v2_api_v2_datasets_dataset_rid_transactions_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **branch_name** | **str**|  | [optional] [default to &#39;master&#39;]

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_delete**
> delete_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_delete(dataset_rid, branch_name)

Delete Branch V2

DELETE /v2/datasets/{datasetRid}/branches/{branchName} — Delete branch.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    branch_name = 'branch_name_example' # str | 

    try:
        # Delete Branch V2
        api_instance.delete_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_delete(dataset_rid, branch_name)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->delete_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_delete: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **branch_name** | **str**|  | 

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_get**
> object get_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_get(dataset_rid, branch_name)

Get Branch V2

GET /v2/datasets/{datasetRid}/branches/{branchName} — Get branch.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    branch_name = 'branch_name_example' # str | 

    try:
        # Get Branch V2
        api_response = api_instance.get_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_get(dataset_rid, branch_name)
        print("The response of FoundryDatasetsV2Api->get_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->get_branch_v2_api_v2_datasets_dataset_rid_branches_branch_name_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **branch_name** | **str**|  | 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_dataset_schema_v2_api_v2_datasets_dataset_rid_get_schema_get**
> object get_dataset_schema_v2_api_v2_datasets_dataset_rid_get_schema_get(dataset_rid, preview, branch_name=branch_name, end_transaction_rid=end_transaction_rid, version_id=version_id)

Get Dataset Schema V2

GET /v2/datasets/{datasetRid}/getSchema — Get dataset schema (preview).

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    preview = True # bool | 
    branch_name = 'master' # str |  (optional) (default to 'master')
    end_transaction_rid = '' # str |  (optional) (default to '')
    version_id = '' # str |  (optional) (default to '')

    try:
        # Get Dataset Schema V2
        api_response = api_instance.get_dataset_schema_v2_api_v2_datasets_dataset_rid_get_schema_get(dataset_rid, preview, branch_name=branch_name, end_transaction_rid=end_transaction_rid, version_id=version_id)
        print("The response of FoundryDatasetsV2Api->get_dataset_schema_v2_api_v2_datasets_dataset_rid_get_schema_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->get_dataset_schema_v2_api_v2_datasets_dataset_rid_get_schema_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **preview** | **bool**|  | 
 **branch_name** | **str**|  | [optional] [default to &#39;master&#39;]
 **end_transaction_rid** | **str**|  | [optional] [default to &#39;&#39;]
 **version_id** | **str**|  | [optional] [default to &#39;&#39;]

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_dataset_v2_api_v2_datasets_dataset_rid_get**
> object get_dataset_v2_api_v2_datasets_dataset_rid_get(dataset_rid)

Get Dataset V2

GET /v2/datasets/{datasetRid} — Get a dataset.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 

    try:
        # Get Dataset V2
        api_response = api_instance.get_dataset_v2_api_v2_datasets_dataset_rid_get(dataset_rid)
        print("The response of FoundryDatasetsV2Api->get_dataset_v2_api_v2_datasets_dataset_rid_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->get_dataset_v2_api_v2_datasets_dataset_rid_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_file_content_v2_api_v2_datasets_dataset_rid_files_file_path_content_get**
> object get_file_content_v2_api_v2_datasets_dataset_rid_files_file_path_content_get(dataset_rid, file_path, branch_name=branch_name, start_transaction_rid=start_transaction_rid, end_transaction_rid=end_transaction_rid)

Get File Content V2

GET /v2/datasets/{datasetRid}/files/{filePath}/content — Download file.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    file_path = 'file_path_example' # str | 
    branch_name = 'master' # str |  (optional) (default to 'master')
    start_transaction_rid = '' # str |  (optional) (default to '')
    end_transaction_rid = '' # str |  (optional) (default to '')

    try:
        # Get File Content V2
        api_response = api_instance.get_file_content_v2_api_v2_datasets_dataset_rid_files_file_path_content_get(dataset_rid, file_path, branch_name=branch_name, start_transaction_rid=start_transaction_rid, end_transaction_rid=end_transaction_rid)
        print("The response of FoundryDatasetsV2Api->get_file_content_v2_api_v2_datasets_dataset_rid_files_file_path_content_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->get_file_content_v2_api_v2_datasets_dataset_rid_files_file_path_content_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **file_path** | **str**|  | 
 **branch_name** | **str**|  | [optional] [default to &#39;master&#39;]
 **start_transaction_rid** | **str**|  | [optional] [default to &#39;&#39;]
 **end_transaction_rid** | **str**|  | [optional] [default to &#39;&#39;]

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_file_v2_api_v2_datasets_dataset_rid_files_file_path_get**
> object get_file_v2_api_v2_datasets_dataset_rid_files_file_path_get(dataset_rid, file_path, branch_name=branch_name, start_transaction_rid=start_transaction_rid, end_transaction_rid=end_transaction_rid)

Get File V2

GET /v2/datasets/{datasetRid}/files/{filePath} — Get file metadata.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    file_path = 'file_path_example' # str | 
    branch_name = 'master' # str |  (optional) (default to 'master')
    start_transaction_rid = '' # str |  (optional) (default to '')
    end_transaction_rid = '' # str |  (optional) (default to '')

    try:
        # Get File V2
        api_response = api_instance.get_file_v2_api_v2_datasets_dataset_rid_files_file_path_get(dataset_rid, file_path, branch_name=branch_name, start_transaction_rid=start_transaction_rid, end_transaction_rid=end_transaction_rid)
        print("The response of FoundryDatasetsV2Api->get_file_v2_api_v2_datasets_dataset_rid_files_file_path_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->get_file_v2_api_v2_datasets_dataset_rid_files_file_path_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **file_path** | **str**|  | 
 **branch_name** | **str**|  | [optional] [default to &#39;master&#39;]
 **start_transaction_rid** | **str**|  | [optional] [default to &#39;&#39;]
 **end_transaction_rid** | **str**|  | [optional] [default to &#39;&#39;]

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_schema_batch_v2_api_v2_datasets_get_schema_batch_post**
> object get_schema_batch_v2_api_v2_datasets_get_schema_batch_post(preview)

Get Schema Batch V2

POST /v2/datasets/getSchemaBatch — Get dataset schemas batch (preview).

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    preview = True # bool | 

    try:
        # Get Schema Batch V2
        api_response = api_instance.get_schema_batch_v2_api_v2_datasets_get_schema_batch_post(preview)
        print("The response of FoundryDatasetsV2Api->get_schema_batch_v2_api_v2_datasets_get_schema_batch_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->get_schema_batch_v2_api_v2_datasets_get_schema_batch_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **preview** | **bool**|  | 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_get**
> object get_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_get(dataset_rid, transaction_rid, branch_name=branch_name)

Get Transaction V2

GET /v2/datasets/{datasetRid}/transactions/{transactionRid} — Get transaction.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    transaction_rid = 'transaction_rid_example' # str | 
    branch_name = 'master' # str |  (optional) (default to 'master')

    try:
        # Get Transaction V2
        api_response = api_instance.get_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_get(dataset_rid, transaction_rid, branch_name=branch_name)
        print("The response of FoundryDatasetsV2Api->get_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->get_transaction_v2_api_v2_datasets_dataset_rid_transactions_transaction_rid_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **transaction_rid** | **str**|  | 
 **branch_name** | **str**|  | [optional] [default to &#39;master&#39;]

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_branches_v2_api_v2_datasets_dataset_rid_branches_get**
> object list_branches_v2_api_v2_datasets_dataset_rid_branches_get(dataset_rid, page_size=page_size, page_token=page_token)

List Branches V2

GET /v2/datasets/{datasetRid}/branches — List branches.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    page_size = 100 # int |  (optional) (default to 100)
    page_token = '' # str |  (optional) (default to '')

    try:
        # List Branches V2
        api_response = api_instance.list_branches_v2_api_v2_datasets_dataset_rid_branches_get(dataset_rid, page_size=page_size, page_token=page_token)
        print("The response of FoundryDatasetsV2Api->list_branches_v2_api_v2_datasets_dataset_rid_branches_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->list_branches_v2_api_v2_datasets_dataset_rid_branches_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **page_size** | **int**|  | [optional] [default to 100]
 **page_token** | **str**|  | [optional] [default to &#39;&#39;]

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_files_v2_api_v2_datasets_dataset_rid_files_get**
> object list_files_v2_api_v2_datasets_dataset_rid_files_get(dataset_rid, branch_name=branch_name, start_transaction_rid=start_transaction_rid, end_transaction_rid=end_transaction_rid, prefix=prefix, page_size=page_size, page_token=page_token)

List Files V2

GET /v2/datasets/{datasetRid}/files — List files in dataset.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    branch_name = 'master' # str |  (optional) (default to 'master')
    start_transaction_rid = '' # str |  (optional) (default to '')
    end_transaction_rid = '' # str |  (optional) (default to '')
    prefix = '' # str |  (optional) (default to '')
    page_size = 100 # int |  (optional) (default to 100)
    page_token = '' # str |  (optional) (default to '')

    try:
        # List Files V2
        api_response = api_instance.list_files_v2_api_v2_datasets_dataset_rid_files_get(dataset_rid, branch_name=branch_name, start_transaction_rid=start_transaction_rid, end_transaction_rid=end_transaction_rid, prefix=prefix, page_size=page_size, page_token=page_token)
        print("The response of FoundryDatasetsV2Api->list_files_v2_api_v2_datasets_dataset_rid_files_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->list_files_v2_api_v2_datasets_dataset_rid_files_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **branch_name** | **str**|  | [optional] [default to &#39;master&#39;]
 **start_transaction_rid** | **str**|  | [optional] [default to &#39;&#39;]
 **end_transaction_rid** | **str**|  | [optional] [default to &#39;&#39;]
 **prefix** | **str**|  | [optional] [default to &#39;&#39;]
 **page_size** | **int**|  | [optional] [default to 100]
 **page_token** | **str**|  | [optional] [default to &#39;&#39;]

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_transactions_v2_api_v2_datasets_dataset_rid_transactions_get**
> object list_transactions_v2_api_v2_datasets_dataset_rid_transactions_get(dataset_rid, preview, branch_name=branch_name, page_size=page_size, page_token=page_token)

List Transactions V2

GET /v2/datasets/{datasetRid}/transactions — List transactions.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    preview = True # bool | 
    branch_name = 'master' # str |  (optional) (default to 'master')
    page_size = 100 # int |  (optional) (default to 100)
    page_token = '' # str |  (optional) (default to '')

    try:
        # List Transactions V2
        api_response = api_instance.list_transactions_v2_api_v2_datasets_dataset_rid_transactions_get(dataset_rid, preview, branch_name=branch_name, page_size=page_size, page_token=page_token)
        print("The response of FoundryDatasetsV2Api->list_transactions_v2_api_v2_datasets_dataset_rid_transactions_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->list_transactions_v2_api_v2_datasets_dataset_rid_transactions_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **preview** | **bool**|  | 
 **branch_name** | **str**|  | [optional] [default to &#39;master&#39;]
 **page_size** | **int**|  | [optional] [default to 100]
 **page_token** | **str**|  | [optional] [default to &#39;&#39;]

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **put_dataset_schema_v2_api_v2_datasets_dataset_rid_put_schema_put**
> put_dataset_schema_v2_api_v2_datasets_dataset_rid_put_schema_put(dataset_rid, preview, branch_name=branch_name)

Put Dataset Schema V2

PUT /v2/datasets/{datasetRid}/putSchema — Put dataset schema (preview).

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    preview = True # bool | 
    branch_name = 'master' # str |  (optional) (default to 'master')

    try:
        # Put Dataset Schema V2
        api_instance.put_dataset_schema_v2_api_v2_datasets_dataset_rid_put_schema_put(dataset_rid, preview, branch_name=branch_name)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->put_dataset_schema_v2_api_v2_datasets_dataset_rid_put_schema_put: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **preview** | **bool**|  | 
 **branch_name** | **str**|  | [optional] [default to &#39;master&#39;]

### Return type

void (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **read_table_v2_api_v2_datasets_dataset_rid_read_table_get**
> object read_table_v2_api_v2_datasets_dataset_rid_read_table_get(dataset_rid, format, branch_name=branch_name, start_transaction_rid=start_transaction_rid, end_transaction_rid=end_transaction_rid, columns=columns, row_limit=row_limit)

Read Table V2

GET /v2/datasets/{datasetRid}/readTable — Read table rows.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    format = 'format_example' # str | 
    branch_name = 'master' # str |  (optional) (default to 'master')
    start_transaction_rid = '' # str |  (optional) (default to '')
    end_transaction_rid = '' # str |  (optional) (default to '')
    columns = ['columns_example'] # List[str] |  (optional)
    row_limit = 100 # int |  (optional) (default to 100)

    try:
        # Read Table V2
        api_response = api_instance.read_table_v2_api_v2_datasets_dataset_rid_read_table_get(dataset_rid, format, branch_name=branch_name, start_transaction_rid=start_transaction_rid, end_transaction_rid=end_transaction_rid, columns=columns, row_limit=row_limit)
        print("The response of FoundryDatasetsV2Api->read_table_v2_api_v2_datasets_dataset_rid_read_table_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->read_table_v2_api_v2_datasets_dataset_rid_read_table_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **format** | **str**|  | 
 **branch_name** | **str**|  | [optional] [default to &#39;master&#39;]
 **start_transaction_rid** | **str**|  | [optional] [default to &#39;&#39;]
 **end_transaction_rid** | **str**|  | [optional] [default to &#39;&#39;]
 **columns** | [**List[str]**](str.md)|  | [optional] 
 **row_limit** | **int**|  | [optional] [default to 100]

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **upload_file_v2_api_v2_datasets_dataset_rid_files_file_path_upload_post**
> object upload_file_v2_api_v2_datasets_dataset_rid_files_file_path_upload_post(dataset_rid, file_path, branch_name=branch_name, transaction_rid=transaction_rid, byte_offset=byte_offset)

Upload File V2

POST /v2/datasets/{datasetRid}/files/{filePath}/upload — Upload file.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.FoundryDatasetsV2Api(api_client)
    dataset_rid = 'dataset_rid_example' # str | 
    file_path = 'file_path_example' # str | 
    branch_name = 'master' # str |  (optional) (default to 'master')
    transaction_rid = '' # str |  (optional) (default to '')
    byte_offset = 56 # int |  (optional)

    try:
        # Upload File V2
        api_response = api_instance.upload_file_v2_api_v2_datasets_dataset_rid_files_file_path_upload_post(dataset_rid, file_path, branch_name=branch_name, transaction_rid=transaction_rid, byte_offset=byte_offset)
        print("The response of FoundryDatasetsV2Api->upload_file_v2_api_v2_datasets_dataset_rid_files_file_path_upload_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryDatasetsV2Api->upload_file_v2_api_v2_datasets_dataset_rid_files_file_path_upload_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_rid** | **str**|  | 
 **file_path** | **str**|  | 
 **branch_name** | **str**|  | [optional] [default to &#39;master&#39;]
 **transaction_rid** | **str**|  | [optional] [default to &#39;&#39;]
 **byte_offset** | **int**|  | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

