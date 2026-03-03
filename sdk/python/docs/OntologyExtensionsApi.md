# spice_harvester_sdk.OntologyExtensionsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_resource_api_v1_databases_db_name_ontology_resources_resource_type_post**](OntologyExtensionsApi.md#create_resource_api_v1_databases_db_name_ontology_resources_resource_type_post) | **POST** /api/v1/databases/{db_name}/ontology/resources/{resource_type} | Create Resource
[**delete_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_delete**](OntologyExtensionsApi.md#delete_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_delete) | **DELETE** /api/v1/databases/{db_name}/ontology/resources/{resource_type}/{resource_id} | Delete Resource
[**get_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_get**](OntologyExtensionsApi.md#get_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_get) | **GET** /api/v1/databases/{db_name}/ontology/resources/{resource_type}/{resource_id} | Get Resource
[**list_resources_api_v1_databases_db_name_ontology_resources_get**](OntologyExtensionsApi.md#list_resources_api_v1_databases_db_name_ontology_resources_get) | **GET** /api/v1/databases/{db_name}/ontology/resources | List Resources
[**list_resources_by_type_api_v1_databases_db_name_ontology_resources_resource_type_get**](OntologyExtensionsApi.md#list_resources_by_type_api_v1_databases_db_name_ontology_resources_resource_type_get) | **GET** /api/v1/databases/{db_name}/ontology/resources/{resource_type} | List Resources By Type
[**record_deployment_api_v1_databases_db_name_ontology_records_deployments_post**](OntologyExtensionsApi.md#record_deployment_api_v1_databases_db_name_ontology_records_deployments_post) | **POST** /api/v1/databases/{db_name}/ontology/records/deployments | Record Deployment
[**update_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_put**](OntologyExtensionsApi.md#update_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_put) | **PUT** /api/v1/databases/{db_name}/ontology/resources/{resource_type}/{resource_id} | Update Resource


# **create_resource_api_v1_databases_db_name_ontology_resources_resource_type_post**
> object create_resource_api_v1_databases_db_name_ontology_resources_resource_type_post(db_name, resource_type, ontology_resource_request, branch=branch, expected_head_commit=expected_head_commit, lang=lang, accept_language=accept_language)

Create Resource

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.ontology_resource_request import OntologyResourceRequest
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
    api_instance = spice_harvester_sdk.OntologyExtensionsApi(api_client)
    db_name = 'db_name_example' # str | 
    resource_type = 'resource_type_example' # str | 
    ontology_resource_request = spice_harvester_sdk.OntologyResourceRequest() # OntologyResourceRequest | 
    branch = 'master' # str | Target branch (optional) (default to 'master')
    expected_head_commit = 'expected_head_commit_example' # str | Optimistic concurrency guard token (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Create Resource
        api_response = api_instance.create_resource_api_v1_databases_db_name_ontology_resources_resource_type_post(db_name, resource_type, ontology_resource_request, branch=branch, expected_head_commit=expected_head_commit, lang=lang, accept_language=accept_language)
        print("The response of OntologyExtensionsApi->create_resource_api_v1_databases_db_name_ontology_resources_resource_type_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling OntologyExtensionsApi->create_resource_api_v1_databases_db_name_ontology_resources_resource_type_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **resource_type** | **str**|  | 
 **ontology_resource_request** | [**OntologyResourceRequest**](OntologyResourceRequest.md)|  | 
 **branch** | **str**| Target branch | [optional] [default to &#39;master&#39;]
 **expected_head_commit** | **str**| Optimistic concurrency guard token | [optional] 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_delete**
> object delete_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_delete(db_name, resource_type, resource_id, branch=branch, expected_head_commit=expected_head_commit, lang=lang, accept_language=accept_language)

Delete Resource

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
    api_instance = spice_harvester_sdk.OntologyExtensionsApi(api_client)
    db_name = 'db_name_example' # str | 
    resource_type = 'resource_type_example' # str | 
    resource_id = 'resource_id_example' # str | 
    branch = 'master' # str | Target branch (optional) (default to 'master')
    expected_head_commit = 'expected_head_commit_example' # str | Optimistic concurrency guard token (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Delete Resource
        api_response = api_instance.delete_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_delete(db_name, resource_type, resource_id, branch=branch, expected_head_commit=expected_head_commit, lang=lang, accept_language=accept_language)
        print("The response of OntologyExtensionsApi->delete_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_delete:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling OntologyExtensionsApi->delete_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_delete: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **resource_type** | **str**|  | 
 **resource_id** | **str**|  | 
 **branch** | **str**| Target branch | [optional] [default to &#39;master&#39;]
 **expected_head_commit** | **str**| Optimistic concurrency guard token | [optional] 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

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

# **get_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_get**
> object get_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_get(db_name, resource_type, resource_id, branch=branch, lang=lang, accept_language=accept_language)

Get Resource

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
    api_instance = spice_harvester_sdk.OntologyExtensionsApi(api_client)
    db_name = 'db_name_example' # str | 
    resource_type = 'resource_type_example' # str | 
    resource_id = 'resource_id_example' # str | 
    branch = 'master' # str | Target branch (optional) (default to 'master')
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Resource
        api_response = api_instance.get_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_get(db_name, resource_type, resource_id, branch=branch, lang=lang, accept_language=accept_language)
        print("The response of OntologyExtensionsApi->get_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling OntologyExtensionsApi->get_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **resource_type** | **str**|  | 
 **resource_id** | **str**|  | 
 **branch** | **str**| Target branch | [optional] [default to &#39;master&#39;]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

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

# **list_resources_api_v1_databases_db_name_ontology_resources_get**
> object list_resources_api_v1_databases_db_name_ontology_resources_get(db_name, resource_type=resource_type, branch=branch, limit=limit, offset=offset, lang=lang, accept_language=accept_language)

List Resources

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
    api_instance = spice_harvester_sdk.OntologyExtensionsApi(api_client)
    db_name = 'db_name_example' # str | 
    resource_type = 'resource_type_example' # str | Resource type filter (optional)
    branch = 'master' # str | Target branch (optional) (default to 'master')
    limit = 200 # int |  (optional) (default to 200)
    offset = 0 # int |  (optional) (default to 0)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Resources
        api_response = api_instance.list_resources_api_v1_databases_db_name_ontology_resources_get(db_name, resource_type=resource_type, branch=branch, limit=limit, offset=offset, lang=lang, accept_language=accept_language)
        print("The response of OntologyExtensionsApi->list_resources_api_v1_databases_db_name_ontology_resources_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling OntologyExtensionsApi->list_resources_api_v1_databases_db_name_ontology_resources_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **resource_type** | **str**| Resource type filter | [optional] 
 **branch** | **str**| Target branch | [optional] [default to &#39;master&#39;]
 **limit** | **int**|  | [optional] [default to 200]
 **offset** | **int**|  | [optional] [default to 0]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

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

# **list_resources_by_type_api_v1_databases_db_name_ontology_resources_resource_type_get**
> object list_resources_by_type_api_v1_databases_db_name_ontology_resources_resource_type_get(db_name, resource_type, branch=branch, limit=limit, offset=offset, lang=lang, accept_language=accept_language)

List Resources By Type

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
    api_instance = spice_harvester_sdk.OntologyExtensionsApi(api_client)
    db_name = 'db_name_example' # str | 
    resource_type = 'resource_type_example' # str | 
    branch = 'master' # str | Target branch (optional) (default to 'master')
    limit = 200 # int |  (optional) (default to 200)
    offset = 0 # int |  (optional) (default to 0)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Resources By Type
        api_response = api_instance.list_resources_by_type_api_v1_databases_db_name_ontology_resources_resource_type_get(db_name, resource_type, branch=branch, limit=limit, offset=offset, lang=lang, accept_language=accept_language)
        print("The response of OntologyExtensionsApi->list_resources_by_type_api_v1_databases_db_name_ontology_resources_resource_type_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling OntologyExtensionsApi->list_resources_by_type_api_v1_databases_db_name_ontology_resources_resource_type_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **resource_type** | **str**|  | 
 **branch** | **str**| Target branch | [optional] [default to &#39;master&#39;]
 **limit** | **int**|  | [optional] [default to 200]
 **offset** | **int**|  | [optional] [default to 0]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

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

# **record_deployment_api_v1_databases_db_name_ontology_records_deployments_post**
> object record_deployment_api_v1_databases_db_name_ontology_records_deployments_post(db_name, ontology_deployment_record_request, lang=lang, accept_language=accept_language)

Record Deployment

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.ontology_deployment_record_request import OntologyDeploymentRecordRequest
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
    api_instance = spice_harvester_sdk.OntologyExtensionsApi(api_client)
    db_name = 'db_name_example' # str | 
    ontology_deployment_record_request = spice_harvester_sdk.OntologyDeploymentRecordRequest() # OntologyDeploymentRecordRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Record Deployment
        api_response = api_instance.record_deployment_api_v1_databases_db_name_ontology_records_deployments_post(db_name, ontology_deployment_record_request, lang=lang, accept_language=accept_language)
        print("The response of OntologyExtensionsApi->record_deployment_api_v1_databases_db_name_ontology_records_deployments_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling OntologyExtensionsApi->record_deployment_api_v1_databases_db_name_ontology_records_deployments_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **ontology_deployment_record_request** | [**OntologyDeploymentRecordRequest**](OntologyDeploymentRecordRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_put**
> object update_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_put(db_name, resource_type, resource_id, ontology_resource_request, branch=branch, expected_head_commit=expected_head_commit, lang=lang, accept_language=accept_language)

Update Resource

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.ontology_resource_request import OntologyResourceRequest
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
    api_instance = spice_harvester_sdk.OntologyExtensionsApi(api_client)
    db_name = 'db_name_example' # str | 
    resource_type = 'resource_type_example' # str | 
    resource_id = 'resource_id_example' # str | 
    ontology_resource_request = spice_harvester_sdk.OntologyResourceRequest() # OntologyResourceRequest | 
    branch = 'master' # str | Target branch (optional) (default to 'master')
    expected_head_commit = 'expected_head_commit_example' # str | Optimistic concurrency guard token (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Update Resource
        api_response = api_instance.update_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_put(db_name, resource_type, resource_id, ontology_resource_request, branch=branch, expected_head_commit=expected_head_commit, lang=lang, accept_language=accept_language)
        print("The response of OntologyExtensionsApi->update_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_put:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling OntologyExtensionsApi->update_resource_api_v1_databases_db_name_ontology_resources_resource_type_resource_id_put: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **resource_type** | **str**|  | 
 **resource_id** | **str**|  | 
 **ontology_resource_request** | [**OntologyResourceRequest**](OntologyResourceRequest.md)|  | 
 **branch** | **str**| Target branch | [optional] [default to &#39;master&#39;]
 **expected_head_commit** | **str**| Optimistic concurrency guard token | [optional] 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

