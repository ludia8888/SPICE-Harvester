# spice_harvester_sdk.ObjectifyApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_mapping_spec_api_v1_objectify_mapping_specs_post**](ObjectifyApi.md#create_mapping_spec_api_v1_objectify_mapping_specs_post) | **POST** /api/v1/objectify/mapping-specs | Create Mapping Spec
[**create_mapping_spec_api_v1_objectify_mapping_specs_post_0**](ObjectifyApi.md#create_mapping_spec_api_v1_objectify_mapping_specs_post_0) | **POST** /api/v1/objectify/mapping-specs | Create Mapping Spec
[**detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post**](ObjectifyApi.md#detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post) | **POST** /api/v1/objectify/databases/{db_name}/datasets/{dataset_id}/detect-relationships | Detect FK relationships in a dataset
[**detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post_0**](ObjectifyApi.md#detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post_0) | **POST** /api/v1/objectify/databases/{db_name}/datasets/{dataset_id}/detect-relationships | Detect FK relationships in a dataset
[**get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get**](ObjectifyApi.md#get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get) | **GET** /api/v1/objectify/mapping-specs/{mapping_spec_id}/watermark | Get objectify watermark
[**get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get_0**](ObjectifyApi.md#get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get_0) | **GET** /api/v1/objectify/mapping-specs/{mapping_spec_id}/watermark | Get objectify watermark
[**list_mapping_specs_api_v1_objectify_mapping_specs_get**](ObjectifyApi.md#list_mapping_specs_api_v1_objectify_mapping_specs_get) | **GET** /api/v1/objectify/mapping-specs | List Mapping Specs
[**list_mapping_specs_api_v1_objectify_mapping_specs_get_0**](ObjectifyApi.md#list_mapping_specs_api_v1_objectify_mapping_specs_get_0) | **GET** /api/v1/objectify/mapping-specs | List Mapping Specs
[**run_objectify_api_v1_objectify_datasets_dataset_id_run_post**](ObjectifyApi.md#run_objectify_api_v1_objectify_datasets_dataset_id_run_post) | **POST** /api/v1/objectify/datasets/{dataset_id}/run | Run objectify for a single dataset
[**run_objectify_api_v1_objectify_datasets_dataset_id_run_post_0**](ObjectifyApi.md#run_objectify_api_v1_objectify_datasets_dataset_id_run_post_0) | **POST** /api/v1/objectify/datasets/{dataset_id}/run | Run objectify for a single dataset
[**run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post**](ObjectifyApi.md#run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post) | **POST** /api/v1/objectify/databases/{db_name}/run-dag | Orchestrate objectify across multiple ontology classes (DAG)
[**run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post_0**](ObjectifyApi.md#run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post_0) | **POST** /api/v1/objectify/databases/{db_name}/run-dag | Orchestrate objectify across multiple ontology classes (DAG)
[**trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post**](ObjectifyApi.md#trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post) | **POST** /api/v1/objectify/mapping-specs/{mapping_spec_id}/trigger-incremental | Trigger incremental objectify
[**trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post_0**](ObjectifyApi.md#trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post_0) | **POST** /api/v1/objectify/mapping-specs/{mapping_spec_id}/trigger-incremental | Trigger incremental objectify


# **create_mapping_spec_api_v1_objectify_mapping_specs_post**
> Dict[str, object] create_mapping_spec_api_v1_objectify_mapping_specs_post(create_mapping_spec_request, lang=lang, accept_language=accept_language)

Create Mapping Spec

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.create_mapping_spec_request import CreateMappingSpecRequest
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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    create_mapping_spec_request = spice_harvester_sdk.CreateMappingSpecRequest() # CreateMappingSpecRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Create Mapping Spec
        api_response = api_instance.create_mapping_spec_api_v1_objectify_mapping_specs_post(create_mapping_spec_request, lang=lang, accept_language=accept_language)
        print("The response of ObjectifyApi->create_mapping_spec_api_v1_objectify_mapping_specs_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->create_mapping_spec_api_v1_objectify_mapping_specs_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_mapping_spec_request** | [**CreateMappingSpecRequest**](CreateMappingSpecRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

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

# **create_mapping_spec_api_v1_objectify_mapping_specs_post_0**
> Dict[str, object] create_mapping_spec_api_v1_objectify_mapping_specs_post_0(create_mapping_spec_request, lang=lang, accept_language=accept_language)

Create Mapping Spec

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.create_mapping_spec_request import CreateMappingSpecRequest
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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    create_mapping_spec_request = spice_harvester_sdk.CreateMappingSpecRequest() # CreateMappingSpecRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Create Mapping Spec
        api_response = api_instance.create_mapping_spec_api_v1_objectify_mapping_specs_post_0(create_mapping_spec_request, lang=lang, accept_language=accept_language)
        print("The response of ObjectifyApi->create_mapping_spec_api_v1_objectify_mapping_specs_post_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->create_mapping_spec_api_v1_objectify_mapping_specs_post_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_mapping_spec_request** | [**CreateMappingSpecRequest**](CreateMappingSpecRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

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

# **detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post**
> Dict[str, object] detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post(db_name, dataset_id, branch=branch, lang=lang, accept_language=accept_language, detect_relationships_request=detect_relationships_request)

Detect FK relationships in a dataset

Analyzes dataset columns to detect potential foreign key relationships based on naming conventions and value overlap.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.detect_relationships_request import DetectRelationshipsRequest
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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    db_name = 'db_name_example' # str | 
    dataset_id = 'dataset_id_example' # str | 
    branch = 'master' # str |  (optional) (default to 'master')
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)
    detect_relationships_request = spice_harvester_sdk.DetectRelationshipsRequest() # DetectRelationshipsRequest |  (optional)

    try:
        # Detect FK relationships in a dataset
        api_response = api_instance.detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post(db_name, dataset_id, branch=branch, lang=lang, accept_language=accept_language, detect_relationships_request=detect_relationships_request)
        print("The response of ObjectifyApi->detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **dataset_id** | **str**|  | 
 **branch** | **str**|  | [optional] [default to &#39;master&#39;]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 
 **detect_relationships_request** | [**DetectRelationshipsRequest**](DetectRelationshipsRequest.md)|  | [optional] 

### Return type

**Dict[str, object]**

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

# **detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post_0**
> Dict[str, object] detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post_0(db_name, dataset_id, branch=branch, lang=lang, accept_language=accept_language, detect_relationships_request=detect_relationships_request)

Detect FK relationships in a dataset

Analyzes dataset columns to detect potential foreign key relationships based on naming conventions and value overlap.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.detect_relationships_request import DetectRelationshipsRequest
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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    db_name = 'db_name_example' # str | 
    dataset_id = 'dataset_id_example' # str | 
    branch = 'master' # str |  (optional) (default to 'master')
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)
    detect_relationships_request = spice_harvester_sdk.DetectRelationshipsRequest() # DetectRelationshipsRequest |  (optional)

    try:
        # Detect FK relationships in a dataset
        api_response = api_instance.detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post_0(db_name, dataset_id, branch=branch, lang=lang, accept_language=accept_language, detect_relationships_request=detect_relationships_request)
        print("The response of ObjectifyApi->detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->detect_relationships_api_v1_objectify_databases_db_name_datasets_dataset_id_detect_relationships_post_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **dataset_id** | **str**|  | 
 **branch** | **str**|  | [optional] [default to &#39;master&#39;]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 
 **detect_relationships_request** | [**DetectRelationshipsRequest**](DetectRelationshipsRequest.md)|  | [optional] 

### Return type

**Dict[str, object]**

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

# **get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get**
> Dict[str, object] get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get(mapping_spec_id, branch=branch, lang=lang, accept_language=accept_language)

Get objectify watermark

Get the current watermark state for a mapping spec.

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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    mapping_spec_id = 'mapping_spec_id_example' # str | 
    branch = 'master' # str |  (optional) (default to 'master')
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get objectify watermark
        api_response = api_instance.get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get(mapping_spec_id, branch=branch, lang=lang, accept_language=accept_language)
        print("The response of ObjectifyApi->get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **mapping_spec_id** | **str**|  | 
 **branch** | **str**|  | [optional] [default to &#39;master&#39;]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

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

# **get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get_0**
> Dict[str, object] get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get_0(mapping_spec_id, branch=branch, lang=lang, accept_language=accept_language)

Get objectify watermark

Get the current watermark state for a mapping spec.

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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    mapping_spec_id = 'mapping_spec_id_example' # str | 
    branch = 'master' # str |  (optional) (default to 'master')
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get objectify watermark
        api_response = api_instance.get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get_0(mapping_spec_id, branch=branch, lang=lang, accept_language=accept_language)
        print("The response of ObjectifyApi->get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->get_mapping_spec_watermark_api_v1_objectify_mapping_specs_mapping_spec_id_watermark_get_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **mapping_spec_id** | **str**|  | 
 **branch** | **str**|  | [optional] [default to &#39;master&#39;]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

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

# **list_mapping_specs_api_v1_objectify_mapping_specs_get**
> Dict[str, object] list_mapping_specs_api_v1_objectify_mapping_specs_get(dataset_id=dataset_id, include_inactive=include_inactive, lang=lang, accept_language=accept_language)

List Mapping Specs

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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    dataset_id = 'dataset_id_example' # str |  (optional)
    include_inactive = False # bool |  (optional) (default to False)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Mapping Specs
        api_response = api_instance.list_mapping_specs_api_v1_objectify_mapping_specs_get(dataset_id=dataset_id, include_inactive=include_inactive, lang=lang, accept_language=accept_language)
        print("The response of ObjectifyApi->list_mapping_specs_api_v1_objectify_mapping_specs_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->list_mapping_specs_api_v1_objectify_mapping_specs_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_id** | **str**|  | [optional] 
 **include_inactive** | **bool**|  | [optional] [default to False]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

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

# **list_mapping_specs_api_v1_objectify_mapping_specs_get_0**
> Dict[str, object] list_mapping_specs_api_v1_objectify_mapping_specs_get_0(dataset_id=dataset_id, include_inactive=include_inactive, lang=lang, accept_language=accept_language)

List Mapping Specs

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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    dataset_id = 'dataset_id_example' # str |  (optional)
    include_inactive = False # bool |  (optional) (default to False)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Mapping Specs
        api_response = api_instance.list_mapping_specs_api_v1_objectify_mapping_specs_get_0(dataset_id=dataset_id, include_inactive=include_inactive, lang=lang, accept_language=accept_language)
        print("The response of ObjectifyApi->list_mapping_specs_api_v1_objectify_mapping_specs_get_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->list_mapping_specs_api_v1_objectify_mapping_specs_get_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_id** | **str**|  | [optional] 
 **include_inactive** | **bool**|  | [optional] [default to False]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

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

# **run_objectify_api_v1_objectify_datasets_dataset_id_run_post**
> Dict[str, object] run_objectify_api_v1_objectify_datasets_dataset_id_run_post(dataset_id, trigger_objectify_request, lang=lang, accept_language=accept_language)

Run objectify for a single dataset

Executes objectify for one specific dataset with explicit mapping spec control.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.trigger_objectify_request import TriggerObjectifyRequest
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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    dataset_id = 'dataset_id_example' # str | 
    trigger_objectify_request = spice_harvester_sdk.TriggerObjectifyRequest() # TriggerObjectifyRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Run objectify for a single dataset
        api_response = api_instance.run_objectify_api_v1_objectify_datasets_dataset_id_run_post(dataset_id, trigger_objectify_request, lang=lang, accept_language=accept_language)
        print("The response of ObjectifyApi->run_objectify_api_v1_objectify_datasets_dataset_id_run_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->run_objectify_api_v1_objectify_datasets_dataset_id_run_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_id** | **str**|  | 
 **trigger_objectify_request** | [**TriggerObjectifyRequest**](TriggerObjectifyRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

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

# **run_objectify_api_v1_objectify_datasets_dataset_id_run_post_0**
> Dict[str, object] run_objectify_api_v1_objectify_datasets_dataset_id_run_post_0(dataset_id, trigger_objectify_request, lang=lang, accept_language=accept_language)

Run objectify for a single dataset

Executes objectify for one specific dataset with explicit mapping spec control.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.trigger_objectify_request import TriggerObjectifyRequest
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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    dataset_id = 'dataset_id_example' # str | 
    trigger_objectify_request = spice_harvester_sdk.TriggerObjectifyRequest() # TriggerObjectifyRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Run objectify for a single dataset
        api_response = api_instance.run_objectify_api_v1_objectify_datasets_dataset_id_run_post_0(dataset_id, trigger_objectify_request, lang=lang, accept_language=accept_language)
        print("The response of ObjectifyApi->run_objectify_api_v1_objectify_datasets_dataset_id_run_post_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->run_objectify_api_v1_objectify_datasets_dataset_id_run_post_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_id** | **str**|  | 
 **trigger_objectify_request** | [**TriggerObjectifyRequest**](TriggerObjectifyRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

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

# **run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post**
> Dict[str, object] run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post(db_name, run_objectify_dag_request, lang=lang, accept_language=accept_language)

Orchestrate objectify across multiple ontology classes (DAG)

Resolves dependency graph and fans out objectify jobs for all specified classes. Use this for full batch processing.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.run_objectify_dag_request import RunObjectifyDAGRequest
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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    db_name = 'db_name_example' # str | 
    run_objectify_dag_request = spice_harvester_sdk.RunObjectifyDAGRequest() # RunObjectifyDAGRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Orchestrate objectify across multiple ontology classes (DAG)
        api_response = api_instance.run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post(db_name, run_objectify_dag_request, lang=lang, accept_language=accept_language)
        print("The response of ObjectifyApi->run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **run_objectify_dag_request** | [**RunObjectifyDAGRequest**](RunObjectifyDAGRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

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

# **run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post_0**
> Dict[str, object] run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post_0(db_name, run_objectify_dag_request, lang=lang, accept_language=accept_language)

Orchestrate objectify across multiple ontology classes (DAG)

Resolves dependency graph and fans out objectify jobs for all specified classes. Use this for full batch processing.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.run_objectify_dag_request import RunObjectifyDAGRequest
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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    db_name = 'db_name_example' # str | 
    run_objectify_dag_request = spice_harvester_sdk.RunObjectifyDAGRequest() # RunObjectifyDAGRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Orchestrate objectify across multiple ontology classes (DAG)
        api_response = api_instance.run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post_0(db_name, run_objectify_dag_request, lang=lang, accept_language=accept_language)
        print("The response of ObjectifyApi->run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->run_objectify_dag_api_v1_objectify_databases_db_name_run_dag_post_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **run_objectify_dag_request** | [**RunObjectifyDAGRequest**](RunObjectifyDAGRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

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

# **trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post**
> Dict[str, object] trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post(mapping_spec_id, branch=branch, lang=lang, accept_language=accept_language, trigger_incremental_request=trigger_incremental_request)

Trigger incremental objectify

Trigger objectify in incremental mode, processing only rows changed since last run.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.trigger_incremental_request import TriggerIncrementalRequest
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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    mapping_spec_id = 'mapping_spec_id_example' # str | 
    branch = 'master' # str |  (optional) (default to 'master')
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)
    trigger_incremental_request = spice_harvester_sdk.TriggerIncrementalRequest() # TriggerIncrementalRequest |  (optional)

    try:
        # Trigger incremental objectify
        api_response = api_instance.trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post(mapping_spec_id, branch=branch, lang=lang, accept_language=accept_language, trigger_incremental_request=trigger_incremental_request)
        print("The response of ObjectifyApi->trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **mapping_spec_id** | **str**|  | 
 **branch** | **str**|  | [optional] [default to &#39;master&#39;]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 
 **trigger_incremental_request** | [**TriggerIncrementalRequest**](TriggerIncrementalRequest.md)|  | [optional] 

### Return type

**Dict[str, object]**

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

# **trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post_0**
> Dict[str, object] trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post_0(mapping_spec_id, branch=branch, lang=lang, accept_language=accept_language, trigger_incremental_request=trigger_incremental_request)

Trigger incremental objectify

Trigger objectify in incremental mode, processing only rows changed since last run.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.trigger_incremental_request import TriggerIncrementalRequest
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
    api_instance = spice_harvester_sdk.ObjectifyApi(api_client)
    mapping_spec_id = 'mapping_spec_id_example' # str | 
    branch = 'master' # str |  (optional) (default to 'master')
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)
    trigger_incremental_request = spice_harvester_sdk.TriggerIncrementalRequest() # TriggerIncrementalRequest |  (optional)

    try:
        # Trigger incremental objectify
        api_response = api_instance.trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post_0(mapping_spec_id, branch=branch, lang=lang, accept_language=accept_language, trigger_incremental_request=trigger_incremental_request)
        print("The response of ObjectifyApi->trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ObjectifyApi->trigger_incremental_objectify_api_v1_objectify_mapping_specs_mapping_spec_id_trigger_incremental_post_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **mapping_spec_id** | **str**|  | 
 **branch** | **str**|  | [optional] [default to &#39;master&#39;]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 
 **trigger_incremental_request** | [**TriggerIncrementalRequest**](TriggerIncrementalRequest.md)|  | [optional] 

### Return type

**Dict[str, object]**

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

