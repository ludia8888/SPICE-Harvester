# spice_harvester_sdk.ContextToolsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**describe_datasets_api_v1_context_tools_datasets_describe_post**](ContextToolsApi.md#describe_datasets_api_v1_context_tools_datasets_describe_post) | **POST** /api/v1/context-tools/datasets/describe | Describe Datasets
[**snapshot_ontology_api_v1_context_tools_ontology_snapshot_post**](ContextToolsApi.md#snapshot_ontology_api_v1_context_tools_ontology_snapshot_post) | **POST** /api/v1/context-tools/ontology/snapshot | Snapshot Ontology


# **describe_datasets_api_v1_context_tools_datasets_describe_post**
> ApiResponse describe_datasets_api_v1_context_tools_datasets_describe_post(dataset_describe_request, lang=lang, accept_language=accept_language)

Describe Datasets

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
from spice_harvester_sdk.models.dataset_describe_request import DatasetDescribeRequest
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
    api_instance = spice_harvester_sdk.ContextToolsApi(api_client)
    dataset_describe_request = spice_harvester_sdk.DatasetDescribeRequest() # DatasetDescribeRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Describe Datasets
        api_response = api_instance.describe_datasets_api_v1_context_tools_datasets_describe_post(dataset_describe_request, lang=lang, accept_language=accept_language)
        print("The response of ContextToolsApi->describe_datasets_api_v1_context_tools_datasets_describe_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ContextToolsApi->describe_datasets_api_v1_context_tools_datasets_describe_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_describe_request** | [**DatasetDescribeRequest**](DatasetDescribeRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**ApiResponse**](ApiResponse.md)

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

# **snapshot_ontology_api_v1_context_tools_ontology_snapshot_post**
> ApiResponse snapshot_ontology_api_v1_context_tools_ontology_snapshot_post(ontology_snapshot_request, lang=lang, accept_language=accept_language)

Snapshot Ontology

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
from spice_harvester_sdk.models.ontology_snapshot_request import OntologySnapshotRequest
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
    api_instance = spice_harvester_sdk.ContextToolsApi(api_client)
    ontology_snapshot_request = spice_harvester_sdk.OntologySnapshotRequest() # OntologySnapshotRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Snapshot Ontology
        api_response = api_instance.snapshot_ontology_api_v1_context_tools_ontology_snapshot_post(ontology_snapshot_request, lang=lang, accept_language=accept_language)
        print("The response of ContextToolsApi->snapshot_ontology_api_v1_context_tools_ontology_snapshot_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ContextToolsApi->snapshot_ontology_api_v1_context_tools_ontology_snapshot_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ontology_snapshot_request** | [**OntologySnapshotRequest**](OntologySnapshotRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**ApiResponse**](ApiResponse.md)

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

