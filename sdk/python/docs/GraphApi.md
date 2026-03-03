# spice_harvester_sdk.GraphApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**execute_graph_query_api_v1_graph_query_db_name_post**](GraphApi.md#execute_graph_query_api_v1_graph_query_db_name_post) | **POST** /api/v1/graph-query/{db_name} | Execute Graph Query
[**execute_multi_hop_query_api_v1_graph_query_db_name_multi_hop_post**](GraphApi.md#execute_multi_hop_query_api_v1_graph_query_db_name_multi_hop_post) | **POST** /api/v1/graph-query/{db_name}/multi-hop | Execute Multi Hop Query
[**execute_simple_graph_query_api_v1_graph_query_db_name_simple_post**](GraphApi.md#execute_simple_graph_query_api_v1_graph_query_db_name_simple_post) | **POST** /api/v1/graph-query/{db_name}/simple | Execute Simple Graph Query
[**find_relationship_paths_api_v1_graph_query_db_name_paths_get**](GraphApi.md#find_relationship_paths_api_v1_graph_query_db_name_paths_get) | **GET** /api/v1/graph-query/{db_name}/paths | Find Relationship Paths
[**graph_service_health_api_v1_graph_query_health_get**](GraphApi.md#graph_service_health_api_v1_graph_query_health_get) | **GET** /api/v1/graph-query/health | Graph Service Health


# **execute_graph_query_api_v1_graph_query_db_name_post**
> GraphQueryResponse execute_graph_query_api_v1_graph_query_db_name_post(db_name, graph_query_request, base_branch=base_branch, overlay_branch=overlay_branch, lang=lang, accept_language=accept_language)

Execute Graph Query

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.graph_query_request import GraphQueryRequest
from spice_harvester_sdk.models.graph_query_response import GraphQueryResponse
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
    api_instance = spice_harvester_sdk.GraphApi(api_client)
    db_name = 'db_name_example' # str | 
    graph_query_request = spice_harvester_sdk.GraphQueryRequest() # GraphQueryRequest | 
    base_branch = 'master' # str | Base branch (default: master) (optional) (default to 'master')
    overlay_branch = 'overlay_branch_example' # str | ES overlay branch (writeback) (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Execute Graph Query
        api_response = api_instance.execute_graph_query_api_v1_graph_query_db_name_post(db_name, graph_query_request, base_branch=base_branch, overlay_branch=overlay_branch, lang=lang, accept_language=accept_language)
        print("The response of GraphApi->execute_graph_query_api_v1_graph_query_db_name_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->execute_graph_query_api_v1_graph_query_db_name_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **graph_query_request** | [**GraphQueryRequest**](GraphQueryRequest.md)|  | 
 **base_branch** | **str**| Base branch (default: master) | [optional] [default to &#39;master&#39;]
 **overlay_branch** | **str**| ES overlay branch (writeback) | [optional] 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**GraphQueryResponse**](GraphQueryResponse.md)

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

# **execute_multi_hop_query_api_v1_graph_query_db_name_multi_hop_post**
> Dict[str, object] execute_multi_hop_query_api_v1_graph_query_db_name_multi_hop_post(db_name, request_body, base_branch=base_branch, overlay_branch=overlay_branch, lang=lang, accept_language=accept_language)

Execute Multi Hop Query

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
    api_instance = spice_harvester_sdk.GraphApi(api_client)
    db_name = 'db_name_example' # str | 
    request_body = None # Dict[str, object] | 
    base_branch = 'master' # str | Base branch (default: master) (optional) (default to 'master')
    overlay_branch = 'overlay_branch_example' # str | ES overlay branch (writeback) (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Execute Multi Hop Query
        api_response = api_instance.execute_multi_hop_query_api_v1_graph_query_db_name_multi_hop_post(db_name, request_body, base_branch=base_branch, overlay_branch=overlay_branch, lang=lang, accept_language=accept_language)
        print("The response of GraphApi->execute_multi_hop_query_api_v1_graph_query_db_name_multi_hop_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->execute_multi_hop_query_api_v1_graph_query_db_name_multi_hop_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **request_body** | [**Dict[str, object]**](object.md)|  | 
 **base_branch** | **str**| Base branch (default: master) | [optional] [default to &#39;master&#39;]
 **overlay_branch** | **str**| ES overlay branch (writeback) | [optional] 
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

# **execute_simple_graph_query_api_v1_graph_query_db_name_simple_post**
> Dict[str, object] execute_simple_graph_query_api_v1_graph_query_db_name_simple_post(db_name, simple_graph_query_request, base_branch=base_branch, overlay_branch=overlay_branch, lang=lang, accept_language=accept_language)

Execute Simple Graph Query

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.simple_graph_query_request import SimpleGraphQueryRequest
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
    api_instance = spice_harvester_sdk.GraphApi(api_client)
    db_name = 'db_name_example' # str | 
    simple_graph_query_request = spice_harvester_sdk.SimpleGraphQueryRequest() # SimpleGraphQueryRequest | 
    base_branch = 'master' # str | Base branch (default: master) (optional) (default to 'master')
    overlay_branch = 'overlay_branch_example' # str | ES overlay branch (writeback) (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Execute Simple Graph Query
        api_response = api_instance.execute_simple_graph_query_api_v1_graph_query_db_name_simple_post(db_name, simple_graph_query_request, base_branch=base_branch, overlay_branch=overlay_branch, lang=lang, accept_language=accept_language)
        print("The response of GraphApi->execute_simple_graph_query_api_v1_graph_query_db_name_simple_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->execute_simple_graph_query_api_v1_graph_query_db_name_simple_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **simple_graph_query_request** | [**SimpleGraphQueryRequest**](SimpleGraphQueryRequest.md)|  | 
 **base_branch** | **str**| Base branch (default: master) | [optional] [default to &#39;master&#39;]
 **overlay_branch** | **str**| ES overlay branch (writeback) | [optional] 
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

# **find_relationship_paths_api_v1_graph_query_db_name_paths_get**
> object find_relationship_paths_api_v1_graph_query_db_name_paths_get(db_name, source_class, target_class, max_depth=max_depth, branch=branch, lang=lang, accept_language=accept_language)

Find Relationship Paths

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
    api_instance = spice_harvester_sdk.GraphApi(api_client)
    db_name = 'db_name_example' # str | 
    source_class = 'source_class_example' # str | 
    target_class = 'target_class_example' # str | 
    max_depth = 5 # int |  (optional) (default to 5)
    branch = 'master' # str | Target branch (default: master) (optional) (default to 'master')
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Find Relationship Paths
        api_response = api_instance.find_relationship_paths_api_v1_graph_query_db_name_paths_get(db_name, source_class, target_class, max_depth=max_depth, branch=branch, lang=lang, accept_language=accept_language)
        print("The response of GraphApi->find_relationship_paths_api_v1_graph_query_db_name_paths_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->find_relationship_paths_api_v1_graph_query_db_name_paths_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **source_class** | **str**|  | 
 **target_class** | **str**|  | 
 **max_depth** | **int**|  | [optional] [default to 5]
 **branch** | **str**| Target branch (default: master) | [optional] [default to &#39;master&#39;]
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

# **graph_service_health_api_v1_graph_query_health_get**
> object graph_service_health_api_v1_graph_query_health_get(lang=lang, accept_language=accept_language)

Graph Service Health

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
    api_instance = spice_harvester_sdk.GraphApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Graph Service Health
        api_response = api_instance.graph_service_health_api_v1_graph_query_health_get(lang=lang, accept_language=accept_language)
        print("The response of GraphApi->graph_service_health_api_v1_graph_query_health_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GraphApi->graph_service_health_api_v1_graph_query_health_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

