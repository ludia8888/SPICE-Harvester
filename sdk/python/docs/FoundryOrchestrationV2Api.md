# spice_harvester_sdk.FoundryOrchestrationV2Api

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**cancel_build_v2_api_v2_orchestration_builds_build_rid_cancel_post**](FoundryOrchestrationV2Api.md#cancel_build_v2_api_v2_orchestration_builds_build_rid_cancel_post) | **POST** /api/v2/orchestration/builds/{buildRid}/cancel | Cancel Build V2
[**create_build_v2_api_v2_orchestration_builds_create_post**](FoundryOrchestrationV2Api.md#create_build_v2_api_v2_orchestration_builds_create_post) | **POST** /api/v2/orchestration/builds/create | Create Build V2
[**create_schedule_v2_api_v2_orchestration_schedules_post**](FoundryOrchestrationV2Api.md#create_schedule_v2_api_v2_orchestration_schedules_post) | **POST** /api/v2/orchestration/schedules | Create Schedule V2
[**delete_schedule_v2_api_v2_orchestration_schedules_schedule_rid_delete**](FoundryOrchestrationV2Api.md#delete_schedule_v2_api_v2_orchestration_schedules_schedule_rid_delete) | **DELETE** /api/v2/orchestration/schedules/{scheduleRid} | Delete Schedule V2
[**deploy_build_v2_api_v2_orchestration_builds_build_rid_deploy_post**](FoundryOrchestrationV2Api.md#deploy_build_v2_api_v2_orchestration_builds_build_rid_deploy_post) | **POST** /api/v2/orchestration/builds/{buildRid}/deploy | Deploy Build V2
[**get_build_v2_api_v2_orchestration_builds_build_rid_get**](FoundryOrchestrationV2Api.md#get_build_v2_api_v2_orchestration_builds_build_rid_get) | **GET** /api/v2/orchestration/builds/{buildRid} | Get Build V2
[**get_builds_batch_v2_api_v2_orchestration_builds_get_batch_post**](FoundryOrchestrationV2Api.md#get_builds_batch_v2_api_v2_orchestration_builds_get_batch_post) | **POST** /api/v2/orchestration/builds/getBatch | Get Builds Batch V2
[**get_schedule_v2_api_v2_orchestration_schedules_schedule_rid_get**](FoundryOrchestrationV2Api.md#get_schedule_v2_api_v2_orchestration_schedules_schedule_rid_get) | **GET** /api/v2/orchestration/schedules/{scheduleRid} | Get Schedule V2
[**list_build_jobs_v2_api_v2_orchestration_builds_build_rid_jobs_get**](FoundryOrchestrationV2Api.md#list_build_jobs_v2_api_v2_orchestration_builds_build_rid_jobs_get) | **GET** /api/v2/orchestration/builds/{buildRid}/jobs | List Build Jobs V2
[**list_schedule_runs_v2_api_v2_orchestration_schedules_schedule_rid_runs_get**](FoundryOrchestrationV2Api.md#list_schedule_runs_v2_api_v2_orchestration_schedules_schedule_rid_runs_get) | **GET** /api/v2/orchestration/schedules/{scheduleRid}/runs | List Schedule Runs V2
[**pause_schedule_v2_api_v2_orchestration_schedules_schedule_rid_pause_post**](FoundryOrchestrationV2Api.md#pause_schedule_v2_api_v2_orchestration_schedules_schedule_rid_pause_post) | **POST** /api/v2/orchestration/schedules/{scheduleRid}/pause | Pause Schedule V2
[**unpause_schedule_v2_api_v2_orchestration_schedules_schedule_rid_unpause_post**](FoundryOrchestrationV2Api.md#unpause_schedule_v2_api_v2_orchestration_schedules_schedule_rid_unpause_post) | **POST** /api/v2/orchestration/schedules/{scheduleRid}/unpause | Unpause Schedule V2


# **cancel_build_v2_api_v2_orchestration_builds_build_rid_cancel_post**
> object cancel_build_v2_api_v2_orchestration_builds_build_rid_cancel_post(build_rid)

Cancel Build V2

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
    api_instance = spice_harvester_sdk.FoundryOrchestrationV2Api(api_client)
    build_rid = 'build_rid_example' # str | 

    try:
        # Cancel Build V2
        api_response = api_instance.cancel_build_v2_api_v2_orchestration_builds_build_rid_cancel_post(build_rid)
        print("The response of FoundryOrchestrationV2Api->cancel_build_v2_api_v2_orchestration_builds_build_rid_cancel_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryOrchestrationV2Api->cancel_build_v2_api_v2_orchestration_builds_build_rid_cancel_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **build_rid** | **str**|  | 

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

# **create_build_v2_api_v2_orchestration_builds_create_post**
> object create_build_v2_api_v2_orchestration_builds_create_post(request_body)

Create Build V2

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
    api_instance = spice_harvester_sdk.FoundryOrchestrationV2Api(api_client)
    request_body = None # Dict[str, object] | 

    try:
        # Create Build V2
        api_response = api_instance.create_build_v2_api_v2_orchestration_builds_create_post(request_body)
        print("The response of FoundryOrchestrationV2Api->create_build_v2_api_v2_orchestration_builds_create_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryOrchestrationV2Api->create_build_v2_api_v2_orchestration_builds_create_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **request_body** | [**Dict[str, object]**](object.md)|  | 

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

# **create_schedule_v2_api_v2_orchestration_schedules_post**
> object create_schedule_v2_api_v2_orchestration_schedules_post(request_body)

Create Schedule V2

POST /v2/orchestration/schedules — Create a schedule.

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
    api_instance = spice_harvester_sdk.FoundryOrchestrationV2Api(api_client)
    request_body = None # Dict[str, object] | 

    try:
        # Create Schedule V2
        api_response = api_instance.create_schedule_v2_api_v2_orchestration_schedules_post(request_body)
        print("The response of FoundryOrchestrationV2Api->create_schedule_v2_api_v2_orchestration_schedules_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryOrchestrationV2Api->create_schedule_v2_api_v2_orchestration_schedules_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **request_body** | [**Dict[str, object]**](object.md)|  | 

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

# **delete_schedule_v2_api_v2_orchestration_schedules_schedule_rid_delete**
> delete_schedule_v2_api_v2_orchestration_schedules_schedule_rid_delete(schedule_rid)

Delete Schedule V2

DELETE /v2/orchestration/schedules/{scheduleRid} — Remove schedule.

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
    api_instance = spice_harvester_sdk.FoundryOrchestrationV2Api(api_client)
    schedule_rid = 'schedule_rid_example' # str | 

    try:
        # Delete Schedule V2
        api_instance.delete_schedule_v2_api_v2_orchestration_schedules_schedule_rid_delete(schedule_rid)
    except Exception as e:
        print("Exception when calling FoundryOrchestrationV2Api->delete_schedule_v2_api_v2_orchestration_schedules_schedule_rid_delete: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **schedule_rid** | **str**|  | 

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

# **deploy_build_v2_api_v2_orchestration_builds_build_rid_deploy_post**
> object deploy_build_v2_api_v2_orchestration_builds_build_rid_deploy_post(build_rid, request_body)

Deploy Build V2

POST /v2/orchestration/builds/{buildRid}/deploy — Deploy a successful build.

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
    api_instance = spice_harvester_sdk.FoundryOrchestrationV2Api(api_client)
    build_rid = 'build_rid_example' # str | 
    request_body = None # Dict[str, object] | 

    try:
        # Deploy Build V2
        api_response = api_instance.deploy_build_v2_api_v2_orchestration_builds_build_rid_deploy_post(build_rid, request_body)
        print("The response of FoundryOrchestrationV2Api->deploy_build_v2_api_v2_orchestration_builds_build_rid_deploy_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryOrchestrationV2Api->deploy_build_v2_api_v2_orchestration_builds_build_rid_deploy_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **build_rid** | **str**|  | 
 **request_body** | [**Dict[str, object]**](object.md)|  | 

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

# **get_build_v2_api_v2_orchestration_builds_build_rid_get**
> object get_build_v2_api_v2_orchestration_builds_build_rid_get(build_rid)

Get Build V2

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
    api_instance = spice_harvester_sdk.FoundryOrchestrationV2Api(api_client)
    build_rid = 'build_rid_example' # str | 

    try:
        # Get Build V2
        api_response = api_instance.get_build_v2_api_v2_orchestration_builds_build_rid_get(build_rid)
        print("The response of FoundryOrchestrationV2Api->get_build_v2_api_v2_orchestration_builds_build_rid_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryOrchestrationV2Api->get_build_v2_api_v2_orchestration_builds_build_rid_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **build_rid** | **str**|  | 

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

# **get_builds_batch_v2_api_v2_orchestration_builds_get_batch_post**
> object get_builds_batch_v2_api_v2_orchestration_builds_get_batch_post(request_body)

Get Builds Batch V2

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
    api_instance = spice_harvester_sdk.FoundryOrchestrationV2Api(api_client)
    request_body = None # List[Dict[str, object]] | 

    try:
        # Get Builds Batch V2
        api_response = api_instance.get_builds_batch_v2_api_v2_orchestration_builds_get_batch_post(request_body)
        print("The response of FoundryOrchestrationV2Api->get_builds_batch_v2_api_v2_orchestration_builds_get_batch_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryOrchestrationV2Api->get_builds_batch_v2_api_v2_orchestration_builds_get_batch_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **request_body** | [**List[Dict[str, object]]**](Dict.md)|  | 

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

# **get_schedule_v2_api_v2_orchestration_schedules_schedule_rid_get**
> object get_schedule_v2_api_v2_orchestration_schedules_schedule_rid_get(schedule_rid)

Get Schedule V2

GET /v2/orchestration/schedules/{scheduleRid} — Get a schedule.

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
    api_instance = spice_harvester_sdk.FoundryOrchestrationV2Api(api_client)
    schedule_rid = 'schedule_rid_example' # str | 

    try:
        # Get Schedule V2
        api_response = api_instance.get_schedule_v2_api_v2_orchestration_schedules_schedule_rid_get(schedule_rid)
        print("The response of FoundryOrchestrationV2Api->get_schedule_v2_api_v2_orchestration_schedules_schedule_rid_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryOrchestrationV2Api->get_schedule_v2_api_v2_orchestration_schedules_schedule_rid_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **schedule_rid** | **str**|  | 

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

# **list_build_jobs_v2_api_v2_orchestration_builds_build_rid_jobs_get**
> object list_build_jobs_v2_api_v2_orchestration_builds_build_rid_jobs_get(build_rid, page_size=page_size, page_token=page_token)

List Build Jobs V2

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
    api_instance = spice_harvester_sdk.FoundryOrchestrationV2Api(api_client)
    build_rid = 'build_rid_example' # str | 
    page_size = 56 # int |  (optional)
    page_token = 'page_token_example' # str |  (optional)

    try:
        # List Build Jobs V2
        api_response = api_instance.list_build_jobs_v2_api_v2_orchestration_builds_build_rid_jobs_get(build_rid, page_size=page_size, page_token=page_token)
        print("The response of FoundryOrchestrationV2Api->list_build_jobs_v2_api_v2_orchestration_builds_build_rid_jobs_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryOrchestrationV2Api->list_build_jobs_v2_api_v2_orchestration_builds_build_rid_jobs_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **build_rid** | **str**|  | 
 **page_size** | **int**|  | [optional] 
 **page_token** | **str**|  | [optional] 

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

# **list_schedule_runs_v2_api_v2_orchestration_schedules_schedule_rid_runs_get**
> object list_schedule_runs_v2_api_v2_orchestration_schedules_schedule_rid_runs_get(schedule_rid, page_size=page_size, page_token=page_token)

List Schedule Runs V2

GET /v2/orchestration/schedules/{scheduleRid}/runs — List schedule runs.

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
    api_instance = spice_harvester_sdk.FoundryOrchestrationV2Api(api_client)
    schedule_rid = 'schedule_rid_example' # str | 
    page_size = 25 # int |  (optional) (default to 25)
    page_token = '' # str |  (optional) (default to '')

    try:
        # List Schedule Runs V2
        api_response = api_instance.list_schedule_runs_v2_api_v2_orchestration_schedules_schedule_rid_runs_get(schedule_rid, page_size=page_size, page_token=page_token)
        print("The response of FoundryOrchestrationV2Api->list_schedule_runs_v2_api_v2_orchestration_schedules_schedule_rid_runs_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling FoundryOrchestrationV2Api->list_schedule_runs_v2_api_v2_orchestration_schedules_schedule_rid_runs_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **schedule_rid** | **str**|  | 
 **page_size** | **int**|  | [optional] [default to 25]
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

# **pause_schedule_v2_api_v2_orchestration_schedules_schedule_rid_pause_post**
> pause_schedule_v2_api_v2_orchestration_schedules_schedule_rid_pause_post(schedule_rid)

Pause Schedule V2

POST /v2/orchestration/schedules/{scheduleRid}/pause — Pause schedule.

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
    api_instance = spice_harvester_sdk.FoundryOrchestrationV2Api(api_client)
    schedule_rid = 'schedule_rid_example' # str | 

    try:
        # Pause Schedule V2
        api_instance.pause_schedule_v2_api_v2_orchestration_schedules_schedule_rid_pause_post(schedule_rid)
    except Exception as e:
        print("Exception when calling FoundryOrchestrationV2Api->pause_schedule_v2_api_v2_orchestration_schedules_schedule_rid_pause_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **schedule_rid** | **str**|  | 

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

# **unpause_schedule_v2_api_v2_orchestration_schedules_schedule_rid_unpause_post**
> unpause_schedule_v2_api_v2_orchestration_schedules_schedule_rid_unpause_post(schedule_rid)

Unpause Schedule V2

POST /v2/orchestration/schedules/{scheduleRid}/unpause — Unpause schedule.

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
    api_instance = spice_harvester_sdk.FoundryOrchestrationV2Api(api_client)
    schedule_rid = 'schedule_rid_example' # str | 

    try:
        # Unpause Schedule V2
        api_instance.unpause_schedule_v2_api_v2_orchestration_schedules_schedule_rid_unpause_post(schedule_rid)
    except Exception as e:
        print("Exception when calling FoundryOrchestrationV2Api->unpause_schedule_v2_api_v2_orchestration_schedules_schedule_rid_unpause_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **schedule_rid** | **str**|  | 

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

