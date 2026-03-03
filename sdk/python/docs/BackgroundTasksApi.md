# spice_harvester_sdk.BackgroundTasksApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**cancel_task_api_v1_tasks_task_id_delete**](BackgroundTasksApi.md#cancel_task_api_v1_tasks_task_id_delete) | **DELETE** /api/v1/tasks/{task_id} | Cancel Task
[**get_task_metrics_api_v1_tasks_metrics_summary_get**](BackgroundTasksApi.md#get_task_metrics_api_v1_tasks_metrics_summary_get) | **GET** /api/v1/tasks/metrics/summary | Get Task Metrics
[**get_task_result_api_v1_tasks_task_id_result_get**](BackgroundTasksApi.md#get_task_result_api_v1_tasks_task_id_result_get) | **GET** /api/v1/tasks/{task_id}/result | Get Task Result
[**get_task_status_api_v1_tasks_task_id_get**](BackgroundTasksApi.md#get_task_status_api_v1_tasks_task_id_get) | **GET** /api/v1/tasks/{task_id} | Get Task Status
[**list_tasks_api_v1_tasks_get**](BackgroundTasksApi.md#list_tasks_api_v1_tasks_get) | **GET** /api/v1/tasks/ | List Tasks


# **cancel_task_api_v1_tasks_task_id_delete**
> Dict[str, object] cancel_task_api_v1_tasks_task_id_delete(task_id, lang=lang, accept_language=accept_language)

Cancel Task

Cancel a running background task.

Allows graceful cancellation of long-running tasks,
preventing resource waste and enabling retry.

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
    api_instance = spice_harvester_sdk.BackgroundTasksApi(api_client)
    task_id = 'task_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Cancel Task
        api_response = api_instance.cancel_task_api_v1_tasks_task_id_delete(task_id, lang=lang, accept_language=accept_language)
        print("The response of BackgroundTasksApi->cancel_task_api_v1_tasks_task_id_delete:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling BackgroundTasksApi->cancel_task_api_v1_tasks_task_id_delete: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **task_id** | **str**|  | 
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

# **get_task_metrics_api_v1_tasks_metrics_summary_get**
> TaskMetricsResponse get_task_metrics_api_v1_tasks_metrics_summary_get(lang=lang, accept_language=accept_language)

Get Task Metrics

Get aggregated metrics for all background tasks.

Provides insights into task execution patterns,
success rates, and performance metrics.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.task_metrics_response import TaskMetricsResponse
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
    api_instance = spice_harvester_sdk.BackgroundTasksApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Task Metrics
        api_response = api_instance.get_task_metrics_api_v1_tasks_metrics_summary_get(lang=lang, accept_language=accept_language)
        print("The response of BackgroundTasksApi->get_task_metrics_api_v1_tasks_metrics_summary_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling BackgroundTasksApi->get_task_metrics_api_v1_tasks_metrics_summary_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**TaskMetricsResponse**](TaskMetricsResponse.md)

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

# **get_task_result_api_v1_tasks_task_id_result_get**
> Dict[str, object] get_task_result_api_v1_tasks_task_id_result_get(task_id, lang=lang, accept_language=accept_language)

Get Task Result

Get the result of a completed task.

Returns the full result data for completed tasks,
including any output data or error information.

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
    api_instance = spice_harvester_sdk.BackgroundTasksApi(api_client)
    task_id = 'task_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Task Result
        api_response = api_instance.get_task_result_api_v1_tasks_task_id_result_get(task_id, lang=lang, accept_language=accept_language)
        print("The response of BackgroundTasksApi->get_task_result_api_v1_tasks_task_id_result_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling BackgroundTasksApi->get_task_result_api_v1_tasks_task_id_result_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **task_id** | **str**|  | 
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

# **get_task_status_api_v1_tasks_task_id_get**
> TaskStatusResponse get_task_status_api_v1_tasks_task_id_get(task_id, lang=lang, accept_language=accept_language)

Get Task Status

Get current status of a background task.

This endpoint allows monitoring of any background task by its ID,
providing real-time status updates and results.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.task_status_response import TaskStatusResponse
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
    api_instance = spice_harvester_sdk.BackgroundTasksApi(api_client)
    task_id = 'task_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Task Status
        api_response = api_instance.get_task_status_api_v1_tasks_task_id_get(task_id, lang=lang, accept_language=accept_language)
        print("The response of BackgroundTasksApi->get_task_status_api_v1_tasks_task_id_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling BackgroundTasksApi->get_task_status_api_v1_tasks_task_id_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **task_id** | **str**|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**TaskStatusResponse**](TaskStatusResponse.md)

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

# **list_tasks_api_v1_tasks_get**
> TaskListResponse list_tasks_api_v1_tasks_get(status=status, task_type=task_type, limit=limit, lang=lang, accept_language=accept_language)

List Tasks

List background tasks with optional filtering.

Provides visibility into all background tasks running in the system,
helping identify stuck or failed tasks.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.task_list_response import TaskListResponse
from spice_harvester_sdk.models.task_status import TaskStatus
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
    api_instance = spice_harvester_sdk.BackgroundTasksApi(api_client)
    status = spice_harvester_sdk.TaskStatus() # TaskStatus | Filter by status (optional)
    task_type = 'task_type_example' # str | Filter by task type (optional)
    limit = 100 # int | Maximum tasks to return (optional) (default to 100)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Tasks
        api_response = api_instance.list_tasks_api_v1_tasks_get(status=status, task_type=task_type, limit=limit, lang=lang, accept_language=accept_language)
        print("The response of BackgroundTasksApi->list_tasks_api_v1_tasks_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling BackgroundTasksApi->list_tasks_api_v1_tasks_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **status** | [**TaskStatus**](.md)| Filter by status | [optional] 
 **task_type** | **str**| Filter by task type | [optional] 
 **limit** | **int**| Maximum tasks to return | [optional] [default to 100]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**TaskListResponse**](TaskListResponse.md)

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

