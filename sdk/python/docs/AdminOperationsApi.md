# spice_harvester_sdk.AdminOperationsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post**](AdminOperationsApi.md#cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post) | **POST** /api/v1/admin/cleanup-old-replays | Cleanup Old Replay Results
[**cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post_0**](AdminOperationsApi.md#cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post_0) | **POST** /api/v1/admin/cleanup-old-replays | Cleanup Old Replay Results
[**get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get**](AdminOperationsApi.md#get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get) | **GET** /api/v1/admin/databases/{db_name}/rebuild-index/{task_id}/status | Check rebuild index task status
[**get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get_0**](AdminOperationsApi.md#get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get_0) | **GET** /api/v1/admin/databases/{db_name}/rebuild-index/{task_id}/status | Check rebuild index task status
[**get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get**](AdminOperationsApi.md#get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get) | **GET** /api/v1/admin/recompute-projection/{task_id}/result | Get recompute projection task result
[**get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get_0**](AdminOperationsApi.md#get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get_0) | **GET** /api/v1/admin/recompute-projection/{task_id}/result | Get recompute projection task result
[**get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get**](AdminOperationsApi.md#get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get) | **GET** /api/v1/admin/replay-instance-state/{task_id}/result | Get instance replay result
[**get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get_0**](AdminOperationsApi.md#get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get_0) | **GET** /api/v1/admin/replay-instance-state/{task_id}/result | Get instance replay result
[**get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get**](AdminOperationsApi.md#get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get) | **GET** /api/v1/admin/replay-instance-state/{task_id}/trace | Get instance replay trace with audit/lineage
[**get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get_0**](AdminOperationsApi.md#get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get_0) | **GET** /api/v1/admin/replay-instance-state/{task_id}/trace | Get instance replay trace with audit/lineage
[**list_lakefs_credentials_api_v1_admin_lakefs_credentials_get**](AdminOperationsApi.md#list_lakefs_credentials_api_v1_admin_lakefs_credentials_get) | **GET** /api/v1/admin/lakefs/credentials | List Lakefs Credentials
[**list_lakefs_credentials_api_v1_admin_lakefs_credentials_get_0**](AdminOperationsApi.md#list_lakefs_credentials_api_v1_admin_lakefs_credentials_get_0) | **GET** /api/v1/admin/lakefs/credentials | List Lakefs Credentials
[**rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post**](AdminOperationsApi.md#rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post) | **POST** /api/v1/admin/databases/{db_name}/rebuild-index | Migrate ES index mappings (alias-swap rebuild)
[**rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post_0**](AdminOperationsApi.md#rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post_0) | **POST** /api/v1/admin/databases/{db_name}/rebuild-index | Migrate ES index mappings (alias-swap rebuild)
[**recompute_projection_api_v1_admin_recompute_projection_post**](AdminOperationsApi.md#recompute_projection_api_v1_admin_recompute_projection_post) | **POST** /api/v1/admin/recompute-projection | Replay S3 events to rebuild a projection
[**recompute_projection_api_v1_admin_recompute_projection_post_0**](AdminOperationsApi.md#recompute_projection_api_v1_admin_recompute_projection_post_0) | **POST** /api/v1/admin/recompute-projection | Replay S3 events to rebuild a projection
[**reindex_instances_endpoint_api_v1_admin_reindex_instances_post**](AdminOperationsApi.md#reindex_instances_endpoint_api_v1_admin_reindex_instances_post) | **POST** /api/v1/admin/reindex-instances | Reindex all instances for a database (dataset-primary rebuild)
[**reindex_instances_endpoint_api_v1_admin_reindex_instances_post_0**](AdminOperationsApi.md#reindex_instances_endpoint_api_v1_admin_reindex_instances_post_0) | **POST** /api/v1/admin/reindex-instances | Reindex all instances for a database (dataset-primary rebuild)
[**replay_instance_state_api_v1_admin_replay_instance_state_post**](AdminOperationsApi.md#replay_instance_state_api_v1_admin_replay_instance_state_post) | **POST** /api/v1/admin/replay-instance-state | Replay single instance event history (debugging)
[**replay_instance_state_api_v1_admin_replay_instance_state_post_0**](AdminOperationsApi.md#replay_instance_state_api_v1_admin_replay_instance_state_post_0) | **POST** /api/v1/admin/replay-instance-state | Replay single instance event history (debugging)
[**upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post**](AdminOperationsApi.md#upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post) | **POST** /api/v1/admin/lakefs/credentials | Upsert Lakefs Credentials
[**upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post_0**](AdminOperationsApi.md#upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post_0) | **POST** /api/v1/admin/lakefs/credentials | Upsert Lakefs Credentials


# **cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post**
> Dict[str, object] cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post(older_than_hours=older_than_hours, lang=lang, accept_language=accept_language)

Cleanup Old Replay Results

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    older_than_hours = 24 # int | Delete results older than N hours (optional) (default to 24)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Cleanup Old Replay Results
        api_response = api_instance.cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post(older_than_hours=older_than_hours, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **older_than_hours** | **int**| Delete results older than N hours | [optional] [default to 24]
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

# **cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post_0**
> Dict[str, object] cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post_0(older_than_hours=older_than_hours, lang=lang, accept_language=accept_language)

Cleanup Old Replay Results

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    older_than_hours = 24 # int | Delete results older than N hours (optional) (default to 24)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Cleanup Old Replay Results
        api_response = api_instance.cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post_0(older_than_hours=older_than_hours, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->cleanup_old_replay_results_api_v1_admin_cleanup_old_replays_post_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **older_than_hours** | **int**| Delete results older than N hours | [optional] [default to 24]
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

# **get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get**
> Dict[str, object] get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get(db_name, task_id, lang=lang, accept_language=accept_language)

Check rebuild index task status

Check the status of a rebuild task.

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    db_name = 'db_name_example' # str | 
    task_id = 'task_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Check rebuild index task status
        api_response = api_instance.get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get(db_name, task_id, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
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

# **get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get_0**
> Dict[str, object] get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get_0(db_name, task_id, lang=lang, accept_language=accept_language)

Check rebuild index task status

Check the status of a rebuild task.

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    db_name = 'db_name_example' # str | 
    task_id = 'task_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Check rebuild index task status
        api_response = api_instance.get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get_0(db_name, task_id, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->get_rebuild_status_api_v1_admin_databases_db_name_rebuild_index_task_id_status_get_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
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

# **get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get**
> Dict[str, object] get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get(task_id, lang=lang, accept_language=accept_language)

Get recompute projection task result

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    task_id = 'task_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get recompute projection task result
        api_response = api_instance.get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get(task_id, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get: %s\n" % e)
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

# **get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get_0**
> Dict[str, object] get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get_0(task_id, lang=lang, accept_language=accept_language)

Get recompute projection task result

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    task_id = 'task_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get recompute projection task result
        api_response = api_instance.get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get_0(task_id, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->get_recompute_projection_result_api_v1_admin_recompute_projection_task_id_result_get_0: %s\n" % e)
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

# **get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get**
> Dict[str, object] get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get(task_id, lang=lang, accept_language=accept_language)

Get instance replay result

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    task_id = 'task_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get instance replay result
        api_response = api_instance.get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get(task_id, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get: %s\n" % e)
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

# **get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get_0**
> Dict[str, object] get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get_0(task_id, lang=lang, accept_language=accept_language)

Get instance replay result

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    task_id = 'task_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get instance replay result
        api_response = api_instance.get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get_0(task_id, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->get_replay_result_api_v1_admin_replay_instance_state_task_id_result_get_0: %s\n" % e)
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

# **get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get**
> Dict[str, object] get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get(task_id, command_id=command_id, include_audit=include_audit, audit_limit=audit_limit, include_lineage=include_lineage, lineage_direction=lineage_direction, lineage_max_depth=lineage_max_depth, lineage_max_nodes=lineage_max_nodes, lineage_max_edges=lineage_max_edges, timeline_limit=timeline_limit, lang=lang, accept_language=accept_language)

Get instance replay trace with audit/lineage

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    task_id = 'task_id_example' # str | 
    command_id = 'command_id_example' # str | Command/event id to trace (defaults to last command in the replayed history) (optional)
    include_audit = True # bool | Include audit logs for the selected command (optional) (default to True)
    audit_limit = 200 # int |  (optional) (default to 200)
    include_lineage = True # bool | Include lineage graph for the selected command (optional) (default to True)
    lineage_direction = both # str | Lineage traversal direction (optional) (default to both)
    lineage_max_depth = 5 # int |  (optional) (default to 5)
    lineage_max_nodes = 500 # int |  (optional) (default to 500)
    lineage_max_edges = 2000 # int |  (optional) (default to 2000)
    timeline_limit = 200 # int | Max command history items to include (optional) (default to 200)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get instance replay trace with audit/lineage
        api_response = api_instance.get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get(task_id, command_id=command_id, include_audit=include_audit, audit_limit=audit_limit, include_lineage=include_lineage, lineage_direction=lineage_direction, lineage_max_depth=lineage_max_depth, lineage_max_nodes=lineage_max_nodes, lineage_max_edges=lineage_max_edges, timeline_limit=timeline_limit, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **task_id** | **str**|  | 
 **command_id** | **str**| Command/event id to trace (defaults to last command in the replayed history) | [optional] 
 **include_audit** | **bool**| Include audit logs for the selected command | [optional] [default to True]
 **audit_limit** | **int**|  | [optional] [default to 200]
 **include_lineage** | **bool**| Include lineage graph for the selected command | [optional] [default to True]
 **lineage_direction** | **str**| Lineage traversal direction | [optional] [default to both]
 **lineage_max_depth** | **int**|  | [optional] [default to 5]
 **lineage_max_nodes** | **int**|  | [optional] [default to 500]
 **lineage_max_edges** | **int**|  | [optional] [default to 2000]
 **timeline_limit** | **int**| Max command history items to include | [optional] [default to 200]
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

# **get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get_0**
> Dict[str, object] get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get_0(task_id, command_id=command_id, include_audit=include_audit, audit_limit=audit_limit, include_lineage=include_lineage, lineage_direction=lineage_direction, lineage_max_depth=lineage_max_depth, lineage_max_nodes=lineage_max_nodes, lineage_max_edges=lineage_max_edges, timeline_limit=timeline_limit, lang=lang, accept_language=accept_language)

Get instance replay trace with audit/lineage

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    task_id = 'task_id_example' # str | 
    command_id = 'command_id_example' # str | Command/event id to trace (defaults to last command in the replayed history) (optional)
    include_audit = True # bool | Include audit logs for the selected command (optional) (default to True)
    audit_limit = 200 # int |  (optional) (default to 200)
    include_lineage = True # bool | Include lineage graph for the selected command (optional) (default to True)
    lineage_direction = both # str | Lineage traversal direction (optional) (default to both)
    lineage_max_depth = 5 # int |  (optional) (default to 5)
    lineage_max_nodes = 500 # int |  (optional) (default to 500)
    lineage_max_edges = 2000 # int |  (optional) (default to 2000)
    timeline_limit = 200 # int | Max command history items to include (optional) (default to 200)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get instance replay trace with audit/lineage
        api_response = api_instance.get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get_0(task_id, command_id=command_id, include_audit=include_audit, audit_limit=audit_limit, include_lineage=include_lineage, lineage_direction=lineage_direction, lineage_max_depth=lineage_max_depth, lineage_max_nodes=lineage_max_nodes, lineage_max_edges=lineage_max_edges, timeline_limit=timeline_limit, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->get_replay_trace_api_v1_admin_replay_instance_state_task_id_trace_get_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **task_id** | **str**|  | 
 **command_id** | **str**| Command/event id to trace (defaults to last command in the replayed history) | [optional] 
 **include_audit** | **bool**| Include audit logs for the selected command | [optional] [default to True]
 **audit_limit** | **int**|  | [optional] [default to 200]
 **include_lineage** | **bool**| Include lineage graph for the selected command | [optional] [default to True]
 **lineage_direction** | **str**| Lineage traversal direction | [optional] [default to both]
 **lineage_max_depth** | **int**|  | [optional] [default to 5]
 **lineage_max_nodes** | **int**|  | [optional] [default to 500]
 **lineage_max_edges** | **int**|  | [optional] [default to 2000]
 **timeline_limit** | **int**| Max command history items to include | [optional] [default to 200]
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

# **list_lakefs_credentials_api_v1_admin_lakefs_credentials_get**
> Dict[str, object] list_lakefs_credentials_api_v1_admin_lakefs_credentials_get(lang=lang, accept_language=accept_language)

List Lakefs Credentials

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Lakefs Credentials
        api_response = api_instance.list_lakefs_credentials_api_v1_admin_lakefs_credentials_get(lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->list_lakefs_credentials_api_v1_admin_lakefs_credentials_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->list_lakefs_credentials_api_v1_admin_lakefs_credentials_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **list_lakefs_credentials_api_v1_admin_lakefs_credentials_get_0**
> Dict[str, object] list_lakefs_credentials_api_v1_admin_lakefs_credentials_get_0(lang=lang, accept_language=accept_language)

List Lakefs Credentials

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Lakefs Credentials
        api_response = api_instance.list_lakefs_credentials_api_v1_admin_lakefs_credentials_get_0(lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->list_lakefs_credentials_api_v1_admin_lakefs_credentials_get_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->list_lakefs_credentials_api_v1_admin_lakefs_credentials_get_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post**
> Dict[str, object] rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post(db_name, branch=branch, lang=lang, accept_language=accept_language)

Migrate ES index mappings (alias-swap rebuild)

Creates a new versioned ES index, copies existing docs with updated mappings, then performs atomic alias swap. Does NOT re-derive from source data.

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    db_name = 'db_name_example' # str | 
    branch = 'master' # str |  (optional) (default to 'master')
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Migrate ES index mappings (alias-swap rebuild)
        api_response = api_instance.rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post(db_name, branch=branch, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
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
**202** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post_0**
> Dict[str, object] rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post_0(db_name, branch=branch, lang=lang, accept_language=accept_language)

Migrate ES index mappings (alias-swap rebuild)

Creates a new versioned ES index, copies existing docs with updated mappings, then performs atomic alias swap. Does NOT re-derive from source data.

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    db_name = 'db_name_example' # str | 
    branch = 'master' # str |  (optional) (default to 'master')
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Migrate ES index mappings (alias-swap rebuild)
        api_response = api_instance.rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post_0(db_name, branch=branch, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->rebuild_instance_index_endpoint_api_v1_admin_databases_db_name_rebuild_index_post_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
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
**202** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **recompute_projection_api_v1_admin_recompute_projection_post**
> RecomputeProjectionResponse recompute_projection_api_v1_admin_recompute_projection_post(recompute_projection_request, lang=lang, accept_language=accept_language)

Replay S3 events to rebuild a projection

Replays LakeFS/S3 immutable events within a time range to reconstruct instances or ontologies projection.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.recompute_projection_request import RecomputeProjectionRequest
from spice_harvester_sdk.models.recompute_projection_response import RecomputeProjectionResponse
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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    recompute_projection_request = spice_harvester_sdk.RecomputeProjectionRequest() # RecomputeProjectionRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Replay S3 events to rebuild a projection
        api_response = api_instance.recompute_projection_api_v1_admin_recompute_projection_post(recompute_projection_request, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->recompute_projection_api_v1_admin_recompute_projection_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->recompute_projection_api_v1_admin_recompute_projection_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **recompute_projection_request** | [**RecomputeProjectionRequest**](RecomputeProjectionRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**RecomputeProjectionResponse**](RecomputeProjectionResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**202** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **recompute_projection_api_v1_admin_recompute_projection_post_0**
> RecomputeProjectionResponse recompute_projection_api_v1_admin_recompute_projection_post_0(recompute_projection_request, lang=lang, accept_language=accept_language)

Replay S3 events to rebuild a projection

Replays LakeFS/S3 immutable events within a time range to reconstruct instances or ontologies projection.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.recompute_projection_request import RecomputeProjectionRequest
from spice_harvester_sdk.models.recompute_projection_response import RecomputeProjectionResponse
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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    recompute_projection_request = spice_harvester_sdk.RecomputeProjectionRequest() # RecomputeProjectionRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Replay S3 events to rebuild a projection
        api_response = api_instance.recompute_projection_api_v1_admin_recompute_projection_post_0(recompute_projection_request, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->recompute_projection_api_v1_admin_recompute_projection_post_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->recompute_projection_api_v1_admin_recompute_projection_post_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **recompute_projection_request** | [**RecomputeProjectionRequest**](RecomputeProjectionRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**RecomputeProjectionResponse**](RecomputeProjectionResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**202** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **reindex_instances_endpoint_api_v1_admin_reindex_instances_post**
> Dict[str, object] reindex_instances_endpoint_api_v1_admin_reindex_instances_post(db_name, branch=branch, delete_index_first=delete_index_first, lang=lang, accept_language=accept_language)

Reindex all instances for a database (dataset-primary rebuild)

Re-executes ALL objectify jobs from source datasets. This is the nuclear rebuild option.

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    db_name = 'db_name_example' # str | Database name
    branch = 'master' # str | Branch (optional) (default to 'master')
    delete_index_first = False # bool | Delete ES index before rebuild (optional) (default to False)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Reindex all instances for a database (dataset-primary rebuild)
        api_response = api_instance.reindex_instances_endpoint_api_v1_admin_reindex_instances_post(db_name, branch=branch, delete_index_first=delete_index_first, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->reindex_instances_endpoint_api_v1_admin_reindex_instances_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->reindex_instances_endpoint_api_v1_admin_reindex_instances_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**| Database name | 
 **branch** | **str**| Branch | [optional] [default to &#39;master&#39;]
 **delete_index_first** | **bool**| Delete ES index before rebuild | [optional] [default to False]
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
**202** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **reindex_instances_endpoint_api_v1_admin_reindex_instances_post_0**
> Dict[str, object] reindex_instances_endpoint_api_v1_admin_reindex_instances_post_0(db_name, branch=branch, delete_index_first=delete_index_first, lang=lang, accept_language=accept_language)

Reindex all instances for a database (dataset-primary rebuild)

Re-executes ALL objectify jobs from source datasets. This is the nuclear rebuild option.

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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    db_name = 'db_name_example' # str | Database name
    branch = 'master' # str | Branch (optional) (default to 'master')
    delete_index_first = False # bool | Delete ES index before rebuild (optional) (default to False)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Reindex all instances for a database (dataset-primary rebuild)
        api_response = api_instance.reindex_instances_endpoint_api_v1_admin_reindex_instances_post_0(db_name, branch=branch, delete_index_first=delete_index_first, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->reindex_instances_endpoint_api_v1_admin_reindex_instances_post_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->reindex_instances_endpoint_api_v1_admin_reindex_instances_post_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**| Database name | 
 **branch** | **str**| Branch | [optional] [default to &#39;master&#39;]
 **delete_index_first** | **bool**| Delete ES index before rebuild | [optional] [default to False]
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
**202** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **replay_instance_state_api_v1_admin_replay_instance_state_post**
> ReplayInstanceStateResponse replay_instance_state_api_v1_admin_replay_instance_state_post(replay_instance_state_request, lang=lang, accept_language=accept_language)

Replay single instance event history (debugging)

Replays the full event history of one specific instance for debugging and state inspection.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.replay_instance_state_request import ReplayInstanceStateRequest
from spice_harvester_sdk.models.replay_instance_state_response import ReplayInstanceStateResponse
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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    replay_instance_state_request = spice_harvester_sdk.ReplayInstanceStateRequest() # ReplayInstanceStateRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Replay single instance event history (debugging)
        api_response = api_instance.replay_instance_state_api_v1_admin_replay_instance_state_post(replay_instance_state_request, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->replay_instance_state_api_v1_admin_replay_instance_state_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->replay_instance_state_api_v1_admin_replay_instance_state_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **replay_instance_state_request** | [**ReplayInstanceStateRequest**](ReplayInstanceStateRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**ReplayInstanceStateResponse**](ReplayInstanceStateResponse.md)

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

# **replay_instance_state_api_v1_admin_replay_instance_state_post_0**
> ReplayInstanceStateResponse replay_instance_state_api_v1_admin_replay_instance_state_post_0(replay_instance_state_request, lang=lang, accept_language=accept_language)

Replay single instance event history (debugging)

Replays the full event history of one specific instance for debugging and state inspection.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.replay_instance_state_request import ReplayInstanceStateRequest
from spice_harvester_sdk.models.replay_instance_state_response import ReplayInstanceStateResponse
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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    replay_instance_state_request = spice_harvester_sdk.ReplayInstanceStateRequest() # ReplayInstanceStateRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Replay single instance event history (debugging)
        api_response = api_instance.replay_instance_state_api_v1_admin_replay_instance_state_post_0(replay_instance_state_request, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->replay_instance_state_api_v1_admin_replay_instance_state_post_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->replay_instance_state_api_v1_admin_replay_instance_state_post_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **replay_instance_state_request** | [**ReplayInstanceStateRequest**](ReplayInstanceStateRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**ReplayInstanceStateResponse**](ReplayInstanceStateResponse.md)

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

# **upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post**
> Dict[str, object] upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post(lake_fs_credentials_upsert_request, lang=lang, accept_language=accept_language)

Upsert Lakefs Credentials

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.lake_fs_credentials_upsert_request import LakeFSCredentialsUpsertRequest
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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    lake_fs_credentials_upsert_request = spice_harvester_sdk.LakeFSCredentialsUpsertRequest() # LakeFSCredentialsUpsertRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Upsert Lakefs Credentials
        api_response = api_instance.upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post(lake_fs_credentials_upsert_request, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **lake_fs_credentials_upsert_request** | [**LakeFSCredentialsUpsertRequest**](LakeFSCredentialsUpsertRequest.md)|  | 
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

# **upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post_0**
> Dict[str, object] upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post_0(lake_fs_credentials_upsert_request, lang=lang, accept_language=accept_language)

Upsert Lakefs Credentials

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.lake_fs_credentials_upsert_request import LakeFSCredentialsUpsertRequest
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
    api_instance = spice_harvester_sdk.AdminOperationsApi(api_client)
    lake_fs_credentials_upsert_request = spice_harvester_sdk.LakeFSCredentialsUpsertRequest() # LakeFSCredentialsUpsertRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Upsert Lakefs Credentials
        api_response = api_instance.upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post_0(lake_fs_credentials_upsert_request, lang=lang, accept_language=accept_language)
        print("The response of AdminOperationsApi->upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post_0:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminOperationsApi->upsert_lakefs_credentials_api_v1_admin_lakefs_credentials_post_0: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **lake_fs_credentials_upsert_request** | [**LakeFSCredentialsUpsertRequest**](LakeFSCredentialsUpsertRequest.md)|  | 
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

