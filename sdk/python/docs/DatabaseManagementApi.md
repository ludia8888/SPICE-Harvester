# spice_harvester_sdk.DatabaseManagementApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_database_api_v1_databases_post**](DatabaseManagementApi.md#create_database_api_v1_databases_post) | **POST** /api/v1/databases | Create Database
[**delete_database_api_v1_databases_db_name_delete**](DatabaseManagementApi.md#delete_database_api_v1_databases_db_name_delete) | **DELETE** /api/v1/databases/{db_name} | Delete Database
[**get_action_log_api_v1_databases_db_name_action_logs_action_log_id_get**](DatabaseManagementApi.md#get_action_log_api_v1_databases_db_name_action_logs_action_log_id_get) | **GET** /api/v1/databases/{db_name}/action-logs/{action_log_id} | Get Action Log
[**get_database_api_v1_databases_db_name_get**](DatabaseManagementApi.md#get_database_api_v1_databases_db_name_get) | **GET** /api/v1/databases/{db_name} | Get Database
[**get_database_expected_seq_api_v1_databases_db_name_expected_seq_get**](DatabaseManagementApi.md#get_database_expected_seq_api_v1_databases_db_name_expected_seq_get) | **GET** /api/v1/databases/{db_name}/expected-seq | Get Database Expected Seq
[**list_database_access_api_v1_databases_db_name_access_get**](DatabaseManagementApi.md#list_database_access_api_v1_databases_db_name_access_get) | **GET** /api/v1/databases/{db_name}/access | List Database Access
[**list_databases_api_v1_databases_get**](DatabaseManagementApi.md#list_databases_api_v1_databases_get) | **GET** /api/v1/databases | List Databases
[**upsert_database_access_api_v1_databases_db_name_access_post**](DatabaseManagementApi.md#upsert_database_access_api_v1_databases_db_name_access_post) | **POST** /api/v1/databases/{db_name}/access | Upsert Database Access


# **create_database_api_v1_databases_post**
> ApiResponse create_database_api_v1_databases_post(database_create_request, lang=lang, accept_language=accept_language)

Create Database

데이터베이스 생성

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
from spice_harvester_sdk.models.database_create_request import DatabaseCreateRequest
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
    api_instance = spice_harvester_sdk.DatabaseManagementApi(api_client)
    database_create_request = spice_harvester_sdk.DatabaseCreateRequest() # DatabaseCreateRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Create Database
        api_response = api_instance.create_database_api_v1_databases_post(database_create_request, lang=lang, accept_language=accept_language)
        print("The response of DatabaseManagementApi->create_database_api_v1_databases_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DatabaseManagementApi->create_database_api_v1_databases_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **database_create_request** | [**DatabaseCreateRequest**](DatabaseCreateRequest.md)|  | 
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
**201** | Direct mode |  -  |
**202** | Event-sourcing mode (async) |  -  |
**409** | Conflict (already exists / OCC) |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **delete_database_api_v1_databases_db_name_delete**
> ApiResponse delete_database_api_v1_databases_db_name_delete(db_name, expected_seq=expected_seq, lang=lang, accept_language=accept_language)

Delete Database

데이터베이스 삭제

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
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
    api_instance = spice_harvester_sdk.DatabaseManagementApi(api_client)
    db_name = 'db_name_example' # str | 
    expected_seq = 56 # int | Expected current aggregate sequence (OCC). When omitted, server resolves the current value. (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Delete Database
        api_response = api_instance.delete_database_api_v1_databases_db_name_delete(db_name, expected_seq=expected_seq, lang=lang, accept_language=accept_language)
        print("The response of DatabaseManagementApi->delete_database_api_v1_databases_db_name_delete:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DatabaseManagementApi->delete_database_api_v1_databases_db_name_delete: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **expected_seq** | **int**| Expected current aggregate sequence (OCC). When omitted, server resolves the current value. | [optional] 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**ApiResponse**](ApiResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Direct mode |  -  |
**202** | Event-sourcing mode (async) |  -  |
**409** | OCC conflict |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_action_log_api_v1_databases_db_name_action_logs_action_log_id_get**
> ApiResponse get_action_log_api_v1_databases_db_name_action_logs_action_log_id_get(db_name, action_log_id, lang=lang, accept_language=accept_language)

Get Action Log

Get an action log record (writeback status + targets) for observability/debugging.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
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
    api_instance = spice_harvester_sdk.DatabaseManagementApi(api_client)
    db_name = 'db_name_example' # str | 
    action_log_id = 'action_log_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Action Log
        api_response = api_instance.get_action_log_api_v1_databases_db_name_action_logs_action_log_id_get(db_name, action_log_id, lang=lang, accept_language=accept_language)
        print("The response of DatabaseManagementApi->get_action_log_api_v1_databases_db_name_action_logs_action_log_id_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DatabaseManagementApi->get_action_log_api_v1_databases_db_name_action_logs_action_log_id_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **action_log_id** | **str**|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**ApiResponse**](ApiResponse.md)

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

# **get_database_api_v1_databases_db_name_get**
> object get_database_api_v1_databases_db_name_get(db_name, lang=lang, accept_language=accept_language)

Get Database

데이터베이스 정보 조회

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
    api_instance = spice_harvester_sdk.DatabaseManagementApi(api_client)
    db_name = 'db_name_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Database
        api_response = api_instance.get_database_api_v1_databases_db_name_get(db_name, lang=lang, accept_language=accept_language)
        print("The response of DatabaseManagementApi->get_database_api_v1_databases_db_name_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DatabaseManagementApi->get_database_api_v1_databases_db_name_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
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

# **get_database_expected_seq_api_v1_databases_db_name_expected_seq_get**
> ApiResponse get_database_expected_seq_api_v1_databases_db_name_expected_seq_get(db_name, lang=lang, accept_language=accept_language)

Get Database Expected Seq

Resolve the current `expected_seq` for database (aggregate) operations.

Frontend policy: OCC tokens should be treated as resource versions, not user input.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
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
    api_instance = spice_harvester_sdk.DatabaseManagementApi(api_client)
    db_name = 'db_name_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Database Expected Seq
        api_response = api_instance.get_database_expected_seq_api_v1_databases_db_name_expected_seq_get(db_name, lang=lang, accept_language=accept_language)
        print("The response of DatabaseManagementApi->get_database_expected_seq_api_v1_databases_db_name_expected_seq_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DatabaseManagementApi->get_database_expected_seq_api_v1_databases_db_name_expected_seq_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**ApiResponse**](ApiResponse.md)

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

# **list_database_access_api_v1_databases_db_name_access_get**
> ApiResponse list_database_access_api_v1_databases_db_name_access_get(db_name, lang=lang, accept_language=accept_language)

List Database Access

List database access entries (RBAC).

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
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
    api_instance = spice_harvester_sdk.DatabaseManagementApi(api_client)
    db_name = 'db_name_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Database Access
        api_response = api_instance.list_database_access_api_v1_databases_db_name_access_get(db_name, lang=lang, accept_language=accept_language)
        print("The response of DatabaseManagementApi->list_database_access_api_v1_databases_db_name_access_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DatabaseManagementApi->list_database_access_api_v1_databases_db_name_access_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**ApiResponse**](ApiResponse.md)

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

# **list_databases_api_v1_databases_get**
> ApiResponse list_databases_api_v1_databases_get(lang=lang, accept_language=accept_language)

List Databases

데이터베이스 목록 조회

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
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
    api_instance = spice_harvester_sdk.DatabaseManagementApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Databases
        api_response = api_instance.list_databases_api_v1_databases_get(lang=lang, accept_language=accept_language)
        print("The response of DatabaseManagementApi->list_databases_api_v1_databases_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DatabaseManagementApi->list_databases_api_v1_databases_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**ApiResponse**](ApiResponse.md)

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

# **upsert_database_access_api_v1_databases_db_name_access_post**
> ApiResponse upsert_database_access_api_v1_databases_db_name_access_post(db_name, upsert_database_access_request, lang=lang, accept_language=accept_language)

Upsert Database Access

Upsert database access entries (RBAC). Requires Owner/Security.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
from spice_harvester_sdk.models.upsert_database_access_request import UpsertDatabaseAccessRequest
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
    api_instance = spice_harvester_sdk.DatabaseManagementApi(api_client)
    db_name = 'db_name_example' # str | 
    upsert_database_access_request = spice_harvester_sdk.UpsertDatabaseAccessRequest() # UpsertDatabaseAccessRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Upsert Database Access
        api_response = api_instance.upsert_database_access_api_v1_databases_db_name_access_post(db_name, upsert_database_access_request, lang=lang, accept_language=accept_language)
        print("The response of DatabaseManagementApi->upsert_database_access_api_v1_databases_db_name_access_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DatabaseManagementApi->upsert_database_access_api_v1_databases_db_name_access_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **upsert_database_access_request** | [**UpsertDatabaseAccessRequest**](UpsertDatabaseAccessRequest.md)|  | 
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

