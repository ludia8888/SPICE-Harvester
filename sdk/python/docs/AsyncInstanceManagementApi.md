# spice_harvester_sdk.AsyncInstanceManagementApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**bulk_create_instances_async_api_v1_databases_db_name_instances_class_label_bulk_create_post**](AsyncInstanceManagementApi.md#bulk_create_instances_async_api_v1_databases_db_name_instances_class_label_bulk_create_post) | **POST** /api/v1/databases/{db_name}/instances/{class_label}/bulk-create | Bulk Create Instances Async
[**create_instance_async_api_v1_databases_db_name_instances_class_label_create_post**](AsyncInstanceManagementApi.md#create_instance_async_api_v1_databases_db_name_instances_class_label_create_post) | **POST** /api/v1/databases/{db_name}/instances/{class_label}/create | Create Instance Async
[**delete_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_delete_delete**](AsyncInstanceManagementApi.md#delete_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_delete_delete) | **DELETE** /api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/delete | Delete Instance Async
[**update_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_update_put**](AsyncInstanceManagementApi.md#update_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_update_put) | **PUT** /api/v1/databases/{db_name}/instances/{class_label}/{instance_id}/update | Update Instance Async


# **bulk_create_instances_async_api_v1_databases_db_name_instances_class_label_bulk_create_post**
> CommandResult bulk_create_instances_async_api_v1_databases_db_name_instances_class_label_bulk_create_post(db_name, class_label, bulk_instance_create_request, branch=branch, user_id=user_id, lang=lang, accept_language=accept_language)

Bulk Create Instances Async

대량 인스턴스 생성 명령을 비동기로 처리 (Label 기반)

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.bulk_instance_create_request import BulkInstanceCreateRequest
from spice_harvester_sdk.models.command_result import CommandResult
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
    api_instance = spice_harvester_sdk.AsyncInstanceManagementApi(api_client)
    db_name = 'db_name_example' # str | 
    class_label = 'class_label_example' # str | 
    bulk_instance_create_request = spice_harvester_sdk.BulkInstanceCreateRequest() # BulkInstanceCreateRequest | 
    branch = 'master' # str | Target branch (default: master) (optional) (default to 'master')
    user_id = 'user_id_example' # str |  (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Bulk Create Instances Async
        api_response = api_instance.bulk_create_instances_async_api_v1_databases_db_name_instances_class_label_bulk_create_post(db_name, class_label, bulk_instance_create_request, branch=branch, user_id=user_id, lang=lang, accept_language=accept_language)
        print("The response of AsyncInstanceManagementApi->bulk_create_instances_async_api_v1_databases_db_name_instances_class_label_bulk_create_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AsyncInstanceManagementApi->bulk_create_instances_async_api_v1_databases_db_name_instances_class_label_bulk_create_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **class_label** | **str**|  | 
 **bulk_instance_create_request** | [**BulkInstanceCreateRequest**](BulkInstanceCreateRequest.md)|  | 
 **branch** | **str**| Target branch (default: master) | [optional] [default to &#39;master&#39;]
 **user_id** | **str**|  | [optional] 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**CommandResult**](CommandResult.md)

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

# **create_instance_async_api_v1_databases_db_name_instances_class_label_create_post**
> CommandResult create_instance_async_api_v1_databases_db_name_instances_class_label_create_post(db_name, class_label, instance_create_request, branch=branch, user_id=user_id, lang=lang, accept_language=accept_language)

Create Instance Async

인스턴스 생성 명령을 비동기로 처리 (Label 기반)

사용자는 Label을 사용하여 데이터를 제공하며,
BFF가 이를 ID로 변환하여 OMS에 전달합니다.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.command_result import CommandResult
from spice_harvester_sdk.models.instance_create_request import InstanceCreateRequest
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
    api_instance = spice_harvester_sdk.AsyncInstanceManagementApi(api_client)
    db_name = 'db_name_example' # str | 
    class_label = 'class_label_example' # str | 
    instance_create_request = spice_harvester_sdk.InstanceCreateRequest() # InstanceCreateRequest | 
    branch = 'master' # str | Target branch (default: master) (optional) (default to 'master')
    user_id = 'user_id_example' # str |  (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Create Instance Async
        api_response = api_instance.create_instance_async_api_v1_databases_db_name_instances_class_label_create_post(db_name, class_label, instance_create_request, branch=branch, user_id=user_id, lang=lang, accept_language=accept_language)
        print("The response of AsyncInstanceManagementApi->create_instance_async_api_v1_databases_db_name_instances_class_label_create_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AsyncInstanceManagementApi->create_instance_async_api_v1_databases_db_name_instances_class_label_create_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **class_label** | **str**|  | 
 **instance_create_request** | [**InstanceCreateRequest**](InstanceCreateRequest.md)|  | 
 **branch** | **str**| Target branch (default: master) | [optional] [default to &#39;master&#39;]
 **user_id** | **str**|  | [optional] 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**CommandResult**](CommandResult.md)

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

# **delete_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_delete_delete**
> CommandResult delete_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_delete_delete(db_name, class_label, instance_id, expected_seq, branch=branch, user_id=user_id, lang=lang, accept_language=accept_language)

Delete Instance Async

인스턴스 삭제 명령을 비동기로 처리 (Label 기반)

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.command_result import CommandResult
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
    api_instance = spice_harvester_sdk.AsyncInstanceManagementApi(api_client)
    db_name = 'db_name_example' # str | 
    class_label = 'class_label_example' # str | 
    instance_id = 'instance_id_example' # str | 
    expected_seq = 56 # int | Expected current aggregate sequence (OCC)
    branch = 'master' # str | Target branch (default: master) (optional) (default to 'master')
    user_id = 'user_id_example' # str |  (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Delete Instance Async
        api_response = api_instance.delete_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_delete_delete(db_name, class_label, instance_id, expected_seq, branch=branch, user_id=user_id, lang=lang, accept_language=accept_language)
        print("The response of AsyncInstanceManagementApi->delete_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_delete_delete:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AsyncInstanceManagementApi->delete_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_delete_delete: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **class_label** | **str**|  | 
 **instance_id** | **str**|  | 
 **expected_seq** | **int**| Expected current aggregate sequence (OCC) | 
 **branch** | **str**| Target branch (default: master) | [optional] [default to &#39;master&#39;]
 **user_id** | **str**|  | [optional] 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**CommandResult**](CommandResult.md)

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

# **update_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_update_put**
> CommandResult update_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_update_put(db_name, class_label, instance_id, expected_seq, instance_update_request, branch=branch, user_id=user_id, lang=lang, accept_language=accept_language)

Update Instance Async

인스턴스 수정 명령을 비동기로 처리 (Label 기반)

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.command_result import CommandResult
from spice_harvester_sdk.models.instance_update_request import InstanceUpdateRequest
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
    api_instance = spice_harvester_sdk.AsyncInstanceManagementApi(api_client)
    db_name = 'db_name_example' # str | 
    class_label = 'class_label_example' # str | 
    instance_id = 'instance_id_example' # str | 
    expected_seq = 56 # int | Expected current aggregate sequence (OCC)
    instance_update_request = spice_harvester_sdk.InstanceUpdateRequest() # InstanceUpdateRequest | 
    branch = 'master' # str | Target branch (default: master) (optional) (default to 'master')
    user_id = 'user_id_example' # str |  (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Update Instance Async
        api_response = api_instance.update_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_update_put(db_name, class_label, instance_id, expected_seq, instance_update_request, branch=branch, user_id=user_id, lang=lang, accept_language=accept_language)
        print("The response of AsyncInstanceManagementApi->update_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_update_put:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AsyncInstanceManagementApi->update_instance_async_api_v1_databases_db_name_instances_class_label_instance_id_update_put: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **class_label** | **str**|  | 
 **instance_id** | **str**|  | 
 **expected_seq** | **int**| Expected current aggregate sequence (OCC) | 
 **instance_update_request** | [**InstanceUpdateRequest**](InstanceUpdateRequest.md)|  | 
 **branch** | **str**| Target branch (default: master) | [optional] [default to &#39;master&#39;]
 **user_id** | **str**|  | [optional] 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**CommandResult**](CommandResult.md)

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

