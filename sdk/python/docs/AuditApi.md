# spice_harvester_sdk.AuditApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_chain_head_api_v1_audit_chain_head_get**](AuditApi.md#get_chain_head_api_v1_audit_chain_head_get) | **GET** /api/v1/audit/chain-head | Get Chain Head
[**list_audit_logs_api_v1_audit_logs_get**](AuditApi.md#list_audit_logs_api_v1_audit_logs_get) | **GET** /api/v1/audit/logs | List Audit Logs


# **get_chain_head_api_v1_audit_chain_head_get**
> object get_chain_head_api_v1_audit_chain_head_get(partition_key, lang=lang, accept_language=accept_language)

Get Chain Head

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
    api_instance = spice_harvester_sdk.AuditApi(api_client)
    partition_key = 'partition_key_example' # str | Audit partition key (e.g. db:<db_name>)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Chain Head
        api_response = api_instance.get_chain_head_api_v1_audit_chain_head_get(partition_key, lang=lang, accept_language=accept_language)
        print("The response of AuditApi->get_chain_head_api_v1_audit_chain_head_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AuditApi->get_chain_head_api_v1_audit_chain_head_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **partition_key** | **str**| Audit partition key (e.g. db:&lt;db_name&gt;) | 
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

# **list_audit_logs_api_v1_audit_logs_get**
> object list_audit_logs_api_v1_audit_logs_get(partition_key=partition_key, action=action, status=status, resource_type=resource_type, resource_id=resource_id, event_id=event_id, command_id=command_id, actor=actor, since=since, until=until, limit=limit, offset=offset, lang=lang, accept_language=accept_language)

List Audit Logs

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
    api_instance = spice_harvester_sdk.AuditApi(api_client)
    partition_key = 'partition_key_example' # str | Audit partition key (e.g. db:<db_name>) (optional)
    action = 'action_example' # str | Action filter (optional)
    status = 'status_example' # str | Status filter (success|failure) (optional)
    resource_type = 'resource_type_example' # str |  (optional)
    resource_id = 'resource_id_example' # str |  (optional)
    event_id = 'event_id_example' # str |  (optional)
    command_id = 'command_id_example' # str |  (optional)
    actor = 'actor_example' # str |  (optional)
    since = '2013-10-20T19:20:30+01:00' # datetime |  (optional)
    until = '2013-10-20T19:20:30+01:00' # datetime |  (optional)
    limit = 100 # int |  (optional) (default to 100)
    offset = 0 # int |  (optional) (default to 0)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Audit Logs
        api_response = api_instance.list_audit_logs_api_v1_audit_logs_get(partition_key=partition_key, action=action, status=status, resource_type=resource_type, resource_id=resource_id, event_id=event_id, command_id=command_id, actor=actor, since=since, until=until, limit=limit, offset=offset, lang=lang, accept_language=accept_language)
        print("The response of AuditApi->list_audit_logs_api_v1_audit_logs_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AuditApi->list_audit_logs_api_v1_audit_logs_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **partition_key** | **str**| Audit partition key (e.g. db:&lt;db_name&gt;) | [optional] 
 **action** | **str**| Action filter | [optional] 
 **status** | **str**| Status filter (success|failure) | [optional] 
 **resource_type** | **str**|  | [optional] 
 **resource_id** | **str**|  | [optional] 
 **event_id** | **str**|  | [optional] 
 **command_id** | **str**|  | [optional] 
 **actor** | **str**|  | [optional] 
 **since** | **datetime**|  | [optional] 
 **until** | **datetime**|  | [optional] 
 **limit** | **int**|  | [optional] [default to 100]
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

