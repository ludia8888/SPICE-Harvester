# spice_harvester_sdk.CommandStatusApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_command_status_api_v1_commands_command_id_status_get**](CommandStatusApi.md#get_command_status_api_v1_commands_command_id_status_get) | **GET** /api/v1/commands/{command_id}/status | Get Command Status


# **get_command_status_api_v1_commands_command_id_status_get**
> CommandResult get_command_status_api_v1_commands_command_id_status_get(command_id, lang=lang, accept_language=accept_language)

Get Command Status

Proxy OMS: `GET /api/v1/commands/{command_id}/status`.

This is part of the async observability contract for all write-side commands.

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
    api_instance = spice_harvester_sdk.CommandStatusApi(api_client)
    command_id = 'command_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Command Status
        api_response = api_instance.get_command_status_api_v1_commands_command_id_status_get(command_id, lang=lang, accept_language=accept_language)
        print("The response of CommandStatusApi->get_command_status_api_v1_commands_command_id_status_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling CommandStatusApi->get_command_status_api_v1_commands_command_id_status_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **command_id** | **str**|  | 
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
**200** | Successful Response |  -  |
**400** | Invalid command_id |  -  |
**404** | Command not found |  -  |
**422** | Validation Error |  -  |
**503** | Upstream command status unavailable |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

