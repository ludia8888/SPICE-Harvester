# spice_harvester_sdk.SummaryApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_summary_api_v1_summary_get**](SummaryApi.md#get_summary_api_v1_summary_get) | **GET** /api/v1/summary | Get Summary


# **get_summary_api_v1_summary_get**
> Dict[str, object] get_summary_api_v1_summary_get(db=db, branch=branch, lang=lang, accept_language=accept_language)

Get Summary

Summarize context + cross-service health for UI.

This is intentionally a small, stable contract. Add fields here instead of
leaking storage-specific details into the frontend.

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
    api_instance = spice_harvester_sdk.SummaryApi(api_client)
    db = 'db_example' # str | Database (project) name (optional)
    branch = 'branch_example' # str | Branch name (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Summary
        api_response = api_instance.get_summary_api_v1_summary_get(db=db, branch=branch, lang=lang, accept_language=accept_language)
        print("The response of SummaryApi->get_summary_api_v1_summary_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SummaryApi->get_summary_api_v1_summary_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db** | **str**| Database (project) name | [optional] 
 **branch** | **str**| Branch name | [optional] 
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

