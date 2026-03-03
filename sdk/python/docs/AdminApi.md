# spice_harvester_sdk.AdminApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ingest_ci_result_api_v1_admin_ci_ci_results_post**](AdminApi.md#ingest_ci_result_api_v1_admin_ci_ci_results_post) | **POST** /api/v1/admin/ci/ci-results | Ingest Ci Result


# **ingest_ci_result_api_v1_admin_ci_ci_results_post**
> ApiResponse ingest_ci_result_api_v1_admin_ci_ci_results_post(agent_session_ci_result_ingest_request, lang=lang, accept_language=accept_language)

Ingest Ci Result

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.agent_session_ci_result_ingest_request import AgentSessionCIResultIngestRequest
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
    api_instance = spice_harvester_sdk.AdminApi(api_client)
    agent_session_ci_result_ingest_request = spice_harvester_sdk.AgentSessionCIResultIngestRequest() # AgentSessionCIResultIngestRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Ingest Ci Result
        api_response = api_instance.ingest_ci_result_api_v1_admin_ci_ci_results_post(agent_session_ci_result_ingest_request, lang=lang, accept_language=accept_language)
        print("The response of AdminApi->ingest_ci_result_api_v1_admin_ci_ci_results_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AdminApi->ingest_ci_result_api_v1_admin_ci_ci_results_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **agent_session_ci_result_ingest_request** | [**AgentSessionCIResultIngestRequest**](AgentSessionCIResultIngestRequest.md)|  | 
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
**201** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

