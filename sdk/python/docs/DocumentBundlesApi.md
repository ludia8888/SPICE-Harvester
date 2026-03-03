# spice_harvester_sdk.DocumentBundlesApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**search_document_bundle_api_v1_document_bundles_bundle_id_search_post**](DocumentBundlesApi.md#search_document_bundle_api_v1_document_bundles_bundle_id_search_post) | **POST** /api/v1/document-bundles/{bundle_id}/search | Search Document Bundle


# **search_document_bundle_api_v1_document_bundles_bundle_id_search_post**
> ApiResponse search_document_bundle_api_v1_document_bundles_bundle_id_search_post(bundle_id, document_bundle_search_request, lang=lang, accept_language=accept_language)

Search Document Bundle

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
from spice_harvester_sdk.models.document_bundle_search_request import DocumentBundleSearchRequest
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
    api_instance = spice_harvester_sdk.DocumentBundlesApi(api_client)
    bundle_id = 'bundle_id_example' # str | 
    document_bundle_search_request = spice_harvester_sdk.DocumentBundleSearchRequest() # DocumentBundleSearchRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Search Document Bundle
        api_response = api_instance.search_document_bundle_api_v1_document_bundles_bundle_id_search_post(bundle_id, document_bundle_search_request, lang=lang, accept_language=accept_language)
        print("The response of DocumentBundlesApi->search_document_bundle_api_v1_document_bundles_bundle_id_search_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DocumentBundlesApi->search_document_bundle_api_v1_document_bundles_bundle_id_search_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bundle_id** | **str**|  | 
 **document_bundle_search_request** | [**DocumentBundleSearchRequest**](DocumentBundleSearchRequest.md)|  | 
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

