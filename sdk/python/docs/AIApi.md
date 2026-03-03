# spice_harvester_sdk.AIApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ai_intent_api_v1_ai_intent_post**](AIApi.md#ai_intent_api_v1_ai_intent_post) | **POST** /api/v1/ai/intent | Ai Intent
[**ai_query_api_v1_ai_query_db_name_post**](AIApi.md#ai_query_api_v1_ai_query_db_name_post) | **POST** /api/v1/ai/query/{db_name} | Ai Query
[**translate_query_plan_api_v1_ai_translate_query_plan_db_name_post**](AIApi.md#translate_query_plan_api_v1_ai_translate_query_plan_db_name_post) | **POST** /api/v1/ai/translate/query-plan/{db_name} | Translate Query Plan


# **ai_intent_api_v1_ai_intent_post**
> AIIntentResponse ai_intent_api_v1_ai_intent_post(ai_intent_request, lang=lang, accept_language=accept_language)

Ai Intent

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.ai_intent_request import AIIntentRequest
from spice_harvester_sdk.models.ai_intent_response import AIIntentResponse
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
    api_instance = spice_harvester_sdk.AIApi(api_client)
    ai_intent_request = spice_harvester_sdk.AIIntentRequest() # AIIntentRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Ai Intent
        api_response = api_instance.ai_intent_api_v1_ai_intent_post(ai_intent_request, lang=lang, accept_language=accept_language)
        print("The response of AIApi->ai_intent_api_v1_ai_intent_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AIApi->ai_intent_api_v1_ai_intent_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ai_intent_request** | [**AIIntentRequest**](AIIntentRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**AIIntentResponse**](AIIntentResponse.md)

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

# **ai_query_api_v1_ai_query_db_name_post**
> AIQueryResponse ai_query_api_v1_ai_query_db_name_post(db_name, ai_query_request, dry_run=dry_run, lang=lang, accept_language=accept_language)

Ai Query

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.ai_query_request import AIQueryRequest
from spice_harvester_sdk.models.ai_query_response import AIQueryResponse
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
    api_instance = spice_harvester_sdk.AIApi(api_client)
    db_name = 'db_name_example' # str | 
    ai_query_request = spice_harvester_sdk.AIQueryRequest() # AIQueryRequest | 
    dry_run = False # bool | When true, return only the query plan without executing (same as translate/query-plan). (optional) (default to False)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Ai Query
        api_response = api_instance.ai_query_api_v1_ai_query_db_name_post(db_name, ai_query_request, dry_run=dry_run, lang=lang, accept_language=accept_language)
        print("The response of AIApi->ai_query_api_v1_ai_query_db_name_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AIApi->ai_query_api_v1_ai_query_db_name_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **ai_query_request** | [**AIQueryRequest**](AIQueryRequest.md)|  | 
 **dry_run** | **bool**| When true, return only the query plan without executing (same as translate/query-plan). | [optional] [default to False]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**AIQueryResponse**](AIQueryResponse.md)

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

# **translate_query_plan_api_v1_ai_translate_query_plan_db_name_post**
> object translate_query_plan_api_v1_ai_translate_query_plan_db_name_post(db_name, ai_query_request, lang=lang, accept_language=accept_language)

Translate Query Plan

Deprecated: use ``POST /ai/query/{db_name}?dry_run=true`` instead.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.ai_query_request import AIQueryRequest
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
    api_instance = spice_harvester_sdk.AIApi(api_client)
    db_name = 'db_name_example' # str | 
    ai_query_request = spice_harvester_sdk.AIQueryRequest() # AIQueryRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Translate Query Plan
        api_response = api_instance.translate_query_plan_api_v1_ai_translate_query_plan_db_name_post(db_name, ai_query_request, lang=lang, accept_language=accept_language)
        print("The response of AIApi->translate_query_plan_api_v1_ai_translate_query_plan_db_name_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling AIApi->translate_query_plan_api_v1_ai_translate_query_plan_db_name_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **ai_query_request** | [**AIQueryRequest**](AIQueryRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

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

