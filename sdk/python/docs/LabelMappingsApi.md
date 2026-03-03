# spice_harvester_sdk.LabelMappingsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**clear_mappings_api_v1_databases_db_name_mappings_delete**](LabelMappingsApi.md#clear_mappings_api_v1_databases_db_name_mappings_delete) | **DELETE** /api/v1/databases/{db_name}/mappings/ | Clear Mappings
[**export_mappings_api_v1_databases_db_name_mappings_export_post**](LabelMappingsApi.md#export_mappings_api_v1_databases_db_name_mappings_export_post) | **POST** /api/v1/databases/{db_name}/mappings/export | Export Mappings
[**get_mappings_summary_api_v1_databases_db_name_mappings_get**](LabelMappingsApi.md#get_mappings_summary_api_v1_databases_db_name_mappings_get) | **GET** /api/v1/databases/{db_name}/mappings/ | Get Mappings Summary
[**import_mappings_api_v1_databases_db_name_mappings_import_post**](LabelMappingsApi.md#import_mappings_api_v1_databases_db_name_mappings_import_post) | **POST** /api/v1/databases/{db_name}/mappings/import | Import Mappings
[**validate_mappings_api_v1_databases_db_name_mappings_validate_post**](LabelMappingsApi.md#validate_mappings_api_v1_databases_db_name_mappings_validate_post) | **POST** /api/v1/databases/{db_name}/mappings/validate | Validate Mappings


# **clear_mappings_api_v1_databases_db_name_mappings_delete**
> object clear_mappings_api_v1_databases_db_name_mappings_delete(db_name, lang=lang, accept_language=accept_language)

Clear Mappings

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
    api_instance = spice_harvester_sdk.LabelMappingsApi(api_client)
    db_name = 'db_name_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Clear Mappings
        api_response = api_instance.clear_mappings_api_v1_databases_db_name_mappings_delete(db_name, lang=lang, accept_language=accept_language)
        print("The response of LabelMappingsApi->clear_mappings_api_v1_databases_db_name_mappings_delete:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LabelMappingsApi->clear_mappings_api_v1_databases_db_name_mappings_delete: %s\n" % e)
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

# **export_mappings_api_v1_databases_db_name_mappings_export_post**
> object export_mappings_api_v1_databases_db_name_mappings_export_post(db_name, lang=lang, accept_language=accept_language)

Export Mappings

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
    api_instance = spice_harvester_sdk.LabelMappingsApi(api_client)
    db_name = 'db_name_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Export Mappings
        api_response = api_instance.export_mappings_api_v1_databases_db_name_mappings_export_post(db_name, lang=lang, accept_language=accept_language)
        print("The response of LabelMappingsApi->export_mappings_api_v1_databases_db_name_mappings_export_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LabelMappingsApi->export_mappings_api_v1_databases_db_name_mappings_export_post: %s\n" % e)
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

# **get_mappings_summary_api_v1_databases_db_name_mappings_get**
> object get_mappings_summary_api_v1_databases_db_name_mappings_get(db_name, lang=lang, accept_language=accept_language)

Get Mappings Summary

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
    api_instance = spice_harvester_sdk.LabelMappingsApi(api_client)
    db_name = 'db_name_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Mappings Summary
        api_response = api_instance.get_mappings_summary_api_v1_databases_db_name_mappings_get(db_name, lang=lang, accept_language=accept_language)
        print("The response of LabelMappingsApi->get_mappings_summary_api_v1_databases_db_name_mappings_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LabelMappingsApi->get_mappings_summary_api_v1_databases_db_name_mappings_get: %s\n" % e)
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

# **import_mappings_api_v1_databases_db_name_mappings_import_post**
> ApiResponse import_mappings_api_v1_databases_db_name_mappings_import_post(db_name, file, lang=lang, accept_language=accept_language)

Import Mappings

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
    api_instance = spice_harvester_sdk.LabelMappingsApi(api_client)
    db_name = 'db_name_example' # str | 
    file = None # bytearray | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Import Mappings
        api_response = api_instance.import_mappings_api_v1_databases_db_name_mappings_import_post(db_name, file, lang=lang, accept_language=accept_language)
        print("The response of LabelMappingsApi->import_mappings_api_v1_databases_db_name_mappings_import_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LabelMappingsApi->import_mappings_api_v1_databases_db_name_mappings_import_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **file** | **bytearray**|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**ApiResponse**](ApiResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: multipart/form-data
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **validate_mappings_api_v1_databases_db_name_mappings_validate_post**
> ApiResponse validate_mappings_api_v1_databases_db_name_mappings_validate_post(db_name, file, lang=lang, accept_language=accept_language)

Validate Mappings

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
    api_instance = spice_harvester_sdk.LabelMappingsApi(api_client)
    db_name = 'db_name_example' # str | 
    file = None # bytearray | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Validate Mappings
        api_response = api_instance.validate_mappings_api_v1_databases_db_name_mappings_validate_post(db_name, file, lang=lang, accept_language=accept_language)
        print("The response of LabelMappingsApi->validate_mappings_api_v1_databases_db_name_mappings_validate_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LabelMappingsApi->validate_mappings_api_v1_databases_db_name_mappings_validate_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **file** | **bytearray**|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

[**ApiResponse**](ApiResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: multipart/form-data
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

