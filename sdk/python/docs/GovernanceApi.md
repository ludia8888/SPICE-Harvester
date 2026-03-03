# spice_harvester_sdk.GovernanceApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_key_spec_api_v1_key_specs_post**](GovernanceApi.md#create_key_spec_api_v1_key_specs_post) | **POST** /api/v1/key-specs | Create Key Spec
[**get_key_spec_api_v1_key_specs_key_spec_id_get**](GovernanceApi.md#get_key_spec_api_v1_key_specs_key_spec_id_get) | **GET** /api/v1/key-specs/{key_spec_id} | Get Key Spec
[**list_access_policies_api_v1_access_policies_get**](GovernanceApi.md#list_access_policies_api_v1_access_policies_get) | **GET** /api/v1/access-policies | List Access Policies
[**list_gate_policies_api_v1_gate_policies_get**](GovernanceApi.md#list_gate_policies_api_v1_gate_policies_get) | **GET** /api/v1/gate-policies | List Gate Policies
[**list_gate_results_api_v1_gate_results_get**](GovernanceApi.md#list_gate_results_api_v1_gate_results_get) | **GET** /api/v1/gate-results | List Gate Results
[**list_key_specs_api_v1_key_specs_get**](GovernanceApi.md#list_key_specs_api_v1_key_specs_get) | **GET** /api/v1/key-specs | List Key Specs
[**list_schema_migration_plans_api_v1_schema_migration_plans_get**](GovernanceApi.md#list_schema_migration_plans_api_v1_schema_migration_plans_get) | **GET** /api/v1/schema-migration-plans | List Schema Migration Plans
[**upsert_access_policy_api_v1_access_policies_post**](GovernanceApi.md#upsert_access_policy_api_v1_access_policies_post) | **POST** /api/v1/access-policies | Upsert Access Policy
[**upsert_gate_policy_api_v1_gate_policies_post**](GovernanceApi.md#upsert_gate_policy_api_v1_gate_policies_post) | **POST** /api/v1/gate-policies | Upsert Gate Policy


# **create_key_spec_api_v1_key_specs_post**
> ApiResponse create_key_spec_api_v1_key_specs_post(create_key_spec_request, lang=lang, accept_language=accept_language)

Create Key Spec

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
from spice_harvester_sdk.models.create_key_spec_request import CreateKeySpecRequest
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
    api_instance = spice_harvester_sdk.GovernanceApi(api_client)
    create_key_spec_request = spice_harvester_sdk.CreateKeySpecRequest() # CreateKeySpecRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Create Key Spec
        api_response = api_instance.create_key_spec_api_v1_key_specs_post(create_key_spec_request, lang=lang, accept_language=accept_language)
        print("The response of GovernanceApi->create_key_spec_api_v1_key_specs_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GovernanceApi->create_key_spec_api_v1_key_specs_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **create_key_spec_request** | [**CreateKeySpecRequest**](CreateKeySpecRequest.md)|  | 
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

# **get_key_spec_api_v1_key_specs_key_spec_id_get**
> ApiResponse get_key_spec_api_v1_key_specs_key_spec_id_get(key_spec_id, lang=lang, accept_language=accept_language)

Get Key Spec

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
    api_instance = spice_harvester_sdk.GovernanceApi(api_client)
    key_spec_id = 'key_spec_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Key Spec
        api_response = api_instance.get_key_spec_api_v1_key_specs_key_spec_id_get(key_spec_id, lang=lang, accept_language=accept_language)
        print("The response of GovernanceApi->get_key_spec_api_v1_key_specs_key_spec_id_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GovernanceApi->get_key_spec_api_v1_key_specs_key_spec_id_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **key_spec_id** | **str**|  | 
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

# **list_access_policies_api_v1_access_policies_get**
> ApiResponse list_access_policies_api_v1_access_policies_get(db_name=db_name, scope=scope, subject_type=subject_type, subject_id=subject_id, status=status, lang=lang, accept_language=accept_language)

List Access Policies

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
    api_instance = spice_harvester_sdk.GovernanceApi(api_client)
    db_name = 'db_name_example' # str |  (optional)
    scope = 'scope_example' # str |  (optional)
    subject_type = 'subject_type_example' # str |  (optional)
    subject_id = 'subject_id_example' # str |  (optional)
    status = 'status_example' # str |  (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Access Policies
        api_response = api_instance.list_access_policies_api_v1_access_policies_get(db_name=db_name, scope=scope, subject_type=subject_type, subject_id=subject_id, status=status, lang=lang, accept_language=accept_language)
        print("The response of GovernanceApi->list_access_policies_api_v1_access_policies_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GovernanceApi->list_access_policies_api_v1_access_policies_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | [optional] 
 **scope** | **str**|  | [optional] 
 **subject_type** | **str**|  | [optional] 
 **subject_id** | **str**|  | [optional] 
 **status** | **str**|  | [optional] 
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

# **list_gate_policies_api_v1_gate_policies_get**
> ApiResponse list_gate_policies_api_v1_gate_policies_get(scope=scope, lang=lang, accept_language=accept_language)

List Gate Policies

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
    api_instance = spice_harvester_sdk.GovernanceApi(api_client)
    scope = 'scope_example' # str |  (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Gate Policies
        api_response = api_instance.list_gate_policies_api_v1_gate_policies_get(scope=scope, lang=lang, accept_language=accept_language)
        print("The response of GovernanceApi->list_gate_policies_api_v1_gate_policies_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GovernanceApi->list_gate_policies_api_v1_gate_policies_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **scope** | **str**|  | [optional] 
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

# **list_gate_results_api_v1_gate_results_get**
> ApiResponse list_gate_results_api_v1_gate_results_get(scope=scope, subject_type=subject_type, subject_id=subject_id, lang=lang, accept_language=accept_language)

List Gate Results

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
    api_instance = spice_harvester_sdk.GovernanceApi(api_client)
    scope = 'scope_example' # str |  (optional)
    subject_type = 'subject_type_example' # str |  (optional)
    subject_id = 'subject_id_example' # str |  (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Gate Results
        api_response = api_instance.list_gate_results_api_v1_gate_results_get(scope=scope, subject_type=subject_type, subject_id=subject_id, lang=lang, accept_language=accept_language)
        print("The response of GovernanceApi->list_gate_results_api_v1_gate_results_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GovernanceApi->list_gate_results_api_v1_gate_results_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **scope** | **str**|  | [optional] 
 **subject_type** | **str**|  | [optional] 
 **subject_id** | **str**|  | [optional] 
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

# **list_key_specs_api_v1_key_specs_get**
> ApiResponse list_key_specs_api_v1_key_specs_get(dataset_id=dataset_id, lang=lang, accept_language=accept_language)

List Key Specs

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
    api_instance = spice_harvester_sdk.GovernanceApi(api_client)
    dataset_id = 'dataset_id_example' # str |  (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Key Specs
        api_response = api_instance.list_key_specs_api_v1_key_specs_get(dataset_id=dataset_id, lang=lang, accept_language=accept_language)
        print("The response of GovernanceApi->list_key_specs_api_v1_key_specs_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GovernanceApi->list_key_specs_api_v1_key_specs_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **dataset_id** | **str**|  | [optional] 
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

# **list_schema_migration_plans_api_v1_schema_migration_plans_get**
> ApiResponse list_schema_migration_plans_api_v1_schema_migration_plans_get(db_name=db_name, subject_type=subject_type, subject_id=subject_id, status=status, lang=lang, accept_language=accept_language)

List Schema Migration Plans

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
    api_instance = spice_harvester_sdk.GovernanceApi(api_client)
    db_name = 'db_name_example' # str |  (optional)
    subject_type = 'subject_type_example' # str |  (optional)
    subject_id = 'subject_id_example' # str |  (optional)
    status = 'status_example' # str |  (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Schema Migration Plans
        api_response = api_instance.list_schema_migration_plans_api_v1_schema_migration_plans_get(db_name=db_name, subject_type=subject_type, subject_id=subject_id, status=status, lang=lang, accept_language=accept_language)
        print("The response of GovernanceApi->list_schema_migration_plans_api_v1_schema_migration_plans_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GovernanceApi->list_schema_migration_plans_api_v1_schema_migration_plans_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | [optional] 
 **subject_type** | **str**|  | [optional] 
 **subject_id** | **str**|  | [optional] 
 **status** | **str**|  | [optional] 
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

# **upsert_access_policy_api_v1_access_policies_post**
> ApiResponse upsert_access_policy_api_v1_access_policies_post(access_policy_request, lang=lang, accept_language=accept_language)

Upsert Access Policy

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.access_policy_request import AccessPolicyRequest
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
    api_instance = spice_harvester_sdk.GovernanceApi(api_client)
    access_policy_request = spice_harvester_sdk.AccessPolicyRequest() # AccessPolicyRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Upsert Access Policy
        api_response = api_instance.upsert_access_policy_api_v1_access_policies_post(access_policy_request, lang=lang, accept_language=accept_language)
        print("The response of GovernanceApi->upsert_access_policy_api_v1_access_policies_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GovernanceApi->upsert_access_policy_api_v1_access_policies_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **access_policy_request** | [**AccessPolicyRequest**](AccessPolicyRequest.md)|  | 
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

# **upsert_gate_policy_api_v1_gate_policies_post**
> ApiResponse upsert_gate_policy_api_v1_gate_policies_post(gate_policy_request, lang=lang, accept_language=accept_language)

Upsert Gate Policy

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
from spice_harvester_sdk.models.gate_policy_request import GatePolicyRequest
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
    api_instance = spice_harvester_sdk.GovernanceApi(api_client)
    gate_policy_request = spice_harvester_sdk.GatePolicyRequest() # GatePolicyRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Upsert Gate Policy
        api_response = api_instance.upsert_gate_policy_api_v1_gate_policies_post(gate_policy_request, lang=lang, accept_language=accept_language)
        print("The response of GovernanceApi->upsert_gate_policy_api_v1_gate_policies_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling GovernanceApi->upsert_gate_policy_api_v1_gate_policies_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **gate_policy_request** | [**GatePolicyRequest**](GatePolicyRequest.md)|  | 
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

