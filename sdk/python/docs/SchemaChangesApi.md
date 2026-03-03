# spice_harvester_sdk.SchemaChangesApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**acknowledge_drift_api_v1_schema_changes_drifts_drift_id_acknowledge_put**](SchemaChangesApi.md#acknowledge_drift_api_v1_schema_changes_drifts_drift_id_acknowledge_put) | **PUT** /api/v1/schema-changes/drifts/{drift_id}/acknowledge | Acknowledge Drift
[**check_mapping_compatibility_api_v1_schema_changes_mappings_mapping_spec_id_compatibility_get**](SchemaChangesApi.md#check_mapping_compatibility_api_v1_schema_changes_mappings_mapping_spec_id_compatibility_get) | **GET** /api/v1/schema-changes/mappings/{mapping_spec_id}/compatibility | Check Mapping Compatibility
[**create_subscription_api_v1_schema_changes_subscriptions_post**](SchemaChangesApi.md#create_subscription_api_v1_schema_changes_subscriptions_post) | **POST** /api/v1/schema-changes/subscriptions | Create Subscription
[**delete_subscription_api_v1_schema_changes_subscriptions_subscription_id_delete**](SchemaChangesApi.md#delete_subscription_api_v1_schema_changes_subscriptions_subscription_id_delete) | **DELETE** /api/v1/schema-changes/subscriptions/{subscription_id} | Delete Subscription
[**get_schema_change_stats_api_v1_schema_changes_stats_get**](SchemaChangesApi.md#get_schema_change_stats_api_v1_schema_changes_stats_get) | **GET** /api/v1/schema-changes/stats | Get Schema Change Stats
[**list_schema_changes_api_v1_schema_changes_history_get**](SchemaChangesApi.md#list_schema_changes_api_v1_schema_changes_history_get) | **GET** /api/v1/schema-changes/history | List Schema Changes
[**list_subscriptions_api_v1_schema_changes_subscriptions_get**](SchemaChangesApi.md#list_subscriptions_api_v1_schema_changes_subscriptions_get) | **GET** /api/v1/schema-changes/subscriptions | List Subscriptions


# **acknowledge_drift_api_v1_schema_changes_drifts_drift_id_acknowledge_put**
> object acknowledge_drift_api_v1_schema_changes_drifts_drift_id_acknowledge_put(drift_id, acknowledge_request, lang=lang, accept_language=accept_language)

Acknowledge Drift

Acknowledge a schema drift.

Marks the drift as reviewed by the specified user.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.acknowledge_request import AcknowledgeRequest
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
    api_instance = spice_harvester_sdk.SchemaChangesApi(api_client)
    drift_id = 'drift_id_example' # str | 
    acknowledge_request = spice_harvester_sdk.AcknowledgeRequest() # AcknowledgeRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Acknowledge Drift
        api_response = api_instance.acknowledge_drift_api_v1_schema_changes_drifts_drift_id_acknowledge_put(drift_id, acknowledge_request, lang=lang, accept_language=accept_language)
        print("The response of SchemaChangesApi->acknowledge_drift_api_v1_schema_changes_drifts_drift_id_acknowledge_put:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SchemaChangesApi->acknowledge_drift_api_v1_schema_changes_drifts_drift_id_acknowledge_put: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **drift_id** | **str**|  | 
 **acknowledge_request** | [**AcknowledgeRequest**](AcknowledgeRequest.md)|  | 
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

# **check_mapping_compatibility_api_v1_schema_changes_mappings_mapping_spec_id_compatibility_get**
> object check_mapping_compatibility_api_v1_schema_changes_mappings_mapping_spec_id_compatibility_get(mapping_spec_id, db_name, dataset_version_id=dataset_version_id, lang=lang, accept_language=accept_language)

Check Mapping Compatibility

Check if a mapping spec is compatible with the current dataset schema.

Returns drift information if the schema has changed since the mapping was created.

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
    api_instance = spice_harvester_sdk.SchemaChangesApi(api_client)
    mapping_spec_id = 'mapping_spec_id_example' # str | 
    db_name = 'db_name_example' # str | Database name
    dataset_version_id = 'dataset_version_id_example' # str | Specific dataset version to check (default: latest) (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Check Mapping Compatibility
        api_response = api_instance.check_mapping_compatibility_api_v1_schema_changes_mappings_mapping_spec_id_compatibility_get(mapping_spec_id, db_name, dataset_version_id=dataset_version_id, lang=lang, accept_language=accept_language)
        print("The response of SchemaChangesApi->check_mapping_compatibility_api_v1_schema_changes_mappings_mapping_spec_id_compatibility_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SchemaChangesApi->check_mapping_compatibility_api_v1_schema_changes_mappings_mapping_spec_id_compatibility_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **mapping_spec_id** | **str**|  | 
 **db_name** | **str**| Database name | 
 **dataset_version_id** | **str**| Specific dataset version to check (default: latest) | [optional] 
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

# **create_subscription_api_v1_schema_changes_subscriptions_post**
> object create_subscription_api_v1_schema_changes_subscriptions_post(subscription_create_request, lang=lang, accept_language=accept_language)

Create Subscription

Create a new schema change subscription.

Subscribes to schema drift notifications for a specific dataset or mapping spec.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.subscription_create_request import SubscriptionCreateRequest
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
    api_instance = spice_harvester_sdk.SchemaChangesApi(api_client)
    subscription_create_request = spice_harvester_sdk.SubscriptionCreateRequest() # SubscriptionCreateRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Create Subscription
        api_response = api_instance.create_subscription_api_v1_schema_changes_subscriptions_post(subscription_create_request, lang=lang, accept_language=accept_language)
        print("The response of SchemaChangesApi->create_subscription_api_v1_schema_changes_subscriptions_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SchemaChangesApi->create_subscription_api_v1_schema_changes_subscriptions_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **subscription_create_request** | [**SubscriptionCreateRequest**](SubscriptionCreateRequest.md)|  | 
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

# **delete_subscription_api_v1_schema_changes_subscriptions_subscription_id_delete**
> object delete_subscription_api_v1_schema_changes_subscriptions_subscription_id_delete(subscription_id, lang=lang, accept_language=accept_language)

Delete Subscription

Delete a schema change subscription.

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
    api_instance = spice_harvester_sdk.SchemaChangesApi(api_client)
    subscription_id = 'subscription_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Delete Subscription
        api_response = api_instance.delete_subscription_api_v1_schema_changes_subscriptions_subscription_id_delete(subscription_id, lang=lang, accept_language=accept_language)
        print("The response of SchemaChangesApi->delete_subscription_api_v1_schema_changes_subscriptions_subscription_id_delete:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SchemaChangesApi->delete_subscription_api_v1_schema_changes_subscriptions_subscription_id_delete: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **subscription_id** | **str**|  | 
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

# **get_schema_change_stats_api_v1_schema_changes_stats_get**
> object get_schema_change_stats_api_v1_schema_changes_stats_get(db_name, days=days, lang=lang, accept_language=accept_language)

Get Schema Change Stats

Get schema change statistics for a database.

Returns aggregate counts by severity and subject type.

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
    api_instance = spice_harvester_sdk.SchemaChangesApi(api_client)
    db_name = 'db_name_example' # str | Database name
    days = 30 # int | Number of days to analyze (optional) (default to 30)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Schema Change Stats
        api_response = api_instance.get_schema_change_stats_api_v1_schema_changes_stats_get(db_name, days=days, lang=lang, accept_language=accept_language)
        print("The response of SchemaChangesApi->get_schema_change_stats_api_v1_schema_changes_stats_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SchemaChangesApi->get_schema_change_stats_api_v1_schema_changes_stats_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**| Database name | 
 **days** | **int**| Number of days to analyze | [optional] [default to 30]
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

# **list_schema_changes_api_v1_schema_changes_history_get**
> object list_schema_changes_api_v1_schema_changes_history_get(db_name, subject_type=subject_type, subject_id=subject_id, severity=severity, since=since, limit=limit, offset=offset, lang=lang, accept_language=accept_language)

List Schema Changes

List schema drift history for a database.

Returns all detected schema changes, optionally filtered by subject and severity.

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
    api_instance = spice_harvester_sdk.SchemaChangesApi(api_client)
    db_name = 'db_name_example' # str | Database name
    subject_type = 'subject_type_example' # str | Filter by subject type (dataset, mapping_spec) (optional)
    subject_id = 'subject_id_example' # str | Filter by subject ID (optional)
    severity = 'severity_example' # str | Filter by severity (info, warning, breaking) (optional)
    since = '2013-10-20T19:20:30+01:00' # datetime | Only return changes after this time (optional)
    limit = 50 # int |  (optional) (default to 50)
    offset = 0 # int |  (optional) (default to 0)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Schema Changes
        api_response = api_instance.list_schema_changes_api_v1_schema_changes_history_get(db_name, subject_type=subject_type, subject_id=subject_id, severity=severity, since=since, limit=limit, offset=offset, lang=lang, accept_language=accept_language)
        print("The response of SchemaChangesApi->list_schema_changes_api_v1_schema_changes_history_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SchemaChangesApi->list_schema_changes_api_v1_schema_changes_history_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**| Database name | 
 **subject_type** | **str**| Filter by subject type (dataset, mapping_spec) | [optional] 
 **subject_id** | **str**| Filter by subject ID | [optional] 
 **severity** | **str**| Filter by severity (info, warning, breaking) | [optional] 
 **since** | **datetime**| Only return changes after this time | [optional] 
 **limit** | **int**|  | [optional] [default to 50]
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

# **list_subscriptions_api_v1_schema_changes_subscriptions_get**
> object list_subscriptions_api_v1_schema_changes_subscriptions_get(db_name=db_name, status=status, limit=limit, lang=lang, accept_language=accept_language)

List Subscriptions

List schema change subscriptions for the current user.

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
    api_instance = spice_harvester_sdk.SchemaChangesApi(api_client)
    db_name = 'db_name_example' # str | Filter by database name (optional)
    status = 'status_example' # str | Filter by status (ACTIVE, PAUSED) (optional)
    limit = 50 # int |  (optional) (default to 50)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # List Subscriptions
        api_response = api_instance.list_subscriptions_api_v1_schema_changes_subscriptions_get(db_name=db_name, status=status, limit=limit, lang=lang, accept_language=accept_language)
        print("The response of SchemaChangesApi->list_subscriptions_api_v1_schema_changes_subscriptions_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling SchemaChangesApi->list_subscriptions_api_v1_schema_changes_subscriptions_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**| Filter by database name | [optional] 
 **status** | **str**| Filter by status (ACTIVE, PAUSED) | [optional] 
 **limit** | **int**|  | [optional] [default to 50]
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

