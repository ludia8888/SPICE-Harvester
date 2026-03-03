# spice_harvester_sdk.MonitoringApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**basic_health_check_api_v1_monitoring_health_get**](MonitoringApi.md#basic_health_check_api_v1_monitoring_health_get) | **GET** /api/v1/monitoring/health | Basic Health Check
[**detailed_health_check_api_v1_monitoring_health_detailed_get**](MonitoringApi.md#detailed_health_check_api_v1_monitoring_health_detailed_get) | **GET** /api/v1/monitoring/health/detailed | Detailed Health Check
[**get_background_task_health_api_v1_monitoring_background_tasks_health_get**](MonitoringApi.md#get_background_task_health_api_v1_monitoring_background_tasks_health_get) | **GET** /api/v1/monitoring/background-tasks/health | Background Task System Health
[**get_configuration_overview_api_v1_monitoring_config_get**](MonitoringApi.md#get_configuration_overview_api_v1_monitoring_config_get) | **GET** /api/v1/monitoring/config | Configuration Overview
[**get_service_metrics_api_v1_monitoring_metrics_get**](MonitoringApi.md#get_service_metrics_api_v1_monitoring_metrics_get) | **GET** /api/v1/monitoring/metrics | Service Metrics
[**liveness_probe_api_v1_monitoring_health_liveness_get**](MonitoringApi.md#liveness_probe_api_v1_monitoring_health_liveness_get) | **GET** /api/v1/monitoring/health/liveness | Kubernetes Liveness Probe
[**readiness_probe_api_v1_monitoring_health_readiness_get**](MonitoringApi.md#readiness_probe_api_v1_monitoring_health_readiness_get) | **GET** /api/v1/monitoring/health/readiness | Kubernetes Readiness Probe


# **basic_health_check_api_v1_monitoring_health_get**
> object basic_health_check_api_v1_monitoring_health_get(lang=lang, accept_language=accept_language)

Basic Health Check

Readiness/liveness signal for traffic routing

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
    api_instance = spice_harvester_sdk.MonitoringApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Basic Health Check
        api_response = api_instance.basic_health_check_api_v1_monitoring_health_get(lang=lang, accept_language=accept_language)
        print("The response of MonitoringApi->basic_health_check_api_v1_monitoring_health_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling MonitoringApi->basic_health_check_api_v1_monitoring_health_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **detailed_health_check_api_v1_monitoring_health_detailed_get**
> object detailed_health_check_api_v1_monitoring_health_detailed_get(include_metrics=include_metrics, lang=lang, accept_language=accept_language)

Detailed Health Check

Comprehensive health check with service details

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
    api_instance = spice_harvester_sdk.MonitoringApi(api_client)
    include_metrics = False # bool | Include performance metrics (optional) (default to False)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Detailed Health Check
        api_response = api_instance.detailed_health_check_api_v1_monitoring_health_detailed_get(include_metrics=include_metrics, lang=lang, accept_language=accept_language)
        print("The response of MonitoringApi->detailed_health_check_api_v1_monitoring_health_detailed_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling MonitoringApi->detailed_health_check_api_v1_monitoring_health_detailed_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **include_metrics** | **bool**| Include performance metrics | [optional] [default to False]
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

# **get_background_task_health_api_v1_monitoring_background_tasks_health_get**
> object get_background_task_health_api_v1_monitoring_background_tasks_health_get(lang=lang, accept_language=accept_language)

Background Task System Health

Check health of background task processing system

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
    api_instance = spice_harvester_sdk.MonitoringApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Background Task System Health
        api_response = api_instance.get_background_task_health_api_v1_monitoring_background_tasks_health_get(lang=lang, accept_language=accept_language)
        print("The response of MonitoringApi->get_background_task_health_api_v1_monitoring_background_tasks_health_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling MonitoringApi->get_background_task_health_api_v1_monitoring_background_tasks_health_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_configuration_overview_api_v1_monitoring_config_get**
> object get_configuration_overview_api_v1_monitoring_config_get(include_sensitive=include_sensitive, lang=lang, accept_language=accept_language)

Configuration Overview

Current application configuration for debugging

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
    api_instance = spice_harvester_sdk.MonitoringApi(api_client)
    include_sensitive = False # bool | Include sensitive configuration values (optional) (default to False)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Configuration Overview
        api_response = api_instance.get_configuration_overview_api_v1_monitoring_config_get(include_sensitive=include_sensitive, lang=lang, accept_language=accept_language)
        print("The response of MonitoringApi->get_configuration_overview_api_v1_monitoring_config_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling MonitoringApi->get_configuration_overview_api_v1_monitoring_config_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **include_sensitive** | **bool**| Include sensitive configuration values | [optional] [default to False]
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

# **get_service_metrics_api_v1_monitoring_metrics_get**
> object get_service_metrics_api_v1_monitoring_metrics_get(service_name=service_name, lang=lang, accept_language=accept_language)

Service Metrics

Comprehensive service performance metrics

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
    api_instance = spice_harvester_sdk.MonitoringApi(api_client)
    service_name = 'service_name_example' # str | (deprecated) Filter by service name (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Service Metrics
        api_response = api_instance.get_service_metrics_api_v1_monitoring_metrics_get(service_name=service_name, lang=lang, accept_language=accept_language)
        print("The response of MonitoringApi->get_service_metrics_api_v1_monitoring_metrics_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling MonitoringApi->get_service_metrics_api_v1_monitoring_metrics_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **service_name** | **str**| (deprecated) Filter by service name | [optional] 
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

# **liveness_probe_api_v1_monitoring_health_liveness_get**
> object liveness_probe_api_v1_monitoring_health_liveness_get(lang=lang, accept_language=accept_language)

Kubernetes Liveness Probe

Liveness probe for Kubernetes deployments

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
    api_instance = spice_harvester_sdk.MonitoringApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Kubernetes Liveness Probe
        api_response = api_instance.liveness_probe_api_v1_monitoring_health_liveness_get(lang=lang, accept_language=accept_language)
        print("The response of MonitoringApi->liveness_probe_api_v1_monitoring_health_liveness_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling MonitoringApi->liveness_probe_api_v1_monitoring_health_liveness_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **readiness_probe_api_v1_monitoring_health_readiness_get**
> object readiness_probe_api_v1_monitoring_health_readiness_get(lang=lang, accept_language=accept_language)

Kubernetes Readiness Probe

Readiness probe for Kubernetes deployments

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
    api_instance = spice_harvester_sdk.MonitoringApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Kubernetes Readiness Probe
        api_response = api_instance.readiness_probe_api_v1_monitoring_health_readiness_get(lang=lang, accept_language=accept_language)
        print("The response of MonitoringApi->readiness_probe_api_v1_monitoring_health_readiness_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling MonitoringApi->readiness_probe_api_v1_monitoring_health_readiness_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

