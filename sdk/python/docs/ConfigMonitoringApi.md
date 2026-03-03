# spice_harvester_sdk.ConfigMonitoringApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**analyze_configuration_health_impact_api_v1_config_config_health_impact_get**](ConfigMonitoringApi.md#analyze_configuration_health_impact_api_v1_config_config_health_impact_get) | **GET** /api/v1/config/config/health-impact | Configuration Health Impact
[**analyze_environment_drift_api_v1_config_config_drift_analysis_get**](ConfigMonitoringApi.md#analyze_environment_drift_api_v1_config_config_drift_analysis_get) | **GET** /api/v1/config/config/drift-analysis | Environment Drift Analysis
[**check_configuration_changes_api_v1_config_config_check_changes_post**](ConfigMonitoringApi.md#check_configuration_changes_api_v1_config_config_check_changes_post) | **POST** /api/v1/config/config/check-changes | Check for Changes
[**get_configuration_changes_api_v1_config_config_changes_get**](ConfigMonitoringApi.md#get_configuration_changes_api_v1_config_config_changes_get) | **GET** /api/v1/config/config/changes | Configuration Changes
[**get_configuration_report_api_v1_config_config_report_get**](ConfigMonitoringApi.md#get_configuration_report_api_v1_config_config_report_get) | **GET** /api/v1/config/config/report | Configuration Report
[**get_current_configuration_api_v1_config_config_current_get**](ConfigMonitoringApi.md#get_current_configuration_api_v1_config_config_current_get) | **GET** /api/v1/config/config/current | Current Configuration
[**get_monitoring_status_api_v1_config_config_monitoring_status_get**](ConfigMonitoringApi.md#get_monitoring_status_api_v1_config_config_monitoring_status_get) | **GET** /api/v1/config/config/monitoring-status | Configuration Monitoring Status
[**perform_security_audit_api_v1_config_config_security_audit_get**](ConfigMonitoringApi.md#perform_security_audit_api_v1_config_config_security_audit_get) | **GET** /api/v1/config/config/security-audit | Security Audit
[**validate_configuration_api_v1_config_config_validation_get**](ConfigMonitoringApi.md#validate_configuration_api_v1_config_config_validation_get) | **GET** /api/v1/config/config/validation | Configuration Validation


# **analyze_configuration_health_impact_api_v1_config_config_health_impact_get**
> object analyze_configuration_health_impact_api_v1_config_config_health_impact_get(lang=lang, accept_language=accept_language)

Configuration Health Impact

Analyze how configuration affects system health

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
    api_instance = spice_harvester_sdk.ConfigMonitoringApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Configuration Health Impact
        api_response = api_instance.analyze_configuration_health_impact_api_v1_config_config_health_impact_get(lang=lang, accept_language=accept_language)
        print("The response of ConfigMonitoringApi->analyze_configuration_health_impact_api_v1_config_config_health_impact_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigMonitoringApi->analyze_configuration_health_impact_api_v1_config_config_health_impact_get: %s\n" % e)
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

# **analyze_environment_drift_api_v1_config_config_drift_analysis_get**
> object analyze_environment_drift_api_v1_config_config_drift_analysis_get(compare_environment, lang=lang, accept_language=accept_language)

Environment Drift Analysis

Analyze configuration drift between environments

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
    api_instance = spice_harvester_sdk.ConfigMonitoringApi(api_client)
    compare_environment = 'compare_environment_example' # str | Environment to compare against (development, staging, production)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Environment Drift Analysis
        api_response = api_instance.analyze_environment_drift_api_v1_config_config_drift_analysis_get(compare_environment, lang=lang, accept_language=accept_language)
        print("The response of ConfigMonitoringApi->analyze_environment_drift_api_v1_config_config_drift_analysis_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigMonitoringApi->analyze_environment_drift_api_v1_config_config_drift_analysis_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **compare_environment** | **str**| Environment to compare against (development, staging, production) | 
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
**400** | Request validation failed |  -  |
**422** | Validation Error |  -  |
**500** | Environment drift analysis failed |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **check_configuration_changes_api_v1_config_config_check_changes_post**
> object check_configuration_changes_api_v1_config_config_check_changes_post(lang=lang, accept_language=accept_language)

Check for Changes

Manually trigger configuration change detection

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
    api_instance = spice_harvester_sdk.ConfigMonitoringApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Check for Changes
        api_response = api_instance.check_configuration_changes_api_v1_config_config_check_changes_post(lang=lang, accept_language=accept_language)
        print("The response of ConfigMonitoringApi->check_configuration_changes_api_v1_config_config_check_changes_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigMonitoringApi->check_configuration_changes_api_v1_config_config_check_changes_post: %s\n" % e)
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

# **get_configuration_changes_api_v1_config_config_changes_get**
> object get_configuration_changes_api_v1_config_config_changes_get(limit=limit, severity=severity, change_type=change_type, since=since, lang=lang, accept_language=accept_language)

Configuration Changes

Get configuration change history and detect new changes

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
    api_instance = spice_harvester_sdk.ConfigMonitoringApi(api_client)
    limit = 50 # int | Maximum number of changes to return (optional) (default to 50)
    severity = 'severity_example' # str | Filter by severity (info, warning, error, critical) (optional)
    change_type = 'change_type_example' # str | Filter by change type (optional)
    since = 'since_example' # str | Get changes since timestamp (ISO format) (optional)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Configuration Changes
        api_response = api_instance.get_configuration_changes_api_v1_config_config_changes_get(limit=limit, severity=severity, change_type=change_type, since=since, lang=lang, accept_language=accept_language)
        print("The response of ConfigMonitoringApi->get_configuration_changes_api_v1_config_config_changes_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigMonitoringApi->get_configuration_changes_api_v1_config_config_changes_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **limit** | **int**| Maximum number of changes to return | [optional] [default to 50]
 **severity** | **str**| Filter by severity (info, warning, error, critical) | [optional] 
 **change_type** | **str**| Filter by change type | [optional] 
 **since** | **str**| Get changes since timestamp (ISO format) | [optional] 
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

# **get_configuration_report_api_v1_config_config_report_get**
> object get_configuration_report_api_v1_config_config_report_get(lang=lang, accept_language=accept_language)

Configuration Report

Get comprehensive configuration report with all monitoring data

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
    api_instance = spice_harvester_sdk.ConfigMonitoringApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Configuration Report
        api_response = api_instance.get_configuration_report_api_v1_config_config_report_get(lang=lang, accept_language=accept_language)
        print("The response of ConfigMonitoringApi->get_configuration_report_api_v1_config_config_report_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigMonitoringApi->get_configuration_report_api_v1_config_config_report_get: %s\n" % e)
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

# **get_current_configuration_api_v1_config_config_current_get**
> object get_current_configuration_api_v1_config_config_current_get(include_validation=include_validation, lang=lang, accept_language=accept_language)

Current Configuration

Get current application configuration with sensitive values masked

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
    api_instance = spice_harvester_sdk.ConfigMonitoringApi(api_client)
    include_validation = False # bool | Include validation results (optional) (default to False)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Current Configuration
        api_response = api_instance.get_current_configuration_api_v1_config_config_current_get(include_validation=include_validation, lang=lang, accept_language=accept_language)
        print("The response of ConfigMonitoringApi->get_current_configuration_api_v1_config_config_current_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigMonitoringApi->get_current_configuration_api_v1_config_config_current_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **include_validation** | **bool**| Include validation results | [optional] [default to False]
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

# **get_monitoring_status_api_v1_config_config_monitoring_status_get**
> object get_monitoring_status_api_v1_config_config_monitoring_status_get(lang=lang, accept_language=accept_language)

Configuration Monitoring Status

Get status of configuration monitoring system

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
    api_instance = spice_harvester_sdk.ConfigMonitoringApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Configuration Monitoring Status
        api_response = api_instance.get_monitoring_status_api_v1_config_config_monitoring_status_get(lang=lang, accept_language=accept_language)
        print("The response of ConfigMonitoringApi->get_monitoring_status_api_v1_config_config_monitoring_status_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigMonitoringApi->get_monitoring_status_api_v1_config_config_monitoring_status_get: %s\n" % e)
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

# **perform_security_audit_api_v1_config_config_security_audit_get**
> object perform_security_audit_api_v1_config_config_security_audit_get(lang=lang, accept_language=accept_language)

Security Audit

Perform comprehensive security audit of configuration

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
    api_instance = spice_harvester_sdk.ConfigMonitoringApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Security Audit
        api_response = api_instance.perform_security_audit_api_v1_config_config_security_audit_get(lang=lang, accept_language=accept_language)
        print("The response of ConfigMonitoringApi->perform_security_audit_api_v1_config_config_security_audit_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigMonitoringApi->perform_security_audit_api_v1_config_config_security_audit_get: %s\n" % e)
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

# **validate_configuration_api_v1_config_config_validation_get**
> object validate_configuration_api_v1_config_config_validation_get(lang=lang, accept_language=accept_language)

Configuration Validation

Validate configuration against security and best practice rules

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
    api_instance = spice_harvester_sdk.ConfigMonitoringApi(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Configuration Validation
        api_response = api_instance.validate_configuration_api_v1_config_config_validation_get(lang=lang, accept_language=accept_language)
        print("The response of ConfigMonitoringApi->validate_configuration_api_v1_config_config_validation_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling ConfigMonitoringApi->validate_configuration_api_v1_config_config_validation_get: %s\n" % e)
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

