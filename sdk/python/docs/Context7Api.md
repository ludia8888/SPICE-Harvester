# spice_harvester_sdk.Context7Api

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_knowledge_api_v1_context7_knowledge_post**](Context7Api.md#add_knowledge_api_v1_context7_knowledge_post) | **POST** /api/v1/context7/knowledge | Add Knowledge
[**analyze_ontology_api_v1_context7_analyze_ontology_post**](Context7Api.md#analyze_ontology_api_v1_context7_analyze_ontology_post) | **POST** /api/v1/context7/analyze/ontology | Analyze Ontology
[**check_context7_health_api_v1_context7_health_get**](Context7Api.md#check_context7_health_api_v1_context7_health_get) | **GET** /api/v1/context7/health | Check Context7 Health
[**create_entity_link_api_v1_context7_link_post**](Context7Api.md#create_entity_link_api_v1_context7_link_post) | **POST** /api/v1/context7/link | Create Entity Link
[**get_entity_context_api_v1_context7_context_entity_id_get**](Context7Api.md#get_entity_context_api_v1_context7_context_entity_id_get) | **GET** /api/v1/context7/context/{entity_id} | Get Entity Context
[**get_ontology_suggestions_api_v1_context7_suggestions_db_name_class_id_get**](Context7Api.md#get_ontology_suggestions_api_v1_context7_suggestions_db_name_class_id_get) | **GET** /api/v1/context7/suggestions/{db_name}/{class_id} | Get Ontology Suggestions
[**search_context7_api_v1_context7_search_post**](Context7Api.md#search_context7_api_v1_context7_search_post) | **POST** /api/v1/context7/search | Search Context7


# **add_knowledge_api_v1_context7_knowledge_post**
> Dict[str, object] add_knowledge_api_v1_context7_knowledge_post(knowledge_request, lang=lang, accept_language=accept_language)

Add Knowledge

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.knowledge_request import KnowledgeRequest
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
    api_instance = spice_harvester_sdk.Context7Api(api_client)
    knowledge_request = spice_harvester_sdk.KnowledgeRequest() # KnowledgeRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Add Knowledge
        api_response = api_instance.add_knowledge_api_v1_context7_knowledge_post(knowledge_request, lang=lang, accept_language=accept_language)
        print("The response of Context7Api->add_knowledge_api_v1_context7_knowledge_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling Context7Api->add_knowledge_api_v1_context7_knowledge_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **knowledge_request** | [**KnowledgeRequest**](KnowledgeRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Not found |  -  |
**422** | Validation Error |  -  |
**500** | Internal server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **analyze_ontology_api_v1_context7_analyze_ontology_post**
> Dict[str, object] analyze_ontology_api_v1_context7_analyze_ontology_post(ontology_analysis_request, lang=lang, accept_language=accept_language)

Analyze Ontology

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.ontology_analysis_request import OntologyAnalysisRequest
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
    api_instance = spice_harvester_sdk.Context7Api(api_client)
    ontology_analysis_request = spice_harvester_sdk.OntologyAnalysisRequest() # OntologyAnalysisRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Analyze Ontology
        api_response = api_instance.analyze_ontology_api_v1_context7_analyze_ontology_post(ontology_analysis_request, lang=lang, accept_language=accept_language)
        print("The response of Context7Api->analyze_ontology_api_v1_context7_analyze_ontology_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling Context7Api->analyze_ontology_api_v1_context7_analyze_ontology_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ontology_analysis_request** | [**OntologyAnalysisRequest**](OntologyAnalysisRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Not found |  -  |
**422** | Validation Error |  -  |
**500** | Internal server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **check_context7_health_api_v1_context7_health_get**
> Dict[str, object] check_context7_health_api_v1_context7_health_get(lang=lang, accept_language=accept_language)

Check Context7 Health

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
    api_instance = spice_harvester_sdk.Context7Api(api_client)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Check Context7 Health
        api_response = api_instance.check_context7_health_api_v1_context7_health_get(lang=lang, accept_language=accept_language)
        print("The response of Context7Api->check_context7_health_api_v1_context7_health_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling Context7Api->check_context7_health_api_v1_context7_health_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
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
**404** | Not found |  -  |
**500** | Internal server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **create_entity_link_api_v1_context7_link_post**
> Dict[str, object] create_entity_link_api_v1_context7_link_post(entity_link_request, lang=lang, accept_language=accept_language)

Create Entity Link

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.entity_link_request import EntityLinkRequest
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
    api_instance = spice_harvester_sdk.Context7Api(api_client)
    entity_link_request = spice_harvester_sdk.EntityLinkRequest() # EntityLinkRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Create Entity Link
        api_response = api_instance.create_entity_link_api_v1_context7_link_post(entity_link_request, lang=lang, accept_language=accept_language)
        print("The response of Context7Api->create_entity_link_api_v1_context7_link_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling Context7Api->create_entity_link_api_v1_context7_link_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **entity_link_request** | [**EntityLinkRequest**](EntityLinkRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Not found |  -  |
**422** | Validation Error |  -  |
**500** | Internal server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_entity_context_api_v1_context7_context_entity_id_get**
> Dict[str, object] get_entity_context_api_v1_context7_context_entity_id_get(entity_id, lang=lang, accept_language=accept_language)

Get Entity Context

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
    api_instance = spice_harvester_sdk.Context7Api(api_client)
    entity_id = 'entity_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Entity Context
        api_response = api_instance.get_entity_context_api_v1_context7_context_entity_id_get(entity_id, lang=lang, accept_language=accept_language)
        print("The response of Context7Api->get_entity_context_api_v1_context7_context_entity_id_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling Context7Api->get_entity_context_api_v1_context7_context_entity_id_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **entity_id** | **str**|  | 
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
**404** | Not found |  -  |
**422** | Validation Error |  -  |
**500** | Internal server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_ontology_suggestions_api_v1_context7_suggestions_db_name_class_id_get**
> Dict[str, object] get_ontology_suggestions_api_v1_context7_suggestions_db_name_class_id_get(db_name, class_id, lang=lang, accept_language=accept_language)

Get Ontology Suggestions

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
    api_instance = spice_harvester_sdk.Context7Api(api_client)
    db_name = 'db_name_example' # str | 
    class_id = 'class_id_example' # str | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Ontology Suggestions
        api_response = api_instance.get_ontology_suggestions_api_v1_context7_suggestions_db_name_class_id_get(db_name, class_id, lang=lang, accept_language=accept_language)
        print("The response of Context7Api->get_ontology_suggestions_api_v1_context7_suggestions_db_name_class_id_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling Context7Api->get_ontology_suggestions_api_v1_context7_suggestions_db_name_class_id_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**|  | 
 **class_id** | **str**|  | 
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
**404** | Not found |  -  |
**422** | Validation Error |  -  |
**500** | Internal server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **search_context7_api_v1_context7_search_post**
> Dict[str, object] search_context7_api_v1_context7_search_post(search_request, lang=lang, accept_language=accept_language)

Search Context7

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.search_request import SearchRequest
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
    api_instance = spice_harvester_sdk.Context7Api(api_client)
    search_request = spice_harvester_sdk.SearchRequest() # SearchRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Search Context7
        api_response = api_instance.search_context7_api_v1_context7_search_post(search_request, lang=lang, accept_language=accept_language)
        print("The response of Context7Api->search_context7_api_v1_context7_search_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling Context7Api->search_context7_api_v1_context7_search_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **search_request** | [**SearchRequest**](SearchRequest.md)|  | 
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**Dict[str, object]**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**404** | Not found |  -  |
**422** | Validation Error |  -  |
**500** | Internal server error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

