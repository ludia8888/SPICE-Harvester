# spice_harvester_sdk.OntologyAgentApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**run_ontology_agent_api_v1_ontology_agent_runs_post**](OntologyAgentApi.md#run_ontology_agent_api_v1_ontology_agent_runs_post) | **POST** /api/v1/ontology-agent/runs | Run ontology agent


# **run_ontology_agent_api_v1_ontology_agent_runs_post**
> ApiResponse run_ontology_agent_api_v1_ontology_agent_runs_post(ontology_agent_run_request, lang=lang, accept_language=accept_language)

Run ontology agent

Execute the autonomous ontology agent to create or modify ontology schemas
    based on natural language instructions.

    This is a convenience endpoint that delegates to the Pipeline Agent with empty dataset_ids.
    For combined pipeline + ontology tasks, use /api/v1/agent/pipeline-runs directly.

    Example goals:
    - "이 데이터셋으로 Customer 클래스 만들어줘"
    - "email, name, phone 필드를 기존 Person 클래스에 매핑해줘"
    - "Customer와 Order 사이에 hasOrders 관계 만들어줘"

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.models.api_response import ApiResponse
from spice_harvester_sdk.models.ontology_agent_run_request import OntologyAgentRunRequest
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
    api_instance = spice_harvester_sdk.OntologyAgentApi(api_client)
    ontology_agent_run_request = spice_harvester_sdk.OntologyAgentRunRequest() # OntologyAgentRunRequest | 
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Run ontology agent
        api_response = api_instance.run_ontology_agent_api_v1_ontology_agent_runs_post(ontology_agent_run_request, lang=lang, accept_language=accept_language)
        print("The response of OntologyAgentApi->run_ontology_agent_api_v1_ontology_agent_runs_post:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling OntologyAgentApi->run_ontology_agent_api_v1_ontology_agent_runs_post: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ontology_agent_run_request** | [**OntologyAgentRunRequest**](OntologyAgentRunRequest.md)|  | 
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

