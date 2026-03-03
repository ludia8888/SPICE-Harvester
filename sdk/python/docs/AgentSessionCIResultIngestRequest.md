# AgentSessionCIResultIngestRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**checks** | **List[object]** |  | [optional] 
**ci_result_id** | **str** |  | [optional] 
**details_url** | **str** |  | [optional] 
**job_id** | **str** |  | [optional] 
**plan_id** | **str** |  | [optional] 
**provider** | **str** |  | [optional] 
**raw** | **Dict[str, object]** |  | [optional] 
**run_id** | **str** |  | [optional] 
**session_id** | **str** |  | 
**status** | **str** |  | [optional] [default to 'unknown']
**summary** | **str** |  | [optional] 
**tenant_id** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.agent_session_ci_result_ingest_request import AgentSessionCIResultIngestRequest

# TODO update the JSON string below
json = "{}"
# create an instance of AgentSessionCIResultIngestRequest from a JSON string
agent_session_ci_result_ingest_request_instance = AgentSessionCIResultIngestRequest.from_json(json)
# print the JSON string representation of the object
print(AgentSessionCIResultIngestRequest.to_json())

# convert the object into a dict
agent_session_ci_result_ingest_request_dict = agent_session_ci_result_ingest_request_instance.to_dict()
# create an instance of AgentSessionCIResultIngestRequest from a dict
agent_session_ci_result_ingest_request_from_dict = AgentSessionCIResultIngestRequest.from_dict(agent_session_ci_result_ingest_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


