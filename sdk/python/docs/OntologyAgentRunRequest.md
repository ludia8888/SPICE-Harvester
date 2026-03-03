# OntologyAgentRunRequest

Request body for ontology agent runs.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**answers** | **Dict[str, object]** |  | [optional] 
**branch** | **str** | Branch name | [optional] [default to 'main']
**dataset_sample** | **Dict[str, object]** |  | [optional] 
**db_name** | **str** | Database name | 
**goal** | **str** | Natural language goal for ontology creation/modification | 
**selected_model** | **str** |  | [optional] 
**target_class_id** | **str** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.ontology_agent_run_request import OntologyAgentRunRequest

# TODO update the JSON string below
json = "{}"
# create an instance of OntologyAgentRunRequest from a JSON string
ontology_agent_run_request_instance = OntologyAgentRunRequest.from_json(json)
# print the JSON string representation of the object
print(OntologyAgentRunRequest.to_json())

# convert the object into a dict
ontology_agent_run_request_dict = ontology_agent_run_request_instance.to_dict()
# create an instance of OntologyAgentRunRequest from a dict
ontology_agent_run_request_from_dict = OntologyAgentRunRequest.from_dict(ontology_agent_run_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


