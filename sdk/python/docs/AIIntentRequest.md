# AIIntentRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**context** | **Dict[str, object]** |  | [optional] 
**db_name** | **str** |  | [optional] 
**language** | **str** |  | [optional] 
**pipeline_name** | **str** |  | [optional] 
**project_name** | **str** |  | [optional] 
**question** | **str** |  | 
**session_id** | **str** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.ai_intent_request import AIIntentRequest

# TODO update the JSON string below
json = "{}"
# create an instance of AIIntentRequest from a JSON string
ai_intent_request_instance = AIIntentRequest.from_json(json)
# print the JSON string representation of the object
print(AIIntentRequest.to_json())

# convert the object into a dict
ai_intent_request_dict = ai_intent_request_instance.to_dict()
# create an instance of AIIntentRequest from a dict
ai_intent_request_from_dict = AIIntentRequest.from_dict(ai_intent_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


