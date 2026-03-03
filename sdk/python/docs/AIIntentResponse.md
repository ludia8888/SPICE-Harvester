# AIIntentResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**clarifying_question** | **str** |  | [optional] 
**confidence** | **float** |  | 
**intent** | [**AIIntentType**](AIIntentType.md) |  | 
**llm** | **Dict[str, object]** |  | [optional] 
**missing_fields** | **List[str]** |  | [optional] 
**reply** | **str** |  | [optional] 
**requires_clarification** | **bool** |  | [optional] [default to False]
**route** | [**AIIntentRoute**](AIIntentRoute.md) |  | 

## Example

```python
from spice_harvester_sdk.models.ai_intent_response import AIIntentResponse

# TODO update the JSON string below
json = "{}"
# create an instance of AIIntentResponse from a JSON string
ai_intent_response_instance = AIIntentResponse.from_json(json)
# print the JSON string representation of the object
print(AIIntentResponse.to_json())

# convert the object into a dict
ai_intent_response_dict = ai_intent_response_instance.to_dict()
# create an instance of AIIntentResponse from a dict
ai_intent_response_from_dict = AIIntentResponse.from_dict(ai_intent_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


