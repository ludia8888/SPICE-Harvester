# AIAnswer


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**answer** | **str** |  | 
**confidence** | **float** |  | 
**follow_ups** | **List[str]** |  | [optional] 
**rationale** | **str** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.ai_answer import AIAnswer

# TODO update the JSON string below
json = "{}"
# create an instance of AIAnswer from a JSON string
ai_answer_instance = AIAnswer.from_json(json)
# print the JSON string representation of the object
print(AIAnswer.to_json())

# convert the object into a dict
ai_answer_dict = ai_answer_instance.to_dict()
# create an instance of AIAnswer from a dict
ai_answer_from_dict = AIAnswer.from_dict(ai_answer_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


