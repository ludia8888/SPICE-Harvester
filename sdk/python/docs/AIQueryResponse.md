# AIQueryResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**answer** | [**AIAnswer**](AIAnswer.md) |  | 
**execution** | **Dict[str, object]** |  | 
**llm** | **Dict[str, object]** |  | [optional] 
**plan** | [**AIQueryPlan**](AIQueryPlan.md) |  | 
**warnings** | **List[str]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.ai_query_response import AIQueryResponse

# TODO update the JSON string below
json = "{}"
# create an instance of AIQueryResponse from a JSON string
ai_query_response_instance = AIQueryResponse.from_json(json)
# print the JSON string representation of the object
print(AIQueryResponse.to_json())

# convert the object into a dict
ai_query_response_dict = ai_query_response_instance.to_dict()
# create an instance of AIQueryResponse from a dict
ai_query_response_from_dict = AIQueryResponse.from_dict(ai_query_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


