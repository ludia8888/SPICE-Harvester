# AIQueryRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**branch** | **str** | Branch for graph queries (default: main) | [optional] [default to 'main']
**include_documents** | **bool** |  | [optional] [default to True]
**include_provenance** | **bool** |  | [optional] [default to True]
**limit** | **int** |  | [optional] [default to 50]
**mode** | [**AIQueryMode**](AIQueryMode.md) |  | [optional] 
**question** | **str** |  | 
**session_id** | **str** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.ai_query_request import AIQueryRequest

# TODO update the JSON string below
json = "{}"
# create an instance of AIQueryRequest from a JSON string
ai_query_request_instance = AIQueryRequest.from_json(json)
# print the JSON string representation of the object
print(AIQueryRequest.to_json())

# convert the object into a dict
ai_query_request_dict = ai_query_request_instance.to_dict()
# create an instance of AIQueryRequest from a dict
ai_query_request_from_dict = AIQueryRequest.from_dict(ai_query_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


