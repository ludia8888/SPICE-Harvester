# KnowledgeRequest

Request to add knowledge to Context7.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**content** | **str** | Knowledge content | 
**metadata** | **Dict[str, object]** |  | [optional] 
**tags** | **List[str]** |  | [optional] 
**title** | **str** | Knowledge title | 

## Example

```python
from spice_harvester_sdk.models.knowledge_request import KnowledgeRequest

# TODO update the JSON string below
json = "{}"
# create an instance of KnowledgeRequest from a JSON string
knowledge_request_instance = KnowledgeRequest.from_json(json)
# print the JSON string representation of the object
print(KnowledgeRequest.to_json())

# convert the object into a dict
knowledge_request_dict = knowledge_request_instance.to_dict()
# create an instance of KnowledgeRequest from a dict
knowledge_request_from_dict = KnowledgeRequest.from_dict(knowledge_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


