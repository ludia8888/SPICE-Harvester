# QueryInput

Query input model

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**class_id** | **str** |  | [optional] 
**class_label** | **str** |  | [optional] 
**filters** | [**List[QueryFilter]**](QueryFilter.md) | Query filters | [optional] 
**limit** | **int** |  | [optional] 
**offset** | **int** |  | [optional] 
**order_by** | **str** |  | [optional] 
**order_direction** | **str** |  | [optional] 
**select** | **List[str]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.query_input import QueryInput

# TODO update the JSON string below
json = "{}"
# create an instance of QueryInput from a JSON string
query_input_instance = QueryInput.from_json(json)
# print the JSON string representation of the object
print(QueryInput.to_json())

# convert the object into a dict
query_input_dict = query_input_instance.to_dict()
# create an instance of QueryInput from a dict
query_input_from_dict = QueryInput.from_dict(query_input_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


