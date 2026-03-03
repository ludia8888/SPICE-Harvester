# SimpleGraphQueryRequest

Request for simple single-class queries.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**class_name** | **str** |  | 
**filters** | **Dict[str, object]** |  | [optional] 
**limit** | **int** |  | [optional] [default to 100]

## Example

```python
from spice_harvester_sdk.models.simple_graph_query_request import SimpleGraphQueryRequest

# TODO update the JSON string below
json = "{}"
# create an instance of SimpleGraphQueryRequest from a JSON string
simple_graph_query_request_instance = SimpleGraphQueryRequest.from_json(json)
# print the JSON string representation of the object
print(SimpleGraphQueryRequest.to_json())

# convert the object into a dict
simple_graph_query_request_dict = simple_graph_query_request_instance.to_dict()
# create an instance of SimpleGraphQueryRequest from a dict
simple_graph_query_request_from_dict = SimpleGraphQueryRequest.from_dict(simple_graph_query_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


