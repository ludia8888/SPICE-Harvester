# GraphHop

Represents a single hop in a graph traversal.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**predicate** | **str** |  | 
**reverse** | **bool** |  | [optional] [default to False]
**target_class** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.graph_hop import GraphHop

# TODO update the JSON string below
json = "{}"
# create an instance of GraphHop from a JSON string
graph_hop_instance = GraphHop.from_json(json)
# print the JSON string representation of the object
print(GraphHop.to_json())

# convert the object into a dict
graph_hop_dict = graph_hop_instance.to_dict()
# create an instance of GraphHop from a dict
graph_hop_from_dict = GraphHop.from_dict(graph_hop_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


