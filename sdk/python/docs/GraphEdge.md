# GraphEdge

Graph edge between nodes.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**from_node** | **str** |  | 
**predicate** | **str** |  | 
**provenance** | **Dict[str, object]** |  | [optional] 
**to_node** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.graph_edge import GraphEdge

# TODO update the JSON string below
json = "{}"
# create an instance of GraphEdge from a JSON string
graph_edge_instance = GraphEdge.from_json(json)
# print the JSON string representation of the object
print(GraphEdge.to_json())

# convert the object into a dict
graph_edge_dict = graph_edge_instance.to_dict()
# create an instance of GraphEdge from a dict
graph_edge_from_dict = GraphEdge.from_dict(graph_edge_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


