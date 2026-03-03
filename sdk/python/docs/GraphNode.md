# GraphNode

Graph node with ES document reference.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**data** | **Dict[str, object]** |  | [optional] 
**data_status** | **str** |  | 
**db_name** | **str** |  | 
**display** | **Dict[str, object]** |  | [optional] 
**es_doc_id** | **str** |  | 
**es_ref** | **Dict[str, str]** |  | 
**id** | **str** |  | 
**index_status** | **Dict[str, object]** |  | [optional] 
**provenance** | **Dict[str, object]** |  | [optional] 
**type** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.graph_node import GraphNode

# TODO update the JSON string below
json = "{}"
# create an instance of GraphNode from a JSON string
graph_node_instance = GraphNode.from_json(json)
# print the JSON string representation of the object
print(GraphNode.to_json())

# convert the object into a dict
graph_node_dict = graph_node_instance.to_dict()
# create an instance of GraphNode from a dict
graph_node_from_dict = GraphNode.from_dict(graph_node_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


