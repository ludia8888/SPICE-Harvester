# GraphQueryResponse

Response model for graph queries.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**base_branch** | **str** |  | [optional] 
**count** | **int** |  | 
**edges** | [**List[GraphEdge]**](GraphEdge.md) |  | 
**index_summary** | **Dict[str, object]** |  | [optional] 
**nodes** | [**List[GraphNode]**](GraphNode.md) |  | 
**overlay_branch** | **str** |  | [optional] 
**overlay_status** | **str** |  | [optional] 
**page** | **Dict[str, object]** |  | [optional] 
**paths** | **List[Dict[str, object]]** |  | [optional] 
**query** | **Dict[str, object]** |  | 
**warnings** | **List[str]** |  | [optional] [default to []]
**writeback_edits_present** | **bool** |  | [optional] 
**writeback_enabled** | **bool** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.graph_query_response import GraphQueryResponse

# TODO update the JSON string below
json = "{}"
# create an instance of GraphQueryResponse from a JSON string
graph_query_response_instance = GraphQueryResponse.from_json(json)
# print the JSON string representation of the object
print(GraphQueryResponse.to_json())

# convert the object into a dict
graph_query_response_dict = graph_query_response_instance.to_dict()
# create an instance of GraphQueryResponse from a dict
graph_query_response_from_dict = GraphQueryResponse.from_dict(graph_query_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


