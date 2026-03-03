# GraphQueryRequest

Request model for multi-hop graph queries.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**filters** | **Dict[str, object]** |  | [optional] 
**hops** | [**List[GraphHop]**](GraphHop.md) |  | 
**include_audit** | **bool** |  | [optional] [default to False]
**include_documents** | **bool** |  | [optional] [default to True]
**include_paths** | **bool** |  | [optional] [default to False]
**include_provenance** | **bool** |  | [optional] [default to False]
**limit** | **int** |  | [optional] [default to 100]
**max_edges** | **int** |  | [optional] [default to 2000]
**max_nodes** | **int** |  | [optional] [default to 500]
**max_paths** | **int** |  | [optional] [default to 100]
**no_cycles** | **bool** |  | [optional] [default to False]
**offset** | **int** |  | [optional] [default to 0]
**path_depth_limit** | **int** |  | [optional] 
**start_class** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.graph_query_request import GraphQueryRequest

# TODO update the JSON string below
json = "{}"
# create an instance of GraphQueryRequest from a JSON string
graph_query_request_instance = GraphQueryRequest.from_json(json)
# print the JSON string representation of the object
print(GraphQueryRequest.to_json())

# convert the object into a dict
graph_query_request_dict = graph_query_request_instance.to_dict()
# create an instance of GraphQueryRequest from a dict
graph_query_request_from_dict = GraphQueryRequest.from_dict(graph_query_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


