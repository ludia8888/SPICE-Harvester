# RecomputeProjectionRequest

Request model for projection recompute (Versioning + Recompute).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**allow_delete_base_index** | **bool** | If base index exists as a concrete index (not alias), allow deleting it to create an alias. | [optional] [default to False]
**branch** | **str** | Branch scope (default: main) | [optional] [default to 'main']
**db_name** | **str** | Database name | 
**from_ts** | **datetime** | Start timestamp (ISO8601; timezone-aware recommended) | 
**max_events** | **int** |  | [optional] 
**projection** | **str** | Projection to rebuild (instances|ontologies) | 
**promote** | **bool** | Promote the rebuilt index to the base index name (alias swap). | [optional] [default to False]
**to_ts** | **datetime** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.recompute_projection_request import RecomputeProjectionRequest

# TODO update the JSON string below
json = "{}"
# create an instance of RecomputeProjectionRequest from a JSON string
recompute_projection_request_instance = RecomputeProjectionRequest.from_json(json)
# print the JSON string representation of the object
print(RecomputeProjectionRequest.to_json())

# convert the object into a dict
recompute_projection_request_dict = recompute_projection_request_instance.to_dict()
# create an instance of RecomputeProjectionRequest from a dict
recompute_projection_request_from_dict = RecomputeProjectionRequest.from_dict(recompute_projection_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


