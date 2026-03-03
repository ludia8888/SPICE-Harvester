# RecomputeProjectionResponse

Response model for projection recompute.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**message** | **str** | Status message | 
**status** | **str** | Task status | 
**status_url** | **str** | URL to check task status | 
**task_id** | **str** | Background task ID | 

## Example

```python
from spice_harvester_sdk.models.recompute_projection_response import RecomputeProjectionResponse

# TODO update the JSON string below
json = "{}"
# create an instance of RecomputeProjectionResponse from a JSON string
recompute_projection_response_instance = RecomputeProjectionResponse.from_json(json)
# print the JSON string representation of the object
print(RecomputeProjectionResponse.to_json())

# convert the object into a dict
recompute_projection_response_dict = recompute_projection_response_instance.to_dict()
# create an instance of RecomputeProjectionResponse from a dict
recompute_projection_response_from_dict = RecomputeProjectionResponse.from_dict(recompute_projection_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


