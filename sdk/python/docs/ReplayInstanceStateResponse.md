# ReplayInstanceStateResponse

Response model for instance state replay.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**message** | **str** | Status message | 
**status** | **str** | Task status | 
**status_url** | **str** | URL to check task status | 
**task_id** | **str** | Background task ID | 

## Example

```python
from spice_harvester_sdk.models.replay_instance_state_response import ReplayInstanceStateResponse

# TODO update the JSON string below
json = "{}"
# create an instance of ReplayInstanceStateResponse from a JSON string
replay_instance_state_response_instance = ReplayInstanceStateResponse.from_json(json)
# print the JSON string representation of the object
print(ReplayInstanceStateResponse.to_json())

# convert the object into a dict
replay_instance_state_response_dict = replay_instance_state_response_instance.to_dict()
# create an instance of ReplayInstanceStateResponse from a dict
replay_instance_state_response_from_dict = ReplayInstanceStateResponse.from_dict(replay_instance_state_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


