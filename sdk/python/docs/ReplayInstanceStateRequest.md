# ReplayInstanceStateRequest

Request model for instance state replay.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**class_id** | **str** | Class ID | 
**db_name** | **str** | Database name | 
**instance_id** | **str** | Instance ID | 
**result_ttl** | **int** | Result TTL in seconds | [optional] [default to 3600]
**store_result** | **bool** | Store result in Redis | [optional] [default to True]

## Example

```python
from spice_harvester_sdk.models.replay_instance_state_request import ReplayInstanceStateRequest

# TODO update the JSON string below
json = "{}"
# create an instance of ReplayInstanceStateRequest from a JSON string
replay_instance_state_request_instance = ReplayInstanceStateRequest.from_json(json)
# print the JSON string representation of the object
print(ReplayInstanceStateRequest.to_json())

# convert the object into a dict
replay_instance_state_request_dict = replay_instance_state_request_instance.to_dict()
# create an instance of ReplayInstanceStateRequest from a dict
replay_instance_state_request_from_dict = ReplayInstanceStateRequest.from_dict(replay_instance_state_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


