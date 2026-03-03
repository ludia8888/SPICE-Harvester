# AccessPolicyRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**db_name** | **str** |  | 
**policy** | **Dict[str, object]** |  | [optional] 
**scope** | **str** |  | [optional] [default to 'data_access']
**status** | [**Status**](Status.md) |  | [optional] 
**subject_id** | **str** |  | 
**subject_type** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.access_policy_request import AccessPolicyRequest

# TODO update the JSON string below
json = "{}"
# create an instance of AccessPolicyRequest from a JSON string
access_policy_request_instance = AccessPolicyRequest.from_json(json)
# print the JSON string representation of the object
print(AccessPolicyRequest.to_json())

# convert the object into a dict
access_policy_request_dict = access_policy_request_instance.to_dict()
# create an instance of AccessPolicyRequest from a dict
access_policy_request_from_dict = AccessPolicyRequest.from_dict(access_policy_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


