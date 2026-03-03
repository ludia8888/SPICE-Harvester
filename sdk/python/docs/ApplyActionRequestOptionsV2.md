# ApplyActionRequestOptionsV2


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**mode** | **str** |  | [optional] 
**return_edits** | **str** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.apply_action_request_options_v2 import ApplyActionRequestOptionsV2

# TODO update the JSON string below
json = "{}"
# create an instance of ApplyActionRequestOptionsV2 from a JSON string
apply_action_request_options_v2_instance = ApplyActionRequestOptionsV2.from_json(json)
# print the JSON string representation of the object
print(ApplyActionRequestOptionsV2.to_json())

# convert the object into a dict
apply_action_request_options_v2_dict = apply_action_request_options_v2_instance.to_dict()
# create an instance of ApplyActionRequestOptionsV2 from a dict
apply_action_request_options_v2_from_dict = ApplyActionRequestOptionsV2.from_dict(apply_action_request_options_v2_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


