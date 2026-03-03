# ApplyActionRequestV2


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**options** | [**ApplyActionRequestOptionsV2**](ApplyActionRequestOptionsV2.md) |  | [optional] 
**parameters** | **Dict[str, object]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.apply_action_request_v2 import ApplyActionRequestV2

# TODO update the JSON string below
json = "{}"
# create an instance of ApplyActionRequestV2 from a JSON string
apply_action_request_v2_instance = ApplyActionRequestV2.from_json(json)
# print the JSON string representation of the object
print(ApplyActionRequestV2.to_json())

# convert the object into a dict
apply_action_request_v2_dict = apply_action_request_v2_instance.to_dict()
# create an instance of ApplyActionRequestV2 from a dict
apply_action_request_v2_from_dict = ApplyActionRequestV2.from_dict(apply_action_request_v2_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


