# GatePolicyRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**description** | **str** |  | [optional] 
**name** | **str** |  | 
**rules** | **Dict[str, object]** |  | [optional] 
**scope** | **str** |  | 
**status** | [**Status**](Status.md) |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.gate_policy_request import GatePolicyRequest

# TODO update the JSON string below
json = "{}"
# create an instance of GatePolicyRequest from a JSON string
gate_policy_request_instance = GatePolicyRequest.from_json(json)
# print the JSON string representation of the object
print(GatePolicyRequest.to_json())

# convert the object into a dict
gate_policy_request_dict = gate_policy_request_instance.to_dict()
# create an instance of GatePolicyRequest from a dict
gate_policy_request_from_dict = GatePolicyRequest.from_dict(gate_policy_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


