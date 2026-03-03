# BatchApplyActionRequestV2


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**options** | [**BatchApplyActionRequestOptionsV2**](BatchApplyActionRequestOptionsV2.md) |  | [optional] 
**requests** | [**List[BatchApplyActionRequestItemV2]**](BatchApplyActionRequestItemV2.md) |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.batch_apply_action_request_v2 import BatchApplyActionRequestV2

# TODO update the JSON string below
json = "{}"
# create an instance of BatchApplyActionRequestV2 from a JSON string
batch_apply_action_request_v2_instance = BatchApplyActionRequestV2.from_json(json)
# print the JSON string representation of the object
print(BatchApplyActionRequestV2.to_json())

# convert the object into a dict
batch_apply_action_request_v2_dict = batch_apply_action_request_v2_instance.to_dict()
# create an instance of BatchApplyActionRequestV2 from a dict
batch_apply_action_request_v2_from_dict = BatchApplyActionRequestV2.from_dict(batch_apply_action_request_v2_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


