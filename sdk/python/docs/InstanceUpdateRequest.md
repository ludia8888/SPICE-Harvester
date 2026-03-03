# InstanceUpdateRequest

인스턴스 수정 요청 (Label 기반)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**data** | **Dict[str, object]** | 수정할 데이터 (Label 키 사용) | 
**metadata** | **Dict[str, object]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.instance_update_request import InstanceUpdateRequest

# TODO update the JSON string below
json = "{}"
# create an instance of InstanceUpdateRequest from a JSON string
instance_update_request_instance = InstanceUpdateRequest.from_json(json)
# print the JSON string representation of the object
print(InstanceUpdateRequest.to_json())

# convert the object into a dict
instance_update_request_dict = instance_update_request_instance.to_dict()
# create an instance of InstanceUpdateRequest from a dict
instance_update_request_from_dict = InstanceUpdateRequest.from_dict(instance_update_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


