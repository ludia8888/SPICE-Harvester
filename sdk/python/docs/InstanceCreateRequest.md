# InstanceCreateRequest

인스턴스 생성 요청 (Label 기반)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**data** | **Dict[str, object]** | 인스턴스 데이터 (Label 키 사용) | 
**metadata** | **Dict[str, object]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.instance_create_request import InstanceCreateRequest

# TODO update the JSON string below
json = "{}"
# create an instance of InstanceCreateRequest from a JSON string
instance_create_request_instance = InstanceCreateRequest.from_json(json)
# print the JSON string representation of the object
print(InstanceCreateRequest.to_json())

# convert the object into a dict
instance_create_request_dict = instance_create_request_instance.to_dict()
# create an instance of InstanceCreateRequest from a dict
instance_create_request_from_dict = InstanceCreateRequest.from_dict(instance_create_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


