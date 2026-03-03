# BulkInstanceCreateRequest

대량 인스턴스 생성 요청 (Label 기반)

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**instances** | **List[Dict[str, object]]** | 인스턴스 데이터 목록 (Label 키 사용) | 
**metadata** | **Dict[str, object]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.bulk_instance_create_request import BulkInstanceCreateRequest

# TODO update the JSON string below
json = "{}"
# create an instance of BulkInstanceCreateRequest from a JSON string
bulk_instance_create_request_instance = BulkInstanceCreateRequest.from_json(json)
# print the JSON string representation of the object
print(BulkInstanceCreateRequest.to_json())

# convert the object into a dict
bulk_instance_create_request_dict = bulk_instance_create_request_instance.to_dict()
# create an instance of BulkInstanceCreateRequest from a dict
bulk_instance_create_request_from_dict = BulkInstanceCreateRequest.from_dict(bulk_instance_create_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


