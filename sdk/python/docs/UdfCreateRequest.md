# UdfCreateRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**code** | **str** |  | 
**description** | **str** |  | [optional] 
**name** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.udf_create_request import UdfCreateRequest

# TODO update the JSON string below
json = "{}"
# create an instance of UdfCreateRequest from a JSON string
udf_create_request_instance = UdfCreateRequest.from_json(json)
# print the JSON string representation of the object
print(UdfCreateRequest.to_json())

# convert the object into a dict
udf_create_request_dict = udf_create_request_instance.to_dict()
# create an instance of UdfCreateRequest from a dict
udf_create_request_from_dict = UdfCreateRequest.from_dict(udf_create_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


