# UdfVersionCreateRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**code** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.udf_version_create_request import UdfVersionCreateRequest

# TODO update the JSON string below
json = "{}"
# create an instance of UdfVersionCreateRequest from a JSON string
udf_version_create_request_instance = UdfVersionCreateRequest.from_json(json)
# print the JSON string representation of the object
print(UdfVersionCreateRequest.to_json())

# convert the object into a dict
udf_version_create_request_dict = udf_version_create_request_instance.to_dict()
# create an instance of UdfVersionCreateRequest from a dict
udf_version_create_request_from_dict = UdfVersionCreateRequest.from_dict(udf_version_create_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


