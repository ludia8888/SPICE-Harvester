# CreateKeySpecRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**dataset_id** | **str** |  | 
**dataset_version_id** | **str** |  | [optional] 
**nullable_fields** | **List[str]** |  | [optional] 
**primary_key** | **List[str]** |  | [optional] 
**required_fields** | **List[str]** |  | [optional] 
**title_key** | **List[str]** |  | [optional] 
**unique_keys** | **List[List[str]]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.create_key_spec_request import CreateKeySpecRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreateKeySpecRequest from a JSON string
create_key_spec_request_instance = CreateKeySpecRequest.from_json(json)
# print the JSON string representation of the object
print(CreateKeySpecRequest.to_json())

# convert the object into a dict
create_key_spec_request_dict = create_key_spec_request_instance.to_dict()
# create an instance of CreateKeySpecRequest from a dict
create_key_spec_request_from_dict = CreateKeySpecRequest.from_dict(create_key_spec_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


