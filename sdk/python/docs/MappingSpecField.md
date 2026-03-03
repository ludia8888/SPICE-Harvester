# MappingSpecField


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**source_field** | **str** |  | 
**target_field** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.mapping_spec_field import MappingSpecField

# TODO update the JSON string below
json = "{}"
# create an instance of MappingSpecField from a JSON string
mapping_spec_field_instance = MappingSpecField.from_json(json)
# print the JSON string representation of the object
print(MappingSpecField.to_json())

# convert the object into a dict
mapping_spec_field_dict = mapping_spec_field_instance.to_dict()
# create an instance of MappingSpecField from a dict
mapping_spec_field_from_dict = MappingSpecField.from_dict(mapping_spec_field_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


