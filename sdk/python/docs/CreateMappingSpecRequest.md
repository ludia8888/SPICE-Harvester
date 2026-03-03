# CreateMappingSpecRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**artifact_output_name** | **str** |  | [optional] 
**auto_sync** | **bool** |  | [optional] [default to True]
**backing_datasource_id** | **str** |  | [optional] 
**backing_datasource_version_id** | **str** |  | [optional] 
**dataset_branch** | **str** |  | [optional] 
**dataset_id** | **str** |  | 
**mappings** | [**List[MappingSpecField]**](MappingSpecField.md) |  | 
**options** | **Dict[str, object]** |  | [optional] 
**schema_hash** | **str** |  | [optional] 
**status** | **str** |  | [optional] [default to 'ACTIVE']
**target_class_id** | **str** |  | 
**target_field_types** | **Dict[str, str]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.create_mapping_spec_request import CreateMappingSpecRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreateMappingSpecRequest from a JSON string
create_mapping_spec_request_instance = CreateMappingSpecRequest.from_json(json)
# print the JSON string representation of the object
print(CreateMappingSpecRequest.to_json())

# convert the object into a dict
create_mapping_spec_request_dict = create_mapping_spec_request_instance.to_dict()
# create an instance of CreateMappingSpecRequest from a dict
create_mapping_spec_request_from_dict = CreateMappingSpecRequest.from_dict(create_mapping_spec_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


