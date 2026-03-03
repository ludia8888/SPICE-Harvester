# ObjectTypeContractUpdateRequestV2


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**backing_dataset_id** | **str** |  | [optional] 
**backing_datasource_id** | **str** |  | [optional] 
**backing_datasource_version_id** | **str** |  | [optional] 
**backing_source** | **Dict[str, object]** |  | [optional] 
**backing_sources** | **List[Dict[str, object]]** |  | [optional] 
**dataset_version_id** | **str** |  | [optional] 
**mapping_spec_id** | **str** |  | [optional] 
**mapping_spec_version** | **int** |  | [optional] 
**metadata** | **Dict[str, object]** |  | [optional] 
**migration** | **Dict[str, object]** |  | [optional] 
**pk_spec** | **Dict[str, object]** |  | [optional] 
**primary_key** | **str** |  | [optional] 
**schema_hash** | **str** |  | [optional] 
**status** | **str** |  | [optional] 
**title_property** | **str** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.object_type_contract_update_request_v2 import ObjectTypeContractUpdateRequestV2

# TODO update the JSON string below
json = "{}"
# create an instance of ObjectTypeContractUpdateRequestV2 from a JSON string
object_type_contract_update_request_v2_instance = ObjectTypeContractUpdateRequestV2.from_json(json)
# print the JSON string representation of the object
print(ObjectTypeContractUpdateRequestV2.to_json())

# convert the object into a dict
object_type_contract_update_request_v2_dict = object_type_contract_update_request_v2_instance.to_dict()
# create an instance of ObjectTypeContractUpdateRequestV2 from a dict
object_type_contract_update_request_v2_from_dict = ObjectTypeContractUpdateRequestV2.from_dict(object_type_contract_update_request_v2_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


