# EntityLinkRequest

Request to create entity relationship.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**properties** | **Dict[str, object]** |  | [optional] 
**relationship** | **str** | Relationship type | 
**source_id** | **str** | Source entity ID | 
**target_id** | **str** | Target entity ID | 

## Example

```python
from spice_harvester_sdk.models.entity_link_request import EntityLinkRequest

# TODO update the JSON string below
json = "{}"
# create an instance of EntityLinkRequest from a JSON string
entity_link_request_instance = EntityLinkRequest.from_json(json)
# print the JSON string representation of the object
print(EntityLinkRequest.to_json())

# convert the object into a dict
entity_link_request_dict = entity_link_request_instance.to_dict()
# create an instance of EntityLinkRequest from a dict
entity_link_request_from_dict = EntityLinkRequest.from_dict(entity_link_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


