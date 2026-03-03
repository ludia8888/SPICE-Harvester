# OntologyResourceRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**description** | [**Description**](Description.md) |  | [optional] 
**id** | [**Id**](Id.md) |  | [optional] 
**label** | **object** | Resource label | 
**metadata** | **Dict[str, object]** |  | [optional] 
**spec** | **Dict[str, object]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.ontology_resource_request import OntologyResourceRequest

# TODO update the JSON string below
json = "{}"
# create an instance of OntologyResourceRequest from a JSON string
ontology_resource_request_instance = OntologyResourceRequest.from_json(json)
# print the JSON string representation of the object
print(OntologyResourceRequest.to_json())

# convert the object into a dict
ontology_resource_request_dict = ontology_resource_request_instance.to_dict()
# create an instance of OntologyResourceRequest from a dict
ontology_resource_request_from_dict = OntologyResourceRequest.from_dict(ontology_resource_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


