# OntologyDeploymentRecordRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**deployed_by** | **str** |  | [optional] 
**metadata** | **Dict[str, object]** |  | [optional] 
**ontology_commit_id** | **str** |  | [optional] 
**snapshot_rid** | **str** |  | [optional] 
**target_branch** | **str** | Deployment target branch | [optional] [default to 'main']

## Example

```python
from spice_harvester_sdk.models.ontology_deployment_record_request import OntologyDeploymentRecordRequest

# TODO update the JSON string below
json = "{}"
# create an instance of OntologyDeploymentRecordRequest from a JSON string
ontology_deployment_record_request_instance = OntologyDeploymentRecordRequest.from_json(json)
# print the JSON string representation of the object
print(OntologyDeploymentRecordRequest.to_json())

# convert the object into a dict
ontology_deployment_record_request_dict = ontology_deployment_record_request_instance.to_dict()
# create an instance of OntologyDeploymentRecordRequest from a dict
ontology_deployment_record_request_from_dict = OntologyDeploymentRecordRequest.from_dict(ontology_deployment_record_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


