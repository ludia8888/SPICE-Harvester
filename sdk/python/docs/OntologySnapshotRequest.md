# OntologySnapshotRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**branch** | **str** |  | [optional] [default to 'main']
**db_name** | **str** |  | 
**ontology_id** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.ontology_snapshot_request import OntologySnapshotRequest

# TODO update the JSON string below
json = "{}"
# create an instance of OntologySnapshotRequest from a JSON string
ontology_snapshot_request_instance = OntologySnapshotRequest.from_json(json)
# print the JSON string representation of the object
print(OntologySnapshotRequest.to_json())

# convert the object into a dict
ontology_snapshot_request_dict = ontology_snapshot_request_instance.to_dict()
# create an instance of OntologySnapshotRequest from a dict
ontology_snapshot_request_from_dict = OntologySnapshotRequest.from_dict(ontology_snapshot_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


