# OntologyAnalysisRequest

Request to analyze ontology with Context7.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**branch** | **str** | Ontology branch | [optional] [default to 'main']
**db_name** | **str** | Database name | 
**include_relationships** | **bool** | Include relationship analysis | [optional] [default to True]
**include_suggestions** | **bool** | Include improvement suggestions | [optional] [default to True]
**ontology_id** | **str** | Ontology ID | 

## Example

```python
from spice_harvester_sdk.models.ontology_analysis_request import OntologyAnalysisRequest

# TODO update the JSON string below
json = "{}"
# create an instance of OntologyAnalysisRequest from a JSON string
ontology_analysis_request_instance = OntologyAnalysisRequest.from_json(json)
# print the JSON string representation of the object
print(OntologyAnalysisRequest.to_json())

# convert the object into a dict
ontology_analysis_request_dict = ontology_analysis_request_instance.to_dict()
# create an instance of OntologyAnalysisRequest from a dict
ontology_analysis_request_from_dict = OntologyAnalysisRequest.from_dict(ontology_analysis_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


