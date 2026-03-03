# DetectRelationshipsRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**confidence_threshold** | **float** |  | [optional] [default to 0.6]
**include_sample_analysis** | **bool** |  | [optional] [default to True]

## Example

```python
from spice_harvester_sdk.models.detect_relationships_request import DetectRelationshipsRequest

# TODO update the JSON string below
json = "{}"
# create an instance of DetectRelationshipsRequest from a JSON string
detect_relationships_request_instance = DetectRelationshipsRequest.from_json(json)
# print the JSON string representation of the object
print(DetectRelationshipsRequest.to_json())

# convert the object into a dict
detect_relationships_request_dict = detect_relationships_request_instance.to_dict()
# create an instance of DetectRelationshipsRequest from a dict
detect_relationships_request_from_dict = DetectRelationshipsRequest.from_dict(detect_relationships_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


