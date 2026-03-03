# AcknowledgeRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**acknowledged_by** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.acknowledge_request import AcknowledgeRequest

# TODO update the JSON string below
json = "{}"
# create an instance of AcknowledgeRequest from a JSON string
acknowledge_request_instance = AcknowledgeRequest.from_json(json)
# print the JSON string representation of the object
print(AcknowledgeRequest.to_json())

# convert the object into a dict
acknowledge_request_dict = acknowledge_request_instance.to_dict()
# create an instance of AcknowledgeRequest from a dict
acknowledge_request_from_dict = AcknowledgeRequest.from_dict(acknowledge_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


