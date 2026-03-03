# TriggerObjectifyRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**allow_partial** | **bool** |  | [optional] [default to False]
**artifact_id** | **str** |  | [optional] 
**artifact_output_name** | **str** |  | [optional] 
**batch_size** | **int** |  | [optional] 
**dataset_version_id** | **str** |  | [optional] 
**mapping_spec_id** | **str** |  | [optional] 
**max_rows** | **int** |  | [optional] 
**options** | **Dict[str, object]** |  | [optional] 
**target_class_id** | **str** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.trigger_objectify_request import TriggerObjectifyRequest

# TODO update the JSON string below
json = "{}"
# create an instance of TriggerObjectifyRequest from a JSON string
trigger_objectify_request_instance = TriggerObjectifyRequest.from_json(json)
# print the JSON string representation of the object
print(TriggerObjectifyRequest.to_json())

# convert the object into a dict
trigger_objectify_request_dict = trigger_objectify_request_instance.to_dict()
# create an instance of TriggerObjectifyRequest from a dict
trigger_objectify_request_from_dict = TriggerObjectifyRequest.from_dict(trigger_objectify_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


