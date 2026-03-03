# TriggerIncrementalRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**batch_size** | **int** |  | [optional] 
**execution_mode** | **str** |  | [optional] [default to 'incremental']
**force_full_refresh** | **bool** |  | [optional] [default to False]
**max_rows** | **int** |  | [optional] 
**watermark_column** | **str** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.trigger_incremental_request import TriggerIncrementalRequest

# TODO update the JSON string below
json = "{}"
# create an instance of TriggerIncrementalRequest from a JSON string
trigger_incremental_request_instance = TriggerIncrementalRequest.from_json(json)
# print the JSON string representation of the object
print(TriggerIncrementalRequest.to_json())

# convert the object into a dict
trigger_incremental_request_dict = trigger_incremental_request_instance.to_dict()
# create an instance of TriggerIncrementalRequest from a dict
trigger_incremental_request_from_dict = TriggerIncrementalRequest.from_dict(trigger_incremental_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


