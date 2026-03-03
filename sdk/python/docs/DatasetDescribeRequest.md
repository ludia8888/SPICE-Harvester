# DatasetDescribeRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**branch** | **str** |  | [optional] [default to 'main']
**dataset_ids** | **List[str]** |  | [optional] 
**db_name** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.dataset_describe_request import DatasetDescribeRequest

# TODO update the JSON string below
json = "{}"
# create an instance of DatasetDescribeRequest from a JSON string
dataset_describe_request_instance = DatasetDescribeRequest.from_json(json)
# print the JSON string representation of the object
print(DatasetDescribeRequest.to_json())

# convert the object into a dict
dataset_describe_request_dict = dataset_describe_request_instance.to_dict()
# create an instance of DatasetDescribeRequest from a dict
dataset_describe_request_from_dict = DatasetDescribeRequest.from_dict(dataset_describe_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


