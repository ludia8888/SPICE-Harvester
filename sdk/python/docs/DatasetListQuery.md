# DatasetListQuery


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**limit** | **int** |  | [optional] [default to 50]
**name_contains** | **str** |  | [optional] 
**source_type** | **str** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.dataset_list_query import DatasetListQuery

# TODO update the JSON string below
json = "{}"
# create an instance of DatasetListQuery from a JSON string
dataset_list_query_instance = DatasetListQuery.from_json(json)
# print the JSON string representation of the object
print(DatasetListQuery.to_json())

# convert the object into a dict
dataset_list_query_dict = dataset_list_query_instance.to_dict()
# create an instance of DatasetListQuery from a dict
dataset_list_query_from_dict = DatasetListQuery.from_dict(dataset_list_query_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


