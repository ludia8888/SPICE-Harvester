# ExecuteQueryRequestV2


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**options** | **Dict[str, object]** |  | [optional] 
**parameters** | **Dict[str, object]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.execute_query_request_v2 import ExecuteQueryRequestV2

# TODO update the JSON string below
json = "{}"
# create an instance of ExecuteQueryRequestV2 from a JSON string
execute_query_request_v2_instance = ExecuteQueryRequestV2.from_json(json)
# print the JSON string representation of the object
print(ExecuteQueryRequestV2.to_json())

# convert the object into a dict
execute_query_request_v2_dict = execute_query_request_v2_instance.to_dict()
# create an instance of ExecuteQueryRequestV2 from a dict
execute_query_request_v2_from_dict = ExecuteQueryRequestV2.from_dict(execute_query_request_v2_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


