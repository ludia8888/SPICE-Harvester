# DatabaseCreateRequest

Request model for creating a database

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**description** | **str** |  | [optional] 
**name** | **str** | Database name | 
**options** | **Dict[str, object]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.database_create_request import DatabaseCreateRequest

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseCreateRequest from a JSON string
database_create_request_instance = DatabaseCreateRequest.from_json(json)
# print the JSON string representation of the object
print(DatabaseCreateRequest.to_json())

# convert the object into a dict
database_create_request_dict = database_create_request_instance.to_dict()
# create an instance of DatabaseCreateRequest from a dict
database_create_request_from_dict = DatabaseCreateRequest.from_dict(database_create_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


