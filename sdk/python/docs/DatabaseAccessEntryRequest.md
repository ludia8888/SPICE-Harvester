# DatabaseAccessEntryRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**principal_id** | **str** | Principal identifier (e.g., JWT subject) | 
**principal_name** | **str** |  | [optional] 
**principal_type** | **str** | Principal type (e.g., user, service) | [optional] [default to 'user']
**role** | **str** | Database role (Owner/Editor/Viewer/DomainModeler/DataEngineer/Security) | 

## Example

```python
from spice_harvester_sdk.models.database_access_entry_request import DatabaseAccessEntryRequest

# TODO update the JSON string below
json = "{}"
# create an instance of DatabaseAccessEntryRequest from a JSON string
database_access_entry_request_instance = DatabaseAccessEntryRequest.from_json(json)
# print the JSON string representation of the object
print(DatabaseAccessEntryRequest.to_json())

# convert the object into a dict
database_access_entry_request_dict = database_access_entry_request_instance.to_dict()
# create an instance of DatabaseAccessEntryRequest from a dict
database_access_entry_request_from_dict = DatabaseAccessEntryRequest.from_dict(database_access_entry_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


