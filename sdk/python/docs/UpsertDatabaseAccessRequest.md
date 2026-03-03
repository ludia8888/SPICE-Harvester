# UpsertDatabaseAccessRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**entries** | [**List[DatabaseAccessEntryRequest]**](DatabaseAccessEntryRequest.md) |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.upsert_database_access_request import UpsertDatabaseAccessRequest

# TODO update the JSON string below
json = "{}"
# create an instance of UpsertDatabaseAccessRequest from a JSON string
upsert_database_access_request_instance = UpsertDatabaseAccessRequest.from_json(json)
# print the JSON string representation of the object
print(UpsertDatabaseAccessRequest.to_json())

# convert the object into a dict
upsert_database_access_request_dict = upsert_database_access_request_instance.to_dict()
# create an instance of UpsertDatabaseAccessRequest from a dict
upsert_database_access_request_from_dict = UpsertDatabaseAccessRequest.from_dict(upsert_database_access_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


