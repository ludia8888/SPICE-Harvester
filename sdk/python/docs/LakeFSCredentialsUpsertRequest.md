# LakeFSCredentialsUpsertRequest

Upsert request for lakeFS credentials stored in Postgres (encrypted).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**access_key_id** | **str** | lakeFS access key id | 
**principal_id** | **str** | User ID or service name | 
**principal_type** | **str** | Principal type (user|service) | 
**secret_access_key** | **str** | lakeFS secret access key | 

## Example

```python
from spice_harvester_sdk.models.lake_fs_credentials_upsert_request import LakeFSCredentialsUpsertRequest

# TODO update the JSON string below
json = "{}"
# create an instance of LakeFSCredentialsUpsertRequest from a JSON string
lake_fs_credentials_upsert_request_instance = LakeFSCredentialsUpsertRequest.from_json(json)
# print the JSON string representation of the object
print(LakeFSCredentialsUpsertRequest.to_json())

# convert the object into a dict
lake_fs_credentials_upsert_request_dict = lake_fs_credentials_upsert_request_instance.to_dict()
# create an instance of LakeFSCredentialsUpsertRequest from a dict
lake_fs_credentials_upsert_request_from_dict = LakeFSCredentialsUpsertRequest.from_dict(lake_fs_credentials_upsert_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


