# UserMeResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**display_name** | **str** |  | 
**email** | **str** |  | 
**org_id** | **str** |  | 
**roles** | **List[str]** |  | 
**tenant_id** | **str** |  | 
**username** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.user_me_response import UserMeResponse

# TODO update the JSON string below
json = "{}"
# create an instance of UserMeResponse from a JSON string
user_me_response_instance = UserMeResponse.from_json(json)
# print the JSON string representation of the object
print(UserMeResponse.to_json())

# convert the object into a dict
user_me_response_dict = user_me_response_instance.to_dict()
# create an instance of UserMeResponse from a dict
user_me_response_from_dict = UserMeResponse.from_dict(user_me_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


