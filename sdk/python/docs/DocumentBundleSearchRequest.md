# DocumentBundleSearchRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**filters** | **Dict[str, object]** |  | [optional] 
**limit** | **int** |  | [optional] [default to 10]
**query** | **str** |  | 

## Example

```python
from spice_harvester_sdk.models.document_bundle_search_request import DocumentBundleSearchRequest

# TODO update the JSON string below
json = "{}"
# create an instance of DocumentBundleSearchRequest from a JSON string
document_bundle_search_request_instance = DocumentBundleSearchRequest.from_json(json)
# print the JSON string representation of the object
print(DocumentBundleSearchRequest.to_json())

# convert the object into a dict
document_bundle_search_request_dict = document_bundle_search_request_instance.to_dict()
# create an instance of DocumentBundleSearchRequest from a dict
document_bundle_search_request_from_dict = DocumentBundleSearchRequest.from_dict(document_bundle_search_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


