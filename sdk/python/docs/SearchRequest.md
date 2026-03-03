# SearchRequest

Context7 search request.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**filters** | **Dict[str, object]** |  | [optional] 
**limit** | **int** | Maximum number of results | [optional] [default to 10]
**query** | **str** | Search query string | 

## Example

```python
from spice_harvester_sdk.models.search_request import SearchRequest

# TODO update the JSON string below
json = "{}"
# create an instance of SearchRequest from a JSON string
search_request_instance = SearchRequest.from_json(json)
# print the JSON string representation of the object
print(SearchRequest.to_json())

# convert the object into a dict
search_request_dict = search_request_instance.to_dict()
# create an instance of SearchRequest from a dict
search_request_from_dict = SearchRequest.from_dict(search_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


