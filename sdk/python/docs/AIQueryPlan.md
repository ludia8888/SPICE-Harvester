# AIQueryPlan

LLM-produced query plan.  Safety contract: - The plan is *only* a recommendation. Server validates and enforces caps. - Output must be JSON (schema-validated).

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**confidence** | **float** |  | 
**dataset_query** | [**DatasetListQuery**](DatasetListQuery.md) |  | [optional] 
**graph_query** | [**GraphQueryRequest**](GraphQueryRequest.md) |  | [optional] 
**interpretation** | **str** | Human-readable interpretation of the question | 
**query** | [**QueryInput**](QueryInput.md) |  | [optional] 
**tool** | [**AIQueryTool**](AIQueryTool.md) |  | 
**warnings** | **List[str]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.ai_query_plan import AIQueryPlan

# TODO update the JSON string below
json = "{}"
# create an instance of AIQueryPlan from a JSON string
ai_query_plan_instance = AIQueryPlan.from_json(json)
# print the JSON string representation of the object
print(AIQueryPlan.to_json())

# convert the object into a dict
ai_query_plan_dict = ai_query_plan_instance.to_dict()
# create an instance of AIQueryPlan from a dict
ai_query_plan_from_dict = AIQueryPlan.from_dict(ai_query_plan_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


