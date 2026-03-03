# TaskMetricsResponse

Task metrics response model.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**metrics** | [**TaskMetrics**](TaskMetrics.md) | Task execution metrics | 
**timestamp** | **datetime** | Metrics timestamp | [optional] 

## Example

```python
from spice_harvester_sdk.models.task_metrics_response import TaskMetricsResponse

# TODO update the JSON string below
json = "{}"
# create an instance of TaskMetricsResponse from a JSON string
task_metrics_response_instance = TaskMetricsResponse.from_json(json)
# print the JSON string representation of the object
print(TaskMetricsResponse.to_json())

# convert the object into a dict
task_metrics_response_dict = task_metrics_response_instance.to_dict()
# create an instance of TaskMetricsResponse from a dict
task_metrics_response_from_dict = TaskMetricsResponse.from_dict(task_metrics_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


