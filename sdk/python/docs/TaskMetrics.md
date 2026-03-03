# TaskMetrics

Aggregated metrics for background tasks.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**average_duration** | **float** | Average task duration in seconds | 
**cancelled_tasks** | **int** | Number of cancelled tasks | 
**completed_tasks** | **int** | Number of completed tasks | 
**failed_tasks** | **int** | Number of failed tasks | 
**pending_tasks** | **int** | Number of pending tasks | 
**processing_tasks** | **int** | Number of currently processing tasks | 
**retrying_tasks** | **int** | Number of tasks being retried | 
**success_rate** | **float** | Task success rate percentage | 
**total_tasks** | **int** | Total number of tasks | 

## Example

```python
from spice_harvester_sdk.models.task_metrics import TaskMetrics

# TODO update the JSON string below
json = "{}"
# create an instance of TaskMetrics from a JSON string
task_metrics_instance = TaskMetrics.from_json(json)
# print the JSON string representation of the object
print(TaskMetrics.to_json())

# convert the object into a dict
task_metrics_dict = task_metrics_instance.to_dict()
# create an instance of TaskMetrics from a dict
task_metrics_from_dict = TaskMetrics.from_dict(task_metrics_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


