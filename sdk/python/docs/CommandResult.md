# CommandResult

명령 실행 결과

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**command_id** | **str** | 명령 ID | 
**completed_at** | **datetime** |  | [optional] 
**error** | **str** |  | [optional] 
**result** | **Dict[str, object]** |  | [optional] 
**retry_count** | **int** | 재시도 횟수 | [optional] [default to 0]
**status** | [**CommandStatus**](CommandStatus.md) | 실행 상태 | 

## Example

```python
from spice_harvester_sdk.models.command_result import CommandResult

# TODO update the JSON string below
json = "{}"
# create an instance of CommandResult from a JSON string
command_result_instance = CommandResult.from_json(json)
# print the JSON string representation of the object
print(CommandResult.to_json())

# convert the object into a dict
command_result_dict = command_result_instance.to_dict()
# create an instance of CommandResult from a dict
command_result_from_dict = CommandResult.from_dict(command_result_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


