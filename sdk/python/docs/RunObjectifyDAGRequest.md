# RunObjectifyDAGRequest

Topologically enqueue objectify jobs based on mapping-spec relationship dependencies.

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**branch** | **str** | Ontology branch to use for all jobs | [optional] [default to 'master']
**class_ids** | **List[str]** | Target ontology class ids to objectify | 
**dry_run** | **bool** | If true, only returns the computed plan (no jobs queued) | [optional] [default to False]
**include_dependencies** | **bool** | If true, automatically include transitive dependencies referenced via relationships | [optional] [default to True]
**max_depth** | **int** | Max dependency expansion depth | [optional] [default to 10]
**options** | **Dict[str, object]** |  | [optional] 

## Example

```python
from spice_harvester_sdk.models.run_objectify_dag_request import RunObjectifyDAGRequest

# TODO update the JSON string below
json = "{}"
# create an instance of RunObjectifyDAGRequest from a JSON string
run_objectify_dag_request_instance = RunObjectifyDAGRequest.from_json(json)
# print the JSON string representation of the object
print(RunObjectifyDAGRequest.to_json())

# convert the object into a dict
run_objectify_dag_request_dict = run_objectify_dag_request_instance.to_dict()
# create an instance of RunObjectifyDAGRequest from a dict
run_objectify_dag_request_from_dict = RunObjectifyDAGRequest.from_dict(run_objectify_dag_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


