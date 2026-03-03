# spice_harvester_sdk.LineageApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_lineage_column_lineage_api_v1_lineage_column_lineage_get**](LineageApi.md#get_lineage_column_lineage_api_v1_lineage_column_lineage_get) | **GET** /api/v1/lineage/column-lineage | Get Lineage Column Lineage
[**get_lineage_diff_api_v1_lineage_diff_get**](LineageApi.md#get_lineage_diff_api_v1_lineage_diff_get) | **GET** /api/v1/lineage/diff | Get Lineage Diff
[**get_lineage_graph_api_v1_lineage_graph_get**](LineageApi.md#get_lineage_graph_api_v1_lineage_graph_get) | **GET** /api/v1/lineage/graph | Get Lineage Graph
[**get_lineage_impact_api_v1_lineage_impact_get**](LineageApi.md#get_lineage_impact_api_v1_lineage_impact_get) | **GET** /api/v1/lineage/impact | Get Lineage Impact
[**get_lineage_metrics_api_v1_lineage_metrics_get**](LineageApi.md#get_lineage_metrics_api_v1_lineage_metrics_get) | **GET** /api/v1/lineage/metrics | Get Lineage Metrics
[**get_lineage_out_of_date_api_v1_lineage_out_of_date_get**](LineageApi.md#get_lineage_out_of_date_api_v1_lineage_out_of_date_get) | **GET** /api/v1/lineage/out-of-date | Get Lineage Out Of Date
[**get_lineage_path_api_v1_lineage_path_get**](LineageApi.md#get_lineage_path_api_v1_lineage_path_get) | **GET** /api/v1/lineage/path | Get Lineage Path
[**get_lineage_run_impact_api_v1_lineage_run_impact_get**](LineageApi.md#get_lineage_run_impact_api_v1_lineage_run_impact_get) | **GET** /api/v1/lineage/run-impact | Get Lineage Run Impact
[**get_lineage_runs_api_v1_lineage_runs_get**](LineageApi.md#get_lineage_runs_api_v1_lineage_runs_get) | **GET** /api/v1/lineage/runs | Get Lineage Runs
[**get_lineage_timeline_api_v1_lineage_timeline_get**](LineageApi.md#get_lineage_timeline_api_v1_lineage_timeline_get) | **GET** /api/v1/lineage/timeline | Get Lineage Timeline


# **get_lineage_column_lineage_api_v1_lineage_column_lineage_get**
> object get_lineage_column_lineage_api_v1_lineage_column_lineage_get(db_name=db_name, branch=branch, run_id=run_id, source_field=source_field, target_field=target_field, target_class_id=target_class_id, since=since, until=until, edge_limit=edge_limit, pair_limit=pair_limit, lang=lang, accept_language=accept_language)

Get Lineage Column Lineage

Resolve column-level lineage from mapping-spec references embedded in lineage edges.

This provides a Foundry-style "which source columns mapped to which target fields"
view without exploding the graph into per-column nodes.

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.LineageApi(api_client)
    db_name = 'db_name_example' # str | Database scope (recommended) (optional)
    branch = 'branch_example' # str | Optional branch scope (optional)
    run_id = 'run_id_example' # str | Optional run id filter (optional)
    source_field = 'source_field_example' # str | Optional source column filter (optional)
    target_field = 'target_field_example' # str | Optional target field filter (optional)
    target_class_id = 'target_class_id_example' # str | Optional target class filter (optional)
    since = '2013-10-20T19:20:30+01:00' # datetime | Window start (ISO-8601), default now-24h (optional)
    until = '2013-10-20T19:20:30+01:00' # datetime | Window end (ISO-8601), default now (optional)
    edge_limit = 5000 # int | Max lineage edges loaded (optional) (default to 5000)
    pair_limit = 2000 # int | Max column lineage pairs returned (optional) (default to 2000)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Lineage Column Lineage
        api_response = api_instance.get_lineage_column_lineage_api_v1_lineage_column_lineage_get(db_name=db_name, branch=branch, run_id=run_id, source_field=source_field, target_field=target_field, target_class_id=target_class_id, since=since, until=until, edge_limit=edge_limit, pair_limit=pair_limit, lang=lang, accept_language=accept_language)
        print("The response of LineageApi->get_lineage_column_lineage_api_v1_lineage_column_lineage_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LineageApi->get_lineage_column_lineage_api_v1_lineage_column_lineage_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**| Database scope (recommended) | [optional] 
 **branch** | **str**| Optional branch scope | [optional] 
 **run_id** | **str**| Optional run id filter | [optional] 
 **source_field** | **str**| Optional source column filter | [optional] 
 **target_field** | **str**| Optional target field filter | [optional] 
 **target_class_id** | **str**| Optional target class filter | [optional] 
 **since** | **datetime**| Window start (ISO-8601), default now-24h | [optional] 
 **until** | **datetime**| Window end (ISO-8601), default now | [optional] 
 **edge_limit** | **int**| Max lineage edges loaded | [optional] [default to 5000]
 **pair_limit** | **int**| Max column lineage pairs returned | [optional] [default to 2000]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_lineage_diff_api_v1_lineage_diff_get**
> object get_lineage_diff_api_v1_lineage_diff_get(root, from_as_of, to_as_of=to_as_of, db_name=db_name, branch=branch, direction=direction, max_depth=max_depth, max_nodes=max_nodes, max_edges=max_edges, lang=lang, accept_language=accept_language)

Get Lineage Diff

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.LineageApi(api_client)
    root = 'root_example' # str | Root node id or event id
    from_as_of = '2013-10-20T19:20:30+01:00' # datetime | Baseline as-of timestamp (ISO-8601)
    to_as_of = '2013-10-20T19:20:30+01:00' # datetime | Target as-of timestamp (ISO-8601), defaults to now (optional)
    db_name = 'db_name_example' # str | Optional database scope (optional)
    branch = 'branch_example' # str | Optional branch scope (optional)
    direction = downstream # str | Diff traversal direction (optional) (default to downstream)
    max_depth = 10 # int |  (optional) (default to 10)
    max_nodes = 5000 # int |  (optional) (default to 5000)
    max_edges = 15000 # int |  (optional) (default to 15000)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Lineage Diff
        api_response = api_instance.get_lineage_diff_api_v1_lineage_diff_get(root, from_as_of, to_as_of=to_as_of, db_name=db_name, branch=branch, direction=direction, max_depth=max_depth, max_nodes=max_nodes, max_edges=max_edges, lang=lang, accept_language=accept_language)
        print("The response of LineageApi->get_lineage_diff_api_v1_lineage_diff_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LineageApi->get_lineage_diff_api_v1_lineage_diff_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **root** | **str**| Root node id or event id | 
 **from_as_of** | **datetime**| Baseline as-of timestamp (ISO-8601) | 
 **to_as_of** | **datetime**| Target as-of timestamp (ISO-8601), defaults to now | [optional] 
 **db_name** | **str**| Optional database scope | [optional] 
 **branch** | **str**| Optional branch scope | [optional] 
 **direction** | **str**| Diff traversal direction | [optional] [default to downstream]
 **max_depth** | **int**|  | [optional] [default to 10]
 **max_nodes** | **int**|  | [optional] [default to 5000]
 **max_edges** | **int**|  | [optional] [default to 15000]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_lineage_graph_api_v1_lineage_graph_get**
> object get_lineage_graph_api_v1_lineage_graph_get(root, db_name=db_name, branch=branch, as_of=as_of, direction=direction, max_depth=max_depth, max_nodes=max_nodes, max_edges=max_edges, lang=lang, accept_language=accept_language)

Get Lineage Graph

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.LineageApi(api_client)
    root = 'root_example' # str | Root node id or event id (e.g. event:<uuid> or <uuid>)
    db_name = 'db_name_example' # str | Optional database scope (recommended) (optional)
    branch = 'branch_example' # str | Optional branch scope (optional)
    as_of = '2013-10-20T19:20:30+01:00' # datetime | Optional as-of timestamp (ISO-8601) (optional)
    direction = both # str | Traversal direction (optional) (default to both)
    max_depth = 5 # int |  (optional) (default to 5)
    max_nodes = 500 # int |  (optional) (default to 500)
    max_edges = 2000 # int |  (optional) (default to 2000)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Lineage Graph
        api_response = api_instance.get_lineage_graph_api_v1_lineage_graph_get(root, db_name=db_name, branch=branch, as_of=as_of, direction=direction, max_depth=max_depth, max_nodes=max_nodes, max_edges=max_edges, lang=lang, accept_language=accept_language)
        print("The response of LineageApi->get_lineage_graph_api_v1_lineage_graph_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LineageApi->get_lineage_graph_api_v1_lineage_graph_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **root** | **str**| Root node id or event id (e.g. event:&lt;uuid&gt; or &lt;uuid&gt;) | 
 **db_name** | **str**| Optional database scope (recommended) | [optional] 
 **branch** | **str**| Optional branch scope | [optional] 
 **as_of** | **datetime**| Optional as-of timestamp (ISO-8601) | [optional] 
 **direction** | **str**| Traversal direction | [optional] [default to both]
 **max_depth** | **int**|  | [optional] [default to 5]
 **max_nodes** | **int**|  | [optional] [default to 500]
 **max_edges** | **int**|  | [optional] [default to 2000]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_lineage_impact_api_v1_lineage_impact_get**
> object get_lineage_impact_api_v1_lineage_impact_get(root, db_name=db_name, branch=branch, as_of=as_of, direction=direction, max_depth=max_depth, artifact_kind=artifact_kind, max_nodes=max_nodes, max_edges=max_edges, lang=lang, accept_language=accept_language)

Get Lineage Impact

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.LineageApi(api_client)
    root = 'root_example' # str | Root node id or event id
    db_name = 'db_name_example' # str | Optional database scope (recommended) (optional)
    branch = 'branch_example' # str | Optional branch scope (optional)
    as_of = '2013-10-20T19:20:30+01:00' # datetime | Optional as-of timestamp (ISO-8601) (optional)
    direction = downstream # str | Impact direction (usually downstream) (optional) (default to downstream)
    max_depth = 10 # int |  (optional) (default to 10)
    artifact_kind = 'artifact_kind_example' # str | Filter by artifact kind (es|s3|graph|...) (optional)
    max_nodes = 2000 # int |  (optional) (default to 2000)
    max_edges = 5000 # int |  (optional) (default to 5000)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Lineage Impact
        api_response = api_instance.get_lineage_impact_api_v1_lineage_impact_get(root, db_name=db_name, branch=branch, as_of=as_of, direction=direction, max_depth=max_depth, artifact_kind=artifact_kind, max_nodes=max_nodes, max_edges=max_edges, lang=lang, accept_language=accept_language)
        print("The response of LineageApi->get_lineage_impact_api_v1_lineage_impact_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LineageApi->get_lineage_impact_api_v1_lineage_impact_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **root** | **str**| Root node id or event id | 
 **db_name** | **str**| Optional database scope (recommended) | [optional] 
 **branch** | **str**| Optional branch scope | [optional] 
 **as_of** | **datetime**| Optional as-of timestamp (ISO-8601) | [optional] 
 **direction** | **str**| Impact direction (usually downstream) | [optional] [default to downstream]
 **max_depth** | **int**|  | [optional] [default to 10]
 **artifact_kind** | **str**| Filter by artifact kind (es|s3|graph|...) | [optional] 
 **max_nodes** | **int**|  | [optional] [default to 2000]
 **max_edges** | **int**|  | [optional] [default to 5000]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_lineage_metrics_api_v1_lineage_metrics_get**
> object get_lineage_metrics_api_v1_lineage_metrics_get(db_name=db_name, branch=branch, window_minutes=window_minutes, lang=lang, accept_language=accept_language)

Get Lineage Metrics

Operational lineage metrics.

- `lineage_lag_seconds`: age of oldest missing-lineage item in the backfill queue
- `missing_lineage_ratio_estimate`: based on EVENT_APPENDED audit count vs recorded lineage edges

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.LineageApi(api_client)
    db_name = 'db_name_example' # str | Database scope (recommended) (optional)
    branch = 'branch_example' # str | Optional branch scope (optional)
    window_minutes = 60 # int | Time window for missing ratio estimate (optional) (default to 60)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Lineage Metrics
        api_response = api_instance.get_lineage_metrics_api_v1_lineage_metrics_get(db_name=db_name, branch=branch, window_minutes=window_minutes, lang=lang, accept_language=accept_language)
        print("The response of LineageApi->get_lineage_metrics_api_v1_lineage_metrics_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LineageApi->get_lineage_metrics_api_v1_lineage_metrics_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**| Database scope (recommended) | [optional] 
 **branch** | **str**| Optional branch scope | [optional] 
 **window_minutes** | **int**| Time window for missing ratio estimate | [optional] [default to 60]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_lineage_out_of_date_api_v1_lineage_out_of_date_get**
> object get_lineage_out_of_date_api_v1_lineage_out_of_date_get(db_name=db_name, branch=branch, artifact_kind=artifact_kind, as_of=as_of, freshness_slo_minutes=freshness_slo_minutes, artifact_limit=artifact_limit, stale_preview_limit=stale_preview_limit, projection_limit=projection_limit, projection_preview_limit=projection_preview_limit, lang=lang, accept_language=accept_language)

Get Lineage Out Of Date

Foundry-style out-of-date diagnostics:
- identify stale artifacts/projections by latest lineage write time
- provide actionable blast-radius remediation hints

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.LineageApi(api_client)
    db_name = 'db_name_example' # str | Database scope (recommended) (optional)
    branch = 'branch_example' # str | Optional branch scope (optional)
    artifact_kind = 'artifact_kind_example' # str | Optional artifact kind filter (es|graph|s3|...) (optional)
    as_of = '2013-10-20T19:20:30+01:00' # datetime | As-of timestamp for freshness evaluation (optional)
    freshness_slo_minutes = 120 # int | Freshness SLO in minutes (optional) (default to 120)
    artifact_limit = 5000 # int | Max artifact rows to evaluate (optional) (default to 5000)
    stale_preview_limit = 200 # int | Max stale artifacts returned (optional) (default to 200)
    projection_limit = 1000 # int | Max projection rows to evaluate (optional) (default to 1000)
    projection_preview_limit = 200 # int | Max projections returned (optional) (default to 200)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Lineage Out Of Date
        api_response = api_instance.get_lineage_out_of_date_api_v1_lineage_out_of_date_get(db_name=db_name, branch=branch, artifact_kind=artifact_kind, as_of=as_of, freshness_slo_minutes=freshness_slo_minutes, artifact_limit=artifact_limit, stale_preview_limit=stale_preview_limit, projection_limit=projection_limit, projection_preview_limit=projection_preview_limit, lang=lang, accept_language=accept_language)
        print("The response of LineageApi->get_lineage_out_of_date_api_v1_lineage_out_of_date_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LineageApi->get_lineage_out_of_date_api_v1_lineage_out_of_date_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**| Database scope (recommended) | [optional] 
 **branch** | **str**| Optional branch scope | [optional] 
 **artifact_kind** | **str**| Optional artifact kind filter (es|graph|s3|...) | [optional] 
 **as_of** | **datetime**| As-of timestamp for freshness evaluation | [optional] 
 **freshness_slo_minutes** | **int**| Freshness SLO in minutes | [optional] [default to 120]
 **artifact_limit** | **int**| Max artifact rows to evaluate | [optional] [default to 5000]
 **stale_preview_limit** | **int**| Max stale artifacts returned | [optional] [default to 200]
 **projection_limit** | **int**| Max projection rows to evaluate | [optional] [default to 1000]
 **projection_preview_limit** | **int**| Max projections returned | [optional] [default to 200]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_lineage_path_api_v1_lineage_path_get**
> object get_lineage_path_api_v1_lineage_path_get(source, target, db_name=db_name, branch=branch, as_of=as_of, direction=direction, max_depth=max_depth, max_nodes=max_nodes, max_edges=max_edges, lang=lang, accept_language=accept_language)

Get Lineage Path

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.LineageApi(api_client)
    source = 'source_example' # str | Path source node id or event id
    target = 'target_example' # str | Path target node id or event id
    db_name = 'db_name_example' # str | Optional database scope (optional)
    branch = 'branch_example' # str | Optional branch scope (optional)
    as_of = '2013-10-20T19:20:30+01:00' # datetime | Optional as-of timestamp (ISO-8601) (optional)
    direction = downstream # str | Traversal direction for path discovery (optional) (default to downstream)
    max_depth = 20 # int |  (optional) (default to 20)
    max_nodes = 5000 # int |  (optional) (default to 5000)
    max_edges = 15000 # int |  (optional) (default to 15000)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Lineage Path
        api_response = api_instance.get_lineage_path_api_v1_lineage_path_get(source, target, db_name=db_name, branch=branch, as_of=as_of, direction=direction, max_depth=max_depth, max_nodes=max_nodes, max_edges=max_edges, lang=lang, accept_language=accept_language)
        print("The response of LineageApi->get_lineage_path_api_v1_lineage_path_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LineageApi->get_lineage_path_api_v1_lineage_path_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **source** | **str**| Path source node id or event id | 
 **target** | **str**| Path target node id or event id | 
 **db_name** | **str**| Optional database scope | [optional] 
 **branch** | **str**| Optional branch scope | [optional] 
 **as_of** | **datetime**| Optional as-of timestamp (ISO-8601) | [optional] 
 **direction** | **str**| Traversal direction for path discovery | [optional] [default to downstream]
 **max_depth** | **int**|  | [optional] [default to 20]
 **max_nodes** | **int**|  | [optional] [default to 5000]
 **max_edges** | **int**|  | [optional] [default to 15000]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_lineage_run_impact_api_v1_lineage_run_impact_get**
> object get_lineage_run_impact_api_v1_lineage_run_impact_get(run_id, db_name=db_name, branch=branch, since=since, until=until, event_limit=event_limit, artifact_preview_limit=artifact_preview_limit, lang=lang, accept_language=accept_language)

Get Lineage Run Impact

Foundry-style run/build impact diagnostics.
Answers: "this run touched what, when, and with what blast radius?"

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.LineageApi(api_client)
    run_id = 'run_id_example' # str | Producer/runtime run id
    db_name = 'db_name_example' # str | Database scope (recommended) (optional)
    branch = 'branch_example' # str | Optional branch scope (optional)
    since = '2013-10-20T19:20:30+01:00' # datetime | Window start (ISO-8601), default now-24h (optional)
    until = '2013-10-20T19:20:30+01:00' # datetime | Window end (ISO-8601), default now (optional)
    event_limit = 5000 # int | Max lineage edges loaded (optional) (default to 5000)
    artifact_preview_limit = 200 # int | Max impacted artifacts returned (optional) (default to 200)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Lineage Run Impact
        api_response = api_instance.get_lineage_run_impact_api_v1_lineage_run_impact_get(run_id, db_name=db_name, branch=branch, since=since, until=until, event_limit=event_limit, artifact_preview_limit=artifact_preview_limit, lang=lang, accept_language=accept_language)
        print("The response of LineageApi->get_lineage_run_impact_api_v1_lineage_run_impact_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LineageApi->get_lineage_run_impact_api_v1_lineage_run_impact_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **run_id** | **str**| Producer/runtime run id | 
 **db_name** | **str**| Database scope (recommended) | [optional] 
 **branch** | **str**| Optional branch scope | [optional] 
 **since** | **datetime**| Window start (ISO-8601), default now-24h | [optional] 
 **until** | **datetime**| Window end (ISO-8601), default now | [optional] 
 **event_limit** | **int**| Max lineage edges loaded | [optional] [default to 5000]
 **artifact_preview_limit** | **int**| Max impacted artifacts returned | [optional] [default to 200]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_lineage_runs_api_v1_lineage_runs_get**
> object get_lineage_runs_api_v1_lineage_runs_get(db_name=db_name, branch=branch, since=since, until=until, edge_type=edge_type, run_limit=run_limit, freshness_slo_minutes=freshness_slo_minutes, include_impact_preview=include_impact_preview, impact_preview_runs_limit=impact_preview_runs_limit, impact_preview_artifacts_limit=impact_preview_artifacts_limit, impact_preview_edge_limit=impact_preview_edge_limit, lang=lang, accept_language=accept_language)

Get Lineage Runs

Foundry-style build/run timeline:
- run windows (first/last activity)
- per-run blast-radius stats
- optional impacted artifact previews

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.LineageApi(api_client)
    db_name = 'db_name_example' # str | Database scope (recommended) (optional)
    branch = 'branch_example' # str | Optional branch scope (optional)
    since = '2013-10-20T19:20:30+01:00' # datetime | Window start (ISO-8601), default now-24h (optional)
    until = '2013-10-20T19:20:30+01:00' # datetime | Window end (ISO-8601), default now (optional)
    edge_type = 'edge_type_example' # str | Optional lineage edge type filter (optional)
    run_limit = 200 # int | Max run/build summaries returned (optional) (default to 200)
    freshness_slo_minutes = 120 # int | Freshness SLO in minutes (optional) (default to 120)
    include_impact_preview = False # bool | Include top impacted artifacts preview per run (optional) (default to False)
    impact_preview_runs_limit = 20 # int | Number of runs to enrich with impact preview (optional) (default to 20)
    impact_preview_artifacts_limit = 20 # int | Max impacted artifacts per run preview (optional) (default to 20)
    impact_preview_edge_limit = 2000 # int | Edges loaded per run for preview (optional) (default to 2000)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Lineage Runs
        api_response = api_instance.get_lineage_runs_api_v1_lineage_runs_get(db_name=db_name, branch=branch, since=since, until=until, edge_type=edge_type, run_limit=run_limit, freshness_slo_minutes=freshness_slo_minutes, include_impact_preview=include_impact_preview, impact_preview_runs_limit=impact_preview_runs_limit, impact_preview_artifacts_limit=impact_preview_artifacts_limit, impact_preview_edge_limit=impact_preview_edge_limit, lang=lang, accept_language=accept_language)
        print("The response of LineageApi->get_lineage_runs_api_v1_lineage_runs_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LineageApi->get_lineage_runs_api_v1_lineage_runs_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**| Database scope (recommended) | [optional] 
 **branch** | **str**| Optional branch scope | [optional] 
 **since** | **datetime**| Window start (ISO-8601), default now-24h | [optional] 
 **until** | **datetime**| Window end (ISO-8601), default now | [optional] 
 **edge_type** | **str**| Optional lineage edge type filter | [optional] 
 **run_limit** | **int**| Max run/build summaries returned | [optional] [default to 200]
 **freshness_slo_minutes** | **int**| Freshness SLO in minutes | [optional] [default to 120]
 **include_impact_preview** | **bool**| Include top impacted artifacts preview per run | [optional] [default to False]
 **impact_preview_runs_limit** | **int**| Number of runs to enrich with impact preview | [optional] [default to 20]
 **impact_preview_artifacts_limit** | **int**| Max impacted artifacts per run preview | [optional] [default to 20]
 **impact_preview_edge_limit** | **int**| Edges loaded per run for preview | [optional] [default to 2000]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_lineage_timeline_api_v1_lineage_timeline_get**
> object get_lineage_timeline_api_v1_lineage_timeline_get(db_name=db_name, branch=branch, since=since, until=until, edge_type=edge_type, projection_name=projection_name, bucket_minutes=bucket_minutes, event_limit=event_limit, event_preview_limit=event_preview_limit, lang=lang, accept_language=accept_language)

Get Lineage Timeline

Foundry-style lineage timeline for operational debugging:
- when lineage events spiked
- which edge/projection/service dominated

### Example


```python
import spice_harvester_sdk
from spice_harvester_sdk.rest import ApiException
from pprint import pprint

# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = spice_harvester_sdk.Configuration(
    host = "http://localhost"
)


# Enter a context with an instance of the API client
with spice_harvester_sdk.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = spice_harvester_sdk.LineageApi(api_client)
    db_name = 'db_name_example' # str | Database scope (recommended) (optional)
    branch = 'branch_example' # str | Optional branch scope (optional)
    since = '2013-10-20T19:20:30+01:00' # datetime | Window start (ISO-8601), default now-24h (optional)
    until = '2013-10-20T19:20:30+01:00' # datetime | Window end (ISO-8601), default now (optional)
    edge_type = 'edge_type_example' # str | Optional lineage edge type filter (optional)
    projection_name = 'projection_name_example' # str | Optional projection filter (optional)
    bucket_minutes = 15 # int | Bucket size in minutes (optional) (default to 15)
    event_limit = 5000 # int | Max events loaded from lineage store (optional) (default to 5000)
    event_preview_limit = 200 # int | Max event rows returned in payload (optional) (default to 200)
    lang = 'lang_example' # str | Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. (optional)
    accept_language = 'en-US,en;q=0.9,ko;q=0.8' # str | Preferred output language for UI-facing fields (fallback when ?lang is not provided). (optional)

    try:
        # Get Lineage Timeline
        api_response = api_instance.get_lineage_timeline_api_v1_lineage_timeline_get(db_name=db_name, branch=branch, since=since, until=until, edge_type=edge_type, projection_name=projection_name, bucket_minutes=bucket_minutes, event_limit=event_limit, event_preview_limit=event_preview_limit, lang=lang, accept_language=accept_language)
        print("The response of LineageApi->get_lineage_timeline_api_v1_lineage_timeline_get:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling LineageApi->get_lineage_timeline_api_v1_lineage_timeline_get: %s\n" % e)
```



### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **db_name** | **str**| Database scope (recommended) | [optional] 
 **branch** | **str**| Optional branch scope | [optional] 
 **since** | **datetime**| Window start (ISO-8601), default now-24h | [optional] 
 **until** | **datetime**| Window end (ISO-8601), default now | [optional] 
 **edge_type** | **str**| Optional lineage edge type filter | [optional] 
 **projection_name** | **str**| Optional projection filter | [optional] 
 **bucket_minutes** | **int**| Bucket size in minutes | [optional] [default to 15]
 **event_limit** | **int**| Max events loaded from lineage store | [optional] [default to 5000]
 **event_preview_limit** | **int**| Max event rows returned in payload | [optional] [default to 200]
 **lang** | **str**| Output language override for UI-facing fields (EN/KR). Overrides Accept-Language. | [optional] 
 **accept_language** | **str**| Preferred output language for UI-facing fields (fallback when ?lang is not provided). | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

