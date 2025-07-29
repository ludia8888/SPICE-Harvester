import React, { useState } from 'react';
import { 
  Button, 
  Card, 
  FormGroup,
  HTMLSelect,
  InputGroup, 
  Intent, 
  TextArea,
  Tag,
  HTMLTable,
  Icon
} from '@blueprintjs/core';
import { useOntologyStore } from '../../../stores/ontology.store';

interface QueryResult {
  data: any[];
  total: number;
  query_time: number;
}

export const QueryBuilder: React.FC = () => {
  const { 
    currentDatabase,
    objectTypes,
    setError,
    clearError
  } = useOntologyStore();

  const [queryType, setQueryType] = useState<'list_classes' | 'find_instances' | 'raw_query'>('list_classes');
  const [selectedClass, setSelectedClass] = useState('');
  const [rawQuery, setRawQuery] = useState('');
  const [limit, setLimit] = useState('10');
  const [isQuerying, setIsQuerying] = useState(false);
  const [queryResult, setQueryResult] = useState<QueryResult | null>(null);

  const handleExecuteQuery = async () => {
    if (!currentDatabase) return;

    setIsQuerying(true);
    clearError('query-builder');

    try {
      let queryData: any = {
        filters: [],
        select: [],
        limit: parseInt(limit) || 10,
        offset: 0
      };

      if (queryType === 'find_instances') {
        if (!selectedClass) {
          setError('query-builder', 'Please select a class to query instances');
          setIsQuerying(false);
          return;
        }
        queryData.class_label = selectedClass;
      } else if (queryType === 'list_classes') {
        // BFF query API requires class_label, so list classes differently
        // For now, skip the query for list_classes
        setQueryResult({
          data: objectTypes.map(obj => ({
            id: obj.id,
            label: obj.label,
            description: obj.description,
            properties_count: obj.properties.length
          })),
          total: objectTypes.length,
          query_time: 0
        });
        setIsQuerying(false);
        return;
      } else if (queryType === 'raw_query' && rawQuery.trim()) {
        queryData.raw_query = rawQuery.trim();
      }

      // Call BFF query API (corrected endpoint)
      const response = await fetch(`http://localhost:8002/api/v1/database/${currentDatabase}/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept-Language': 'ko',
        },
        body: JSON.stringify(queryData),
      });

      if (!response.ok) {
        throw new Error(`Query failed: ${response.statusText}`);
      }

      const result = await response.json();
      setQueryResult(result);

    } catch (error) {
      const message = error instanceof Error ? error.message : 'Query execution failed';
      setError('query-builder', message);
    } finally {
      setIsQuerying(false);
    }
  };

  const renderQueryForm = () => (
    <div className="query-form">
      <FormGroup label="Query Type" labelFor="query-type">
        <HTMLSelect
          id="query-type"
          value={queryType}
          onChange={(e) => setQueryType(e.target.value as any)}
          options={[
            { value: 'list_classes', label: 'List All Classes' },
            { value: 'find_instances', label: 'Find Class Instances' },
            { value: 'raw_query', label: 'Raw Query' }
          ]}
        />
      </FormGroup>

      {queryType === 'find_instances' && (
        <FormGroup label="Select Class" labelFor="class-select">
          <HTMLSelect
            id="class-select"
            value={selectedClass}
            onChange={(e) => setSelectedClass(e.target.value)}
            options={[
              { value: '', label: 'Select a class...' },
              ...objectTypes.map(obj => ({
                value: obj.id,
                label: obj.label || obj.id
              }))
            ]}
          />
        </FormGroup>
      )}

      {queryType === 'raw_query' && (
        <FormGroup label="Raw Query" labelFor="raw-query">
          <TextArea
            id="raw-query"
            value={rawQuery}
            onChange={(e) => setRawQuery(e.target.value)}
            placeholder="Enter WOQL query..."
            rows={6}
            fill
          />
        </FormGroup>
      )}

      <FormGroup label="Limit" labelFor="limit">
        <InputGroup
          id="limit"
          value={limit}
          onChange={(e) => setLimit(e.target.value)}
          type="number"
          min="1"
          max="1000"
        />
      </FormGroup>

      <div className="query-actions">
        <Button
          text="Execute Query"
          icon="search"
          intent={Intent.PRIMARY}
          onClick={handleExecuteQuery}
          disabled={isQuerying || !currentDatabase}
          loading={isQuerying}
        />
      </div>
    </div>
  );

  const renderQueryResults = () => {
    if (!queryResult) return null;

    return (
      <div className="query-results">
        <div className="results-header">
          <h4>Query Results</h4>
          <div className="results-stats">
            <Tag minimal>
              {queryResult.total} results
            </Tag>
            <Tag minimal>
              {queryResult.query_time}ms
            </Tag>
          </div>
        </div>

        {queryResult.data.length === 0 ? (
          <div className="no-results">
            <Icon icon="search" size={32} />
            <p>No results found</p>
          </div>
        ) : (
          <HTMLTable striped className="results-table">
            <thead>
              <tr>
                {Object.keys(queryResult.data[0] || {}).map(key => (
                  <th key={key}>{key}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {queryResult.data.slice(0, 100).map((row, index) => (
                <tr key={index}>
                  {Object.values(row).map((value, cellIndex) => (
                    <td key={cellIndex}>
                      {typeof value === 'object' 
                        ? JSON.stringify(value) 
                        : String(value)
                      }
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </HTMLTable>
        )}
      </div>
    );
  };

  return (
    <div className="query-builder">
      <Card className="query-panel">
        <div className="panel-header">
          <h3>
            <Icon icon="search" />
            Query Builder
          </h3>
        </div>
        
        {renderQueryForm()}
      </Card>

      {queryResult && (
        <Card className="results-panel">
          {renderQueryResults()}
        </Card>
      )}
    </div>
  );
};