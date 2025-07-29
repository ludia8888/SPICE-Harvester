import React, { useState, useEffect } from 'react';
import { 
  Button, 
  Card, 
  FormGroup,
  HTMLSelect,
  Intent, 
  Tag,
  HTMLTable,
  Icon,
  Spinner,
  ProgressBar,
  Alert
} from '@blueprintjs/core';
import clsx from 'clsx';
import { useOntologyStore } from '../../../stores/ontology.store';

interface NetworkHealth {
  score: number;
  grade: string;
  color: string;
  description: string;
}

interface RelationshipAnalysis {
  network_health: NetworkHealth;
  statistics: {
    total_ontologies: number;
    total_relationships: number;
    relationship_types: number;
    total_entities: number;
    average_connections: number;
  };
  quality_metrics: {
    validation_errors: number;
    validation_warnings: number;
    critical_cycles: number;
    total_cycles: number;
    inverse_coverage: string;
  };
  recommendations: Array<{
    priority: 'high' | 'medium' | 'low';
    message: string;
  }>;
}

interface RelationshipPath {
  start: string;
  end: string;
  path: string;
  predicates: string[];
  length: number;
  weight: number;
  confidence: number;
  path_type: string;
}

export const RelationshipManager: React.FC = () => {
  const { 
    currentDatabase,
    objectTypes,
    setError,
    clearError
  } = useOntologyStore();

  const [analysis, setAnalysis] = useState<RelationshipAnalysis | null>(null);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [isLoadingPaths, setIsLoadingPaths] = useState(false);
  
  // Path finder state
  const [startEntity, setStartEntity] = useState('');
  const [endEntity, setEndEntity] = useState('');
  const [maxDepth, setMaxDepth] = useState('3');
  const [pathType, setPathType] = useState('shortest');
  const [foundPaths, setFoundPaths] = useState<RelationshipPath[]>([]);

  useEffect(() => {
    if (currentDatabase) {
      analyzeNetwork();
    }
  }, [currentDatabase]);

  const analyzeNetwork = async () => {
    if (!currentDatabase) return;

    setIsAnalyzing(true);
    clearError('relationship-manager');

    try {
      const response = await fetch(`http://localhost:8002/api/v1/database/${currentDatabase}/relationship-network/analyze`, {
        headers: {
          'Content-Type': 'application/json',
          'Accept-Language': 'ko',
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to analyze network: ${response.statusText}`);
      }

      const result = await response.json();
      setAnalysis(result);

    } catch (error) {
      const message = error instanceof Error ? error.message : 'Network analysis failed';
      setError('relationship-manager', message);
    } finally {
      setIsAnalyzing(false);
    }
  };

  const findRelationshipPaths = async () => {
    if (!currentDatabase || !startEntity) return;

    setIsLoadingPaths(true);
    clearError('path-finder');

    try {
      const params = new URLSearchParams({
        start_entity: startEntity,
        max_depth: maxDepth,
        path_type: pathType
      });

      if (endEntity) {
        params.append('end_entity', endEntity);
      }

      const response = await fetch(`http://localhost:8002/api/v1/database/${currentDatabase}/relationship-paths?${params}`, {
        headers: {
          'Content-Type': 'application/json',
          'Accept-Language': 'ko',
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to find paths: ${response.statusText}`);
      }

      const result = await response.json();
      setFoundPaths(result.paths || []);

    } catch (error) {
      const message = error instanceof Error ? error.message : 'Path finding failed';
      setError('path-finder', message);
    } finally {
      setIsLoadingPaths(false);
    }
  };

  const getHealthColor = (score: number) => {
    if (score >= 90) return Intent.SUCCESS;
    if (score >= 70) return Intent.PRIMARY;
    if (score >= 50) return Intent.WARNING;
    return Intent.DANGER;
  };

  const renderNetworkHealth = () => {
    if (!analysis) return null;

    const { network_health, statistics, quality_metrics } = analysis;

    return (
      <Card className="network-health-card">
        <div className="health-header">
          <h4>
            <Icon icon="graph" />
            Network Health Analysis
          </h4>
          <Button
            icon="refresh"
            text="Refresh"
            onClick={analyzeNetwork}
            loading={isAnalyzing}
            disabled={isAnalyzing}
          />
        </div>

        <div className="health-score">
          <div className="score-display">
            <div className="score-number">
              {network_health.score}
            </div>
            <div className="score-label">
              <Tag intent={getHealthColor(network_health.score)} large>
                {network_health.grade}
              </Tag>
            </div>
          </div>
          <ProgressBar
            value={network_health.score / 100}
            intent={getHealthColor(network_health.score)}
            animate={false}
          />
          <p className="health-description">{network_health.description}</p>
        </div>

        <div className="network-statistics">
          <div className="stats-grid">
            <div className="stat-item">
              <div className="stat-value">{statistics.total_ontologies}</div>
              <div className="stat-label">Ontologies</div>
            </div>
            <div className="stat-item">
              <div className="stat-value">{statistics.total_relationships}</div>
              <div className="stat-label">Relationships</div>
            </div>
            <div className="stat-item">
              <div className="stat-value">{statistics.total_entities}</div>
              <div className="stat-label">Entities</div>
            </div>
            <div className="stat-item">
              <div className="stat-value">{statistics.average_connections}</div>
              <div className="stat-label">Avg Connections</div>
            </div>
          </div>
        </div>

        <div className="quality-metrics">
          <h5>Quality Metrics</h5>
          <div className="metrics-list">
            <div className={clsx('metric-item', { 'has-issues': quality_metrics.validation_errors > 0 })}>
              <Icon icon={quality_metrics.validation_errors > 0 ? 'error' : 'tick'} />
              <span>Validation Errors: {quality_metrics.validation_errors}</span>
            </div>
            <div className={clsx('metric-item', { 'has-warnings': quality_metrics.validation_warnings > 0 })}>
              <Icon icon={quality_metrics.validation_warnings > 0 ? 'warning-sign' : 'tick'} />
              <span>Warnings: {quality_metrics.validation_warnings}</span>
            </div>
            <div className={clsx('metric-item', { 'has-issues': quality_metrics.critical_cycles > 0 })}>
              <Icon icon={quality_metrics.critical_cycles > 0 ? 'error' : 'tick'} />
              <span>Critical Cycles: {quality_metrics.critical_cycles}</span>
            </div>
          </div>
        </div>

        {analysis.recommendations.length > 0 && (
          <div className="recommendations">
            <h5>Recommendations</h5>
            {analysis.recommendations.map((rec, index) => (
              <Alert
                key={index}
                isOpen={true}
                intent={rec.priority === 'high' ? Intent.DANGER : rec.priority === 'medium' ? Intent.WARNING : Intent.NONE}
                icon={rec.priority === 'high' ? 'error' : rec.priority === 'medium' ? 'warning-sign' : 'info-sign'}
              >
                {rec.message}
              </Alert>
            ))}
          </div>
        )}
      </Card>
    );
  };

  const renderPathFinder = () => (
    <Card className="path-finder-card">
      <div className="path-finder-header">
        <h4>
          <Icon icon="path-search" />
          Relationship Path Finder
        </h4>
      </div>

      <div className="path-finder-form">
        <div className="form-row">
          <FormGroup label="Start Entity" labelFor="start-entity">
            <HTMLSelect
              id="start-entity"
              value={startEntity}
              onChange={(e) => setStartEntity(e.target.value)}
              options={[
                { value: '', label: 'Select start entity...' },
                ...objectTypes.map(obj => ({
                  value: obj.id,
                  label: obj.label || obj.id
                }))
              ]}
            />
          </FormGroup>

          <FormGroup label="End Entity (Optional)" labelFor="end-entity">
            <HTMLSelect
              id="end-entity"
              value={endEntity}
              onChange={(e) => setEndEntity(e.target.value)}
              options={[
                { value: '', label: 'Any reachable entity...' },
                ...objectTypes.map(obj => ({
                  value: obj.id,
                  label: obj.label || obj.id
                }))
              ]}
            />
          </FormGroup>
        </div>

        <div className="form-row">
          <FormGroup label="Max Depth" labelFor="max-depth">
            <HTMLSelect
              id="max-depth"
              value={maxDepth}
              onChange={(e) => setMaxDepth(e.target.value)}
              options={[
                { value: '1', label: '1 hop' },
                { value: '2', label: '2 hops' },
                { value: '3', label: '3 hops' },
                { value: '4', label: '4 hops' },
                { value: '5', label: '5 hops' }
              ]}
            />
          </FormGroup>

          <FormGroup label="Path Type" labelFor="path-type">
            <HTMLSelect
              id="path-type"
              value={pathType}
              onChange={(e) => setPathType(e.target.value)}
              options={[
                { value: 'shortest', label: 'Shortest' },
                { value: 'all', label: 'All paths' },
                { value: 'weighted', label: 'Weighted' },
                { value: 'semantic', label: 'Semantic' }
              ]}
            />
          </FormGroup>
        </div>

        <Button
          text="Find Paths"
          icon="search"
          intent={Intent.PRIMARY}
          onClick={findRelationshipPaths}
          disabled={!startEntity || isLoadingPaths}
          loading={isLoadingPaths}
        />
      </div>

      {foundPaths.length > 0 && (
        <div className="found-paths">
          <h5>Found Paths ({foundPaths.length})</h5>
          <HTMLTable striped className="paths-table">
            <thead>
              <tr>
                <th>Path</th>
                <th>Length</th>
                <th>Type</th>
                <th>Confidence</th>
              </tr>
            </thead>
            <tbody>
              {foundPaths.map((path, index) => (
                <tr key={index}>
                  <td>
                    <div className="path-display">
                      {path.path}
                    </div>
                  </td>
                  <td>
                    <Tag minimal>{path.length}</Tag>
                  </td>
                  <td>
                    <Tag minimal>{path.path_type}</Tag>
                  </td>
                  <td>
                    <ProgressBar
                      value={path.confidence}
                      intent={path.confidence > 0.8 ? Intent.SUCCESS : path.confidence > 0.6 ? Intent.WARNING : Intent.DANGER}
                      stripes={false}
                    />
                  </td>
                </tr>
              ))}
            </tbody>
          </HTMLTable>
        </div>
      )}
    </Card>
  );

  if (isAnalyzing && !analysis) {
    return (
      <div className="relationship-manager loading">
        <Spinner size={50} />
        <p>Analyzing relationship network...</p>
      </div>
    );
  }

  return (
    <div className="relationship-manager">
      {renderNetworkHealth()}
      {renderPathFinder()}
    </div>
  );
};