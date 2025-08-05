import React, { useState, useEffect } from 'react';
import {
  Card,
  Classes,
  Button,
  Icon,
  Tag,
  HTMLTable,
  ProgressBar,
  Callout,
  Intent,
  Tooltip,
  Position,
  Tabs,
  Tab,
  NonIdealState,
  Spinner,
  Switch,
  HTMLSelect,
  Collapse,
} from '@blueprintjs/core';
import { useOntologyStore } from '../../../stores/ontology.store';
import { extractDatabaseName } from '../../../utils/database';
import './OntologyMappingPanel.scss';

interface OntologyMappingPanelProps {
  sourceSchema: Array<{
    name: string;
    type: string;
    label?: string;
    description?: string;
  }>;
  targetOntologies: Array<{
    id: string;
    label: string;
    properties: Array<{
      name: string;
      type: string;
      label?: string;
      required?: boolean;
    }>;
  }>;
  sampleData?: any[];
  onMappingComplete: (mappings: MappingResult[]) => void;
  onBack?: () => void;
}

interface MappingCandidate {
  source_field: string;
  target_field: string;
  confidence: number;
  match_type: string;
  reasons: string[];
}

interface MappingResult {
  sourceField: string;
  targetClass: string;
  targetProperty: string;
  confidence: number;
  accepted: boolean;
}

export const OntologyMappingPanel: React.FC<OntologyMappingPanelProps> = ({
  sourceSchema,
  targetOntologies,
  sampleData,
  onMappingComplete,
  onBack,
}) => {
  const { currentDatabase, setError: setGlobalError, clearError: clearGlobalError } = useOntologyStore();
  
  // Helper functions for error handling
  const setError = (context: string, message: string) => {
    setLocalError(message);
    setGlobalError(context, message);
  };
  
  const clearError = (context: string) => {
    setLocalError(null);
    clearGlobalError(context);
  };
  
  const [isLoading, setIsLoading] = useState(false);
  const [selectedTargetClass, setSelectedTargetClass] = useState<string>('');
  const [mappingSuggestions, setMappingSuggestions] = useState<MappingCandidate[]>([]);
  const [acceptedMappings, setAcceptedMappings] = useState<Map<string, MappingResult>>(new Map());
  const [showDetails, setShowDetails] = useState<Map<string, boolean>>(new Map());
  const [autoAcceptThreshold, setAutoAcceptThreshold] = useState(0.8);
  const [showOnlyUnmapped, setShowOnlyUnmapped] = useState(false);
  const [targetSampleData, setTargetSampleData] = useState<any[]>([]);
  const [localError, setLocalError] = useState<string | null>(null);
  
  // Statistics
  const [mappingStats, setMappingStats] = useState({
    totalFields: 0,
    mappedFields: 0,
    highConfidence: 0,
    mediumConfidence: 0,
    lowConfidence: 0,
  });

  useEffect(() => {
    // Set default target class if only one exists
    if (targetOntologies.length === 1) {
      setSelectedTargetClass(targetOntologies[0].id);
    }
  }, [targetOntologies]);

  useEffect(() => {
    // Auto-fetch suggestions when target class is selected
    if (selectedTargetClass) {
      fetchTargetSampleDataAndMappings();
    }
  }, [selectedTargetClass]);

  const fetchTargetSampleDataAndMappings = async () => {
    try {
      // First fetch target sample data
      const samples = await fetchTargetSampleData();
      // Then fetch mapping suggestions with the sample data
      await fetchMappingSuggestions(samples);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to fetch data';
      setError('mapping-panel', message);
      console.error('Error in fetchTargetSampleDataAndMappings:', error);
    }
  };

  const fetchTargetSampleData = async (): Promise<any[]> => {
    if (!selectedTargetClass || !currentDatabase) return [];
    
    try {
      const dbName = extractDatabaseName(currentDatabase);
      if (!dbName) {
        throw new Error('No database selected');
      }
      
      // Fetch sample instances for the selected class
      const response = await fetch(
        `${import.meta.env.VITE_BFF_BASE_URL || 'http://localhost:8002/api/v1'}/database/${dbName}/class/${selectedTargetClass}/instances?limit=200`
      );
      
      if (response.ok) {
        const result = await response.json();
        const samples = result.instances || [];
        setTargetSampleData(samples);
        return samples;
      } else {
        const errorText = await response.text();
        console.warn('Failed to fetch target sample data:', response.status, errorText);
        setTargetSampleData([]);
        // Show warning but don't fail - mapping can still work without target samples
        setError('mapping-panel', `Warning: Could not fetch target sample data (${response.status}). Value distribution matching will be unavailable.`);
        return [];
      }
    } catch (error) {
      console.error('Error fetching target sample data:', error);
      setTargetSampleData([]);
      // Show warning but don't fail - mapping can still work without target samples
      const message = error instanceof Error ? error.message : 'Unknown error';
      setError('mapping-panel', `Warning: Could not fetch target sample data (${message}). Value distribution matching will be unavailable.`);
      return [];
    }
  };

  const fetchMappingSuggestions = async (targetSamples?: any[]) => {
    if (!selectedTargetClass || !currentDatabase) return;
    
    setIsLoading(true);
    clearError('mapping-panel');
    
    try {
      const dbName = extractDatabaseName(currentDatabase);
      if (!dbName) {
        throw new Error('No database selected');
      }
      
      // Find selected target ontology
      const targetOntology = targetOntologies.find(o => o.id === selectedTargetClass);
      if (!targetOntology) {
        throw new Error('Target ontology not found');
      }
      
      // Prepare target schema from ontology properties
      const targetSchema = targetOntology.properties.map(prop => ({
        name: prop.name,
        type: prop.type,
        label: prop.label || prop.name,
      }));
      
      // Call mapping suggestion endpoint
      const response = await fetch(
        `${import.meta.env.VITE_BFF_BASE_URL || 'http://localhost:8002/api/v1'}/database/${dbName}/suggest-mappings`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            source_schema: sourceSchema,
            target_schema: targetSchema,
            sample_data: sampleData,
            target_sample_data: targetSampleData,
          }),
        }
      );
      
      if (!response.ok) {
        const errorText = await response.text();
        let errorMessage = 'Failed to get mapping suggestions';
        try {
          const errorJson = JSON.parse(errorText);
          errorMessage = errorJson.detail || errorJson.message || errorMessage;
        } catch {
          // If not JSON, use the text directly
          errorMessage = errorText || errorMessage;
        }
        throw new Error(errorMessage);
      }
      
      const result = await response.json();
      
      setMappingSuggestions(result.mappings || []);
      
      // Auto-accept high confidence mappings
      const newAcceptedMappings = new Map(acceptedMappings);
      result.mappings.forEach((mapping: MappingCandidate) => {
        if (mapping.confidence >= autoAcceptThreshold) {
          const key = `${mapping.source_field}-${selectedTargetClass}-${mapping.target_field}`;
          if (!newAcceptedMappings.has(key)) {
            newAcceptedMappings.set(key, {
              sourceField: mapping.source_field,
              targetClass: selectedTargetClass,
              targetProperty: mapping.target_field,
              confidence: mapping.confidence,
              accepted: true,
            });
          }
        }
      });
      setAcceptedMappings(newAcceptedMappings);
      
      // Update statistics
      updateMappingStatistics(result);
      
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to get mapping suggestions';
      setError('mapping-panel', message);
    } finally {
      setIsLoading(false);
    }
  };

  const updateMappingStatistics = (result: any) => {
    const stats = result.statistics || {};
    setMappingStats({
      totalFields: stats.total_source_fields || sourceSchema.length,
      mappedFields: stats.mapped_fields || 0,
      highConfidence: stats.high_confidence_mappings || 0,
      mediumConfidence: stats.medium_confidence_mappings || 0,
      lowConfidence: (stats.mapped_fields || 0) - (stats.high_confidence_mappings || 0) - (stats.medium_confidence_mappings || 0),
    });
  };

  const handleAcceptMapping = (mapping: MappingCandidate, accept: boolean) => {
    const key = `${mapping.source_field}-${selectedTargetClass}-${mapping.target_field}`;
    const newAcceptedMappings = new Map(acceptedMappings);
    
    if (accept) {
      newAcceptedMappings.set(key, {
        sourceField: mapping.source_field,
        targetClass: selectedTargetClass,
        targetProperty: mapping.target_field,
        confidence: mapping.confidence,
        accepted: true,
      });
    } else {
      newAcceptedMappings.delete(key);
    }
    
    setAcceptedMappings(newAcceptedMappings);
  };

  const handleManualMapping = (sourceField: string, targetProperty: string) => {
    if (!sourceField || !targetProperty || !selectedTargetClass) {
      setError('mapping-panel', 'Invalid mapping selection');
      return;
    }
    
    const key = `${sourceField}-${selectedTargetClass}-${targetProperty}`;
    const newAcceptedMappings = new Map(acceptedMappings);
    
    // Remove any existing mapping for this source field
    for (const [k, v] of newAcceptedMappings.entries()) {
      if (v.sourceField === sourceField && v.targetClass === selectedTargetClass) {
        newAcceptedMappings.delete(k);
      }
    }
    
    // Add new mapping
    newAcceptedMappings.set(key, {
      sourceField,
      targetClass: selectedTargetClass,
      targetProperty,
      confidence: 1.0, // Manual mappings have full confidence
      accepted: true,
    });
    
    setAcceptedMappings(newAcceptedMappings);
  };

  const toggleDetails = (sourceField: string) => {
    const newShowDetails = new Map(showDetails);
    newShowDetails.set(sourceField, !showDetails.get(sourceField));
    setShowDetails(newShowDetails);
  };

  const proceedWithMappings = () => {
    try {
      const finalMappings = Array.from(acceptedMappings.values());
      
      if (finalMappings.length === 0) {
        setError('mapping-panel', 'No mappings selected. Please accept at least one mapping before proceeding.');
        return;
      }
      
      // Clear any existing errors
      clearError('mapping-panel');
      
      onMappingComplete(finalMappings);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to process mappings';
      setError('mapping-panel', message);
    }
  };

  const getConfidenceIntent = (confidence: number): Intent => {
    if (confidence >= 0.9) return Intent.SUCCESS;
    if (confidence >= 0.7) return Intent.WARNING;
    return Intent.NONE;
  };

  const getMatchTypeIcon = (matchType: string): string => {
    switch (matchType) {
      case 'exact': return 'tick-circle';
      case 'fuzzy': return 'search-around';
      case 'semantic': return 'lightbulb';
      case 'type_based': return 'data-lineage';
      case 'pattern': return 'regex';
      default: return 'help';
    }
  };

  const renderMappingSuggestions = () => {
    const targetOntology = targetOntologies.find(o => o.id === selectedTargetClass);
    if (!targetOntology) {
      return <NonIdealState icon="search" title="Select a target ontology" />;
    }

    const groupedBySource = sourceSchema.map(sourceField => {
      const suggestions = mappingSuggestions.filter(
        m => m.source_field === sourceField.name
      );
      const hasAcceptedMapping = Array.from(acceptedMappings.values()).some(
        m => m.sourceField === sourceField.name && m.targetClass === selectedTargetClass
      );
      
      return {
        sourceField,
        suggestions,
        hasAcceptedMapping,
      };
    });

    const filteredGroups = showOnlyUnmapped 
      ? groupedBySource.filter(g => !g.hasAcceptedMapping)
      : groupedBySource;

    return (
      <div className="mapping-suggestions">
        {filteredGroups.map(({ sourceField, suggestions, hasAcceptedMapping }) => {
          const isExpanded = showDetails.get(sourceField.name) || false;
          
          return (
            <Card key={sourceField.name} className="source-field-card" interactive>
              <div className="field-header">
                <div className="field-info">
                  <h4>
                    <Icon icon="dot" />
                    {sourceField.name}
                    <Tag minimal>{sourceField.type}</Tag>
                    {hasAcceptedMapping && (
                      <Tag intent={Intent.SUCCESS} icon="tick" minimal>
                        Mapped
                      </Tag>
                    )}
                  </h4>
                </div>
                <Button
                  icon={isExpanded ? 'chevron-up' : 'chevron-down'}
                  minimal
                  onClick={() => toggleDetails(sourceField.name)}
                />
              </div>

              <Collapse isOpen={isExpanded}>
                <div className="mapping-content">
                  {suggestions.length > 0 ? (
                    <div className="suggestions-list">
                      <h5>Suggested Mappings:</h5>
                      {suggestions.map((suggestion, idx) => {
                        const key = `${suggestion.source_field}-${selectedTargetClass}-${suggestion.target_field}`;
                        const isAccepted = acceptedMappings.has(key);
                        
                        return (
                          <div key={idx} className="suggestion-item">
                            <div className="suggestion-header">
                              <div className="suggestion-info">
                                <Icon icon={getMatchTypeIcon(suggestion.match_type)} />
                                <strong>{suggestion.target_field}</strong>
                                <ProgressBar
                                  value={suggestion.confidence}
                                  intent={getConfidenceIntent(suggestion.confidence)}
                                  stripes={false}
                                  className="confidence-bar"
                                />
                                <Tag minimal intent={getConfidenceIntent(suggestion.confidence)}>
                                  {(suggestion.confidence * 100).toFixed(0)}%
                                </Tag>
                              </div>
                              <Switch
                                checked={isAccepted}
                                onChange={() => handleAcceptMapping(suggestion, !isAccepted)}
                                alignIndicator="right"
                              />
                            </div>
                            <div className="suggestion-reasons">
                              {suggestion.reasons.map((reason, i) => (
                                <Tag key={i} minimal small>
                                  {reason}
                                </Tag>
                              ))}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  ) : (
                    <Callout intent={Intent.WARNING} icon="info-sign">
                      No automatic suggestions found for this field.
                    </Callout>
                  )}

                  <div className="manual-mapping">
                    <h5>Manual Mapping:</h5>
                    <HTMLSelect
                      fill
                      onChange={(e) => {
                        if (e.target.value) {
                          handleManualMapping(sourceField.name, e.target.value);
                        }
                      }}
                      value=""
                    >
                      <option value="">Select target property...</option>
                      {targetOntology.properties.map(prop => (
                        <option key={prop.name} value={prop.name}>
                          {prop.label || prop.name} ({prop.type})
                        </option>
                      ))}
                    </HTMLSelect>
                  </div>
                </div>
              </Collapse>
            </Card>
          );
        })}
      </div>
    );
  };

  const renderStatistics = () => {
    const acceptedCount = acceptedMappings.size;
    const completionRate = mappingStats.totalFields > 0 
      ? (acceptedCount / mappingStats.totalFields) * 100 
      : 0;

    return (
      <div className="mapping-statistics">
        <Card>
          <h3>Mapping Progress</h3>
          <ProgressBar
            value={completionRate / 100}
            intent={completionRate >= 80 ? Intent.SUCCESS : Intent.WARNING}
            stripes={false}
          />
          <p>{completionRate.toFixed(0)}% Complete ({acceptedCount}/{mappingStats.totalFields} fields)</p>
          
          <div className="stats-grid">
            <div className="stat">
              <Icon icon="tick-circle" intent={Intent.SUCCESS} />
              <span>{mappingStats.highConfidence} High Confidence</span>
            </div>
            <div className="stat">
              <Icon icon="warning-sign" intent={Intent.WARNING} />
              <span>{mappingStats.mediumConfidence} Medium Confidence</span>
            </div>
            <div className="stat">
              <Icon icon="help" />
              <span>{mappingStats.totalFields - acceptedCount} Unmapped</span>
            </div>
          </div>
        </Card>

        <Card>
          <h3>Settings</h3>
          <div className="settings-item">
            <label>Auto-accept threshold:</label>
            <HTMLSelect
              value={autoAcceptThreshold}
              onChange={(e) => setAutoAcceptThreshold(parseFloat(e.target.value))}
            >
              <option value="0.9">90% and above</option>
              <option value="0.8">80% and above</option>
              <option value="0.7">70% and above</option>
              <option value="1.0">Disabled</option>
            </HTMLSelect>
          </div>
          <div className="settings-item">
            <Switch
              checked={showOnlyUnmapped}
              label="Show only unmapped fields"
              onChange={() => setShowOnlyUnmapped(!showOnlyUnmapped)}
            />
          </div>
        </Card>
      </div>
    );
  };

  return (
    <div className="ontology-mapping-panel">
      <div className="panel-header">
        <h2>Map Your Data to Ontology</h2>
        <div className="header-actions">
          <Callout intent={Intent.PRIMARY} icon="info-sign">
            Review and accept the mapping suggestions. You can also manually map fields that weren't automatically matched.
          </Callout>
        </div>
      </div>

      {/* Error Display */}
      {localError && (
        <Callout 
          intent={localError.includes('Warning') ? Intent.WARNING : Intent.DANGER}
          icon={localError.includes('Warning') ? 'warning-sign' : 'error'}
          style={{ marginBottom: '20px' }}
          onDismiss={() => {
            setLocalError(null);
            clearGlobalError('mapping-panel');
          }}
        >
          {localError}
        </Callout>
      )}

      <div className="mapping-content-wrapper">
        <div className="main-content">
          <Card className="target-selector">
            <h3>Target Ontology Class</h3>
            <HTMLSelect
              fill
              large
              value={selectedTargetClass}
              onChange={(e) => setSelectedTargetClass(e.target.value)}
              disabled={isLoading}
            >
              <option value="">Select target class...</option>
              {targetOntologies.map(ontology => (
                <option key={ontology.id} value={ontology.id}>
                  {ontology.label} ({ontology.properties.length} properties)
                </option>
              ))}
            </HTMLSelect>
          </Card>

          {isLoading ? (
            <NonIdealState
              icon={<Spinner />}
              title="Analyzing mappings..."
              description="Our AI is finding the best matches for your fields"
            />
          ) : (
            renderMappingSuggestions()
          )}
        </div>

        <div className="sidebar">
          {renderStatistics()}
        </div>
      </div>

      <div className="panel-footer">
        <div className="footer-actions">
          {onBack && (
            <Button
              text="Back"
              icon="arrow-left"
              onClick={onBack}
            />
          )}
          <Button
            text={`Proceed with ${acceptedMappings.size} Mappings`}
            icon="arrow-right"
            intent={Intent.PRIMARY}
            onClick={proceedWithMappings}
            disabled={acceptedMappings.size === 0}
          />
        </div>
      </div>
    </div>
  );
};