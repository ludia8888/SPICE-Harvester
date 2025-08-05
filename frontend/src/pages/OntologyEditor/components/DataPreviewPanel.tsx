import React, { useState, useEffect } from 'react';
import {
  Card,
  Classes,
  Button,
  Icon,
  InputGroup,
  Tag,
  Spinner,
  HTMLTable,
  NonIdealState,
  Collapse,
  Tooltip,
  Position,
  Intent,
  Switch,
  Divider,
} from '@blueprintjs/core';
import { Bar, Pie } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip as ChartTooltip,
  Legend,
  ArcElement,
} from 'chart.js';

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  ChartTooltip,
  Legend,
  ArcElement
);
import { useOntologyStore } from '../../../stores/ontology.store';
import { extractDatabaseName } from '../../../utils/database';
import './DataPreviewPanel.scss';

interface DataPreviewPanelProps {
  objectTypeId: string;
  isOpen: boolean;
  onClose: () => void;
}

interface PreviewData {
  headers: string[];
  rows: any[][];
  totalCount: number;
}

export const DataPreviewPanel: React.FC<DataPreviewPanelProps> = ({
  objectTypeId,
  isOpen,
  onClose,
}) => {
  const { objectTypes, currentDatabase } = useOntologyStore();
  const [isLoading, setIsLoading] = useState(false);
  const [previewData, setPreviewData] = useState<PreviewData | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [isExpanded, setIsExpanded] = useState(true);
  const [page, setPage] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [showInspector, setShowInspector] = useState(false);
  const [selectedInstance, setSelectedInstance] = useState<any>(null);
  const pageSize = 10;

  const selectedObject = objectTypes.find(obj => obj.id === objectTypeId);

  useEffect(() => {
    if (isOpen && objectTypeId) {
      loadPreviewData();
    }
  }, [isOpen, objectTypeId, page]);

  const loadPreviewData = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const dbName = extractDatabaseName(currentDatabase);
      if (!dbName) {
        throw new Error('No database selected');
      }

      // Fetch actual instance data from the API
      const response = await fetch(
        `${import.meta.env.VITE_BFF_BASE_URL || 'http://localhost:8002/api/v1'}/database/${dbName}/class/${objectTypeId}/instances?` +
        new URLSearchParams({
          limit: pageSize.toString(),
          offset: (page * pageSize).toString(),
          ...(searchQuery && { search: searchQuery })
        }),
        {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
          },
        }
      );

      if (!response.ok) {
        throw new Error(`Failed to fetch instances: ${response.status}`);
      }

      const result = await response.json();
      
      // Transform the API response to match our PreviewData format
      const instances = result.instances || [];
      const headers = selectedObject?.properties.map(p => p.name) || [];
      
      // Convert instances to rows format
      const rows = instances.map((instance: any) => {
        return headers.map(header => {
          const value = instance[header];
          // Handle null/undefined values
          if (value === null || value === undefined) {
            return null;
          }
          // Handle different data types
          const property = selectedObject?.properties.find(p => p.name === header);
          if (property) {
            switch (property.type) {
              case 'xsd:boolean':
              case 'boolean':
                return typeof value === 'boolean' ? value : value === 'true';
              case 'xsd:integer':
              case 'integer':
                return parseInt(value, 10);
              case 'xsd:decimal':
              case 'decimal':
                return parseFloat(value);
              case 'xsd:date':
              case 'date':
                // Format date if it's an ISO string
                if (typeof value === 'string' && value.includes('T')) {
                  return value.split('T')[0];
                }
                return value;
              default:
                return value;
            }
          }
          return value;
        });
      });

      const previewData: PreviewData = {
        headers,
        rows,
        totalCount: result.total || instances.length
      };
      
      setPreviewData(previewData);
    } catch (error) {
      console.error('Failed to load preview data:', error);
      const errorMessage = error instanceof Error ? error.message : 'Failed to load data';
      setError(errorMessage);
      setPreviewData({
        headers: selectedObject?.properties.map(p => p.name) || [],
        rows: [],
        totalCount: 0
      });
    } finally {
      setIsLoading(false);
    }
  };

  const handleSearch = () => {
    setPage(0);
    loadPreviewData();
  };

  const formatCellValue = (value: any, type?: string): string => {
    if (value === null || value === undefined) return '-';
    
    // Remove xsd: prefix if present
    const cleanType = type?.replace('xsd:', '') || '';
    
    switch (cleanType) {
      case 'boolean':
        return value ? '✓' : '✗';
      case 'date':
      case 'dateTime':
        if (typeof value === 'string') {
          try {
            return new Date(value).toLocaleDateString();
          } catch {
            return value;
          }
        }
        return value instanceof Date ? value.toLocaleDateString() : String(value);
      case 'decimal':
      case 'float':
      case 'double':
        return typeof value === 'number' ? value.toFixed(2) : String(value);
      case 'integer':
      case 'int':
      case 'long':
        return String(value);
      default:
        return String(value);
    }
  };

  const renderInstanceInspector = () => {
    if (!selectedInstance || !selectedObject) {
      return (
        <Card className="inspector-placeholder">
          <NonIdealState
            icon="selection"
            title="Select an Instance"
            description="Click on a row in the table to inspect instance details"
          />
        </Card>
      );
    }

    // Calculate value distribution for each property
    const getValueDistribution = (propertyName: string) => {
      if (!previewData) return [];
      
      const headerIndex = previewData.headers.indexOf(propertyName);
      if (headerIndex === -1) return [];
      
      const values = previewData.rows.map(row => row[headerIndex]).filter(v => v !== null && v !== undefined);
      const distribution = values.reduce((acc, value) => {
        const key = String(value);
        acc[key] = (acc[key] || 0) + 1;
        return acc;
      }, {} as Record<string, number>);
      
      return Object.entries(distribution)
        .sort(([,a], [,b]) => b - a)
        .slice(0, 10)
        .map(([value, count]) => ({ value, count, percentage: (count / values.length) * 100 }));
    };

    return (
      <Card className="instance-inspector">
        <div className="inspector-header">
          <h4>
            <Icon icon="info-sign" />
            Instance Details
          </h4>
          <Button
            icon="cross"
            minimal
            small
            onClick={() => setSelectedInstance(null)}
          />
        </div>
        
        <Divider />
        
        <div className="inspector-content">
          {selectedObject.properties.map((property, idx) => {
            const value = selectedInstance[property.name];
            const distribution = getValueDistribution(property.name);
            
            return (
              <div key={property.name} className="property-detail">
                <div className="property-header">
                  <strong>{property.name}</strong>
                  <Tag minimal>{property.type?.replace('xsd:', '') || 'unknown'}</Tag>
                </div>
                
                <div className="property-value">
                  <Tag intent={value !== null && value !== undefined ? Intent.PRIMARY : Intent.NONE}>
                    {formatCellValue(value, property.type)}
                  </Tag>
                </div>
                
                {distribution.length > 1 && (
                  <div className="value-distribution">
                    <h5>Value Distribution</h5>
                    <div className="distribution-chart">
                      <Bar
                        data={{
                          labels: distribution.map(d => d.value.length > 20 ? d.value.substring(0, 20) + '...' : d.value),
                          datasets: [{
                            label: 'Count',
                            data: distribution.map(d => d.count),
                            backgroundColor: 'rgba(72, 175, 240, 0.6)',
                            borderColor: 'rgba(72, 175, 240, 1)',
                            borderWidth: 1,
                          }],
                        }}
                        options={{
                          responsive: true,
                          maintainAspectRatio: false,
                          plugins: {
                            legend: { display: false },
                            tooltip: {
                              callbacks: {
                                label: (context) => {
                                  const item = distribution[context.dataIndex];
                                  return `${item.count} (${item.percentage.toFixed(1)}%)`;
                                }
                              }
                            }
                          },
                          scales: {
                            y: { beginAtZero: true },
                            x: { 
                              ticks: { 
                                maxRotation: 45,
                                minRotation: 0 
                              }
                            }
                          },
                        }}
                        height={120}
                      />
                    </div>
                    <div className="distribution-stats">
                      <small>
                        {distribution.length} unique values out of {previewData?.rows.length || 0} total
                      </small>
                    </div>
                  </div>
                )}
                
                {idx < selectedObject.properties.length - 1 && <Divider />}
              </div>
            );
          })}
        </div>
      </Card>
    );
  };

  if (!isOpen) return null;

  return (
    <div className={`data-preview-panel ${isExpanded ? 'expanded' : 'collapsed'}`}>
      <Card className="preview-container" elevation={3}>
        {/* Header */}
        <div className="preview-header">
          <div className="header-left">
            <Icon icon="database" size={18} />
            <h4>Ontology Data: {selectedObject?.label}</h4>
            {previewData && previewData.totalCount > 0 ? (
              <Tag minimal intent={Intent.PRIMARY}>
                {previewData.totalCount} records
              </Tag>
            ) : (
              <Tag minimal intent={Intent.NONE}>
                No data imported yet
              </Tag>
            )}
          </div>
          
          <div className="header-actions">
            <InputGroup
              leftIcon="search"
              placeholder="Search data..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
              small
              className="search-input"
            />
            
            <Switch
              checked={showInspector}
              label="Inspector"
              onChange={() => {
                setShowInspector(!showInspector);
                if (!showInspector) {
                  setSelectedInstance(null); // Clear selection when turning off inspector
                }
              }}
              style={{ margin: '0 8px' }}
            />
            
            <Tooltip content={isExpanded ? "Collapse" : "Expand"} position={Position.TOP}>
              <Button
                icon={isExpanded ? "chevron-down" : "chevron-up"}
                minimal
                small
                onClick={() => setIsExpanded(!isExpanded)}
              />
            </Tooltip>
            
            <Tooltip content="Close" position={Position.TOP}>
              <Button
                icon="cross"
                minimal
                small
                onClick={onClose}
              />
            </Tooltip>
          </div>
        </div>

        {/* Content */}
        <Collapse isOpen={isExpanded}>
          <div className="preview-content">
            {isLoading ? (
              <div className="loading-state">
                <Spinner size={30} />
                <p>Loading data preview...</p>
              </div>
            ) : error ? (
              <NonIdealState
                icon="error"
                title="Failed to Load Data"
                description={error}
                action={
                  <Button
                    text="Retry"
                    icon="refresh"
                    onClick={loadPreviewData}
                  />
                }
              />
            ) : previewData && previewData.rows.length > 0 ? (
              <>
                <div className={`preview-layout ${showInspector ? 'with-inspector' : 'table-only'}`}>
                  <div className="table-section">
                    <div className="table-wrapper">
                  <HTMLTable
                    striped
                    interactive
                    className="preview-table"
                  >
                    <thead>
                      <tr>
                        {previewData.headers.map((header, idx) => {
                          const property = selectedObject?.properties.find(p => p.name === header);
                          return (
                            <th key={idx}>
                              <div className="header-cell">
                                <span>{header}</span>
                                <Tag minimal className="type-tag">
                                  {property?.type?.replace('xsd:', '') || 'unknown'}
                                </Tag>
                              </div>
                            </th>
                          );
                        })}
                      </tr>
                    </thead>
                    <tbody>
                      {previewData.rows.map((row, rowIdx) => {
                        const isSelected = selectedInstance && selectedInstance._rowIndex === rowIdx;
                        return (
                          <tr 
                            key={rowIdx}
                            className={`${isSelected ? 'selected-row' : ''} ${showInspector ? 'clickable-row' : ''}`}
                            onClick={() => {
                              if (showInspector) {
                                const instanceData = previewData.headers.reduce((acc, header, idx) => {
                                  acc[header] = row[idx];
                                  return acc;
                                }, {} as any);
                                instanceData._rowIndex = rowIdx;
                                setSelectedInstance(instanceData);
                              }
                            }}
                            style={{
                              cursor: showInspector ? 'pointer' : 'default',
                              backgroundColor: isSelected ? 'rgba(19, 124, 189, 0.15)' : 'transparent'
                            }}
                          >
                            {row.map((cell, cellIdx) => {
                              const property = selectedObject?.properties[cellIdx];
                              return (
                                <td key={cellIdx}>
                                  {formatCellValue(cell, property?.type)}
                                </td>
                              );
                            })}
                          </tr>
                        );
                      })}
                    </tbody>
                  </HTMLTable>
                    </div>

                    {/* Pagination */}
                    <div className="preview-footer">
                  <div className="pagination">
                    <Button
                      icon="chevron-left"
                      minimal
                      small
                      disabled={page === 0}
                      onClick={() => setPage(Math.max(0, page - 1))}
                    />
                    <span className="page-info">
                      Page {page + 1} of {Math.ceil(previewData.totalCount / pageSize)}
                    </span>
                    <Button
                      icon="chevron-right"
                      minimal
                      small
                      disabled={(page + 1) * pageSize >= previewData.totalCount}
                      onClick={() => setPage(page + 1)}
                    />
                  </div>
                  
                  <Button
                    text="Export Data"
                    icon="export"
                    minimal
                    small
                  />
                    </div>
                  </div>

                  {/* Inspector Panel */}
                  {showInspector && (
                    <div className="inspector-section">
                      {renderInstanceInspector()}
                    </div>
                  )}
                </div>
              </>
            ) : (
              <NonIdealState
                icon={searchQuery ? "search" : "import"}
                title={searchQuery ? "No Data Found" : "Ready to Import Data"}
                description={
                  searchQuery 
                    ? `No results found for "${searchQuery}" in ontology data`
                    : `This ontology class exists but has no instance data yet. Import CSV data to populate it.`
                }
                action={
                  searchQuery ? (
                    <Button
                      text="Clear Search"
                      onClick={() => {
                        setSearchQuery('');
                        loadPreviewData();
                      }}
                    />
                  ) : (
                    <div style={{ display: 'flex', gap: '8px', justifyContent: 'center' }}>
                      <Button
                        text="Import CSV Data"
                        icon="import"
                        intent={Intent.PRIMARY}
                        onClick={() => {
                          // TODO: Open data connector or show import guide
                          alert('Data import functionality will open the Data Connector dialog');
                        }}
                      />
                      <Button
                        text="Learn More"
                        icon="help"
                        onClick={() => {
                          alert('Guide: Use the Data Connector to import CSV files into this ontology class');
                        }}
                      />
                    </div>
                  )
                }
              />
            )}
          </div>
        </Collapse>
      </Card>
    </div>
  );
};