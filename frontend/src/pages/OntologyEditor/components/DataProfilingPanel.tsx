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
  Dialog,
  FormGroup,
  HTMLSelect,
  TextArea,
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
import './DataProfilingPanel.scss';

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

interface DataProfilingPanelProps {
  data: {
    headers: string[];
    rows: any[];
  };
  analysisResult: {
    analysis_summary?: {
      total_columns: number;
      high_confidence_columns: number;
      average_confidence: number;
    };
    detailed_analysis?: {
      columns: Array<{
        column_name: string;
        inferred_type: {
          type: string;
          confidence: number;
          reason?: string;
          metadata?: any;
        };
        non_empty_count: number;
        unique_count: number;
        null_count: number;
        sample_values: any[];
        distribution?: Record<string, number>;
        statistics?: {
          min?: number;
          max?: number;
          mean?: number;
          median?: number;
          std_dev?: number;
        };
      }>;
    };
    suggested_schema?: any;
  };
  onTypeChange?: (columnIndex: number, newType: string) => void;
  onMappingChange?: (columnIndex: number, ontologyProperty: string) => void;
  onProceed?: () => void;
}

interface ColumnProfile {
  name: string;
  inferredType: string;
  confidence: number;
  nullCount: number;
  nullRatio: number;
  uniqueCount: number;
  uniqueRatio: number;
  sampleValues: any[];
  distribution?: Record<string, number>;
  statistics?: {
    min?: number;
    max?: number;
    mean?: number;
    median?: number;
    stdDev?: number;
  };
}

export const DataProfilingPanel: React.FC<DataProfilingPanelProps> = ({
  data,
  analysisResult,
  onTypeChange,
  onMappingChange,
  onProceed,
}) => {
  const [columnProfiles, setColumnProfiles] = useState<ColumnProfile[]>([]);
  const [selectedColumn, setSelectedColumn] = useState<number>(0);
  const [isProfiled, setIsProfiled] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);
  const [typeEditorOpen, setTypeEditorOpen] = useState(false);
  const [editingColumn, setEditingColumn] = useState<number | null>(null);
  const [editedType, setEditedType] = useState<string>('');
  const [editNotes, setEditNotes] = useState<string>('');

  useEffect(() => {
    if (data && data.rows.length > 0 && analysisResult?.detailed_analysis) {
      processBackendAnalysis();
    }
  }, [data, analysisResult]);

  const processBackendAnalysis = () => {
    setIsProcessing(true);
    
    const analysis = analysisResult.detailed_analysis;
    if (!analysis || !analysis.columns) {
      setIsProcessing(false);
      return;
    }

    const profiles: ColumnProfile[] = analysis.columns.map((col: any) => {
      // Find the correct column index by name
      const columnIndex = data.headers.findIndex(h => h === col.column_name);
      if (columnIndex === -1) {
        console.warn(`Column ${col.column_name} not found in headers`);
        return null;
      }
      
      // Get values for this specific column
      const values = data.rows.map(row => row[col.column_name]);
      const nonNullValues = values.filter(v => v !== null && v !== undefined && v !== '');
      const uniqueValues = new Set(nonNullValues);
      
      // Calculate distribution for categorical data if not provided by backend
      let distribution: Record<string, number> | undefined = col.distribution;
      if (!distribution && uniqueValues.size < 20) {
        distribution = {};
        nonNullValues.forEach(value => {
          const key = String(value);
          distribution![key] = (distribution![key] || 0) + 1;
        });
      }

      return {
        name: col.column_name,
        inferredType: col.inferred_type.type,
        confidence: col.inferred_type.confidence,
        nullCount: col.null_count,
        nullRatio: values.length > 0 ? col.null_count / values.length : 0,
        uniqueCount: col.unique_count,
        uniqueRatio: values.length > 0 ? col.unique_count / values.length : 0,
        sampleValues: col.sample_values || [],
        distribution: distribution,
        statistics: col.statistics,
      };
    }).filter(p => p !== null);

    setColumnProfiles(profiles);
    setIsProfiled(true);
    setIsProcessing(false);
  };

  // Helper functions for statistics calculation
  // These are still needed for display purposes even though type inference is done by backend

  // Format type names from backend to user-friendly names
  const formatTypeName = (type: string): string => {
    // Convert xsd:string to string, xsd:integer to integer, etc.
    if (type.startsWith('xsd:')) {
      return type.substring(4);
    }
    return type;
  };

  const renderColumnOverview = () => {
    if (columnProfiles.length === 0) {
      return (
        <NonIdealState
          icon="database"
          title="No data to profile"
        />
      );
    }

    return (
      <div className="column-overview">
        <HTMLTable striped interactive className="column-table">
          <thead>
            <tr>
              <th>Column Name</th>
              <th>Inferred Type</th>
              <th>Confidence</th>
              <th>Null %</th>
              <th>Unique %</th>
              <th>Sample Values</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {columnProfiles.map((profile, index) => (
              <tr
                key={index}
                className={selectedColumn === index ? 'selected' : ''}
                onClick={() => setSelectedColumn(index)}
              >
                <td>
                  <strong>{profile.name}</strong>
                </td>
                <td>
                  <Tag intent={profile.confidence > 0.9 ? Intent.SUCCESS : Intent.WARNING}>
                    {formatTypeName(profile.inferredType)}
                  </Tag>
                </td>
                <td>
                  <ProgressBar
                    value={profile.confidence}
                    intent={profile.confidence > 0.9 ? Intent.SUCCESS : Intent.WARNING}
                    stripes={false}
                  />
                  <small>{(profile.confidence * 100).toFixed(0)}%</small>
                </td>
                <td>
                  <Tag minimal intent={profile.nullRatio > 0.1 ? Intent.WARNING : Intent.NONE}>
                    {(profile.nullRatio * 100).toFixed(1)}%
                  </Tag>
                </td>
                <td>
                  <Tag minimal intent={profile.uniqueRatio > 0.95 ? Intent.PRIMARY : Intent.NONE}>
                    {(profile.uniqueRatio * 100).toFixed(1)}%
                  </Tag>
                </td>
                <td className="sample-values">
                  {profile.sampleValues.slice(0, 3).map((value, i) => (
                    <Tag key={i} minimal>
                      {String(value).length > 20
                        ? String(value).substring(0, 20) + '...'
                        : String(value)}
                    </Tag>
                  ))}
                  {profile.sampleValues.length > 3 && (
                    <Tag minimal>+{profile.sampleValues.length - 3} more</Tag>
                  )}
                </td>
                <td>
                  <Button
                    icon="edit"
                    minimal
                    small
                    onClick={(e) => {
                      e.stopPropagation();
                      setEditingColumn(index);
                      setEditedType(profile.inferredType);
                      setEditNotes('');
                      setTypeEditorOpen(true);
                    }}
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </HTMLTable>
      </div>
    );
  };

  const renderColumnDetails = () => {
    if (selectedColumn >= columnProfiles.length) {
      return null;
    }

    const profile = columnProfiles[selectedColumn];

    return (
      <div className="column-details">
        <Card className="details-card">
          <h3>{profile.name}</h3>
          
          <div className="type-section">
            <h4>Data Type</h4>
            <div className="type-info">
              <Tag large intent={profile.confidence > 0.9 ? Intent.SUCCESS : Intent.WARNING}>
                {formatTypeName(profile.inferredType)}
              </Tag>
              <span className="confidence">
                Confidence: {(profile.confidence * 100).toFixed(0)}%
              </span>
            </div>
          </div>

          <div className="statistics-section">
            <h4>Statistics</h4>
            <div className="stat-grid">
              <div className="stat">
                <span className="label">Total Records</span>
                <span className="value">{data.rows.length}</span>
              </div>
              <div className="stat">
                <span className="label">Null Values</span>
                <span className="value">
                  {profile.nullCount} ({(profile.nullRatio * 100).toFixed(1)}%)
                </span>
              </div>
              <div className="stat">
                <span className="label">Unique Values</span>
                <span className="value">
                  {profile.uniqueCount} ({(profile.uniqueRatio * 100).toFixed(1)}%)
                </span>
              </div>
              {profile.statistics && (
                <>
                  <div className="stat">
                    <span className="label">Min</span>
                    <span className="value">{profile.statistics.min?.toFixed(2)}</span>
                  </div>
                  <div className="stat">
                    <span className="label">Max</span>
                    <span className="value">{profile.statistics.max?.toFixed(2)}</span>
                  </div>
                  <div className="stat">
                    <span className="label">Mean</span>
                    <span className="value">{profile.statistics.mean?.toFixed(2)}</span>
                  </div>
                </>
              )}
            </div>
          </div>

          {profile.distribution && (
            <div className="distribution-section">
              <h4>Value Distribution</h4>
              <div className="chart-container">
                <Bar
                  data={{
                    labels: Object.keys(profile.distribution).slice(0, 10),
                    datasets: [{
                      label: 'Count',
                      data: Object.values(profile.distribution).slice(0, 10),
                      backgroundColor: 'rgba(72, 175, 240, 0.6)',
                      borderColor: 'rgba(72, 175, 240, 1)',
                      borderWidth: 1,
                    }],
                  }}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                      legend: {
                        display: false,
                      },
                    },
                    scales: {
                      y: {
                        beginAtZero: true,
                      },
                    },
                  }}
                />
              </div>
            </div>
          )}

          <div className="sample-section">
            <h4>Sample Values</h4>
            <div className="sample-grid">
              {profile.sampleValues.map((value, i) => (
                <Tag key={i} minimal large>
                  {String(value)}
                </Tag>
              ))}
            </div>
          </div>
        </Card>
      </div>
    );
  };

  return (
    <div className="data-profiling-panel">
      <div className="panel-header">
        <h2>Data Profiling Results</h2>
        <div className="header-actions">
          <Callout intent={Intent.PRIMARY} icon="info-sign">
            {analysisResult.analysis_summary ? (
              <>
                Our AI has analyzed your data: {analysisResult.analysis_summary.high_confidence_columns}/{analysisResult.analysis_summary.total_columns} columns 
                with high confidence type detection (avg confidence: {(analysisResult.analysis_summary.average_confidence * 100).toFixed(0)}%).
              </>
            ) : (
              'Our AI has analyzed your data and detected patterns. Review the inferred types and make adjustments if needed.'
            )}
          </Callout>
          <Button
            text="Proceed to Mapping"
            icon="arrow-right"
            intent={Intent.PRIMARY}
            onClick={onProceed}
            disabled={columnProfiles.length === 0}
          />
        </div>
      </div>

      <Tabs
        id="profiling-tabs"
        className="profiling-tabs"
        defaultSelectedTabId="overview"
        renderActiveTabPanelOnly
      >
        <Tab
          id="overview"
          title="Column Overview"
          panel={renderColumnOverview()}
        />
        <Tab
          id="details"
          title="Column Details"
          panel={renderColumnDetails()}
          disabled={columnProfiles.length === 0}
        />
        <Tab
          id="preview"
          title="Data Preview"
          panel={
            <div className="data-preview">
              <HTMLTable striped bordered compact className="preview-table">
                <thead>
                  <tr>
                    {data.headers.map((header, i) => (
                      <th key={i}>{header}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {data.rows.slice(0, 20).map((row, i) => (
                    <tr key={i}>
                      {data.headers.map((header, j) => (
                        <td key={j}>
                          {row[header] === null || row[header] === '' ? (
                            <Tag minimal>NULL</Tag>
                          ) : (
                            String(row[header])
                          )}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </HTMLTable>
              {data.rows.length > 20 && (
                <div className="preview-footer">
                  <Tag minimal>Showing 20 of {data.rows.length} rows</Tag>
                </div>
              )}
            </div>
          }
        />
      </Tabs>

      {/* Type Editor Dialog */}
      <Dialog
        isOpen={typeEditorOpen}
        onClose={() => {
          setTypeEditorOpen(false);
          setEditingColumn(null);
        }}
        title="Edit Column Type"
        icon="edit"
        className={Classes.DARK}
      >
        <div className={Classes.DIALOG_BODY}>
          {editingColumn !== null && columnProfiles[editingColumn] && (
            <>
              <Callout intent={Intent.PRIMARY} icon="info-sign">
                Editing type for column: <strong>{columnProfiles[editingColumn].name}</strong>
                <br />
                AI detected: <Tag intent={Intent.SUCCESS}>{formatTypeName(columnProfiles[editingColumn].inferredType)}</Tag> 
                with {(columnProfiles[editingColumn].confidence * 100).toFixed(0)}% confidence
              </Callout>
              
              <FormGroup label="Data Type" labelFor="type-select" style={{ marginTop: '20px' }}>
                <HTMLSelect
                  id="type-select"
                  fill
                  large
                  value={editedType}
                  onChange={(e) => setEditedType(e.target.value)}
                >
                  <optgroup label="Text Types">
                    <option value="xsd:string">String (General Text)</option>
                    <option value="xsd:normalizedString">Normalized String</option>
                    <option value="xsd:token">Token (No whitespace)</option>
                    <option value="xsd:language">Language Code</option>
                    <option value="xsd:Name">Name</option>
                    <option value="xsd:NCName">NCName (XML Name)</option>
                  </optgroup>
                  <optgroup label="Numeric Types">
                    <option value="xsd:integer">Integer</option>
                    <option value="xsd:decimal">Decimal</option>
                    <option value="xsd:float">Float</option>
                    <option value="xsd:double">Double</option>
                    <option value="xsd:long">Long</option>
                    <option value="xsd:int">Int (32-bit)</option>
                    <option value="xsd:short">Short (16-bit)</option>
                    <option value="xsd:byte">Byte (8-bit)</option>
                    <option value="xsd:nonNegativeInteger">Non-negative Integer</option>
                    <option value="xsd:positiveInteger">Positive Integer</option>
                  </optgroup>
                  <optgroup label="Date/Time Types">
                    <option value="xsd:dateTime">DateTime</option>
                    <option value="xsd:date">Date</option>
                    <option value="xsd:time">Time</option>
                    <option value="xsd:duration">Duration</option>
                    <option value="xsd:gYearMonth">Year-Month</option>
                    <option value="xsd:gYear">Year</option>
                    <option value="xsd:gMonthDay">Month-Day</option>
                    <option value="xsd:gDay">Day</option>
                    <option value="xsd:gMonth">Month</option>
                  </optgroup>
                  <optgroup label="Other Types">
                    <option value="xsd:boolean">Boolean</option>
                    <option value="xsd:base64Binary">Base64 Binary</option>
                    <option value="xsd:hexBinary">Hex Binary</option>
                    <option value="xsd:anyURI">URI</option>
                    <option value="xsd:QName">QName</option>
                  </optgroup>
                </HTMLSelect>
              </FormGroup>
              
              <FormGroup label="Notes (Optional)" labelFor="type-notes">
                <TextArea
                  id="type-notes"
                  fill
                  rows={3}
                  value={editNotes}
                  onChange={(e) => setEditNotes(e.target.value)}
                  placeholder="Add any notes about why you changed this type..."
                />
              </FormGroup>
              
              {editedType !== columnProfiles[editingColumn].inferredType && (
                <Callout intent={Intent.WARNING} icon="warning-sign">
                  You are overriding the AI's type detection. Make sure this type is appropriate for the data.
                </Callout>
              )}
            </>
          )}
        </div>
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button 
              text="Cancel" 
              onClick={() => {
                setTypeEditorOpen(false);
                setEditingColumn(null);
              }}
            />
            <Button
              text="Save Type"
              intent={Intent.PRIMARY}
              onClick={() => {
                if (editingColumn !== null && onTypeChange) {
                  // Update the column profile
                  const updatedProfiles = [...columnProfiles];
                  updatedProfiles[editingColumn] = {
                    ...updatedProfiles[editingColumn],
                    inferredType: editedType,
                    confidence: editedType === columnProfiles[editingColumn].inferredType ? 
                      columnProfiles[editingColumn].confidence : 1.0 // Manual override has 100% confidence
                  };
                  setColumnProfiles(updatedProfiles);
                  
                  // Call the parent callback
                  onTypeChange(editingColumn, editedType);
                  
                  // Close the dialog
                  setTypeEditorOpen(false);
                  setEditingColumn(null);
                }
              }}
              disabled={editingColumn === null || editedType === ''}
            />
          </div>
        </div>
      </Dialog>
    </div>
  );
};