import React, { useState } from 'react';
import { 
  Button, 
  ButtonGroup,
  Card, 
  Classes,
  Dialog,
  Divider,
  FileInput,
  FormGroup,
  Icon, 
  InputGroup, 
  Intent, 
  Tab,
  Tabs,
  Tag,
  TextArea,
  Tree,
  TreeNodeInfo,
  HTMLTable,
  Switch,
  ProgressBar
} from '@blueprintjs/core';
import clsx from 'clsx';
import { useOntologyStore } from '../../../stores/ontology.store';
import { ontologyApi, typeInferenceApi, OntologyApiError } from '../../../api/ontologyClient';

interface InferredProperty {
  name: string;
  type: string;
  confidence: number;
  constraints?: Record<string, any>;
  sample_values?: any[];
}

interface InferredObjectType {
  id: string;
  label: string;
  properties: InferredProperty[];
  confidence: number;
}

interface TypeInferencePanelProps {
  isOpen: boolean;
  onClose: () => void;
}

export const TypeInferencePanel: React.FC<TypeInferencePanelProps> = ({
  isOpen,
  onClose
}) => {
  const {
    currentDatabase,
    addObjectType,
    setError,
    clearError
  } = useOntologyStore();

  const [selectedTab, setSelectedTab] = useState<'data' | 'sheets' | 'results'>('data');
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [analysisProgress, setAnalysisProgress] = useState(0);
  
  // Data input states
  const [rawData, setRawData] = useState('');
  const [googleSheetsUrl, setGoogleSheetsUrl] = useState('');
  const [sheetsRange, setSheetsRange] = useState('A1:Z1000');
  
  // Results state
  const [inferredTypes, setInferredTypes] = useState<InferredObjectType[]>([]);
  const [selectedInferences, setSelectedInferences] = useState<Set<string>>(new Set());
  
  // File upload state
  const [uploadedFile, setUploadedFile] = useState<File | null>(null);

  const handleDataAnalysis = async () => {
    if (!rawData.trim()) {
      setError('type-inference', 'Please provide sample data');
      return;
    }

    setIsAnalyzing(true);
    setAnalysisProgress(0);
    clearError('type-inference');

    try {
      // Parse the raw data
      const lines = rawData.trim().split('\n');
      const headers = lines[0].split(',').map(h => h.trim());
      const dataRows = lines.slice(1).map(line => {
        const values = line.split(',').map(v => v.trim());
        const obj: any = {};
        headers.forEach((header, index) => {
          obj[header] = values[index] || '';
        });
        return obj;
      });

      setAnalysisProgress(30);

      // Call the Funnel service for type inference
      const response = await ontologyApi.typeInference.suggestFromData(dataRows);
      
      setAnalysisProgress(70);

      // Transform the response to our format
      const inferred: InferredObjectType[] = response.suggested_schema.object_types.map(obj => ({
        id: obj.id,
        label: obj.label,
        properties: obj.properties.map(prop => ({
          name: prop.name,
          type: prop.type,
          confidence: prop.confidence,
          constraints: prop.constraints,
          sample_values: dataRows.slice(0, 3).map(row => row[prop.name]).filter(v => v)
        })),
        confidence: obj.properties.reduce((avg, prop) => avg + prop.confidence, 0) / obj.properties.length
      }));

      setInferredTypes(inferred);
      setSelectedTab('results');
      setAnalysisProgress(100);

    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : 'Failed to analyze data';
      setError('type-inference', message);
    } finally {
      setIsAnalyzing(false);
      setTimeout(() => setAnalysisProgress(0), 1000);
    }
  };

  const handleGoogleSheetsAnalysis = async () => {
    if (!googleSheetsUrl.trim()) {
      setError('type-inference', 'Please provide a Google Sheets URL');
      return;
    }

    setIsAnalyzing(true);
    setAnalysisProgress(0);
    clearError('type-inference');

    try {
      setAnalysisProgress(20);

      const response = await ontologyApi.typeInference.suggestFromGoogleSheets(
        googleSheetsUrl,
        sheetsRange
      );
      
      setAnalysisProgress(70);

      // Transform the response
      const inferred: InferredObjectType[] = response.suggested_schema.object_types.map(obj => ({
        id: obj.id,
        label: obj.label,
        properties: obj.properties.map(prop => ({
          name: prop.name,
          type: prop.type,
          confidence: prop.confidence,
          constraints: prop.constraints
        })),
        confidence: obj.properties.reduce((avg, prop) => avg + prop.confidence, 0) / obj.properties.length
      }));

      setInferredTypes(inferred);
      setSelectedTab('results');
      setAnalysisProgress(100);

    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : 'Failed to analyze Google Sheets data';
      setError('type-inference', message);
    } finally {
      setIsAnalyzing(false);
      setTimeout(() => setAnalysisProgress(0), 1000);
    }
  };

  const handleFileUpload = (event: React.FormEvent<HTMLInputElement>) => {
    const file = (event.target as HTMLInputElement).files?.[0];
    if (file) {
      setUploadedFile(file);
      
      // Read CSV file
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target?.result as string;
        setRawData(text);
      };
      reader.readAsText(file);
    }
  };

  const handleInferenceToggle = (objectTypeId: string) => {
    const newSelected = new Set(selectedInferences);
    if (newSelected.has(objectTypeId)) {
      newSelected.delete(objectTypeId);
    } else {
      newSelected.add(objectTypeId);
    }
    setSelectedInferences(newSelected);
  };

  const handleApplyInferences = async () => {
    if (!currentDatabase || selectedInferences.size === 0) return;

    setIsAnalyzing(true);
    let successCount = 0;
    const errors: string[] = [];

    try {
      for (const objectTypeId of selectedInferences) {
        const inferredType = inferredTypes.find(t => t.id === objectTypeId);
        if (!inferredType) continue;

        try {
          await ontologyApi.objectType.create(currentDatabase, {
            id: inferredType.id,
            label: inferredType.label,
            description: `Auto-generated from data analysis (${Math.round(inferredType.confidence * 100)}% confidence)`,
            properties: inferredType.properties.map(prop => ({
              name: prop.name,
              label: prop.name.charAt(0).toUpperCase() + prop.name.slice(1),
              type: prop.type,
              required: prop.confidence > 0.8,
              constraints: prop.constraints || {}
            }))
          });
          
          successCount++;
        } catch (error) {
          errors.push(`${inferredType.label}: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
      }

      if (successCount > 0) {
        // Refresh the ontology store
        // This would typically reload the object types
        onClose();
      }

      if (errors.length > 0) {
        setError('type-inference', `Created ${successCount} object types. Errors: ${errors.join(', ')}`);
      }

    } catch (error) {
      setError('type-inference', 'Failed to apply inferences');
    } finally {
      setIsAnalyzing(false);
    }
  };

  const getConfidenceColor = (confidence: number) => {
    if (confidence >= 0.8) return Intent.SUCCESS;
    if (confidence >= 0.6) return Intent.WARNING;
    return Intent.DANGER;
  };

  const renderDataTab = () => (
    <div className="data-input-tab">
      <FormGroup label="Upload CSV File" labelFor="file-input">
        <FileInput
          id="file-input"
          text={uploadedFile?.name || "Choose CSV file..."}
          onInputChange={handleFileUpload}
          hasSelection={!!uploadedFile}
          inputProps={{ accept: '.csv' }}
        />
      </FormGroup>

      <Divider />

      <FormGroup label="Sample Data (CSV Format)" labelFor="raw-data">
        <div className="data-input-help">
          <p>Paste CSV data with headers in the first row. The AI will analyze patterns and suggest object types.</p>
        </div>
        <TextArea
          id="raw-data"
          value={rawData}
          onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setRawData(e.target.value)}
          placeholder={`name,email,department,salary\nJohn Doe,john@example.com,Engineering,75000\nJane Smith,jane@example.com,Marketing,68000`}
          rows={10}
          fill
          className={Classes.CODE}
        />
      </FormGroup>

      <div className="analysis-actions">
        <Button
          text="Analyze Data"
          icon="predictive-analysis"
          intent={Intent.PRIMARY}
          onClick={handleDataAnalysis}
          disabled={!rawData.trim() || isAnalyzing}
          loading={isAnalyzing}
          large
        />
      </div>
    </div>
  );

  const renderSheetsTab = () => (
    <div className="sheets-input-tab">
      <FormGroup label="Google Sheets URL" labelFor="sheets-url" required>
        <InputGroup
          id="sheets-url"
          value={googleSheetsUrl}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => setGoogleSheetsUrl(e.target.value)}
          placeholder="https://docs.google.com/spreadsheets/d/..."
        />
      </FormGroup>

      <FormGroup label="Range" labelFor="sheets-range">
        <InputGroup
          id="sheets-range"
          value={sheetsRange}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSheetsRange(e.target.value)}
          placeholder="A1:Z1000"
        />
        <div className="range-help">
          <p>Specify the range of cells to analyze (e.g., A1:Z1000)</p>
        </div>
      </FormGroup>

      <div className="analysis-actions">
        <Button
          text="Analyze Google Sheets"
          icon="link"
          intent={Intent.PRIMARY}
          onClick={handleGoogleSheetsAnalysis}
          disabled={!googleSheetsUrl.trim() || isAnalyzing}
          loading={isAnalyzing}
          large
        />
      </div>

      <Card className="sheets-info">
        <h5>Requirements</h5>
        <ul>
          <li>The Google Sheets document must be publicly accessible</li>
          <li>First row should contain column headers</li>
          <li>Data should be consistent within columns</li>
        </ul>
      </Card>
    </div>
  );

  const renderResultsTab = () => (
    <div className="results-tab">
      {inferredTypes.length === 0 ? (
        <div className="no-results">
          <Icon icon="search" size={32} />
          <h4>No Analysis Results</h4>
          <p>Run data analysis from the Data or Sheets tab to see suggestions.</p>
        </div>
      ) : (
        <div className="inference-results">
          <div className="results-header">
            <h4>Inferred Object Types ({inferredTypes.length})</h4>
            <div className="results-actions">
              <Button
                text="Select All"
                small
                onClick={() => setSelectedInferences(new Set(inferredTypes.map(t => t.id)))}
              />
              <Button
                text="Clear All"
                small
                onClick={() => setSelectedInferences(new Set())}
              />
              <Button
                text={`Apply Selected (${selectedInferences.size})`}
                intent={Intent.SUCCESS}
                disabled={selectedInferences.size === 0 || isAnalyzing}
                loading={isAnalyzing}
                onClick={handleApplyInferences}
              />
            </div>
          </div>

          <div className="inference-list">
            {inferredTypes.map(objectType => (
              <Card
                key={objectType.id}
                className={clsx('inference-card', {
                  'selected': selectedInferences.has(objectType.id)
                })}
              >
                <div className="inference-header">
                  <div className="inference-info">
                    <Switch
                      checked={selectedInferences.has(objectType.id)}
                      onChange={() => handleInferenceToggle(objectType.id)}
                    />
                    <div className="inference-details">
                      <h5>{objectType.label}</h5>
                      <div className="confidence-indicator">
                        <Tag
                          intent={getConfidenceColor(objectType.confidence)}
                          minimal
                        >
                          {Math.round(objectType.confidence * 100)}% confidence
                        </Tag>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="properties-preview">
                  <h6>Properties ({objectType.properties.length})</h6>
                  <HTMLTable small striped className="properties-table">
                    <thead>
                      <tr>
                        <th>Name</th>
                        <th>Type</th>
                        <th>Confidence</th>
                        <th>Sample Values</th>
                      </tr>
                    </thead>
                    <tbody>
                      {objectType.properties.map(property => (
                        <tr key={property.name}>
                          <td><code>{property.name}</code></td>
                          <td>
                            <Tag minimal small>
                              {property.type.replace('xsd:', '').replace('custom:', '')}
                            </Tag>
                          </td>
                          <td>
                            <ProgressBar
                              value={property.confidence}
                              intent={getConfidenceColor(property.confidence)}
                              stripes={false}
                            />
                          </td>
                          <td>
                            {property.sample_values && (
                              <div className="sample-values">
                                {property.sample_values.slice(0, 2).map((value, idx) => (
                                  <code key={idx} className="sample-value">
                                    {String(value).substring(0, 20)}
                                    {String(value).length > 20 ? '...' : ''}
                                  </code>
                                ))}
                              </div>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </HTMLTable>
                </div>
              </Card>
            ))}
          </div>
        </div>
      )}
    </div>
  );

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title={
        <div className="dialog-title">
          <Icon icon="predictive-analysis" />
          <span>AI Type Inference</span>
        </div>
      }
      className="type-inference-dialog"
      style={{ width: '900px', maxHeight: '90vh' }}
    >
      <div className={Classes.DIALOG_BODY}>
        {isAnalyzing && analysisProgress > 0 && (
          <div className="analysis-progress">
            <ProgressBar value={analysisProgress / 100} intent={Intent.PRIMARY} />
            <p>Analyzing data and inferring types...</p>
          </div>
        )}

        <Tabs
          id="inference-tabs"
          selectedTabId={selectedTab}
          onChange={(tabId) => setSelectedTab(tabId as any)}
        >
          <Tab
            id="data"
            title={
              <span>
                <Icon icon="th" />
                Raw Data
              </span>
            }
            panel={renderDataTab()}
          />
          
          <Tab
            id="sheets"
            title={
              <span>
                <Icon icon="link" />
                Google Sheets
              </span>
            }
            panel={renderSheetsTab()}
          />
          
          <Tab
            id="results"
            title={
              <span>
                <Icon icon="predictive-analysis" />
                Results
                {inferredTypes.length > 0 && (
                  <Tag minimal round className="tab-badge">
                    {inferredTypes.length}
                  </Tag>
                )}
              </span>
            }
            panel={renderResultsTab()}
          />
        </Tabs>
      </div>
      
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
        </div>
      </div>
    </Dialog>
  );
};