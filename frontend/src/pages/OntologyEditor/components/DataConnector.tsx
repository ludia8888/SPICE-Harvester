import React, { useState } from 'react';
import {
  Dialog,
  Classes,
  Button,
  Intent,
  Tabs,
  Tab,
  FormGroup,
  InputGroup,
  FileInput,
  Card,
  Icon,
  Spinner,
  Tag,
  Callout,
  ProgressBar,
} from '@blueprintjs/core';
import { useOntologyStore } from '../../../stores/ontology.store';
import { ontologyApi, OntologyApiError } from '../../../api/ontologyClient';
import { extractDatabaseName } from '../../../utils/database';
import './DataConnector.scss';

interface DataConnectorProps {
  isOpen: boolean;
  onClose: () => void;
  onDataConnected: () => void;
}

export const DataConnector: React.FC<DataConnectorProps> = ({
  isOpen,
  onClose,
  onDataConnected,
}) => {
  const { currentDatabase, setError, clearError } = useOntologyStore();
  
  const [activeTab, setActiveTab] = useState<'csv' | 'sheets'>('csv');
  const [isProcessing, setIsProcessing] = useState(false);
  const [progress, setProgress] = useState(0);
  
  // CSV State
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvContent, setCsvContent] = useState('');
  
  // Google Sheets State
  const [sheetsUrl, setSheetsUrl] = useState('');
  const [sheetsRange, setSheetsRange] = useState('A1:Z1000');
  
  // Processing State
  const [processingStep, setProcessingStep] = useState('');

  const handleCsvUpload = (event: React.FormEvent<HTMLInputElement>) => {
    const file = (event.target as HTMLInputElement).files?.[0];
    if (file) {
      setCsvFile(file);
      
      const reader = new FileReader();
      reader.onload = (e) => {
        const content = e.target?.result as string;
        setCsvContent(content);
      };
      reader.readAsText(file);
    }
  };

  const processCsvData = async () => {
    if (!csvContent || !currentDatabase) return;
    
    setIsProcessing(true);
    setProgress(0);
    clearError('data-connector');
    
    try {
      // Step 1: Parse CSV
      setProcessingStep('Parsing CSV data...');
      setProgress(20);
      
      const lines = csvContent.trim().split('\n');
      const headers = lines[0].split(',').map(h => h.trim());
      const dataRows = lines.slice(1).map(line => {
        const values = line.split(',').map(v => v.trim());
        const obj: any = {};
        headers.forEach((header, index) => {
          obj[header] = values[index] || '';
        });
        return obj;
      });
      
      // Step 2: Analyze with AI
      setProcessingStep('Analyzing data patterns...');
      setProgress(50);
      
      const response = await ontologyApi.typeInference.suggestFromData(dataRows);
      
      // Step 3: Create object types
      setProcessingStep('Creating ontology structure...');
      setProgress(80);
      
      for (const suggestedType of response.suggested_schema.object_types) {
        const dbName = extractDatabaseName(currentDatabase);
        if (!dbName) {
          throw new Error('No database selected');
        }
        await ontologyApi.objectType.create(dbName, {
          id: suggestedType.id,
          label: suggestedType.label,
          description: `Imported from ${csvFile?.name || 'CSV data'}`,
          properties: suggestedType.properties.map(prop => ({
            name: prop.name,
            label: prop.name.charAt(0).toUpperCase() + prop.name.slice(1),
            type: prop.type,
            required: prop.confidence > 0.8,
            constraints: prop.constraints || {}
          }))
        });
      }
      
      setProgress(100);
      setProcessingStep('Data imported successfully!');
      
      setTimeout(() => {
        onDataConnected();
      }, 1000);
      
    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : 'Failed to process CSV data';
      setError('data-connector', message);
    } finally {
      setIsProcessing(false);
    }
  };

  const processGoogleSheets = async () => {
    if (!sheetsUrl || !currentDatabase) return;
    
    setIsProcessing(true);
    setProgress(0);
    clearError('data-connector');
    
    try {
      // Step 1: Connect to Google Sheets
      setProcessingStep('Connecting to Google Sheets...');
      setProgress(30);
      
      // Step 2: Analyze data
      setProcessingStep('Analyzing spreadsheet data...');
      setProgress(60);
      
      const response = await ontologyApi.typeInference.suggestFromGoogleSheets(
        sheetsUrl,
        sheetsRange
      );
      
      // Step 3: Create object types
      setProcessingStep('Building ontology from sheets...');
      setProgress(90);
      
      for (const suggestedType of response.suggested_schema.object_types) {
        const dbName = extractDatabaseName(currentDatabase);
        if (!dbName) {
          throw new Error('No database selected');
        }
        await ontologyApi.objectType.create(dbName, {
          id: suggestedType.id,
          label: suggestedType.label,
          description: `Imported from Google Sheets`,
          properties: suggestedType.properties.map(prop => ({
            name: prop.name,
            label: prop.name.charAt(0).toUpperCase() + prop.name.slice(1),
            type: prop.type,
            required: prop.confidence > 0.8,
            constraints: prop.constraints || {}
          }))
        });
      }
      
      setProgress(100);
      setProcessingStep('Google Sheets imported successfully!');
      
      setTimeout(() => {
        onDataConnected();
      }, 1000);
      
    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : 'Failed to connect to Google Sheets';
      setError('data-connector', message);
    } finally {
      setIsProcessing(false);
    }
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title="Connect Your Data"
      icon="link"
      className={`data-connector-dialog ${Classes.DARK}`}
      style={{ width: 600 }}
    >
      <div className={Classes.DIALOG_BODY}>
        {isProcessing ? (
          <div className="processing-view">
            <div className="processing-icon">
              <Spinner size={50} intent={Intent.PRIMARY} />
            </div>
            <h3>{processingStep}</h3>
            <ProgressBar 
              value={progress / 100} 
              intent={Intent.PRIMARY}
              stripes={false}
            />
          </div>
        ) : (
          <>
            <Callout intent={Intent.PRIMARY} icon="info-sign">
              Connect your data source to automatically generate an ontology structure.
              Our AI will analyze your data and suggest object types and relationships.
            </Callout>

            <Tabs
              id="data-source-tabs"
              selectedTabId={activeTab}
              onChange={(tabId) => setActiveTab(tabId as 'csv' | 'sheets')}
              className="data-source-tabs"
            >
              <Tab
                id="csv"
                title="CSV Upload"
                panel={
                  <div className="csv-panel">
                    <FormGroup label="Select CSV File" labelFor="csv-upload">
                      <FileInput
                        id="csv-upload"
                        text={csvFile?.name || "Choose file..."}
                        onInputChange={handleCsvUpload}
                        inputProps={{ accept: '.csv' }}
                        fill
                      />
                    </FormGroup>
                    
                    {csvFile && (
                      <Card className="file-preview">
                        <div className="file-info">
                          <Icon icon="document" size={20} />
                          <div>
                            <p className="file-name">{csvFile.name}</p>
                            <p className="file-size">
                              {(csvFile.size / 1024).toFixed(2)} KB
                            </p>
                          </div>
                        </div>
                        {csvContent && (
                          <Tag intent={Intent.SUCCESS} icon="tick">
                            File loaded successfully
                          </Tag>
                        )}
                      </Card>
                    )}
                    
                    <Button
                      text="Import CSV Data"
                      icon="import"
                      intent={Intent.PRIMARY}
                      large
                      fill
                      disabled={!csvContent}
                      onClick={processCsvData}
                    />
                  </div>
                }
              />
              
              <Tab
                id="sheets"
                title="Google Sheets"
                panel={
                  <div className="sheets-panel">
                    <FormGroup label="Google Sheets URL" labelFor="sheets-url">
                      <InputGroup
                        id="sheets-url"
                        placeholder="https://docs.google.com/spreadsheets/d/..."
                        value={sheetsUrl}
                        onChange={(e) => setSheetsUrl(e.target.value)}
                        large
                      />
                    </FormGroup>
                    
                    <FormGroup label="Cell Range (Optional)" labelFor="sheets-range">
                      <InputGroup
                        id="sheets-range"
                        placeholder="A1:Z1000"
                        value={sheetsRange}
                        onChange={(e) => setSheetsRange(e.target.value)}
                      />
                    </FormGroup>
                    
                    <Callout intent={Intent.WARNING} icon="warning-sign">
                      Make sure your Google Sheets is publicly accessible or 
                      shared with the service account.
                    </Callout>
                    
                    <Button
                      text="Connect to Google Sheets"
                      icon="cloud-download"
                      intent={Intent.PRIMARY}
                      large
                      fill
                      disabled={!sheetsUrl}
                      onClick={processGoogleSheets}
                    />
                  </div>
                }
              />
            </Tabs>
          </>
        )}
      </div>
      
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button 
            text="Cancel" 
            onClick={onClose}
            disabled={isProcessing}
          />
        </div>
      </div>
    </Dialog>
  );
};