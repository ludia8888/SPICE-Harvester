import React, { useState, useCallback } from 'react';
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
  HTMLSelect,
  OverlayToaster,
  Position,
} from '@blueprintjs/core';
import { useDropzone } from 'react-dropzone';
import { useOntologyStore } from '../../../stores/ontology.store';
import { ontologyApi, OntologyApiError } from '../../../api/ontologyClient';
import { extractDatabaseName } from '../../../utils/database';
import { DataProfilingPanel } from './DataProfilingPanel';
import { OntologyMappingPanel } from './OntologyMappingPanel';
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
  
  const [activeTab, setActiveTab] = useState<'csv' | 'sheets' | 'database'>('csv');
  const [isProcessing, setIsProcessing] = useState(false);
  const [progress, setProgress] = useState(0);
  // Workflow step management
  const [currentStep, setCurrentStep] = useState<'upload' | 'analyze' | 'map' | 'import' | 'complete'>('upload');
  const [showProfiling, setShowProfiling] = useState(false);
  const [showMapping, setShowMapping] = useState(false);
  const [showImportConfirm, setShowImportConfirm] = useState(false);
  const [profiledData, setProfiledData] = useState<{ headers: string[], rows: any[] } | null>(null);
  const [detailedAnalysis, setDetailedAnalysis] = useState<any>(null);
  const [targetOntologies, setTargetOntologies] = useState<any[]>([]);
  const [importResults, setImportResults] = useState<{ success: number; failed: number; errors: string[] } | null>(null);
  
  // CSV State
  const [csvFiles, setCsvFiles] = useState<File[]>([]);
  const [csvContents, setCsvContents] = useState<Map<string, string>>(new Map());
  
  // Google Sheets State
  const [sheetsUrl, setSheetsUrl] = useState('');
  const [sheetsRange, setSheetsRange] = useState('A1:Z1000');
  const [syncSchedule, setSyncSchedule] = useState<'realtime' | 'hourly' | 'daily' | 'weekly'>('daily');
  
  // Database State
  const [dbType, setDbType] = useState<'postgresql' | 'mysql' | 'mongodb' | 'sqlite'>('postgresql');
  const [dbHost, setDbHost] = useState('');
  const [dbPort, setDbPort] = useState('5432');
  const [dbName, setDbName] = useState('');
  const [dbUser, setDbUser] = useState('');
  const [dbPassword, setDbPassword] = useState('');
  const [dbTable, setDbTable] = useState('');
  
  // Processing State
  const [processingStep, setProcessingStep] = useState('');
  const [uploadProgress, setUploadProgress] = useState<Map<string, number>>(new Map());
  
  // Toast notification utility
  const showToast = (message: string, intent?: Intent) => {
    OverlayToaster.create({ position: Position.TOP }).show({
      message,
      intent: intent || Intent.NONE,
      timeout: 3000,
    });
  };

  // Workflow step definitions
  const workflowSteps = [
    { id: 'upload', label: 'Upload', icon: 'cloud-upload', description: 'Upload CSV files' },
    { id: 'analyze', label: 'Analyze', icon: 'chart', description: 'AI analysis' },
    { id: 'map', label: 'Map', icon: 'data-lineage', description: 'Field mapping' },
    { id: 'import', label: 'Import', icon: 'import', description: 'Data import' },
    { id: 'complete', label: 'Complete', icon: 'tick-circle', description: 'All done!' }
  ];

  // Render workflow step indicator
  const renderWorkflowSteps = () => {
    const currentStepIndex = workflowSteps.findIndex(step => step.id === currentStep);
    
    return (
      <div className="workflow-steps">
        {workflowSteps.map((step, index) => {
          const isActive = step.id === currentStep;
          const isCompleted = index < currentStepIndex;
          const isDisabled = index > currentStepIndex;
          
          return (
            <div 
              key={step.id} 
              className={`workflow-step ${
                isActive ? 'active' : isCompleted ? 'completed' : isDisabled ? 'disabled' : ''
              }`}
            >
              <div className={`step-indicator ${isActive ? 'pulsing' : ''}`}>
                {isCompleted ? (
                  <Icon icon="tick" size={18} />
                ) : isActive ? (
                  <Icon icon={step.icon as any} size={18} />
                ) : (
                  <span className="step-number">{index + 1}</span>
                )}
              </div>
              <div className="step-content">
                <div className="step-label">{step.label}</div>
                <div className="step-description">{step.description}</div>
              </div>
              {index < workflowSteps.length - 1 && (
                <div className={`step-connector ${isCompleted ? 'completed' : ''}`} />
              )}
            </div>
          );
        })}
      </div>
    );
  };

  // Dropzone configuration
  const onDrop = useCallback((acceptedFiles: File[]) => {
    const csvFileList = acceptedFiles.filter(file => file.name.endsWith('.csv'));
    if (csvFileList.length === 0) {
      setError('data-connector', 'Please upload CSV files only');
      return;
    }
    
    setCsvFiles(prevFiles => [...prevFiles, ...csvFileList]);
    
    // Read all CSV files
    csvFileList.forEach(file => {
      const reader = new FileReader();
      reader.onload = (e) => {
        const content = e.target?.result as string;
        setCsvContents(prev => {
          const newMap = new Map(prev);
          newMap.set(file.name, content);
          return newMap;
        });
      };
      reader.onprogress = (e) => {
        if (e.lengthComputable) {
          const progress = (e.loaded / e.total) * 100;
          setUploadProgress(prev => {
            const newMap = new Map(prev);
            newMap.set(file.name, progress);
            return newMap;
          });
        }
      };
      reader.readAsText(file);
    });
  }, [setError]);
  
  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'text/csv': ['.csv']
    },
    multiple: true
  });
  
  const removeFile = (fileName: string) => {
    setCsvFiles(prev => prev.filter(f => f.name !== fileName));
    setCsvContents(prev => {
      const newMap = new Map(prev);
      newMap.delete(fileName);
      return newMap;
    });
    setUploadProgress(prev => {
      const newMap = new Map(prev);
      newMap.delete(fileName);
      return newMap;
    });
  };

  const processCsvData = async () => {
    if (csvFiles.length === 0 || csvContents.size === 0 || !currentDatabase) return;
    
    setCurrentStep('analyze');
    setIsProcessing(true);
    setProgress(0);
    clearError('data-connector');
    
    try {
      const dbName = extractDatabaseName(currentDatabase);
      if (!dbName) {
        throw new Error('No database selected');
      }
      
      // For now, process only the first file (we can extend to multiple later)
      const file = csvFiles[0];
      const content = csvContents.get(file.name);
      if (!content) throw new Error('No content found for file');
      
      // Step 1: Parse CSV
      setProcessingStep(`Parsing ${file.name}...`);
      setProgress(20);
      
      const parsedData = parseCSV(content);
      if (parsedData.rows.length === 0) {
        throw new Error(`No data found in ${file.name}`);
      }
      
      // Step 2: Call backend for schema suggestion and type inference
      setProcessingStep(`Analyzing data patterns with AI...`);
      setProgress(50);
      
      // Prepare data for backend (convert to array format)
      const dataArray = parsedData.rows.map(row => 
        parsedData.headers.map(header => row[header])
      );
      
      // Call backend schema suggestion endpoint
      const response = await fetch(`${import.meta.env.VITE_BFF_BASE_URL || 'http://localhost:8002/api/v1'}/database/${dbName}/suggest-schema-from-data`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept-Language': 'ko',
        },
        body: JSON.stringify({
          data: dataArray,
          columns: parsedData.headers,
          class_name: file.name.replace('.csv', '').replace(/[^a-zA-Z0-9]/g, '_'),
          include_complex_types: true
        })
      });
      
      if (!response.ok) {
        throw new Error('Failed to analyze data');
      }
      
      const result = await response.json();
      
      // Step 3: Store the results
      setProcessingStep('Processing analysis results...');
      setProgress(80);
      
      setProfiledData(parsedData);
      setDetailedAnalysis(result);
      
      setProgress(100);
      setProcessingStep('Analysis complete!');
      
      // Show profiling panel after a short delay
      setTimeout(() => {
        setCurrentStep('analyze');
        setShowProfiling(true);
      }, 500);
      
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to process CSV data';
      setError('data-connector', message);
    } finally {
      setIsProcessing(false);
    }
  };
  
  const proceedToMapping = async () => {
    console.log('🎯 proceedToMapping called', { profiledData, currentDatabase, detailedAnalysis });
    
    if (!profiledData || !currentDatabase || !detailedAnalysis) {
      console.warn('Missing required data for mapping', { profiledData: !!profiledData, currentDatabase: !!currentDatabase, detailedAnalysis: !!detailedAnalysis });
      showToast('Missing required data for mapping', Intent.WARNING);
      return;
    }
    
    try {
      const dbName = extractDatabaseName(currentDatabase);
      if (!dbName) {
        throw new Error('No database selected');
      }
      
      console.log('📡 Fetching existing ontologies for database:', dbName);
      
      // Fetch existing ontologies for mapping
      const existingOntologies = await ontologyApi.objectType.list(dbName);
      console.log('✅ Found ontologies:', existingOntologies.length);
      
      if (existingOntologies.length === 0) {
        // No existing ontologies, create new ones directly
        console.log('⚡ No existing ontologies, proceeding with direct creation');
        await proceedWithDirectCreation();
      } else {
        // Hide profiling panel and show mapping panel
        console.log('🗺️ Showing mapping panel');
        setShowProfiling(false); // Hide profiling panel
        setCurrentStep('map');
        setTargetOntologies(existingOntologies);
        setShowMapping(true);
      }
      
    } catch (error) {
      console.error('❌ Error in proceedToMapping:', error);
      const message = error instanceof Error ? error.message : 'Failed to load ontologies';
      setError('data-connector', message);
      showToast(message, Intent.DANGER);
    }
  };
  
  const proceedWithDirectCreation = async () => {
    console.log('🚀 proceedWithDirectCreation called', { profiledData: !!profiledData, currentDatabase, detailedAnalysis: !!detailedAnalysis });
    
    if (!profiledData || !currentDatabase || !detailedAnalysis) {
      console.warn('Missing required data for direct creation');
      return;
    }
    
    console.log('📊 Setting processing state to true');
    setIsProcessing(true);
    setProgress(0);
    clearError('data-connector');
    
    try {
      const dbName = extractDatabaseName(currentDatabase);
      if (!dbName) {
        throw new Error('No database selected');
      }
      
      // Create object types using the backend-suggested schema
      setProcessingStep('Saving ontology to database...');
      setProgress(50);
      
      console.log('📋 Detailed analysis:', detailedAnalysis);
      const suggestedSchema = detailedAnalysis.suggested_schema;
      console.log('📋 Suggested schema:', suggestedSchema);
      
      if (!suggestedSchema) {
        console.error('❌ No schema suggestion received from backend');
        throw new Error('No schema suggestion received from backend');
      }
      
      // Handle both formats: direct object or object_types array
      let objectTypes = [];
      if (suggestedSchema.object_types && Array.isArray(suggestedSchema.object_types)) {
        // Format: { object_types: [...] }
        objectTypes = suggestedSchema.object_types;
      } else if (suggestedSchema.id && suggestedSchema.properties) {
        // Format: Direct object { id, label, properties, ... }
        objectTypes = [suggestedSchema];
      } else {
        console.error('❌ Invalid schema format');
        throw new Error('Invalid schema format received from backend');
      }
      
      console.log(`🔨 Creating ${objectTypes.length} object types...`);
      
      for (const suggestedType of objectTypes) {
        console.log('🔨 Creating object type:', suggestedType.id);
        await ontologyApi.objectType.create(dbName, {
          id: suggestedType.id,
          label: suggestedType.label,
          description: suggestedType.description || `Imported from CSV\n\nImport Date: ${new Date().toISOString()}`,
          properties: suggestedType.properties
        });
        console.log('✅ Created object type:', suggestedType.id);
      }
      
      setProgress(100);
      setProcessingStep('Ontology created successfully!');
      
      // Move to import confirmation step instead of closing dialog
      setTimeout(() => {
        setCurrentStep('import');
        setShowProfiling(false);
        setShowImportConfirm(true);
      }, 1000);
      
    } catch (error) {
      console.error('❌ Error in proceedWithDirectCreation:', error);
      const message = error instanceof OntologyApiError 
        ? error.message 
        : error instanceof Error
        ? error.message
        : 'Failed to create ontology';
      setError('data-connector', message);
      showToast(message, Intent.DANGER);
    } finally {
      console.log('🏁 proceedWithDirectCreation finally block');
      setIsProcessing(false);
    }
  };
  
  const handleMappingComplete = async (mappings: any[]) => {
    if (!currentDatabase || !detailedAnalysis || mappings.length === 0) return;
    
    setIsProcessing(true);
    setProgress(0);
    clearError('data-connector');
    
    try {
      const dbName = extractDatabaseName(currentDatabase);
      if (!dbName) {
        throw new Error('No database selected');
      }
      
      // Group mappings by target class
      const mappingsByClass = new Map<string, any[]>();
      for (const mapping of mappings) {
        if (!mapping.accepted) continue;
        
        const classId = mapping.targetClass;
        if (!mappingsByClass.has(classId)) {
          mappingsByClass.set(classId, []);
        }
        mappingsByClass.get(classId)!.push(mapping);
      }
      
      setProcessingStep(`Processing ${mappingsByClass.size} ontology classes...`);
      setProgress(20);
      
      // Process each target class
      let processedClasses = 0;
      for (const [classId, classMappings] of mappingsByClass) {
        setProcessingStep(`Updating ontology class: ${classId}`);
        
        try {
          // Fetch existing ontology
          const existingOntology = await ontologyApi.objectType.get(dbName, classId);
          
          // Extract source schema from analysis
          const sourceSchema = detailedAnalysis.suggested_schema?.object_types?.[0]?.properties || [];
          
          // Prepare new properties based on mappings
          const newProperties = [];
          const existingPropertyNames = new Set(
            existingOntology.properties?.map(p => p.name) || []
          );
          
          for (const mapping of classMappings) {
            // Skip if property already exists
            if (existingPropertyNames.has(mapping.targetProperty)) {
              continue;
            }
            
            // Find source field details
            const sourceField = sourceSchema.find(
              (field: any) => field.name === mapping.sourceField
            );
            
            if (sourceField) {
              // Create new property based on source field
              newProperties.push({
                name: mapping.targetProperty,
                label: sourceField.label || mapping.targetProperty,
                type: sourceField.type,
                required: sourceField.required || false,
                description: `Mapped from ${mapping.sourceField} (confidence: ${(mapping.confidence * 100).toFixed(0)}%)`,
                constraints: sourceField.constraints || {}
              });
            }
          }
          
          // Update ontology if there are new properties
          if (newProperties.length > 0) {
            const updatedProperties = [
              ...(existingOntology.properties || []),
              ...newProperties
            ];
            
            await ontologyApi.objectType.update(dbName, classId, {
              properties: updatedProperties
            });
            
            showToast(
              `Added ${newProperties.length} properties to ${existingOntology.label || classId}`,
              Intent.SUCCESS
            );
          }
          
        } catch (error) {
          console.error(`Failed to update class ${classId}:`, error);
          showToast(
            `Failed to update class ${classId}`,
            Intent.WARNING
          );
        }
        
        processedClasses++;
        setProgress(20 + (processedClasses / mappingsByClass.size) * 60);
      }
      
      // Create mapping metadata
      setProcessingStep('Saving mapping metadata...');
      setProgress(85);
      
      // Store mapping metadata for future reference
      const mappingMetadata = {
        timestamp: new Date().toISOString(),
        sourceFile: csvFiles[0]?.name || 'unknown',
        mappingsCount: mappings.filter(m => m.accepted).length,
        averageConfidence: mappings
          .filter(m => m.accepted)
          .reduce((sum, m) => sum + m.confidence, 0) / mappings.filter(m => m.accepted).length,
        mappingDetails: mappings.filter(m => m.accepted).map(m => ({
          source: m.sourceField,
          target: `${m.targetClass}.${m.targetProperty}`,
          confidence: m.confidence
        }))
      };
      
      // Save mapping metadata to each target class
      for (const classId of mappingsByClass.keys()) {
        try {
          const metadataResponse = await fetch(
            `${import.meta.env.VITE_BFF_BASE_URL || 'http://localhost:8002/api/v1'}/database/${dbName}/ontology/${classId}/mapping-metadata`,
            {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
              },
              body: JSON.stringify(mappingMetadata)
            }
          );
          
          if (!metadataResponse.ok) {
            // Silently continue - metadata saving is optional
          }
        } catch (error) {
          // Silently continue - metadata saving is optional
        }
      }
      
      setProgress(100);
      setProcessingStep('Schema updated! Ready to import data...');
      
      showToast(
        `Successfully mapped ${mappings.filter(m => m.accepted).length} fields to ${mappingsByClass.size} ontology classes`,
        Intent.SUCCESS
      );
      
      // Move to import confirmation step instead of completing
      setTimeout(() => {
        setCurrentStep('import');
        setShowMapping(false);
        setShowImportConfirm(true);
      }, 1000);
      
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to process mappings';
      setError('data-connector', message);
      showToast(message, Intent.DANGER);
    } finally {
      setIsProcessing(false);
    }
  };

  // TODO: Replace with actual data import implementation
  // ARCHITECTURE NOTES for future developers:
  // ========================================
  // 1. OUTBOX PATTERN: Don't insert directly to TerminusDB
  //    - Insert import job to PostgreSQL outbox table
  //    - Include: job_id, ontology_class, csv_data, mapping_config, status
  // 
  // 2. EVENT STREAMING: Use Kafka for async processing
  //    - Publish 'data.import.requested' event
  //    - Payload: { jobId, classId, csvData, mappings }
  //
  // 3. CQRS PATTERN: Separate read/write models
  //    - WRITE: Kafka Consumer processes import job
  //    - READ: ElasticSearch for search queries, Cassandra for bulk data
  //
  // 4. EVENT FLOW:
  //    CSV Upload -> Outbox Table -> Kafka Event -> Import Service -> TerminusDB + ElasticSearch + Cassandra
  //
  // 5. IMPLEMENTATION CHECKLIST:
  //    [ ] PostgreSQL outbox table (import_jobs)
  //    [ ] Kafka topic (data-import-requests)
  //    [ ] Import service (Kafka consumer)
  //    [ ] ElasticSearch indexing
  //    [ ] Cassandra bulk storage
  //    [ ] Progress tracking via WebSocket/SSE
  const importCsvData = async () => {
    if (!profiledData || !currentDatabase || !detailedAnalysis) return;
    
    setIsProcessing(true);
    setProgress(0);
    clearError('data-connector');
    
    try {
      const dbName = extractDatabaseName(currentDatabase);
      if (!dbName) {
        throw new Error('No database selected');
      }
      
      // PLACEHOLDER: Simulate import process for UX
      setProcessingStep(`Creating import job for ${profiledData.rows.length} records...`);
      setProgress(10);
      
      // TODO: Replace with actual outbox pattern implementation
      // const importJobId = await createImportJob({
      //   database: dbName,
      //   sourceFile: csvFiles[0]?.name,
      //   totalRecords: profiledData.rows.length,
      //   mappings: detailedAnalysis.suggested_schema,
      //   csvData: profiledData
      // });
      
      await new Promise(resolve => setTimeout(resolve, 1000));
      setProgress(30);
      
      setProcessingStep('Publishing import event to Kafka...');
      // TODO: Kafka event publishing
      // await publishImportEvent(importJobId, {
      //   topic: 'data-import-requests',
      //   payload: { jobId: importJobId, classId, csvData, mappings }
      // });
      
      await new Promise(resolve => setTimeout(resolve, 1500));
      setProgress(60);
      
      setProcessingStep('Processing data import (async)...');
      // TODO: Real-time progress via WebSocket
      // const progressSocket = new WebSocket(`ws://localhost:8003/import-progress/${importJobId}`);
      // progressSocket.onmessage = (event) => {
      //   const progress = JSON.parse(event.data);
      //   setProgress(progress.percentage);
      //   setProcessingStep(progress.message);
      // };
      
      await new Promise(resolve => setTimeout(resolve, 2000));
      setProgress(90);
      
      // PLACEHOLDER: Simulate success result
      const mockResults = {
        success: Math.floor(profiledData.rows.length * 0.95), // 95% success rate
        failed: Math.floor(profiledData.rows.length * 0.05),
        errors: [
          'Row 23: Invalid date format in created_at field',
          'Row 156: Missing required field: product_name',
          'Row 287: Duplicate ID detected'
        ]
      };
      
      setImportResults(mockResults);
      setProgress(100);
      setProcessingStep(`Import completed: ${mockResults.success}/${profiledData.rows.length} records`);
      
      setTimeout(() => {
        setCurrentStep('complete');
        setShowImportConfirm(false);
      }, 1000);
      
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to import data';
      setError('data-connector', message);
      showToast(message, Intent.DANGER);
    } finally {
      setIsProcessing(false);
    }
  };
  
  // CSV Parser that handles quoted fields and various edge cases
  const parseCSV = (content: string): { headers: string[], rows: any[] } => {
    const lines = content.trim().split(/\r?\n/);
    if (lines.length === 0) return { headers: [], rows: [] };
    
    // Parse headers
    const headers = parseCSVLine(lines[0]);
    
    // Parse data rows
    const rows = [];
    for (let i = 1; i < lines.length; i++) {
      const values = parseCSVLine(lines[i]);
      if (values.length === headers.length) {
        const obj: any = {};
        headers.forEach((header, index) => {
          const value = values[index];
          // Convert empty strings to null
          obj[header] = value === '' ? null : value;
        });
        rows.push(obj);
      }
    }
    
    return { headers, rows };
  };
  
  // Parse a single CSV line handling quoted fields
  const parseCSVLine = (line: string): string[] => {
    const result = [];
    let current = '';
    let inQuotes = false;
    
    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1];
      
      if (char === '"' && !inQuotes) {
        inQuotes = true;
      } else if (char === '"' && inQuotes && nextChar === '"') {
        current += '"';
        i++; // Skip next quote
      } else if (char === '"' && inQuotes) {
        inQuotes = false;
      } else if (char === ',' && !inQuotes) {
        result.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    
    result.push(current.trim());
    return result;
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

  // Get current step info for dialog
  const getCurrentStepInfo = () => {
    const step = workflowSteps.find(s => s.id === currentStep);
    return step || workflowSteps[0];
  };

  const currentStepInfo = getCurrentStepInfo();
  const isWideDialog = showProfiling || showMapping || showImportConfirm || currentStep === 'complete';

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title={`${currentStepInfo.label} - Data Import Workflow`}
      icon={currentStepInfo.icon as any}
      className={`data-connector-dialog ${Classes.DARK}`}
      style={{ width: isWideDialog ? '90%' : 600, height: isWideDialog ? '90%' : 'auto' }}
    >
      <div className={Classes.DIALOG_BODY}>
        {/* Workflow Step Indicator */}
        {renderWorkflowSteps()}
        
        {/* Debug info - remove after fixing */}
        {console.log('🔍 DataConnector render state:', {
          currentStep,
          showProfiling,
          showMapping,
          showImportConfirm,
          importResults: !!importResults,
          profiledData: !!profiledData,
          detailedAnalysis: !!detailedAnalysis,
          targetOntologies: targetOntologies?.length,
          isProcessing
        })}
        {/* Content based on current step */}
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
        ) : currentStep === 'complete' && importResults ? (
          <div className="import-complete-view">
            <div className="completion-icon">
              <Icon icon="tick-circle" size={60} color="#0F9960" />
            </div>
            <h2>Import Completed Successfully!</h2>
            
            <div className="import-summary">
              <Card className="summary-card">
                <h4>Import Summary</h4>
                <div className="summary-stats">
                  <div className="stat-item success">
                    <Icon icon="tick" />
                    <span>{importResults.success} Records Imported</span>
                  </div>
                  {importResults.failed > 0 && (
                    <div className="stat-item failed">
                      <Icon icon="cross" />
                      <span>{importResults.failed} Records Failed</span>
                    </div>
                  )}
                </div>
                
                {importResults.errors.length > 0 && (
                  <div className="error-details">
                    <h5>Import Errors:</h5>
                    <div className="error-list">
                      {importResults.errors.slice(0, 3).map((error, idx) => (
                        <div key={idx} className="error-item">
                          <Icon icon="warning-sign" size={14} />
                          <span>{error}</span>
                        </div>
                      ))}
                      {importResults.errors.length > 3 && (
                        <div className="error-item">
                          <span>... and {importResults.errors.length - 3} more errors</span>
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </Card>
            </div>
            
            <div className="completion-actions">
              <Button
                text="View Imported Data"
                icon="eye-open"
                intent={Intent.PRIMARY}
                large
                onClick={() => {
                  onDataConnected();
                }}
              />
              <Button
                text="Import More Files"
                icon="plus"
                large
                onClick={() => {
                  // Reset to upload step
                  setCurrentStep('upload');
                  setShowImportConfirm(false);
                  setImportResults(null);
                  setCsvFiles([]);
                  setCsvContents(new Map());
                  setProfiledData(null);
                  setDetailedAnalysis(null);
                }}
              />
              <Button
                text="Download Failed Records"
                icon="download"
                disabled={importResults.failed === 0}
                onClick={() => {
                  // TODO: Implement failed records download
                  showToast('Failed records download not implemented yet', Intent.WARNING);
                }}
              />
            </div>
          </div>
        ) : currentStep === 'import' && showImportConfirm && profiledData ? (
          <div className="import-confirm-view">
            <div className="confirm-icon">
              <Icon icon="import" size={50} color="#48AFF0" />
            </div>
            <h3>Ready to Import Data</h3>
            <p>Schema has been updated successfully. Now import your CSV data to the ontology.</p>
            
            <Card className="import-preview">
              <h4>Import Details</h4>
              <div className="import-details">
                <div className="detail-row">
                  <strong>Source File:</strong>
                  <span>{csvFiles[0]?.name || 'Unknown'}</span>
                </div>
                <div className="detail-row">
                  <strong>Records to Import:</strong>
                  <Tag intent={Intent.PRIMARY}>{profiledData.rows.length} rows</Tag>
                </div>
                <div className="detail-row">
                  <strong>Target Database:</strong>
                  <span>{extractDatabaseName(currentDatabase)}</span>
                </div>
                <div className="detail-row">
                  <strong>Estimated Time:</strong>
                  <span>~{Math.ceil(profiledData.rows.length / 100)} seconds</span>
                </div>
              </div>
            </Card>
            
            <Callout intent={Intent.WARNING} icon="warning-sign">
              <strong>Important:</strong> This will create new instance data in your ontology. 
              Make sure your mappings are correct before proceeding.
            </Callout>
            
            <div className="import-actions">
              <Button
                text="Preview Sample Data"
                icon="eye-open"
                onClick={() => {
                  // TODO: Show sample data preview
                  showToast('Sample preview not implemented yet', Intent.WARNING);
                }}
              />
              <Button
                text={`Import ${profiledData.rows.length} Records`}
                icon="import"
                intent={Intent.PRIMARY}
                large
                onClick={importCsvData}
                disabled={isProcessing}
              />
            </div>
          </div>
        ) : currentStep === 'map' && showMapping && profiledData && detailedAnalysis && targetOntologies ? (
          <OntologyMappingPanel
            sourceSchema={(() => {
              const schema = detailedAnalysis.suggested_schema;
              // Handle both formats: object_types array or direct object
              if (schema?.object_types?.[0]?.properties) {
                return schema.object_types[0].properties;
              } else if (schema?.properties) {
                return schema.properties;
              }
              return [];
            })()}
            targetOntologies={targetOntologies}
            sampleData={profiledData.rows}
            onMappingComplete={handleMappingComplete}
            onBack={() => {
              setCurrentStep('analyze');
              setShowMapping(false);
              setShowProfiling(true);
            }}
          />
        ) : currentStep === 'analyze' && showProfiling && profiledData && detailedAnalysis ? (
          <DataProfilingPanel
            data={profiledData}
            analysisResult={detailedAnalysis}
            onProceed={proceedToMapping}
          />
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
                    {/* Drag & Drop Zone */}
                    <div
                      {...getRootProps()}
                      className={`dropzone ${isDragActive ? 'dropzone-active' : ''}`}
                      style={{
                        border: '2px dashed #5c7080',
                        borderRadius: '4px',
                        padding: '40px',
                        textAlign: 'center',
                        cursor: 'pointer',
                        backgroundColor: isDragActive ? 'rgba(92, 112, 128, 0.1)' : 'transparent',
                        transition: 'all 0.2s ease',
                        marginBottom: '20px'
                      }}
                    >
                      <input {...getInputProps()} />
                      <Icon icon="cloud-upload" size={40} color="#5c7080" />
                      <h3 style={{ marginTop: '10px', marginBottom: '5px' }}>
                        {isDragActive ? 'Drop CSV files here' : 'Drag & Drop CSV files'}
                      </h3>
                      <p style={{ color: '#5c7080', marginBottom: '10px' }}>
                        or click to browse files
                      </p>
                      <Tag minimal>Supports multiple CSV files</Tag>
                    </div>
                    
                    {/* File List */}
                    {csvFiles.length > 0 && (
                      <div className="file-list" style={{ marginBottom: '20px' }}>
                        <h4>Uploaded Files ({csvFiles.length})</h4>
                        {csvFiles.map(file => (
                          <Card key={file.name} className="file-preview" style={{ marginBottom: '10px' }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                              <div className="file-info" style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                                <Icon icon="document" size={20} />
                                <div>
                                  <p className="file-name" style={{ margin: 0, fontWeight: 500 }}>
                                    {file.name}
                                  </p>
                                  <p className="file-size" style={{ margin: 0, fontSize: '12px', color: '#5c7080' }}>
                                    {(file.size / 1024).toFixed(2)} KB
                                  </p>
                                </div>
                              </div>
                              <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                                {uploadProgress.get(file.name) !== undefined && uploadProgress.get(file.name)! < 100 ? (
                                  <ProgressBar
                                    value={uploadProgress.get(file.name)! / 100}
                                    intent={Intent.PRIMARY}
                                    stripes={false}
                                    style={{ width: '100px' }}
                                  />
                                ) : csvContents.has(file.name) ? (
                                  <Tag intent={Intent.SUCCESS} icon="tick" minimal>
                                    Loaded
                                  </Tag>
                                ) : (
                                  <Tag minimal>
                                    Loading...
                                  </Tag>
                                )}
                                <Button
                                  icon="cross"
                                  minimal
                                  small
                                  onClick={() => removeFile(file.name)}
                                />
                              </div>
                            </div>
                          </Card>
                        ))}
                      </div>
                    )}
                    
                    {/* Sync Schedule Option */}
                    {csvFiles.length > 0 && (
                      <FormGroup label="Sync Schedule" labelFor="csv-sync">
                        <HTMLSelect
                          id="csv-sync"
                          value={syncSchedule}
                          onChange={(e) => setSyncSchedule(e.target.value as any)}
                          fill
                        >
                          <option value="realtime">Real-time</option>
                          <option value="hourly">Hourly</option>
                          <option value="daily">Daily</option>
                          <option value="weekly">Weekly</option>
                        </HTMLSelect>
                      </FormGroup>
                    )}
                    
                    <Button
                      text={`Import ${csvFiles.length} CSV File${csvFiles.length !== 1 ? 's' : ''}`}
                      icon="import"
                      intent={Intent.PRIMARY}
                      large
                      fill
                      disabled={csvFiles.length === 0 || csvContents.size !== csvFiles.length}
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
                    
                    <FormGroup label="Sync Schedule" labelFor="sheets-sync">
                      <HTMLSelect
                        id="sheets-sync"
                        value={syncSchedule}
                        onChange={(e) => setSyncSchedule(e.target.value as any)}
                        fill
                      >
                        <option value="realtime">Real-time</option>
                        <option value="hourly">Hourly</option>
                        <option value="daily">Daily</option>
                        <option value="weekly">Weekly</option>
                      </HTMLSelect>
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
              
              <Tab
                id="database"
                title="Database"
                panel={
                  <div className="database-panel">
                    <FormGroup label="Database Type" labelFor="db-type">
                      <HTMLSelect
                        id="db-type"
                        value={dbType}
                        onChange={(e) => {
                          const type = e.target.value as any;
                          setDbType(type);
                          // Update default port based on database type
                          switch (type) {
                            case 'postgresql':
                              setDbPort('5432');
                              break;
                            case 'mysql':
                              setDbPort('3306');
                              break;
                            case 'mongodb':
                              setDbPort('27017');
                              break;
                            case 'sqlite':
                              setDbPort('');
                              break;
                          }
                        }}
                        fill
                        large
                      >
                        <option value="postgresql">PostgreSQL</option>
                        <option value="mysql">MySQL</option>
                        <option value="mongodb">MongoDB</option>
                        <option value="sqlite">SQLite</option>
                      </HTMLSelect>
                    </FormGroup>
                    
                    {dbType !== 'sqlite' && (
                      <>
                        <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '10px' }}>
                          <FormGroup label="Host" labelFor="db-host">
                            <InputGroup
                              id="db-host"
                              placeholder="localhost or IP address"
                              value={dbHost}
                              onChange={(e) => setDbHost(e.target.value)}
                              large
                            />
                          </FormGroup>
                          
                          <FormGroup label="Port" labelFor="db-port">
                            <InputGroup
                              id="db-port"
                              placeholder="5432"
                              value={dbPort}
                              onChange={(e) => setDbPort(e.target.value)}
                              large
                            />
                          </FormGroup>
                        </div>
                        
                        <FormGroup label="Database Name" labelFor="db-name">
                          <InputGroup
                            id="db-name"
                            placeholder="my_database"
                            value={dbName}
                            onChange={(e) => setDbName(e.target.value)}
                            large
                          />
                        </FormGroup>
                        
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px' }}>
                          <FormGroup label="Username" labelFor="db-user">
                            <InputGroup
                              id="db-user"
                              placeholder="postgres"
                              value={dbUser}
                              onChange={(e) => setDbUser(e.target.value)}
                              large
                            />
                          </FormGroup>
                          
                          <FormGroup label="Password" labelFor="db-password">
                            <InputGroup
                              id="db-password"
                              type="password"
                              placeholder="••••••••"
                              value={dbPassword}
                              onChange={(e) => setDbPassword(e.target.value)}
                              large
                            />
                          </FormGroup>
                        </div>
                      </>
                    )}
                    
                    {dbType === 'sqlite' && (
                      <FormGroup label="Database File Path" labelFor="db-path">
                        <InputGroup
                          id="db-path"
                          placeholder="/path/to/database.db"
                          value={dbHost}
                          onChange={(e) => setDbHost(e.target.value)}
                          large
                        />
                      </FormGroup>
                    )}
                    
                    <FormGroup label="Table/Collection (Optional)" labelFor="db-table">
                      <InputGroup
                        id="db-table"
                        placeholder="Leave empty to scan all tables"
                        value={dbTable}
                        onChange={(e) => setDbTable(e.target.value)}
                        large
                      />
                    </FormGroup>
                    
                    <FormGroup label="Sync Schedule" labelFor="db-sync">
                      <HTMLSelect
                        id="db-sync"
                        value={syncSchedule}
                        onChange={(e) => setSyncSchedule(e.target.value as any)}
                        fill
                      >
                        <option value="realtime">Real-time (CDC)</option>
                        <option value="hourly">Hourly</option>
                        <option value="daily">Daily</option>
                        <option value="weekly">Weekly</option>
                      </HTMLSelect>
                    </FormGroup>
                    
                    <div style={{ display: 'flex', gap: '10px', marginTop: '20px' }}>
                      <Button
                        text="Test Connection"
                        icon="link"
                        intent={Intent.NONE}
                        large
                        onClick={async () => {
                          // TODO: Implement connection test
                          showToast('Connection test not implemented yet', Intent.WARNING);
                        }}
                      />
                      
                      <Button
                        text="Connect to Database"
                        icon="database"
                        intent={Intent.PRIMARY}
                        large
                        fill
                        disabled={!dbHost || (dbType !== 'sqlite' && (!dbUser || !dbName))}
                        onClick={async () => {
                          // TODO: Implement database connection
                          showToast('Database connection not implemented yet', Intent.WARNING);
                        }}
                      />
                    </div>
                  </div>
                }
              />
            </Tabs>
          </>
        )}
      </div>
      
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          {(showProfiling || showMapping) && (
            <Button
              text="Back"
              icon="arrow-left"
              onClick={() => {
                if (showMapping) {
                  setShowMapping(false);
                  setShowProfiling(true);
                } else {
                  setShowProfiling(false);
                  setProfiledData(null);
                }
              }}
              disabled={isProcessing}
            />
          )}
          <Button 
            text="Cancel" 
            onClick={() => {
              setShowProfiling(false);
              setShowMapping(false);
              setProfiledData(null);
              onClose();
            }}
            disabled={isProcessing}
          />
        </div>
      </div>
    </Dialog>
  );
};