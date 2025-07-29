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
} from '@blueprintjs/core';
import { useOntologyStore } from '../../../stores/ontology.store';
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
  const pageSize = 10;

  const selectedObject = objectTypes.find(obj => obj.id === objectTypeId);

  useEffect(() => {
    if (isOpen && objectTypeId) {
      loadPreviewData();
    }
  }, [isOpen, objectTypeId, page]);

  const loadPreviewData = async () => {
    setIsLoading(true);
    try {
      // TODO: Replace with actual API call
      // const data = await ontologyApi.query.getObjectInstances(currentDatabase, objectTypeId, {
      //   limit: pageSize,
      //   offset: page * pageSize,
      //   search: searchQuery
      // });
      
      // Mock data for now
      setTimeout(() => {
        const mockData: PreviewData = {
          headers: selectedObject?.properties.map(p => p.name) || [],
          rows: Array(pageSize).fill(null).map((_, idx) => 
            selectedObject?.properties.map(p => {
              switch (p.type) {
                case 'integer': return Math.floor(Math.random() * 1000);
                case 'decimal': return (Math.random() * 1000).toFixed(2);
                case 'boolean': return Math.random() > 0.5;
                case 'date': return new Date().toISOString().split('T')[0];
                default: return `Sample ${p.name} ${idx + 1}`;
              }
            }) || []
          ),
          totalCount: 142
        };
        setPreviewData(mockData);
        setIsLoading(false);
      }, 500);
    } catch (error) {
      console.error('Failed to load preview data:', error);
      setIsLoading(false);
    }
  };

  const handleSearch = () => {
    setPage(0);
    loadPreviewData();
  };

  const formatCellValue = (value: any, type?: string): string => {
    if (value === null || value === undefined) return '-';
    if (type === 'boolean') return value ? '✓' : '✗';
    if (type === 'date' && value instanceof Date) return value.toLocaleDateString();
    return String(value);
  };

  if (!isOpen) return null;

  return (
    <div className={`data-preview-panel ${isExpanded ? 'expanded' : 'collapsed'}`}>
      <Card className="preview-container" elevation={3}>
        {/* Header */}
        <div className="preview-header">
          <div className="header-left">
            <Icon icon="database" size={18} />
            <h4>Data Preview: {selectedObject?.label}</h4>
            {previewData && (
              <Tag minimal intent={Intent.PRIMARY}>
                {previewData.totalCount} records
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
            ) : previewData && previewData.rows.length > 0 ? (
              <>
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
                                  {property?.type || 'unknown'}
                                </Tag>
                              </div>
                            </th>
                          );
                        })}
                      </tr>
                    </thead>
                    <tbody>
                      {previewData.rows.map((row, rowIdx) => (
                        <tr key={rowIdx}>
                          {row.map((cell, cellIdx) => {
                            const property = selectedObject?.properties[cellIdx];
                            return (
                              <td key={cellIdx}>
                                {formatCellValue(cell, property?.type)}
                              </td>
                            );
                          })}
                        </tr>
                      ))}
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
              </>
            ) : (
              <NonIdealState
                icon="search"
                title="No Data Found"
                description={
                  searchQuery 
                    ? `No results found for "${searchQuery}"`
                    : "No data available for this object type yet."
                }
                action={
                  searchQuery && (
                    <Button
                      text="Clear Search"
                      onClick={() => {
                        setSearchQuery('');
                        loadPreviewData();
                      }}
                    />
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