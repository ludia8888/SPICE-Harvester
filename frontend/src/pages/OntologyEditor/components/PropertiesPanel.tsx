import React, { useState } from 'react';
import { 
  Button, 
  ButtonGroup,
  Card, 
  Classes, 
  FormGroup,
  HTMLSelect,
  Icon, 
  InputGroup, 
  Intent, 
  Switch,
  TextArea,
  Tag,
  Divider,
  Tooltip
} from '@blueprintjs/core';
import clsx from 'clsx';
import { useOntologyStore, useSelectedObjectType } from '../../../stores/ontology.store';
import { Property } from '../../../stores/ontology.store';

// Complex data types available in the system
const DATA_TYPES = [
  { value: 'xsd:string', label: 'String', icon: 'text-highlight' },
  { value: 'xsd:integer', label: 'Integer', icon: 'numerical' },
  { value: 'xsd:decimal', label: 'Decimal', icon: 'decimal' },
  { value: 'xsd:boolean', label: 'Boolean', icon: 'tick-circle' },
  { value: 'xsd:date', label: 'Date', icon: 'calendar' },
  { value: 'xsd:dateTime', label: 'DateTime', icon: 'time' },
  { value: 'xsd:anyURI', label: 'URL', icon: 'link' },
  // Complex types
  { value: 'custom:email', label: 'Email', icon: 'envelope' },
  { value: 'custom:phone', label: 'Phone', icon: 'phone' },
  { value: 'custom:money', label: 'Money', icon: 'dollar' },
  { value: 'custom:array', label: 'Array', icon: 'array' },
  { value: 'custom:object', label: 'Object', icon: 'cube' },
];

export const PropertiesPanel: React.FC = () => {
  const selectedObjectType = useSelectedObjectType();
  const { 
    selectedProperty,
    selectProperty,
    addProperty,
    updateProperty,
    deleteProperty,
    isEditing,
    setEditing
  } = useOntologyStore();

  const [editingProperty, setEditingProperty] = useState<Property | null>(null);
  const [isCreating, setIsCreating] = useState(false);

  const handleCreateProperty = () => {
    setIsCreating(true);
    setEditingProperty({
      name: '',
      label: '',
      type: 'xsd:string',
      required: false,
      description: '',
      constraints: {}
    });
    setEditing(true);
  };

  const handleEditProperty = (property: Property) => {
    setEditingProperty({...property});
    selectProperty(property.name);
    setEditing(true);
  };

  const handleSaveProperty = () => {
    if (!editingProperty || !selectedObjectType) return;

    if (isCreating) {
      addProperty(selectedObjectType.id, editingProperty);
    } else {
      updateProperty(selectedObjectType.id, editingProperty.name, editingProperty);
    }

    setEditingProperty(null);
    setIsCreating(false);
    setEditing(false);
  };

  const handleCancelEdit = () => {
    setEditingProperty(null);
    setIsCreating(false);
    setEditing(false);
    selectProperty(null);
  };

  const handleDeleteProperty = (propertyName: string) => {
    if (!selectedObjectType) return;
    
    if (confirm(`Are you sure you want to delete property "${propertyName}"?`)) {
      deleteProperty(selectedObjectType.id, propertyName);
    }
  };

  const updateEditingProperty = (field: keyof Property, value: any) => {
    if (!editingProperty) return;
    
    setEditingProperty({
      ...editingProperty,
      [field]: value
    });
  };

  const getTypeIcon = (type: string) => {
    const dataType = DATA_TYPES.find(dt => dt.value === type);
    return dataType?.icon || 'property';
  };

  const getTypeLabel = (type: string) => {
    const dataType = DATA_TYPES.find(dt => dt.value === type);
    return dataType?.label || type.replace('xsd:', '').replace('custom:', '');
  };

  if (!selectedObjectType) {
    return (
      <div className="properties-panel">
        <div className="empty-state">
          <Icon icon="properties" size={32} />
          <h4>No Object Type Selected</h4>
          <p>Select an object type to view and edit its properties.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="properties-panel">
      <div className="panel-header">
        <div className="header-info">
          <Icon icon="cube" size={16} />
          <span className="object-type-name">{selectedObjectType.label}</span>
          <Tag minimal>{selectedObjectType.properties.length} properties</Tag>
        </div>
        
        <Button
          icon="add"
          text="Add Property"
          intent={Intent.PRIMARY}
          small
          onClick={handleCreateProperty}
          disabled={isEditing}
        />
      </div>

      <div className="properties-list">
        {selectedObjectType.properties.length === 0 ? (
          <div className="empty-properties">
            <Icon icon="properties" size={24} />
            <p>No properties defined</p>
            <Button
              text="Add First Property"
              icon="add"
              intent={Intent.PRIMARY}
              onClick={handleCreateProperty}
            />
          </div>
        ) : (
          selectedObjectType.properties.map(property => (
            <Card 
              key={property.name}
              className={clsx('property-card', {
                'selected': selectedProperty === property.name,
                'editing': editingProperty?.name === property.name
              })}
              interactive={!isEditing}
              onClick={() => !isEditing && selectProperty(property.name)}
            >
              <div className="property-header">
                <div className="property-info">
                  <Icon 
                    icon={getTypeIcon(property.type)} 
                    size={14}
                    intent={property.required ? Intent.WARNING : Intent.NONE}
                  />
                  <span className="property-name">
                    {property.label || property.name}
                  </span>
                  {property.required && (
                    <Tooltip content="Required property">
                      <Icon icon="asterisk" size={10} intent={Intent.WARNING} />
                    </Tooltip>
                  )}
                </div>
                
                <div className="property-actions">
                  <Tag minimal small>
                    {getTypeLabel(property.type)}
                  </Tag>
                  
                  <ButtonGroup minimal>
                    <Tooltip content="Edit property">
                      <Button
                        icon="edit"
                        small
                        onClick={(e) => {
                          e.stopPropagation();
                          handleEditProperty(property);
                        }}
                        disabled={isEditing}
                      />
                    </Tooltip>
                    
                    <Tooltip content="Delete property">
                      <Button
                        icon="trash"
                        small
                        intent={Intent.DANGER}
                        onClick={(e) => {
                          e.stopPropagation();
                          handleDeleteProperty(property.name);
                        }}
                        disabled={isEditing}
                      />
                    </Tooltip>
                  </ButtonGroup>
                </div>
              </div>

              {property.description && (
                <div className="property-description">
                  {property.description}
                </div>
              )}

              {property.constraints && Object.keys(property.constraints).length > 0 && (
                <div className="property-constraints">
                  <Icon icon="settings" size={12} />
                  <span>Has constraints</span>
                </div>
              )}
            </Card>
          ))
        )}
      </div>

      {/* Property Editor */}
      {editingProperty && (
        <div className="property-editor">
          <Card className="editor-card">
            <div className="editor-header">
              <h5>{isCreating ? 'Create Property' : 'Edit Property'}</h5>
              <ButtonGroup>
                <Button
                  text="Cancel"
                  onClick={handleCancelEdit}
                />
                <Button
                  text="Save"
                  intent={Intent.PRIMARY}
                  onClick={handleSaveProperty}
                  disabled={!editingProperty.name || !editingProperty.label}
                />
              </ButtonGroup>
            </div>

            <Divider />

            <div className="editor-form">
              <FormGroup label="Property Name" labelFor="prop-name" required>
                <InputGroup
                  id="prop-name"
                  value={editingProperty.name}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => 
                    updateEditingProperty('name', e.target.value)
                  }
                  placeholder="Enter property name..."
                  disabled={!isCreating} // Can't change name after creation
                />
              </FormGroup>

              <FormGroup label="Display Label" labelFor="prop-label" required>
                <InputGroup
                  id="prop-label"
                  value={editingProperty.label}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => 
                    updateEditingProperty('label', e.target.value)
                  }
                  placeholder="Enter display label..."
                />
              </FormGroup>

              <FormGroup label="Data Type" labelFor="prop-type">
                <HTMLSelect
                  id="prop-type"
                  value={editingProperty.type}
                  onChange={(e: React.ChangeEvent<HTMLSelectElement>) => 
                    updateEditingProperty('type', e.target.value)
                  }
                  fill
                >
                  {DATA_TYPES.map(dataType => (
                    <option key={dataType.value} value={dataType.value}>
                      {dataType.label}
                    </option>
                  ))}
                </HTMLSelect>
              </FormGroup>

              <FormGroup label="Description" labelFor="prop-description">
                <TextArea
                  id="prop-description"
                  value={editingProperty.description || ''}
                  onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => 
                    updateEditingProperty('description', e.target.value)
                  }
                  placeholder="Enter property description..."
                  rows={3}
                  fill
                />
              </FormGroup>

              <Switch
                checked={editingProperty.required}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => 
                  updateEditingProperty('required', e.target.checked)
                }
                label="Required property"
              />

              {/* TODO: Add constraints editor based on type */}
              {editingProperty.type.startsWith('custom:') && (
                <FormGroup label="Constraints" labelFor="prop-constraints">
                  <TextArea
                    id="prop-constraints"
                    value={JSON.stringify(editingProperty.constraints || {}, null, 2)}
                    onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => {
                      try {
                        const constraints = JSON.parse(e.target.value);
                        updateEditingProperty('constraints', constraints);
                      } catch {
                        // Invalid JSON, ignore
                      }
                    }}
                    placeholder="Enter constraints as JSON..."
                    rows={4}
                    fill
                    className={Classes.CODE}
                  />
                </FormGroup>
              )}
            </div>
          </Card>
        </div>
      )}
    </div>
  );
};