import React, { useState, useEffect } from 'react';
import { 
  Button, 
  ButtonGroup,
  Card, 
  Classes,
  Dialog,
  FormGroup,
  HTMLSelect,
  Icon, 
  InputGroup, 
  Intent, 
  Switch,
  Tab,
  Tabs,
  Tag,
  TextArea,
  HTMLTable
} from '@blueprintjs/core';
import clsx from 'clsx';
import { useOntologyStore } from '../../../stores/ontology.store';
import { ontologyApi, OntologyApiError } from '../../../api/ontologyClient';
import { ObjectType, Property } from '../../../stores/ontology.store';
import { extractDatabaseName } from '../../../utils/database';

// Data types available in the system (from documentation)
const DATA_TYPES = [
  // Basic XSD types
  { value: 'xsd:string', label: 'String', category: 'basic' },
  { value: 'xsd:integer', label: 'Integer', category: 'basic' },
  { value: 'xsd:decimal', label: 'Decimal', category: 'basic' },
  { value: 'xsd:boolean', label: 'Boolean', category: 'basic' },
  { value: 'xsd:date', label: 'Date', category: 'basic' },
  { value: 'xsd:dateTime', label: 'DateTime', category: 'basic' },
  { value: 'xsd:time', label: 'Time', category: 'basic' },
  { value: 'xsd:float', label: 'Float', category: 'basic' },
  { value: 'xsd:double', label: 'Double', category: 'basic' },
  { value: 'xsd:anyURI', label: 'URI', category: 'basic' },
  
  // Complex types from SPICE HARVESTER system
  { value: 'custom:email', label: 'Email', category: 'complex' },
  { value: 'custom:phone', label: 'Phone', category: 'complex' },
  { value: 'custom:money', label: 'Money', category: 'complex' },
  { value: 'custom:array', label: 'Array', category: 'complex' },
  { value: 'custom:object', label: 'Object', category: 'complex' },
  { value: 'custom:coordinates', label: 'Coordinates', category: 'complex' },
  { value: 'custom:file', label: 'File', category: 'complex' },
  { value: 'custom:json', label: 'JSON', category: 'complex' },
];

interface ObjectTypeEditorProps {
  isOpen: boolean;
  onClose: () => void;
  objectType?: ObjectType | null; // null for create, ObjectType for edit
}

export const ObjectTypeEditor: React.FC<ObjectTypeEditorProps> = ({
  isOpen,
  onClose,
  objectType
}) => {
  const {
    currentDatabase,
    addObjectType,
    updateObjectType,
    setError,
    clearError
  } = useOntologyStore();

  const [selectedTab, setSelectedTab] = useState<'general' | 'properties' | 'validation'>('general');
  const [isSaving, setIsSaving] = useState(false);
  
  // Form state
  const [formData, setFormData] = useState<{
    id: string;
    label: string;
    description: string;
    properties: Property[];
  }>({
    id: '',
    label: '',
    description: '',
    properties: []
  });

  // Property editing state
  const [editingProperty, setEditingProperty] = useState<Property | null>(null);
  const [isEditingProperty, setIsEditingProperty] = useState(false);

  // Initialize form data
  useEffect(() => {
    if (objectType) {
      setFormData({
        id: objectType.id,
        label: objectType.label,
        description: objectType.description || '',
        properties: [...objectType.properties]
      });
    } else {
      setFormData({
        id: '',
        label: '',
        description: '',
        properties: []
      });
    }
    setSelectedTab('general');
    clearError('object-type-editor');
  }, [objectType, isOpen]);



  const isCreateMode = !objectType;

  const handleSave = async () => {
    if (!currentDatabase) return;

    // Validation
    if (!formData.id.trim() || !formData.label.trim()) {
      setError('object-type-editor', 'ID and Label are required');
      return;
    }

    if (formData.properties.length === 0) {
      setError('object-type-editor', 'At least one property is required');
      return;
    }

    const primaryKeyProps = formData.properties.filter(p => 
      p.constraints?.primary_key === true
    );
    
    if (primaryKeyProps.length === 0) {
      setError('object-type-editor', 'At least one property must be marked as primary key');
      return;
    }

    setIsSaving(true);
    clearError('object-type-editor');

    try {
      const objectTypeData: ObjectType = {
        id: formData.id,
        label: formData.label,
        description: formData.description,
        properties: formData.properties
      };

      const dbName = extractDatabaseName(currentDatabase);
      if (!dbName) {
        setError('save', 'No database selected');
        return;
      }

      if (isCreateMode) {
        const created = await ontologyApi.objectType.create(dbName, objectTypeData);
        addObjectType(created);
      } else {
        const updated = await ontologyApi.objectType.update(dbName, formData.id, objectTypeData);
        updateObjectType(formData.id, updated);
      }

      onClose();
    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : `Failed to ${isCreateMode ? 'create' : 'update'} object type`;
      setError('object-type-editor', message);
    } finally {
      setIsSaving(false);
    }
  };

  const handlePropertyAdd = () => {
    setEditingProperty({
      name: '',
      label: '',
      type: 'xsd:string',
      required: false,
      constraints: {}
    });
    setIsEditingProperty(true);
  };

  const handlePropertyEdit = (property: Property) => {
    setEditingProperty({...property});
    setIsEditingProperty(true);
  };

  const handlePropertySave = () => {
    if (!editingProperty || !editingProperty.name.trim() || !editingProperty.label.trim()) {
      return;
    }

    const existingIndex = formData.properties.findIndex(p => p.name === editingProperty.name);
    
    if (existingIndex >= 0) {
      // Update existing
      const updatedProperties = [...formData.properties];
      updatedProperties[existingIndex] = editingProperty;
      setFormData({
        ...formData,
        properties: updatedProperties
      });
    } else {
      // Add new
      setFormData({
        ...formData,
        properties: [...formData.properties, editingProperty]
      });
    }

    setEditingProperty(null);
    setIsEditingProperty(false);
  };

  const handlePropertyDelete = (propertyName: string) => {
    if (confirm(`Are you sure you want to delete property "${propertyName}"?`)) {
      setFormData({
        ...formData,
        properties: formData.properties.filter(p => p.name !== propertyName)
      });
    }
  };

  const handlePropertyCancel = () => {
    setEditingProperty(null);
    setIsEditingProperty(false);
  };

  const renderGeneralTab = () => (
    <div className="general-tab">
      <FormGroup label="Object Type ID" labelFor="obj-id">
        <InputGroup
          id="obj-id"
          value={formData.id}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => 
            setFormData({...formData, id: e.target.value})
          }
          placeholder="Enter unique identifier..."
          disabled={!isCreateMode} // Can't change ID after creation
        />
      </FormGroup>

      <FormGroup label="Display Label" labelFor="obj-label">
        <InputGroup
          id="obj-label"
          value={formData.label}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => 
            setFormData({...formData, label: e.target.value})
          }
          placeholder="Enter display name..."
        />
      </FormGroup>

      <FormGroup label="Description" labelFor="obj-description">
        <TextArea
          id="obj-description"
          value={formData.description}
          onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => 
            setFormData({...formData, description: e.target.value})
          }
          placeholder="Enter description..."
          rows={4}
          fill
        />
      </FormGroup>
    </div>
  );

  const renderPropertiesTab = () => (
    <div className="properties-tab">
      <div className="properties-toolbar">
        <div className="properties-info">
          <span>{formData.properties.length} properties defined</span>
        </div>
        <Button
          text="Add Property"
          icon="add"
          intent={Intent.PRIMARY}
          onClick={handlePropertyAdd}
          disabled={isEditingProperty}
        />
      </div>

      {formData.properties.length === 0 ? (
        <div className="empty-properties">
          <Icon icon="properties" size={32} />
          <h4>No Properties</h4>
          <p>Add properties to define the structure of this object type.</p>
          <Button
            text="Add First Property"
            icon="add"
            intent={Intent.PRIMARY}
            onClick={handlePropertyAdd}
          />
        </div>
      ) : (
        <HTMLTable className="properties-table" striped>
          <thead>
            <tr>
              <th>Name</th>
              <th>Label</th>
              <th>Type</th>
              <th>Required</th>
              <th>Primary Key</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {formData.properties.map(property => (
              <tr key={property.name}>
                <td>
                  <code>{property.name}</code>
                </td>
                <td>{property.label}</td>
                <td>
                  <Tag minimal>
                    {DATA_TYPES.find(dt => dt.value === property.type)?.label || property.type}
                  </Tag>
                </td>
                <td>
                  {property.required && (
                    <Icon icon="tick" intent={Intent.SUCCESS} />
                  )}
                </td>
                <td>
                  {property.constraints?.primary_key && (
                    <Icon icon="key" intent={Intent.WARNING} />
                  )}
                </td>
                <td>
                  <ButtonGroup minimal>
                    <Button
                      icon="edit"
                      small
                      onClick={() => handlePropertyEdit(property)}
                      disabled={isEditingProperty}
                    />
                    <Button
                      icon="trash"
                      small
                      intent={Intent.DANGER}
                      onClick={() => handlePropertyDelete(property.name)}
                      disabled={isEditingProperty}
                    />
                  </ButtonGroup>
                </td>
              </tr>
            ))}
          </tbody>
        </HTMLTable>
      )}

      {/* Property Editor Dialog */}
      <Dialog
        isOpen={isEditingProperty}
        onClose={handlePropertyCancel}
        title={editingProperty ? 
          (formData.properties.some(p => p.name === editingProperty.name) ? 'Edit Property' : 'Add Property')
          : 'Add Property'
        }
        className="property-editor-dialog"
        canEscapeKeyClose={true}
        canOutsideClickClose={true}
        enforceFocus={false}
        autoFocus={false}
      >
        <div className={Classes.DIALOG_BODY}>
          {editingProperty && (
            <div className="property-form">
              <FormGroup label="Property Name" labelFor="prop-name">
                <InputGroup
                  id="prop-name"
                  value={editingProperty.name}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => 
                    setEditingProperty({...editingProperty, name: e.target.value})
                  }
                  onKeyDown={(e) => {
                    // Prevent dialog from intercepting keyboard events
                    e.stopPropagation();
                  }}
                  placeholder="Enter property name..."
                />
              </FormGroup>

              <FormGroup label="Display Label" labelFor="prop-label">
                <InputGroup
                  id="prop-label"
                  value={editingProperty.label}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => 
                    setEditingProperty({...editingProperty, label: e.target.value})
                  }
                  onKeyDown={(e) => {
                    // Prevent dialog from intercepting keyboard events
                    e.stopPropagation();
                  }}
                  placeholder="Enter display label..."
                />
              </FormGroup>

              <FormGroup label="Data Type" labelFor="prop-type">
                <HTMLSelect
                  id="prop-type"
                  value={editingProperty.type}
                  onChange={(e: React.ChangeEvent<HTMLSelectElement>) => 
                    setEditingProperty({...editingProperty, type: e.target.value})
                  }
                  fill
                >
                  <optgroup label="Basic Types">
                    {DATA_TYPES.filter(dt => dt.category === 'basic').map(dataType => (
                      <option key={dataType.value} value={dataType.value}>
                        {dataType.label}
                      </option>
                    ))}
                  </optgroup>
                  <optgroup label="Complex Types">
                    {DATA_TYPES.filter(dt => dt.category === 'complex').map(dataType => (
                      <option key={dataType.value} value={dataType.value}>
                        {dataType.label}
                      </option>
                    ))}
                  </optgroup>
                </HTMLSelect>
              </FormGroup>

              <FormGroup label="Description" labelFor="prop-desc">
                <TextArea
                  id="prop-desc"
                  value={editingProperty.description || ''}
                  onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => 
                    setEditingProperty({...editingProperty, description: e.target.value})
                  }
                  onKeyDown={(e) => {
                    // Prevent dialog from intercepting keyboard events
                    e.stopPropagation();
                  }}
                  placeholder="Enter property description..."
                  rows={3}
                  fill
                />
              </FormGroup>

              <div className="property-options">
                <Switch
                  checked={editingProperty.required}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => 
                    setEditingProperty({...editingProperty, required: e.target.checked})
                  }
                  label="Required property"
                />

                <Switch
                  checked={editingProperty.constraints?.primary_key === true}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => 
                    setEditingProperty({
                      ...editingProperty, 
                      constraints: {
                        ...editingProperty.constraints,
                        primary_key: e.target.checked
                      }
                    })
                  }
                  label="Primary key"
                />
              </div>

              {/* Complex type constraints */}
              {editingProperty.type.startsWith('custom:') && (
                <FormGroup label="Type Constraints" labelFor="prop-constraints">
                  <TextArea
                    id="prop-constraints"
                    value={JSON.stringify(editingProperty.constraints || {}, null, 2)}
                    onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => {
                      try {
                        const constraints = JSON.parse(e.target.value);
                        setEditingProperty({...editingProperty, constraints});
                      } catch {
                        // Invalid JSON, ignore
                      }
                    }}
                    onKeyDown={(e) => {
                      // Prevent dialog from intercepting keyboard events
                      e.stopPropagation();
                    }}
                    placeholder="Enter constraints as JSON..."
                    rows={4}
                    fill
                    className={Classes.CODE}
                  />
                </FormGroup>
              )}
            </div>
          )}
        </div>
        
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button text="Cancel" onClick={handlePropertyCancel} />
            <Button
              text="Save Property"
              intent={Intent.PRIMARY}
              onClick={handlePropertySave}
              disabled={!editingProperty?.name.trim() || !editingProperty?.label.trim()}
            />
          </div>
        </div>
      </Dialog>
    </div>
  );

  const renderValidationTab = () => (
    <div className="validation-tab">
      <Card>
        <h5>Validation Summary</h5>
        
        <div className="validation-checks">
          <div className={clsx('validation-item', {
            'valid': formData.id.trim() && formData.label.trim(),
            'invalid': !formData.id.trim() || !formData.label.trim()
          })}>
            <Icon icon={formData.id.trim() && formData.label.trim() ? "tick" : "cross"} />
            <span>Basic information complete</span>
          </div>

          <div className={clsx('validation-item', {
            'valid': formData.properties.length > 0,
            'invalid': formData.properties.length === 0
          })}>
            <Icon icon={formData.properties.length > 0 ? "tick" : "cross"} />
            <span>Has properties ({formData.properties.length})</span>
          </div>

          <div className={clsx('validation-item', {
            'valid': formData.properties.some(p => p.constraints?.primary_key),
            'invalid': !formData.properties.some(p => p.constraints?.primary_key)
          })}>
            <Icon icon={formData.properties.some(p => p.constraints?.primary_key) ? "tick" : "cross"} />
            <span>Has primary key</span>
          </div>

          <div className={clsx('validation-item', {
            'valid': formData.properties.some(p => p.required),
            'warning': !formData.properties.some(p => p.required)
          })}>
            <Icon icon={formData.properties.some(p => p.required) ? "tick" : "warning-sign"} />
            <span>Has required properties (recommended)</span>
          </div>
        </div>
      </Card>
    </div>
  );

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title={
        <div className="dialog-title">
          <Icon icon="cube" />
          <span>{isCreateMode ? 'Create Object Type' : `Edit ${objectType?.label}`}</span>
        </div>
      }
      className="object-type-editor-dialog"
      style={{ width: '800px', maxHeight: '90vh' }}
      canEscapeKeyClose={true}
      canOutsideClickClose={true}
      enforceFocus={false}
      autoFocus={false}
    >
      <div className={Classes.DIALOG_BODY}>
        <Tabs
          id="object-type-tabs"
          selectedTabId={selectedTab}
          onChange={(tabId) => setSelectedTab(tabId as any)}
        >
          <Tab
            id="general"
            title="General"
            panel={renderGeneralTab()}
          />
          <Tab
            id="properties"
            title={
              <span>
                Properties
                {formData.properties.length > 0 && (
                  <Tag minimal round className="tab-badge">
                    {formData.properties.length}
                  </Tag>
                )}
              </span>
            }
            panel={renderPropertiesTab()}
          />
          <Tab
            id="validation"
            title="Validation"
            panel={renderValidationTab()}
          />
        </Tabs>
      </div>
      
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Cancel" onClick={onClose} disabled={isSaving} />
          <Button
            text={isCreateMode ? 'Create Object Type' : 'Save Changes'}
            intent={Intent.PRIMARY}
            onClick={handleSave}
            disabled={
              !formData.id.trim() || 
              !formData.label.trim() || 
              formData.properties.length === 0 ||
              !formData.properties.some(p => p.constraints?.primary_key) ||
              isSaving ||
              isEditingProperty
            }
            loading={isSaving}
          />
        </div>
      </div>
    </Dialog>
  );
};