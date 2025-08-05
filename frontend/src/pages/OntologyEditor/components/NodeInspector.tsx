import React, { useState, useEffect } from 'react';
import {
  Drawer,
  Classes,
  Button,
  FormGroup,
  InputGroup,
  TextArea,
  Divider,
  H4,
  Tag,
  Intent,
  Card,
  Icon,
  ButtonGroup,
  Switch,
  HTMLSelect,
  Collapse,
  Menu,
  MenuItem,
  Position,
  Popover,
  Tooltip,
} from '@blueprintjs/core';
import { Node } from 'reactflow';
import { OntologyNodeData } from './canvas/OntologyNode';
import './NodeInspector.scss';

interface NodeInspectorProps {
  node: Node<OntologyNodeData>;
  isOpen: boolean;
  onClose: () => void;
  onUpdate: (node: Node<OntologyNodeData>) => void;
}

interface Property {
  id: string;
  name: string;
  label: string;
  type: string;
  required: boolean;
  description?: string;
  constraints?: Record<string, any>;
}

const DATA_TYPES = [
  { value: 'string', label: 'Text', icon: 'font' },
  { value: 'integer', label: 'Number', icon: 'numerical' },
  { value: 'decimal', label: 'Decimal', icon: 'floating-point' },
  { value: 'boolean', label: 'Yes/No', icon: 'segmented-control' },
  { value: 'date', label: 'Date', icon: 'calendar' },
  { value: 'datetime', label: 'Date & Time', icon: 'time' },
  { value: 'object', label: 'Object', icon: 'cube' },
  { value: 'array', label: 'List', icon: 'list' },
];

export const NodeInspector: React.FC<NodeInspectorProps> = ({
  node,
  isOpen,
  onClose,
  onUpdate,
}) => {
  const [nodeData, setNodeData] = useState<OntologyNodeData>(node.data);
  const [properties, setProperties] = useState<Property[]>([]);
  const [expandedProperties, setExpandedProperties] = useState<Set<string>>(new Set());
  const [hasChanges, setHasChanges] = useState(false);

  useEffect(() => {
    setNodeData(node.data);
    // Convert properties to editable format
    const props = node.data.properties?.map((p, idx) => ({
      id: `prop-${idx}`,
      name: p.name,
      label: p.name.charAt(0).toUpperCase() + p.name.slice(1),
      type: p.type,
      required: p.required || false,
      description: '',
      constraints: {},
    })) || [];
    setProperties(props);
  }, [node]);

  const handleNodeDataChange = (field: keyof OntologyNodeData, value: any) => {
    setNodeData(prev => ({ ...prev, [field]: value }));
    setHasChanges(true);
  };

  const handleAddProperty = () => {
    const newProperty: Property = {
      id: `prop-${Date.now()}`,
      name: `property_${properties.length + 1}`,
      label: `Property ${properties.length + 1}`,
      type: 'string',
      required: false,
    };
    setProperties([...properties, newProperty]);
    setExpandedProperties(prev => new Set(prev).add(newProperty.id));
    setHasChanges(true);
  };

  const handlePropertyChange = (propId: string, field: keyof Property, value: any) => {
    setProperties(prev =>
      prev.map(p => (p.id === propId ? { ...p, [field]: value } : p))
    );
    setHasChanges(true);
  };

  const handleDeleteProperty = (propId: string) => {
    setProperties(prev => prev.filter(p => p.id !== propId));
    setHasChanges(true);
  };

  const handleSave = () => {
    const updatedNode = {
      ...node,
      data: {
        ...nodeData,
        properties: properties.map(p => ({
          name: p.name,
          type: p.type,
          required: p.required,
        })),
      },
    };
    onUpdate(updatedNode);
    setHasChanges(false);
    // TODO: Call API to update object type
  };

  const togglePropertyExpansion = (propId: string) => {
    setExpandedProperties(prev => {
      const next = new Set(prev);
      if (next.has(propId)) {
        next.delete(propId);
      } else {
        next.add(propId);
      }
      return next;
    });
  };

  return (
    <Drawer
      isOpen={isOpen}
      onClose={onClose}
      position={Position.RIGHT}
      size="400px"
      canEscapeKeyClose={true}
      canOutsideClickClose={true}
      title={
        <div className="inspector-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Icon icon="cube" size={20} />
            <span>Object Type Properties</span>
          </div>
          <Button
            icon="cross"
            minimal
            onClick={onClose}
            title="닫기 (ESC)"
          />
        </div>
      }
      className={`node-inspector ${Classes.DARK}`}
    >
      <div className={Classes.DRAWER_BODY}>
        <div className="inspector-content">
          {/* Basic Information */}
          <Card className="inspector-section">
            <H4>Basic Information</H4>
            
            <FormGroup label="Label" labelFor="node-label">
              <InputGroup
                id="node-label"
                value={nodeData.label}
                onChange={(e) => handleNodeDataChange('label', e.target.value)}
                onKeyDown={(e) => e.stopPropagation()}
                placeholder="Object type name"
              />
            </FormGroup>
            
            <FormGroup label="Description" labelFor="node-description">
              <TextArea
                id="node-description"
                value={nodeData.description || ''}
                onChange={(e) => handleNodeDataChange('description', e.target.value)}
                onKeyDown={(e) => e.stopPropagation()}
                placeholder="Describe this object type..."
                rows={3}
                fill
              />
            </FormGroup>
            
            <FormGroup label="Icon" labelFor="node-icon">
              <Popover
                content={
                  <Menu>
                    {['cube', 'person', 'document', 'folder-close', 'database', 'globe'].map(icon => (
                      <MenuItem
                        key={icon}
                        text={icon}
                        icon={icon as any}
                        onClick={() => handleNodeDataChange('icon', icon)}
                      />
                    ))}
                  </Menu>
                }
                position={Position.BOTTOM}
              >
                <Button
                  id="node-icon"
                  text={nodeData.icon || 'cube'}
                  icon={nodeData.icon as any || 'cube'}
                  rightIcon="caret-down"
                  fill
                />
              </Popover>
            </FormGroup>
          </Card>

          {/* Properties */}
          <Card className="inspector-section">
            <div className="section-header">
              <H4>Properties ({properties.length})</H4>
              <Button
                icon="add"
                text="Add Property"
                intent={Intent.PRIMARY}
                small
                onClick={handleAddProperty}
              />
            </div>
            
            <div className="properties-list">
              {properties.map((property) => (
                <Card
                  key={property.id}
                  className="property-card"
                  interactive
                >
                  <div className="property-header" onClick={() => togglePropertyExpansion(property.id)}>
                    <div className="property-info">
                      <Icon 
                        icon={expandedProperties.has(property.id) ? 'chevron-down' : 'chevron-right'} 
                        size={12}
                      />
                      <span className="property-name">{property.label}</span>
                      <Tag minimal intent={property.required ? Intent.SUCCESS : Intent.NONE}>
                        {property.type}
                      </Tag>
                      {property.required && (
                        <Tag minimal intent={Intent.WARNING}>Required</Tag>
                      )}
                    </div>
                    <Button
                      icon="trash"
                      minimal
                      small
                      intent={Intent.DANGER}
                      onClick={(e) => {
                        e.stopPropagation();
                        handleDeleteProperty(property.id);
                      }}
                    />
                  </div>
                  
                  <Collapse isOpen={expandedProperties.has(property.id)}>
                    <div className="property-details" onClick={(e) => e.stopPropagation()}>
                      <FormGroup label="Property Name" labelFor={`prop-name-${property.id}`}>
                        <InputGroup
                          id={`prop-name-${property.id}`}
                          value={property.name}
                          onChange={(e) => handlePropertyChange(property.id, 'name', e.target.value)}
                          onKeyDown={(e) => e.stopPropagation()}
                          placeholder="property_name"
                          small
                        />
                      </FormGroup>
                      
                      <FormGroup label="Display Label" labelFor={`prop-label-${property.id}`}>
                        <InputGroup
                          id={`prop-label-${property.id}`}
                          value={property.label}
                          onChange={(e) => handlePropertyChange(property.id, 'label', e.target.value)}
                          onKeyDown={(e) => e.stopPropagation()}
                          placeholder="Property Label"
                          small
                        />
                      </FormGroup>
                      
                      <FormGroup label="Data Type" labelFor={`prop-type-${property.id}`}>
                        <HTMLSelect
                          id={`prop-type-${property.id}`}
                          value={property.type}
                          onChange={(e) => handlePropertyChange(property.id, 'type', e.target.value)}
                          fill
                        >
                          {DATA_TYPES.map(type => (
                            <option key={type.value} value={type.value}>
                              {type.label}
                            </option>
                          ))}
                        </HTMLSelect>
                      </FormGroup>
                      
                      <Switch
                        label="Required field"
                        checked={property.required}
                        onChange={(e) => handlePropertyChange(property.id, 'required', e.currentTarget.checked)}
                      />
                    </div>
                  </Collapse>
                </Card>
              ))}
            </div>
          </Card>
          
          {/* Advanced Settings */}
          <Card className="inspector-section">
            <H4>Advanced Settings</H4>
            <p className="help-text">
              Additional configuration options will be available here.
            </p>
          </Card>
        </div>
      </div>
      
      <div className={Classes.DRAWER_FOOTER}>
        <ButtonGroup fill>
          <Button text="Cancel" onClick={onClose} />
          <Button
            text="Save Changes"
            intent={Intent.PRIMARY}
            onClick={handleSave}
            disabled={!hasChanges}
          />
        </ButtonGroup>
      </div>
    </Drawer>
  );
};