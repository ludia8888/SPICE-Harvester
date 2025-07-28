import React, { useState } from 'react';
import { 
  Button, 
  ButtonGroup,
  Card, 
  Classes, 
  Icon, 
  InputGroup, 
  Intent, 
  Menu, 
  MenuItem, 
  Popover, 
  Position,
  Tag,
  Tooltip,
  Tree,
  TreeNodeInfo
} from '@blueprintjs/core';
import clsx from 'clsx';
import { useOntologyStore, useFilteredObjectTypes } from '../../../stores/ontology.store';
import { ObjectType } from '../../../stores/ontology.store';
import { ObjectTypeEditor } from './ObjectTypeEditor';

export const ObjectTypePanel: React.FC = () => {
  const filteredObjectTypes = useFilteredObjectTypes();
  const { 
    selectedObjectType, 
    selectObjectType, 
    searchQuery, 
    setSearchQuery 
  } = useOntologyStore();
  
  const [expandedNodes, setExpandedNodes] = useState<{ [key: string]: boolean }>({});
  const [isEditorOpen, setIsEditorOpen] = useState(false);
  const [editingObjectType, setEditingObjectType] = useState<ObjectType | null>(null);

  const handleNodeClick = (nodeData: TreeNodeInfo) => {
    const objectTypeId = nodeData.id as string;
    selectObjectType(objectTypeId);
  };

  const handleNodeExpand = (nodeData: TreeNodeInfo) => {
    setExpandedNodes({
      ...expandedNodes,
      [nodeData.id as string]: true
    });
  };

  const handleNodeCollapse = (nodeData: TreeNodeInfo) => {
    setExpandedNodes({
      ...expandedNodes,
      [nodeData.id as string]: false
    });
  };

  const transformObjectTypesToNodes = (objectTypes: ObjectType[]): TreeNodeInfo[] => {
    return objectTypes.map(objectType => ({
      id: objectType.id,
      label: (
        <div className="object-type-node">
          <div className="node-header">
            <Icon icon="cube" size={14} />
            <span className="node-label">{objectType.label}</span>
            {objectType.properties.length > 0 && (
              <Tag minimal round>
                {objectType.properties.length}
              </Tag>
            )}
          </div>
          {objectType.description && (
            <div className="node-description">
              {objectType.description}
            </div>
          )}
        </div>
      ),
      isSelected: selectedObjectType === objectType.id,
      isExpanded: expandedNodes[objectType.id],
      childNodes: objectType.properties.map(property => ({
        id: `${objectType.id}.${property.name}`,
        label: (
          <div className="property-node">
            <Icon 
              icon={property.required ? "key" : "property"} 
              size={12}
              intent={property.required ? Intent.WARNING : Intent.NONE}
            />
            <span>{property.label || property.name}</span>
            <Tag minimal className="property-type">
              {property.type.replace('xsd:', '').replace('custom:', '')}
            </Tag>
          </div>
        ),
        secondaryLabel: property.required ? (
          <Tooltip content="Required property">
            <Icon icon="asterisk" size={10} intent={Intent.WARNING} />
          </Tooltip>
        ) : undefined,
      }))
    }));
  };

  const nodes = transformObjectTypesToNodes(filteredObjectTypes);

  return (
    <div className="object-type-panel">
      <div className="panel-toolbar">
        <InputGroup
          leftIcon="search"
          placeholder="Search object types..."
          value={searchQuery}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSearchQuery(e.target.value)}
          small
          fill
        />
        
        <ButtonGroup minimal>
          <Tooltip content="Create new object type">
            <Button
              icon="add"
              intent={Intent.PRIMARY}
              small
              onClick={() => {
                setEditingObjectType(null);
                setIsEditorOpen(true);
              }}
            />
          </Tooltip>
          
          <Popover
            content={
              <Menu>
                <MenuItem text="Sort by Name" icon="sort-alphabetical" />
                <MenuItem text="Sort by Modified" icon="sort-numerical" />
                <MenuItem text="Sort by Properties" icon="sort-desc" />
              </Menu>
            }
            position={Position.BOTTOM_RIGHT}
          >
            <Button icon="sort" small />
          </Popover>
        </ButtonGroup>
      </div>

      <div className="panel-content">
        {nodes.length === 0 ? (
          <div className="empty-state">
            <Icon icon="cube" size={32} />
            <h4>No Object Types</h4>
            <p>
              {searchQuery 
                ? `No object types match "${searchQuery}"`
                : 'Create your first object type to get started'
              }
            </p>
            <Button
              text="Create Object Type"
              icon="add"
              intent={Intent.PRIMARY}
              onClick={() => {
                setEditingObjectType(null);
                setIsEditorOpen(true);
              }}
            />
          </div>
        ) : (
          <Tree
            contents={nodes}
            onNodeClick={handleNodeClick}
            onNodeExpand={handleNodeExpand}
            onNodeCollapse={handleNodeCollapse}
            className={Classes.ELEVATION_0}
          />
        )}
      </div>

      {selectedObjectType && (
        <div className="selected-object-details">
          <Card className="details-card">
            <h5>Object Type Details</h5>
            {/* TODO: Add detailed view of selected object type */}
            <p>Selected: {selectedObjectType}</p>
            <ButtonGroup>
              <Button 
                text="Edit" 
                icon="edit" 
                small 
                onClick={() => {
                  const objectType = filteredObjectTypes.find(obj => obj.id === selectedObjectType);
                  if (objectType) {
                    setEditingObjectType(objectType);
                    setIsEditorOpen(true);
                  }
                }}
              />
              <Button 
                text="Delete" 
                icon="trash" 
                small 
                intent={Intent.DANGER}
                onClick={() => {
                  if (selectedObjectType && confirm(`Are you sure you want to delete "${selectedObjectType}"?`)) {
                    // TODO: Implement delete
                    console.log('Delete object type:', selectedObjectType);
                  }
                }}
              />
            </ButtonGroup>
          </Card>
        </div>
      )}

      {/* Object Type Editor Dialog */}
      <ObjectTypeEditor
        isOpen={isEditorOpen}
        onClose={() => {
          setIsEditorOpen(false);
          setEditingObjectType(null);
        }}
        objectType={editingObjectType}
      />
    </div>
  );
};