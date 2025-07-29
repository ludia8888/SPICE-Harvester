import React, { memo, useState } from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import {
  Card,
  Icon,
  Tag,
  Intent,
  Popover,
  Menu,
  MenuItem,
  Classes,
  Text,
  Button,
} from '@blueprintjs/core';
import clsx from 'clsx';
import './OntologyNode.scss';

export interface OntologyNodeData {
  label: string;
  description?: string;
  properties: Array<{
    name: string;
    type: string;
    required?: boolean;
  }>;
  isSelected?: boolean;
  icon?: string;
  color?: string;
}

export const OntologyNodeComponent = memo<NodeProps<OntologyNodeData>>(({ 
  data, 
  isConnectable,
  selected 
}) => {
  const [isHovered, setIsHovered] = useState(false);
  const propertyCount = data.properties?.length || 0;
  
  // Determine node color based on properties or custom color
  const nodeColor = data.color || (propertyCount > 5 ? Intent.SUCCESS : 
                                   propertyCount > 2 ? Intent.PRIMARY : 
                                   Intent.NONE);

  return (
    <div 
      className={clsx('ontology-node', {
        'selected': selected || data.isSelected,
        'hovered': isHovered,
        [Classes.DARK]: true
      })}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <Card 
        elevation={selected ? 3 : isHovered ? 2 : 1}
        className="node-card"
        interactive
      >
        {/* Node Header */}
        <div className="node-header">
          <div className="node-icon">
            <Icon 
              icon={data.icon || 'cube'} 
              size={20} 
              intent={nodeColor}
            />
          </div>
          <div className="node-title">
            <Text ellipsize className="node-label">
              {data.label}
            </Text>
            {data.description && (
              <Text className="node-description" ellipsize>
                {data.description}
              </Text>
            )}
          </div>
        </div>

        {/* Property Count Badge */}
        <div className="node-stats">
          <Tag 
            minimal 
            intent={nodeColor}
            icon="properties"
          >
            {propertyCount} properties
          </Tag>
        </div>

        {/* Quick Actions (visible on hover) */}
        {isHovered && (
          <div className="node-actions">
            <Popover
              content={
                <Menu>
                  <MenuItem text="Edit Properties" icon="edit" />
                  <MenuItem text="View Data" icon="eye-open" />
                  <MenuItem text="Duplicate" icon="duplicate" />
                  <MenuItem text="Delete" icon="trash" intent={Intent.DANGER} />
                </Menu>
              }
              position={Position.Bottom}
              minimal
            >
              <Button 
                icon="more" 
                minimal 
                small 
                className="action-button"
              />
            </Popover>
          </div>
        )}

        {/* Connection Handles */}
        <Handle
          type="target"
          position={Position.Top}
          isConnectable={isConnectable}
          className="node-handle handle-target"
        />
        <Handle
          type="source"
          position={Position.Bottom}
          isConnectable={isConnectable}
          className="node-handle handle-source"
        />
        
        {/* Side handles for horizontal connections */}
        <Handle
          type="target"
          position={Position.Left}
          id="left"
          isConnectable={isConnectable}
          className="node-handle handle-target"
        />
        <Handle
          type="source"
          position={Position.Right}
          id="right"
          isConnectable={isConnectable}
          className="node-handle handle-source"
        />
      </Card>

      {/* Selection Indicator */}
      {selected && (
        <div className="selection-indicator">
          <div className="selection-border" />
        </div>
      )}
    </div>
  );
});

OntologyNodeComponent.displayName = 'OntologyNode';