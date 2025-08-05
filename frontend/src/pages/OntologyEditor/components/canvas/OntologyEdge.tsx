import React, { memo } from 'react';
import { 
  EdgeProps, 
  getBezierPath, 
  EdgeLabelRenderer,
  BaseEdge,
  getStraightPath,
  getSmoothStepPath,
} from 'reactflow';
import { Tag, Intent, Popover, Menu, MenuItem } from '@blueprintjs/core';
import './OntologyEdge.scss';

export interface OntologyEdgeData {
  label: string;
  cardinality?: string;
  description?: string;
  bidirectional?: boolean;
}

interface OntologyEdgeProps extends EdgeProps<OntologyEdgeData> {
  onEdit?: () => void;
  onDelete?: () => void;
  onReverse?: () => void;
  onMakeBidirectional?: () => void;
}

export const OntologyEdgeComponent = memo<OntologyEdgeProps>(({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
  selected,
  markerEnd,
  style = {},
  onEdit,
  onDelete,
  onReverse,
  onMakeBidirectional,
}) => {
  // Calculate path based on node positions
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  // Determine edge color based on state
  const edgeColor = selected ? '#48aff0' : '#5c7080';
  const strokeWidth = selected ? 3 : 2;

  return (
    <>
      <BaseEdge 
        id={id}
        path={edgePath} 
        markerEnd={markerEnd}
        style={{
          ...style,
          stroke: edgeColor,
          strokeWidth,
          strokeDasharray: data?.bidirectional ? '5, 5' : undefined,
        }}
      />
      
      <EdgeLabelRenderer>
        <div
          style={{
            position: 'absolute',
            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
            pointerEvents: 'all',
          }}
          className="edge-label-container"
        >
          <Popover
            content={
              <Menu>
                <MenuItem 
                  text="Edit Relationship" 
                  icon="edit" 
                  onClick={onEdit}
                />
                <MenuItem 
                  text="Reverse Direction" 
                  icon="swap-horizontal" 
                  onClick={onReverse}
                />
                <MenuItem 
                  text={data?.bidirectional ? "Make Unidirectional" : "Make Bidirectional"} 
                  icon="exchange" 
                  onClick={onMakeBidirectional}
                />
                <MenuItem 
                  text="Delete" 
                  icon="trash" 
                  intent={Intent.DANGER} 
                  onClick={onDelete}
                />
              </Menu>
            }
            minimal
          >
            <div className={`edge-label ${selected ? 'selected' : ''}`}>
              <span className="edge-label-text">{data?.label || 'relates_to'}</span>
              {data?.cardinality && (
                <Tag 
                  minimal 
                  className="cardinality-tag"
                  intent={selected ? Intent.PRIMARY : Intent.NONE}
                >
                  {data.cardinality}
                </Tag>
              )}
            </div>
          </Popover>
        </div>
      </EdgeLabelRenderer>
    </>
  );
});

OntologyEdgeComponent.displayName = 'OntologyEdge';