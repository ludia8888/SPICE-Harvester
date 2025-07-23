import React, { memo } from 'react';
import {
  EdgeProps,
  getSmoothStepPath,
  EdgeLabelRenderer,
  BaseEdge,
} from 'reactflow';
import {
  Box,
  Typography,
  Chip,
  Paper,
} from '@mui/material';

interface RelationshipEdgeData {
  relationship: {
    predicate: string;
    label: string;
    cardinality: string;
    description?: string;
  };
}

export const RelationshipEdge = memo<EdgeProps<RelationshipEdgeData>>(({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  style = {},
  data,
  selected,
}) => {
  const [edgePath, labelX, labelY] = getSmoothStepPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  const relationship = data?.relationship;
  if (!relationship) return null;

  return (
    <>
      <BaseEdge
        path={edgePath}
        style={{
          ...style,
          strokeWidth: selected ? 3 : 2,
          stroke: selected ? '#1976d2' : '#666',
        }}
        markerEnd={`url(#arrow-${selected ? 'selected' : 'default'})`}
      />
      
      <EdgeLabelRenderer>
        <Paper
          elevation={selected ? 3 : 1}
          sx={{
            position: 'absolute',
            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
            padding: '4px 8px',
            borderRadius: '12px',
            backgroundColor: selected ? '#e3f2fd' : 'white',
            border: selected ? '2px solid #1976d2' : '1px solid #e0e0e0',
            pointerEvents: 'all',
            cursor: 'pointer',
            transition: 'all 0.2s ease-in-out',
            '&:hover': {
              boxShadow: 2,
              backgroundColor: '#f5f5f5',
            },
          }}
          className="nodrag nopan"
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
            <Typography
              variant="caption"
              sx={{
                fontSize: '0.7rem',
                fontWeight: 600,
                color: selected ? '#1976d2' : '#333',
              }}
            >
              {relationship.label}
            </Typography>
            <Chip
              label={relationship.cardinality}
              size="small"
              color={selected ? 'primary' : 'default'}
              sx={{
                fontSize: '0.6rem',
                height: 16,
                '& .MuiChip-label': {
                  px: 0.5,
                },
              }}
            />
          </Box>
        </Paper>
      </EdgeLabelRenderer>

      {/* Custom arrow markers */}
      <defs>
        <marker
          id="arrow-default"
          markerWidth="12"
          markerHeight="12"
          refX="10"
          refY="3"
          orient="auto"
          markerUnits="strokeWidth"
        >
          <path
            d="M0,0 L0,6 L9,3 z"
            fill="#666"
          />
        </marker>
        <marker
          id="arrow-selected"
          markerWidth="12"
          markerHeight="12"
          refX="10"
          refY="3"
          orient="auto"
          markerUnits="strokeWidth"
        >
          <path
            d="M0,0 L0,6 L9,3 z"
            fill="#1976d2"
          />
        </marker>
      </defs>
    </>
  );
});