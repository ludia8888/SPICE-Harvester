import React, { memo } from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import {
  Card,
  CardContent,
  Typography,
  Chip,
  Box,
  IconButton,
  Avatar,
  Tooltip,
} from '@mui/material';
import {
  AccountTree,
  Edit as EditIcon,
  Visibility as ViewIcon,
  MoreVert as MoreIcon,
} from '@mui/icons-material';
import { OntologyClass } from '../../../types/ontology';

interface ObjectTypeNodeData {
  class: OntologyClass;
  onEdit?: () => void;
  onView?: () => void;
}

export const ObjectTypeNode = memo<NodeProps<ObjectTypeNodeData>>(({ data, selected }) => {
  const { class: ontologyClass, onEdit, onView } = data;

  return (
    <>
      {/* Connection handles */}
      <Handle
        type="target"
        position={Position.Top}
        style={{
          background: '#1976d2',
          width: 8,
          height: 8,
        }}
      />
      <Handle
        type="source"
        position={Position.Bottom}
        style={{
          background: '#1976d2',
          width: 8,
          height: 8,
        }}
      />

      <Card
        sx={{
          width: 250,
          minHeight: 150,
          border: selected ? '2px solid #1976d2' : '1px solid #e0e0e0',
          boxShadow: selected ? 3 : 1,
          transition: 'all 0.2s ease-in-out',
          '&:hover': {
            boxShadow: 3,
            transform: 'translateY(-2px)',
          },
        }}
      >
        <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
          {/* Header */}
          <Box sx={{ display: 'flex', alignItems: 'center', mb: 1.5 }}>
            <Avatar
              sx={{
                bgcolor: 'primary.main',
                width: 32,
                height: 32,
                mr: 1.5,
              }}
            >
              <AccountTree fontSize="small" />
            </Avatar>
            <Box sx={{ flexGrow: 1, minWidth: 0 }}>
              <Typography
                variant="h6"
                sx={{
                  fontSize: '0.9rem',
                  fontWeight: 600,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {ontologyClass.label}
              </Typography>
              <Typography
                variant="caption"
                color="text.secondary"
                sx={{
                  display: 'block',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {ontologyClass.id}
              </Typography>
            </Box>
            <Box>
              <Tooltip title="편집">
                <IconButton size="small" onClick={onEdit}>
                  <EditIcon fontSize="small" />
                </IconButton>
              </Tooltip>
            </Box>
          </Box>

          {/* Status chips */}
          <Box sx={{ display: 'flex', gap: 0.5, mb: 1.5, flexWrap: 'wrap' }}>
            {ontologyClass.abstract && (
              <Chip
                label="Abstract"
                size="small"
                color="warning"
                sx={{ fontSize: '0.65rem', height: 20 }}
              />
            )}
            <Chip
              label={`${ontologyClass.properties.length} props`}
              size="small"
              variant="outlined"
              sx={{ fontSize: '0.65rem', height: 20 }}
            />
            {ontologyClass.relationships.length > 0 && (
              <Chip
                label={`${ontologyClass.relationships.length} rels`}
                size="small"
                variant="outlined"
                color="secondary"
                sx={{ fontSize: '0.65rem', height: 20 }}
              />
            )}
          </Box>

          {/* Description */}
          {ontologyClass.description && (
            <Typography
              variant="body2"
              color="text.secondary"
              sx={{
                fontSize: '0.75rem',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                display: '-webkit-box',
                WebkitLineClamp: 2,
                WebkitBoxOrient: 'vertical',
                lineHeight: 1.3,
              }}
            >
              {ontologyClass.description}
            </Typography>
          )}

          {/* Key properties preview */}
          {ontologyClass.properties.length > 0 && (
            <Box sx={{ mt: 1 }}>
              <Typography
                variant="caption"
                color="text.secondary"
                sx={{ fontSize: '0.65rem', fontWeight: 600 }}
              >
                주요 속성:
              </Typography>
              <Box sx={{ mt: 0.5 }}>
                {ontologyClass.properties.slice(0, 3).map((prop) => (
                  <Chip
                    key={prop.name}
                    label={prop.name}
                    size="small"
                    variant="outlined"
                    sx={{
                      fontSize: '0.6rem',
                      height: 18,
                      mr: 0.5,
                      mb: 0.5,
                      '& .MuiChip-label': {
                        px: 0.5,
                      },
                    }}
                  />
                ))}
                {ontologyClass.properties.length > 3 && (
                  <Chip
                    label={`+${ontologyClass.properties.length - 3}`}
                    size="small"
                    variant="filled"
                    color="primary"
                    sx={{
                      fontSize: '0.6rem',
                      height: 18,
                      '& .MuiChip-label': {
                        px: 0.5,
                      },
                    }}
                  />
                )}
              </Box>
            </Box>
          )}
        </CardContent>
      </Card>
    </>
  );
});