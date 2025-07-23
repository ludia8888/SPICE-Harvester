import React, { useCallback, useMemo, useState } from 'react';
import {
  ReactFlow,
  MiniMap,
  Controls,
  Background,
  useNodesState,
  useEdgesState,
  addEdge,
  Node,
  Edge,
  Connection,
  BackgroundVariant,
  Position,
} from 'reactflow';
import dagre from 'dagre';
import {
  Box,
  Paper,
  Toolbar,
  IconButton,
  Typography,
  Tooltip,
  ButtonGroup,
  Button,
  Divider,
} from '@mui/material';
import {
  Add as AddIcon,
  ZoomIn as ZoomInIcon,
  ZoomOut as ZoomOutIcon,
  CenterFocusStrong as CenterIcon,
  AccountTree,
  Link as LinkIcon,
  Refresh,
  Save,
} from '@mui/icons-material';
import { OntologyClass } from '../../types/ontology';
import { ObjectTypeNode } from './nodes/ObjectTypeNode';
import { RelationshipEdge } from './edges/RelationshipEdge';

import 'reactflow/dist/style.css';

// Custom node and edge types
const nodeTypes = {
  objectType: ObjectTypeNode,
};

const edgeTypes = {
  relationship: RelationshipEdge,
};

interface OntologyCanvasProps {
  classes: OntologyClass[];
  onNodeClick?: (node: Node) => void;
  onEdgeClick?: (edge: Edge) => void;
  onCanvasClick?: () => void;
}

// Auto-layout function using Dagre
const getLayoutedElements = (nodes: Node[], edges: Edge[], direction = 'TB') => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir: direction });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: 250, height: 150 });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  nodes.forEach((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    node.targetPosition = Position.Top;
    node.sourcePosition = Position.Bottom;
    node.position = {
      x: nodeWithPosition.x - 125, // Center node (width / 2)
      y: nodeWithPosition.y - 75,  // Center node (height / 2)
    };
  });

  return { nodes, edges };
};

export const OntologyCanvas: React.FC<OntologyCanvasProps> = ({
  classes,
  onNodeClick,
  onEdgeClick,
  onCanvasClick,
}) => {
  const [layoutDirection, setLayoutDirection] = useState<'TB' | 'LR'>('TB');

  // Convert OntologyClass data to React Flow nodes
  const initialNodes: Node[] = useMemo(() => {
    return classes.map((cls, index) => ({
      id: cls.id,
      type: 'objectType',
      position: { x: index * 300, y: index * 200 },
      data: {
        class: cls,
        onEdit: () => onNodeClick?.({ id: cls.id } as Node),
      },
      draggable: true,
    }));
  }, [classes, onNodeClick]);

  // Convert relationships to React Flow edges
  const initialEdges: Edge[] = useMemo(() => {
    const edges: Edge[] = [];
    classes.forEach((cls) => {
      cls.relationships.forEach((rel) => {
        edges.push({
          id: `${cls.id}-${rel.target}-${rel.predicate}`,
          source: cls.id,
          target: rel.target,
          type: 'relationship',
          animated: true,
          data: {
            relationship: rel,
          },
        });
      });
    });
    return edges;
  }, [classes]);

  const layoutedElements = useMemo(() => {
    return getLayoutedElements(initialNodes, initialEdges, layoutDirection);
  }, [initialNodes, initialEdges, layoutDirection]);

  const [nodes, setNodes, onNodesChange] = useNodesState(layoutedElements.nodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(layoutedElements.edges);

  const onConnect = useCallback(
    (params: Connection) => setEdges((eds) => addEdge(params, eds)),
    [setEdges]
  );

  const handleAutoLayout = useCallback(() => {
    const layouted = getLayoutedElements(nodes, edges, layoutDirection);
    setNodes(layouted.nodes);
    setEdges(layouted.edges);
  }, [nodes, edges, layoutDirection, setNodes, setEdges]);

  const handleDirectionChange = useCallback((direction: 'TB' | 'LR') => {
    setLayoutDirection(direction);
    const layouted = getLayoutedElements(nodes, edges, direction);
    setNodes(layouted.nodes);
    setEdges(layouted.edges);
  }, [nodes, edges, setNodes, setEdges]);

  return (
    <Box sx={{ height: '100vh', width: '100%', position: 'relative' }}>
      {/* Canvas Toolbar */}
      <Paper
        elevation={2}
        sx={{
          position: 'absolute',
          top: 16,
          left: 16,
          zIndex: 1000,
          minWidth: 400,
        }}
      >
        <Toolbar variant="dense" sx={{ gap: 1 }}>
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            온톨로지 캔버스
          </Typography>
          
          <Divider orientation="vertical" flexItem />
          
          <Tooltip title="새 Object Type 추가">
            <IconButton size="small" color="primary">
              <AddIcon />
            </IconButton>
          </Tooltip>
          
          <Tooltip title="새 관계 추가">
            <IconButton size="small" color="primary">
              <LinkIcon />
            </IconButton>
          </Tooltip>
          
          <Divider orientation="vertical" flexItem />
          
          <ButtonGroup size="small" variant="outlined">
            <Button
              variant={layoutDirection === 'TB' ? 'contained' : 'outlined'}
              onClick={() => handleDirectionChange('TB')}
            >
              세로
            </Button>
            <Button
              variant={layoutDirection === 'LR' ? 'contained' : 'outlined'}
              onClick={() => handleDirectionChange('LR')}
            >
              가로
            </Button>
          </ButtonGroup>
          
          <Tooltip title="자동 정렬">
            <IconButton size="small" onClick={handleAutoLayout}>
              <Refresh />
            </IconButton>
          </Tooltip>
          
          <Divider orientation="vertical" flexItem />
          
          <Tooltip title="저장">
            <IconButton size="small" color="success">
              <Save />
            </IconButton>
          </Tooltip>
        </Toolbar>
      </Paper>

      {/* React Flow Canvas */}
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={(event, node) => onNodeClick?.(node)}
        onEdgeClick={(event, edge) => onEdgeClick?.(edge)}
        onPaneClick={onCanvasClick}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        attributionPosition="bottom-left"
        style={{
          background: '#fafafa',
        }}
      >
        <Controls 
          position="bottom-right"
          showZoom={true}
          showFitView={true}
          showInteractive={true}
        />
        <MiniMap
          position="bottom-left"
          nodeColor={(node) => {
            switch (node.type) {
              case 'objectType':
                return '#1976d2';
              default:
                return '#ff6b6b';
            }
          }}
          nodeStrokeWidth={3}
          zoomable
          pannable
        />
        <Background 
          variant={BackgroundVariant.Dots} 
          gap={20} 
          size={1}
          color="#e0e0e0"
        />
      </ReactFlow>
    </Box>
  );
};