import React, { useCallback, useRef, useState, useEffect, useMemo } from 'react';
import ReactFlow, {
  Node,
  Edge,
  Controls,
  Background,
  BackgroundVariant,
  useNodesState,
  useEdgesState,
  addEdge,
  Connection,
  ConnectionMode,
  useReactFlow,
  ReactFlowProvider,
  NodeTypes,
  EdgeTypes,
  MarkerType,
  Panel,
  applyNodeChanges,
  applyEdgeChanges,
  NodeChange,
  EdgeChange,
} from 'reactflow';
import 'reactflow/dist/style.css';

import { 
  Button,
  Menu,
  MenuItem,
  Card,
  Intent,
  Tag,
  Position,
  Popover,
  Dialog,
  Classes,
  OverlayToaster
} from '@blueprintjs/core';

import * as d3 from 'd3';
import { forceSimulation, forceLink, forceManyBody, forceCenter, forceCollide } from 'd3-force';

import { useOntologyStore } from '../../../stores/ontology.store';
import { useKeyPress } from '../../../hooks/useKeyPress';
import { OntologyNodeComponent, OntologyNodeData } from './canvas/OntologyNode';
import { OntologyEdgeComponent, OntologyEdgeData } from './canvas/OntologyEdge';
import { NodeInspector } from './NodeInspector';
import { DataPreviewPanel } from './DataPreviewPanel';
import { DataConnector } from './DataConnector';
import { CanvasToolbar } from './canvas/CanvasToolbar';
import { MiniMap } from './canvas/MiniMap';
import { ZoomControls } from './canvas/ZoomControls';
import './OntologyCanvas.scss';

// Custom node and edge types - defined outside component to prevent re-creation
const nodeTypes: NodeTypes = {
  ontologyNode: OntologyNodeComponent,
};

const edgeTypes: EdgeTypes = {
  ontologyEdge: OntologyEdgeComponent,
};

// D3 Layout algorithms
const layoutAlgorithms = {
  force: (nodes: Node[], edges: Edge[]) => {
    const simulation = forceSimulation(nodes)
      .force('link', forceLink(edges).id((d: any) => d.id).distance(200))
      .force('charge', forceManyBody().strength(-1000))
      .force('center', forceCenter(window.innerWidth / 2, window.innerHeight / 2))
      .force('collision', forceCollide(100));

    simulation.stop();
    for (let i = 0; i < 300; ++i) simulation.tick();

    return nodes.map((node) => {
      const simNode = simulation.nodes().find((n: any) => n.id === node.id) as any;
      return {
        ...node,
        position: { x: simNode.x, y: simNode.y }
      };
    });
  },
  
  hierarchical: (nodes: Node[], edges: Edge[]) => {
    // Create hierarchy using d3
    const stratify = d3.stratify<Node>()
      .id(d => d.id)
      .parentId(d => {
        const parentEdge = edges.find(e => e.target === d.id);
        return parentEdge?.source;
      });
    
    try {
      const root = stratify(nodes);
      const treeLayout = d3.tree<Node>()
        .size([window.innerWidth - 200, window.innerHeight - 200])
        .separation((a, b) => 2);
      
      treeLayout(root as any);
      
      return nodes.map(node => {
        const treeNode = root.descendants().find(d => d.id === node.id);
        return {
          ...node,
          position: {
            x: treeNode?.x || node.position.x,
            y: treeNode?.y || node.position.y
          }
        };
      });
    } catch {
      // Fallback to force layout if hierarchy fails
      return layoutAlgorithms.force(nodes, edges);
    }
  },
  
  circular: (nodes: Node[], edges: Edge[]) => {
    const radius = Math.min(window.innerWidth, window.innerHeight) * 0.3;
    const center = { x: window.innerWidth / 2, y: window.innerHeight / 2 };
    const angleStep = (2 * Math.PI) / nodes.length;
    
    return nodes.map((node, index) => ({
      ...node,
      position: {
        x: center.x + radius * Math.cos(index * angleStep),
        y: center.y + radius * Math.sin(index * angleStep)
      }
    }));
  }
};

interface OntologyCanvasProps {
  onNodeDoubleClick?: (node: Node) => void;
}

const OntologyCanvasInner: React.FC<OntologyCanvasProps> = ({ onNodeDoubleClick }) => {
  const {
    objectTypes,
    linkTypes,
    selectedObjectType,
    selectObjectType,
    addObjectType,
    addLinkType,
    currentDatabase,
  } = useOntologyStore();

  const reactFlowInstance = useReactFlow();
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  
  const [showNodeInspector, setShowNodeInspector] = useState(false);
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [showDataPreview, setShowDataPreview] = useState(false);
  const [showDataConnector, setShowDataConnector] = useState(false);
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number } | null>(null);
  
  const deletePressed = useKeyPress(['Delete', 'Backspace']);
  const spacePressed = useKeyPress(' ');

  // Toast notification utility
  const showToast = (message: string, intent?: Intent, timeout?: number) => {
    OverlayToaster.create({ position: Position.TOP }).show({
      message,
      intent,
      timeout: timeout || 3000,
    });
  };

  // Convert store data to ReactFlow nodes and edges
  useEffect(() => {
    const newNodes = objectTypes.map(obj => ({
      id: obj.id,
      type: 'ontologyNode',
      position: { x: Math.random() * 800, y: Math.random() * 600 },
      data: {
        label: obj.label,
        description: obj.description,
        properties: obj.properties,
        isSelected: selectedObjectType === obj.id,
      } as OntologyNodeData,
    }));

    const newEdges = linkTypes.map(link => ({
      id: link.id,
      source: link.from_class,
      target: link.to_class,
      type: 'ontologyEdge',
      animated: true,
      markerEnd: {
        type: MarkerType.ArrowClosed,
        width: 20,
        height: 20,
      },
      data: {
        label: link.label,
        cardinality: link.cardinality,
      } as OntologyEdgeData,
    }));

    setNodes(newNodes);
    setEdges(newEdges);
  }, [objectTypes, linkTypes, selectedObjectType]);

  // Handle node double click
  const handleNodeDoubleClick = useCallback((event: React.MouseEvent, node: Node) => {
    setSelectedNode(node);
    setShowNodeInspector(true);
    selectObjectType(node.id);
    onNodeDoubleClick?.(node);
  }, [selectObjectType, onNodeDoubleClick]);

  // Handle node click
  const handleNodeClick = useCallback((event: React.MouseEvent, node: Node) => {
    selectObjectType(node.id);
    setShowDataPreview(true);
  }, [selectObjectType]);

  // Handle connection
  const onConnect = useCallback((params: Connection) => {
    const newEdge = {
      ...params,
      id: `edge-${Date.now()}`,
      type: 'ontologyEdge',
      animated: true,
      markerEnd: {
        type: MarkerType.ArrowClosed,
        width: 20,
        height: 20,
      },
      data: {
        label: 'relates_to',
        cardinality: '1:n',
      } as OntologyEdgeData,
    };
    
    setEdges((eds) => addEdge(newEdge, eds));
    
    // TODO: Call API to create link type
  }, [setEdges]);

  // Handle canvas right click
  const handleCanvasContextMenu = useCallback((event: React.MouseEvent) => {
    event.preventDefault();
    const rect = (event.target as HTMLElement).getBoundingClientRect();
    setContextMenu({
      x: event.clientX - rect.left,
      y: event.clientY - rect.top,
    });
  }, []);

  // Handle add node from context menu
  const handleAddNode = useCallback((fromContextMenu: boolean = true) => {
    const position = fromContextMenu && contextMenu 
      ? reactFlowInstance.project(contextMenu)
      : { x: window.innerWidth / 2 - 100, y: window.innerHeight / 2 - 50 };
    
    const newNode: Node = {
      id: `node-${Date.now()}`,
      type: 'ontologyNode',
      position,
      data: {
        label: 'New Object',
        description: '',
        properties: [],
        isSelected: false,
      } as OntologyNodeData,
    };
    
    setNodes((nds) => [...nds, newNode]);
    if (fromContextMenu) setContextMenu(null);
    
    // Open inspector for new node
    setSelectedNode(newNode);
    setShowNodeInspector(true);
  }, [contextMenu, reactFlowInstance, setNodes]);

  // Apply layout
  const applyLayout = useCallback((algorithm: keyof typeof layoutAlgorithms) => {
    const layoutedNodes = layoutAlgorithms[algorithm](nodes, edges);
    setNodes(layoutedNodes);
  }, [nodes, edges, setNodes]);

  // Handle keyboard shortcuts
  useEffect(() => {
    if (deletePressed && selectedObjectType) {
      handleDeleteNode();
    }
  }, [deletePressed, selectedObjectType]);
  
  // Delete selected node
  const handleDeleteNode = useCallback(() => {
    if (selectedObjectType) {
      setNodes((nds) => nds.filter((n) => n.id !== selectedObjectType));
      setEdges((eds) => eds.filter((e) => e.source !== selectedObjectType && e.target !== selectedObjectType));
      selectObjectType(null);
      // TODO: Call API to delete object type
    }
  }, [selectedObjectType, setNodes, setEdges, selectObjectType]);
  
  // Duplicate selected node
  const handleDuplicateNode = useCallback(() => {
    const selectedNode = nodes.find(n => n.id === selectedObjectType);
    if (selectedNode) {
      const newNode: Node = {
        ...selectedNode,
        id: `${selectedNode.id}-copy-${Date.now()}`,
        position: {
          x: selectedNode.position.x + 50,
          y: selectedNode.position.y + 50,
        },
        data: {
          ...selectedNode.data,
          label: `${selectedNode.data.label} (Copy)`,
        },
      };
      setNodes((nds) => [...nds, newNode]);
    }
  }, [selectedObjectType, nodes, setNodes]);
  
  // Undo/Redo placeholders
  const handleUndo = useCallback(() => {
    // TODO: Implement undo stack
    showToast('Undo functionality coming soon', Intent.WARNING);
  }, []);
  
  const handleRedo = useCallback(() => {
    // TODO: Implement redo stack
    showToast('Redo functionality coming soon', Intent.WARNING);
  }, []);
  
  // Group selected nodes
  const handleGroupNodes = useCallback(() => {
    // TODO: Implement node grouping
    showToast('Node grouping coming soon', Intent.WARNING);
  }, []);
  
  // Search nodes
  const handleSearch = useCallback(() => {
    // TODO: Implement search dialog
    showToast('Search functionality coming soon', Intent.WARNING);
  }, []);

  // Show/hide panels with space key
  useEffect(() => {
    if (spacePressed) {
      setShowDataPreview(prev => !prev);
    }
  }, [spacePressed]);

  // Render empty state only if no database
  if (!currentDatabase) {
    return (
      <div className="ontology-canvas-empty">
        <Card className="empty-state-card" elevation={2}>
          <h2>Welcome to Ontology Canvas</h2>
          <p>No database selected. Please select or create a database.</p>
        </Card>
      </div>
    );
  }

  return (
    <>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={handleNodeClick}
        onNodeDoubleClick={handleNodeDoubleClick}
        onContextMenu={handleCanvasContextMenu}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        connectionMode={ConnectionMode.Loose}
        defaultViewport={{ x: 0, y: 0, zoom: 1 }}
        fitView
        attributionPosition="bottom-left"
      >
        <Background variant={BackgroundVariant.Dots} gap={16} size={1} />
        
        <Panel position="top-center" className="toolbar-panel">
          <CanvasToolbar 
            onLayoutChange={applyLayout}
            onAddNode={() => handleAddNode(false)}
            onImport={() => setShowDataConnector(true)}
            onUndo={handleUndo}
            onRedo={handleRedo}
            onDelete={handleDeleteNode}
            onDuplicate={handleDuplicateNode}
            onGroup={handleGroupNodes}
            onSearch={handleSearch}
          />
        </Panel>
        
        <Panel position="bottom-left">
          <MiniMap nodes={nodes} />
        </Panel>
        
        <Panel position="bottom-right">
          <ZoomControls />
        </Panel>
      </ReactFlow>

      {/* Context Menu */}
      {contextMenu && (
        <div
          className="canvas-context-menu"
          style={{
            position: 'absolute',
            top: contextMenu.y,
            left: contextMenu.x,
            zIndex: 1000,
          }}
        >
          <Menu>
            <MenuItem
              text="Add Object Type"
              icon="cube"
              onClick={handleAddNode}
            />
            <MenuItem
              text="Paste"
              icon="clipboard"
              disabled
            />
            <li className="bp5-menu-divider" />
            <MenuItem
              text="Connect Data"
              icon="link"
              onClick={() => {
                setContextMenu(null);
                setShowDataConnector(true);
              }}
            />
          </Menu>
        </div>
      )}

      {/* Node Inspector */}
      {showNodeInspector && selectedNode && (
        <NodeInspector
          node={selectedNode}
          isOpen={showNodeInspector}
          onClose={() => setShowNodeInspector(false)}
          onUpdate={(updatedNode) => {
            setNodes((nds) =>
              nds.map((n) => (n.id === updatedNode.id ? updatedNode : n))
            );
          }}
        />
      )}

      {/* Data Preview Panel */}
      {showDataPreview && selectedObjectType && (
        <DataPreviewPanel
          objectTypeId={selectedObjectType}
          isOpen={showDataPreview}
          onClose={() => setShowDataPreview(false)}
        />
      )}

      {/* Data Connector */}
      {showDataConnector && (
        <DataConnector
          isOpen={showDataConnector}
          onClose={() => setShowDataConnector(false)}
          onDataConnected={() => {
            setShowDataConnector(false);
          }}
        />
      )}
    </>
  );
};

// Wrap with ReactFlowProvider
export const OntologyCanvas: React.FC<OntologyCanvasProps> = (props) => {
  return (
    <div className="ontology-canvas" style={{ width: '100%', height: '100%' }}>
      <ReactFlowProvider>
        <OntologyCanvasInner {...props} />
      </ReactFlowProvider>
    </div>
  );
};