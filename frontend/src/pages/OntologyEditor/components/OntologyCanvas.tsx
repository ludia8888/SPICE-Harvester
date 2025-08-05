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
  MiniMap,
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
  OverlayToaster,
  FormGroup,
  InputGroup,
  HTMLSelect
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
import { ZoomControls } from './canvas/ZoomControls';
import './OntologyCanvas.scss';

// Custom node and edge types - defined outside component to prevent re-creation
const nodeTypes: NodeTypes = {
  ontologyNode: OntologyNodeComponent,
};

// Create edge types with handlers
const createEdgeTypes = (
  onEdgeEdit: (edgeId: string) => void,
  onEdgeDelete: (edgeId: string) => void,
  onEdgeReverse: (edgeId: string) => void,
  onEdgeMakeBidirectional: (edgeId: string) => void
): EdgeTypes => ({
  ontologyEdge: (props: any) => (
    <OntologyEdgeComponent
      {...props}
      onEdit={() => onEdgeEdit(props.id)}
      onDelete={() => onEdgeDelete(props.id)}
      onReverse={() => onEdgeReverse(props.id)}
      onMakeBidirectional={() => onEdgeMakeBidirectional(props.id)}
    />
  ),
});

// D3 Layout algorithms
const layoutAlgorithms = {
  force: (nodes: Node[], edges: Edge[]) => {
    if (nodes.length === 0) return nodes;
    
    // Copy nodes to avoid mutation
    const nodesCopy = nodes.map(n => ({ ...n, x: n.position.x, y: n.position.y }));
    
    const simulation = forceSimulation(nodesCopy)
      .force('link', forceLink(edges).id((d: any) => d.id).distance(200).strength(0.5))
      .force('charge', forceManyBody().strength(-1000))
      .force('center', forceCenter(500, 300))
      .force('collision', forceCollide(80));

    simulation.stop();
    for (let i = 0; i < 300; ++i) simulation.tick();

    return nodes.map((node) => {
      const simNode = nodesCopy.find((n: any) => n.id === node.id) as any;
      return {
        ...node,
        position: { x: simNode.x || node.position.x, y: simNode.y || node.position.y }
      };
    });
  },
  
  hierarchical: (nodes: Node[], edges: Edge[]) => {
    if (nodes.length === 0) return nodes;
    
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
        .size([800, 600])
        .separation((a, b) => 1.5);
      
      treeLayout(root as any);
      
      return nodes.map(node => {
        const treeNode = root.descendants().find(d => d.id === node.id);
        return {
          ...node,
          position: {
            x: (treeNode?.x || node.position.x) + 100,
            y: (treeNode?.y || node.position.y) + 100
          }
        };
      });
    } catch {
      // If hierarchy fails, arrange nodes in levels based on connections
      const levels = new Map<string, number>();
      const positioned = new Set<string>();
      
      // Find root nodes (no incoming edges)
      const rootNodes = nodes.filter(node => 
        !edges.some(edge => edge.target === node.id)
      );
      
      // Assign levels
      rootNodes.forEach(node => levels.set(node.id, 0));
      
      let maxLevel = 0;
      const assignLevel = (nodeId: string, level: number) => {
        levels.set(nodeId, level);
        maxLevel = Math.max(maxLevel, level);
        
        edges.filter(e => e.source === nodeId).forEach(edge => {
          if (!levels.has(edge.target) || levels.get(edge.target)! < level + 1) {
            assignLevel(edge.target, level + 1);
          }
        });
      };
      
      rootNodes.forEach(node => assignLevel(node.id, 0));
      
      // Position nodes by level
      const levelCounts = new Map<number, number>();
      return nodes.map(node => {
        const level = levels.get(node.id) || 0;
        const count = levelCounts.get(level) || 0;
        levelCounts.set(level, count + 1);
        
        return {
          ...node,
          position: {
            x: 200 + count * 150,
            y: 100 + level * 150
          }
        };
      });
    }
  },
  
  circular: (nodes: Node[], edges: Edge[]) => {
    if (nodes.length === 0) return nodes;
    
    const radius = Math.min(400, 300 * Math.sqrt(nodes.length / 10));
    const center = { x: 500, y: 300 };
    const angleStep = (2 * Math.PI) / nodes.length;
    
    // Sort nodes by connectivity for better layout
    const nodeDegree = new Map<string, number>();
    nodes.forEach(node => {
      const degree = edges.filter(e => e.source === node.id || e.target === node.id).length;
      nodeDegree.set(node.id, degree);
    });
    
    const sortedNodes = [...nodes].sort((a, b) => 
      (nodeDegree.get(b.id) || 0) - (nodeDegree.get(a.id) || 0)
    );
    
    return nodes.map((node) => {
      const index = sortedNodes.findIndex(n => n.id === node.id);
      const angle = index * angleStep - Math.PI / 2; // Start from top
      return {
        ...node,
        position: {
          x: center.x + radius * Math.cos(angle),
          y: center.y + radius * Math.sin(angle)
        }
      };
    });
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
  const [nodes, setNodes] = useNodesState([]);
  const [edges, setEdges] = useEdgesState([]);
  const nodePositions = useRef<Map<string, { x: number; y: number }>>(new Map());
  
  const [showNodeInspector, setShowNodeInspector] = useState(false);
  const [selectedNode, setSelectedNode] = useState<Node | null>(null);
  const [showDataPreview, setShowDataPreview] = useState(false);
  const [showDataConnector, setShowDataConnector] = useState(false);
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number } | null>(null);
  const [editingEdge, setEditingEdge] = useState<Edge | null>(null);
  const [showEdgeDialog, setShowEdgeDialog] = useState(false);
  
  // Remove useKeyPress and handle keyboard events directly
  const spacePressed = useKeyPress(' ');

  // Toast notification utility
  const showToast = (message: string, intent?: Intent, timeout?: number) => {
    OverlayToaster.create({ position: Position.TOP }).show({
      message,
      intent,
      timeout: timeout || 3000,
    });
  };

  // Convert store data to ReactFlow nodes and edges - only when objectTypes or linkTypes change
  useEffect(() => {
    const newNodes = objectTypes.map(obj => {
      // Use stored position or generate new one
      let position = nodePositions.current.get(obj.id);
      if (!position) {
        position = { x: Math.random() * 800, y: Math.random() * 600 };
        nodePositions.current.set(obj.id, position);
      }
      
      return {
        id: obj.id,
        type: 'ontologyNode',
        position,
        data: {
          label: obj.label,
          description: obj.description,
          properties: obj.properties,
          isSelected: selectedObjectType === obj.id,
        } as OntologyNodeData,
      };
    });

    const newEdges = linkTypes.map(link => ({
      id: link.id,
      source: link.from_class,
      target: link.to_class,
      type: 'ontologyEdge',
      animated: true,
      markerEnd: {
        type: MarkerType.ArrowClosed,
        width: 12,
        height: 12,
      },
      data: {
        label: link.label,
        cardinality: link.cardinality,
      } as OntologyEdgeData,
    }));

    setNodes(newNodes);
    setEdges(newEdges);
  }, [objectTypes, linkTypes]); // Remove selectedObjectType from dependencies

  // Update selection state separately
  useEffect(() => {
    setNodes(currentNodes =>
      currentNodes.map(node => ({
        ...node,
        data: {
          ...node.data,
          isSelected: selectedObjectType === node.id,
        },
      }))
    );
  }, [selectedObjectType]);

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
        width: 12,
        height: 12,
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
    const existingNodesCount = nodes.length;
    const position = fromContextMenu && contextMenu 
      ? reactFlowInstance.project(contextMenu)
      : { 
          x: 300 + (existingNodesCount % 5) * 150, 
          y: 200 + Math.floor(existingNodesCount / 5) * 150 
        };
    
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
    
    // Store position
    nodePositions.current.set(newNode.id, position);
    
    // Open inspector for new node
    setSelectedNode(newNode);
    setShowNodeInspector(true);
  }, [contextMenu, reactFlowInstance, setNodes, nodes.length]);

  // Apply layout
  const applyLayout = useCallback((algorithm: keyof typeof layoutAlgorithms) => {
    const layoutedNodes = layoutAlgorithms[algorithm](nodes, edges);
    // Update node positions reference
    layoutedNodes.forEach(node => {
      nodePositions.current.set(node.id, node.position);
    });
    setNodes(layoutedNodes);
    // Fit view after layout change
    setTimeout(() => {
      reactFlowInstance?.fitView({ padding: 0.2, duration: 500 });
    }, 50);
    showToast(`Layout applied: ${algorithm}`, Intent.SUCCESS);
  }, [nodes, edges, setNodes, reactFlowInstance]);

  // Handle keyboard shortcuts directly
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      // Only handle delete/backspace keys
      if ((event.key === 'Delete' || event.key === 'Backspace') && selectedObjectType) {
        // Check if the delete key was pressed in an input field
        const activeElement = document.activeElement;
        const isInputField = activeElement instanceof HTMLInputElement || 
                            activeElement instanceof HTMLTextAreaElement || 
                            activeElement instanceof HTMLSelectElement ||
                            (activeElement as HTMLElement)?.contentEditable === 'true';
        
        if (!isInputField) {
          event.preventDefault();
          // Delete node logic
          setNodes((nds) => nds.filter((n) => n.id !== selectedObjectType));
          setEdges((eds) => eds.filter((e) => e.source !== selectedObjectType && e.target !== selectedObjectType));
          selectObjectType(null);
          // TODO: Call API to delete object type
        }
      }
    };
    
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [selectedObjectType, setNodes, setEdges, selectObjectType]);
  
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

  // Edge handlers
  const handleEdgeEdit = useCallback((edgeId: string) => {
    const edge = edges.find(e => e.id === edgeId);
    if (edge) {
      setEditingEdge(edge);
      setShowEdgeDialog(true);
    }
  }, [edges]);

  const handleEdgeDelete = useCallback((edgeId: string) => {
    setEdges(edges => edges.filter(e => e.id !== edgeId));
    showToast('Edge deleted', Intent.SUCCESS);
    // TODO: Call API to delete link type
  }, [setEdges]);

  const handleEdgeReverse = useCallback((edgeId: string) => {
    setEdges(edges =>
      edges.map(edge => {
        if (edge.id === edgeId) {
          return {
            ...edge,
            source: edge.target,
            target: edge.source,
          };
        }
        return edge;
      })
    );
    showToast('Edge direction reversed', Intent.SUCCESS);
  }, [setEdges]);

  const handleEdgeMakeBidirectional = useCallback((edgeId: string) => {
    setEdges(edges =>
      edges.map(edge => {
        if (edge.id === edgeId) {
          return {
            ...edge,
            data: {
              ...edge.data,
              bidirectional: !edge.data?.bidirectional,
            },
          };
        }
        return edge;
      })
    );
    showToast('Edge bidirectionality toggled', Intent.SUCCESS);
  }, [setEdges]);

  // Create edge types with handlers
  const edgeTypes = useMemo(
    () => createEdgeTypes(
      handleEdgeEdit,
      handleEdgeDelete,
      handleEdgeReverse,
      handleEdgeMakeBidirectional
    ),
    [handleEdgeEdit, handleEdgeDelete, handleEdgeReverse, handleEdgeMakeBidirectional]
  );

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
        onNodesChange={(changes: NodeChange[]) => {
          // Update node positions when they are dragged
          changes.forEach(change => {
            if (change.type === 'position' && change.position) {
              nodePositions.current.set(change.id, change.position);
            }
          });
          setNodes(currentNodes => applyNodeChanges(changes, currentNodes));
        }}
        onEdgesChange={(changes: EdgeChange[]) => {
          setEdges(currentEdges => applyEdgeChanges(changes, currentEdges));
        }}
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
        deleteKeyCode={null} // Disable default delete behavior
        selectionKeyCode={null} // Disable default selection behavior
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
        
        <MiniMap 
          nodeStrokeColor={(n) => {
            if (n.data?.isSelected) return '#48aff0';
            return '#5c7080';
          }}
          nodeColor={(n) => {
            if (n.data?.isSelected) return '#48aff0';
            return '#394b59';
          }}
          style={{
            backgroundColor: 'rgba(16, 22, 26, 0.8)',
            border: '1px solid #394b59',
          }}
          position="bottom-left"
          pannable
          zoomable
        />
        
        <Panel position="bottom-right" style={{ display: 'flex', gap: '8px', alignItems: 'flex-end' }}>
          <Button
            icon="th"
            title="데이터 프리뷰 보기/숨기기 (Space)"
            onClick={() => setShowDataPreview(prev => !prev)}
            active={showDataPreview}
            intent={showDataPreview ? Intent.PRIMARY : Intent.NONE}
            style={{ 
              marginRight: '8px',
              boxShadow: '0 1px 3px rgba(0,0,0,0.12), 0 1px 2px rgba(0,0,0,0.24)'
            }}
          />
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

      {/* Edge Edit Dialog */}
      <Dialog
        isOpen={showEdgeDialog}
        onClose={() => {
          setShowEdgeDialog(false);
          setEditingEdge(null);
        }}
        title="Edit Relationship"
        style={{ width: '500px' }}
      >
        <div className={Classes.DIALOG_BODY}>
          {editingEdge && (
            <>
              <FormGroup label="Relationship Label" labelFor="edge-label">
                <InputGroup
                  id="edge-label"
                  value={editingEdge.data?.label || ''}
                  onChange={(e) => {
                    if (editingEdge) {
                      setEditingEdge({
                        ...editingEdge,
                        data: {
                          ...editingEdge.data,
                          label: e.target.value,
                        },
                      });
                    }
                  }}
                  placeholder="Enter relationship label..."
                />
              </FormGroup>

              <FormGroup label="Cardinality" labelFor="edge-cardinality">
                <HTMLSelect
                  id="edge-cardinality"
                  value={editingEdge.data?.cardinality || '1:n'}
                  onChange={(e) => {
                    if (editingEdge) {
                      setEditingEdge({
                        ...editingEdge,
                        data: {
                          ...editingEdge.data,
                          cardinality: e.target.value,
                        },
                      });
                    }
                  }}
                  options={[
                    { value: '1:1', label: 'One to One' },
                    { value: '1:n', label: 'One to Many' },
                    { value: 'n:1', label: 'Many to One' },
                    { value: 'n:n', label: 'Many to Many' },
                  ]}
                />
              </FormGroup>
            </>
          )}
        </div>
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button
              text="Cancel"
              onClick={() => {
                setShowEdgeDialog(false);
                setEditingEdge(null);
              }}
            />
            <Button
              text="Save"
              intent={Intent.PRIMARY}
              onClick={() => {
                if (editingEdge) {
                  setEdges(edges =>
                    edges.map(edge =>
                      edge.id === editingEdge.id ? editingEdge : edge
                    )
                  );
                  showToast('Relationship updated', Intent.SUCCESS);
                  setShowEdgeDialog(false);
                  setEditingEdge(null);
                }
              }}
            />
          </div>
        </div>
      </Dialog>
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