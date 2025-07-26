import { DragSourceMonitor, DropTargetMonitor } from 'react-dnd';

// Drag item types
export enum DragItemTypes {
  // Ontology items
  OBJECT_TYPE = 'objectType',
  LINK_TYPE = 'linkType',
  PROPERTY = 'property',
  ACTION = 'action',
  FUNCTION = 'function',
  
  // Graph items
  NODE = 'node',
  EDGE = 'edge',
  GROUP = 'group',
  
  // Pipeline items
  PIPELINE_STEP = 'pipelineStep',
  TRANSFORMATION = 'transformation',
  DATA_SOURCE = 'dataSource',
  DATA_SINK = 'dataSink',
  
  // Workshop items
  WIDGET = 'widget',
  SECTION = 'section',
  LAYOUT = 'layout',
  
  // File items
  FILE = 'file',
  FOLDER = 'folder',
  DATASET = 'dataset',
}

// Drag item interface
export interface IDragItem {
  type: DragItemTypes;
  id: string;
  data: any;
  sourcePosition?: { x: number; y: number };
  sourceContainer?: string;
}

// Drop result interface
export interface IDropResult {
  targetId: string;
  targetType: string;
  position?: { x: number; y: number };
  index?: number;
  accepted: boolean;
}

// Drag and drop configuration
export const dragDropConfig = {
  // Animation settings
  animation: {
    duration: 200,
    easing: 'cubic-bezier(0.4, 0, 0.2, 1)',
  },
  
  // Visual feedback
  preview: {
    opacity: 0.8,
    scale: 1.05,
    rotation: 2,
  },
  
  // Drop zone highlight
  dropZone: {
    highlightColor: 'rgba(0, 114, 255, 0.1)',
    borderColor: '#0072FF',
    borderWidth: 2,
    borderStyle: 'dashed',
  },
  
  // Drag constraints
  constraints: {
    minDragDistance: 5,
    scrollSpeed: 10,
    scrollBoundary: 50,
  },
};

// Drag source configuration
export const createDragSource = (type: DragItemTypes) => ({
  type,
  
  item: (props: any, monitor: DragSourceMonitor): IDragItem => ({
    type,
    id: props.id,
    data: props.data,
    sourcePosition: monitor.getInitialClientOffset(),
    sourceContainer: props.containerId,
  }),
  
  collect: (monitor: DragSourceMonitor) => ({
    isDragging: monitor.isDragging(),
    draggedItem: monitor.getItem(),
    currentOffset: monitor.getClientOffset(),
    initialOffset: monitor.getInitialClientOffset(),
    differenceFromInitialOffset: monitor.getDifferenceFromInitialOffset(),
  }),
  
  canDrag: (props: any) => !props.disabled && !props.locked,
  
  isDragging: (props: any, monitor: DragSourceMonitor) => {
    return props.id === monitor.getItem()?.id;
  },
  
  endDrag: (props: any, monitor: DragSourceMonitor) => {
    const dropResult = monitor.getDropResult() as IDropResult | null;
    if (dropResult && props.onDragEnd) {
      props.onDragEnd(monitor.getItem(), dropResult);
    }
  },
});

// Drop target configuration
export const createDropTarget = (acceptedTypes: DragItemTypes[]) => ({
  accept: acceptedTypes,
  
  drop: (props: any, monitor: DropTargetMonitor): IDropResult => {
    const item = monitor.getItem() as IDragItem;
    const clientOffset = monitor.getClientOffset();
    
    return {
      targetId: props.id,
      targetType: props.type,
      position: clientOffset ? { x: clientOffset.x, y: clientOffset.y } : undefined,
      index: props.index,
      accepted: true,
    };
  },
  
  collect: (monitor: DropTargetMonitor) => ({
    isOver: monitor.isOver(),
    isOverShallow: monitor.isOver({ shallow: true }),
    canDrop: monitor.canDrop(),
    draggedItem: monitor.getItem(),
    draggedItemType: monitor.getItemType(),
    clientOffset: monitor.getClientOffset(),
    sourceClientOffset: monitor.getSourceClientOffset(),
    initialClientOffset: monitor.getInitialClientOffset(),
    differenceFromInitialOffset: monitor.getDifferenceFromInitialOffset(),
  }),
  
  canDrop: (props: any, monitor: DropTargetMonitor) => {
    const item = monitor.getItem() as IDragItem;
    
    // Prevent dropping on itself
    if (item.id === props.id) return false;
    
    // Check custom validation
    if (props.canDrop) {
      return props.canDrop(item, props);
    }
    
    return true;
  },
  
  hover: (props: any, monitor: DropTargetMonitor) => {
    if (props.onHover) {
      const item = monitor.getItem() as IDragItem;
      const clientOffset = monitor.getClientOffset();
      props.onHover(item, clientOffset);
    }
  },
});

// Drag layer configuration for custom preview
export const dragLayerCollect = (monitor: any) => ({
  item: monitor.getItem(),
  itemType: monitor.getItemType(),
  currentOffset: monitor.getSourceClientOffset(),
  isDragging: monitor.isDragging(),
});

// Helper functions
export const dragDropHelpers = {
  // Calculate drop position relative to container
  getDropPosition: (
    clientOffset: { x: number; y: number },
    containerRect: DOMRect,
    gridSize?: number
  ) => {
    let x = clientOffset.x - containerRect.left;
    let y = clientOffset.y - containerRect.top;
    
    // Snap to grid if specified
    if (gridSize) {
      x = Math.round(x / gridSize) * gridSize;
      y = Math.round(y / gridSize) * gridSize;
    }
    
    return { x, y };
  },
  
  // Check if point is inside rectangle
  isPointInside: (
    point: { x: number; y: number },
    rect: { x: number; y: number; width: number; height: number }
  ) => {
    return (
      point.x >= rect.x &&
      point.x <= rect.x + rect.width &&
      point.y >= rect.y &&
      point.y <= rect.y + rect.height
    );
  },
  
  // Get insertion index for list drop
  getInsertionIndex: (
    clientY: number,
    containerElement: HTMLElement,
    itemSelector: string
  ) => {
    const items = containerElement.querySelectorAll(itemSelector);
    let index = items.length;
    
    for (let i = 0; i < items.length; i++) {
      const item = items[i] as HTMLElement;
      const rect = item.getBoundingClientRect();
      const midY = rect.top + rect.height / 2;
      
      if (clientY < midY) {
        index = i;
        break;
      }
    }
    
    return index;
  },
  
  // Create ghost image for drag preview
  createDragPreview: (element: HTMLElement, scale = 1) => {
    const preview = element.cloneNode(true) as HTMLElement;
    preview.style.position = 'absolute';
    preview.style.top = '-1000px';
    preview.style.left = '-1000px';
    preview.style.transform = `scale(${scale})`;
    preview.style.opacity = '0.8';
    document.body.appendChild(preview);
    
    return () => {
      document.body.removeChild(preview);
    };
  },
  
  // Handle auto-scroll during drag
  handleAutoScroll: (
    clientPosition: { x: number; y: number },
    scrollContainer: HTMLElement,
    scrollSpeed = 10,
    scrollBoundary = 50
  ) => {
    const rect = scrollContainer.getBoundingClientRect();
    const { scrollTop, scrollLeft, scrollHeight, scrollWidth } = scrollContainer;
    const { clientHeight, clientWidth } = scrollContainer;
    
    // Vertical scroll
    if (clientPosition.y < rect.top + scrollBoundary) {
      scrollContainer.scrollTop = Math.max(0, scrollTop - scrollSpeed);
    } else if (clientPosition.y > rect.bottom - scrollBoundary) {
      scrollContainer.scrollTop = Math.min(scrollHeight - clientHeight, scrollTop + scrollSpeed);
    }
    
    // Horizontal scroll
    if (clientPosition.x < rect.left + scrollBoundary) {
      scrollContainer.scrollLeft = Math.max(0, scrollLeft - scrollSpeed);
    } else if (clientPosition.x > rect.right - scrollBoundary) {
      scrollContainer.scrollLeft = Math.min(scrollWidth - clientWidth, scrollLeft + scrollSpeed);
    }
  },
};