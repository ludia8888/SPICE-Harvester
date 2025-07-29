import React, { useState } from 'react';
import {
  Button,
  ButtonGroup,
  Card,
  Divider,
  Icon,
  Menu,
  MenuItem,
  Popover,
  Position,
  Tooltip,
  Classes,
  Intent,
} from '@blueprintjs/core';
import './CanvasToolbar.scss';

interface CanvasToolbarProps {
  onLayoutChange: (layout: 'force' | 'hierarchical' | 'circular') => void;
  onAddNode: () => void;
  onExport?: () => void;
  onImport?: () => void;
  onUndo?: () => void;
  onRedo?: () => void;
  onDelete?: () => void;
  onDuplicate?: () => void;
  onGroup?: () => void;
  onSearch?: () => void;
}

export const CanvasToolbar: React.FC<CanvasToolbarProps> = ({
  onLayoutChange,
  onAddNode,
  onExport,
  onImport,
  onUndo,
  onRedo,
  onDelete,
  onDuplicate,
  onGroup,
  onSearch,
}) => {
  const [selectedLayout, setSelectedLayout] = useState<'force' | 'hierarchical' | 'circular'>('force');

  const handleLayoutChange = (layout: 'force' | 'hierarchical' | 'circular') => {
    setSelectedLayout(layout);
    onLayoutChange(layout);
  };

  const layoutMenu = (
    <Menu>
      <MenuItem
        text="Force-Directed"
        icon="graph"
        active={selectedLayout === 'force'}
        onClick={() => handleLayoutChange('force')}
      />
      <MenuItem
        text="Hierarchical"
        icon="diagram-tree"
        active={selectedLayout === 'hierarchical'}
        onClick={() => handleLayoutChange('hierarchical')}
      />
      <MenuItem
        text="Circular"
        icon="circle"
        active={selectedLayout === 'circular'}
        onClick={() => handleLayoutChange('circular')}
      />
    </Menu>
  );

  const addMenu = (
    <Menu>
      <MenuItem
        text="Object Type"
        icon="cube"
        onClick={onAddNode}
      />
      <MenuItem
        text="Link Type"
        icon="link"
        disabled
      />
      <li className="bp5-menu-divider" />
      <MenuItem
        text="From Template"
        icon="duplicate"
        disabled
      />
    </Menu>
  );

  return (
    <Card className={`canvas-toolbar horizontal ${Classes.DARK}`} elevation={2}>
      {/* Primary Creation */}
      <div className="toolbar-section primary">
        <Popover content={addMenu} position={Position.BOTTOM}>
          <Tooltip content="Add New" position={Position.BOTTOM}>
            <Button
              icon="add"
              intent={Intent.PRIMARY}
              className="toolbar-button"
              rightIcon="caret-down"
            />
          </Tooltip>
        </Popover>
      </div>

      <Divider />

      {/* Edit Actions */}
      <div className="toolbar-section edit">
        <ButtonGroup minimal>
          <Tooltip content="Undo (Ctrl+Z)" position={Position.BOTTOM}>
            <Button 
              icon="undo" 
              onClick={onUndo}
              className="toolbar-button"
            />
          </Tooltip>
          <Tooltip content="Redo (Ctrl+Y)" position={Position.BOTTOM}>
            <Button 
              icon="redo"
              onClick={onRedo}
              className="toolbar-button"
            />
          </Tooltip>
        </ButtonGroup>
      </div>

      <Divider />

      {/* Object Operations */}
      <div className="toolbar-section operations">
        <ButtonGroup minimal>
          <Tooltip content="Duplicate (Ctrl+D)" position={Position.BOTTOM}>
            <Button 
              icon="duplicate" 
              onClick={onDuplicate}
              className="toolbar-button"
            />
          </Tooltip>
          <Tooltip content="Group Selected" position={Position.BOTTOM}>
            <Button 
              icon="group-objects" 
              onClick={onGroup}
              className="toolbar-button"
            />
          </Tooltip>
          <Tooltip content="Delete (Delete)" position={Position.BOTTOM}>
            <Button 
              icon="trash" 
              onClick={onDelete}
              className="toolbar-button"
              intent={Intent.DANGER}
            />
          </Tooltip>
        </ButtonGroup>
      </div>

      <Divider />

      {/* Layout Options */}
      <div className="toolbar-section layout">
        <ButtonGroup minimal>
          <Popover content={layoutMenu} position={Position.BOTTOM}>
            <Tooltip content="Arrange Layout" position={Position.BOTTOM}>
              <Button 
                icon="layout" 
                className="toolbar-button"
                rightIcon="caret-down"
              />
            </Tooltip>
          </Popover>
          <Tooltip content="Auto Arrange" position={Position.BOTTOM}>
            <Button 
              icon="layout-auto" 
              onClick={() => onLayoutChange(selectedLayout)}
              className="toolbar-button" 
            />
          </Tooltip>
        </ButtonGroup>
      </div>

      <Divider />

      {/* Data Operations */}
      <div className="toolbar-section data">
        <ButtonGroup minimal>
          <Tooltip content="Import Data" position={Position.BOTTOM}>
            <Button icon="import" onClick={onImport} className="toolbar-button" />
          </Tooltip>
        </ButtonGroup>
      </div>

      <Divider />

      {/* Search & Help */}
      <div className="toolbar-section utility">
        <ButtonGroup minimal>
          <Tooltip content="Search Nodes (Ctrl+F)" position={Position.BOTTOM}>
            <Button icon="search" onClick={onSearch} className="toolbar-button" />
          </Tooltip>
          <Tooltip content="Keyboard Shortcuts" position={Position.BOTTOM}>
            <Button icon="key-command" className="toolbar-button" />
          </Tooltip>
        </ButtonGroup>
      </div>
    </Card>
  );
};