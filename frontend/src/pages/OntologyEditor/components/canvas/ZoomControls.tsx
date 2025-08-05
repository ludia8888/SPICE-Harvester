import React from 'react';
import { useReactFlow, useStore } from 'reactflow';
import {
  Button,
  ButtonGroup,
  Card,
  Classes,
  Tooltip,
  Position,
  Icon,
} from '@blueprintjs/core';
import './ZoomControls.scss';

export const ZoomControls: React.FC = () => {
  const { zoomIn, zoomOut, fitView } = useReactFlow();
  
  // Use React Flow's store to reactively get zoom level
  const zoom = useStore((state) => state.transform[2]);
  const currentZoom = Math.round(zoom * 100);

  const handleZoomIn = () => {
    zoomIn();
  };

  const handleZoomOut = () => {
    zoomOut();
  };

  const handleFitView = () => {
    fitView({ padding: 0.1, duration: 200 });
  };

  const handleResetZoom = () => {
    fitView({ padding: 0.1, maxZoom: 1, minZoom: 1, duration: 200 });
  };

  return (
    <Card className={`zoom-controls ${Classes.DARK}`} elevation={2}>
      <div className="zoom-info">
        <span className="zoom-level">{currentZoom}%</span>
      </div>
      
      <ButtonGroup vertical minimal>
        <Tooltip content="Zoom In" position={Position.LEFT}>
          <Button
            icon="plus"
            onClick={handleZoomIn}
            className="zoom-button"
          />
        </Tooltip>
        
        <Tooltip content="Zoom Out" position={Position.LEFT}>
          <Button
            icon="minus"
            onClick={handleZoomOut}
            className="zoom-button"
          />
        </Tooltip>
        
        <div className="zoom-divider" />
        
        <Tooltip content="Fit to Screen" position={Position.LEFT}>
          <Button
            icon="zoom-to-fit"
            onClick={handleFitView}
            className="zoom-button"
          />
        </Tooltip>
        
        <Tooltip content="Reset Zoom (100%)" position={Position.LEFT}>
          <Button
            icon="reset"
            onClick={handleResetZoom}
            className="zoom-button"
          />
        </Tooltip>
      </ButtonGroup>
    </Card>
  );
};