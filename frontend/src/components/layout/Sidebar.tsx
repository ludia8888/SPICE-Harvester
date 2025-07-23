import React from 'react';
import {
  Drawer,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Divider,
  Typography,
  Box,
  Chip,
  Collapse,
} from '@mui/material';
import {
  Dashboard as DashboardIcon,
  History as HistoryIcon,
  Assignment as ProposalsIcon,
  AccountTree as ObjectTypesIcon,
  Link as LinkTypesIcon,
  Functions as FunctionsIcon,
  Group as GroupIcon,
  Settings as SettingsIcon,
  ExpandLess,
  ExpandMore,
  Category,
  DataObject,
  Hub as CanvasIcon,
} from '@mui/icons-material';

const drawerWidth = 240;

interface SidebarProps {
  open: boolean;
  selectedItem?: string;
  onItemClick?: (item: string) => void;
}

export const Sidebar: React.FC<SidebarProps> = ({ 
  open, 
  selectedItem = 'discover',
  onItemClick 
}) => {
  const [expandedSections, setExpandedSections] = React.useState<Record<string, boolean>>({
    resources: true,
  });

  const handleSectionClick = (section: string) => {
    setExpandedSections(prev => ({
      ...prev,
      [section]: !prev[section],
    }));
  };

  const menuItems = [
    { id: 'discover', label: 'Discover', icon: <DashboardIcon /> },
    { id: 'canvas', label: '캔버스', icon: <CanvasIcon /> },
    { id: 'proposals', label: 'Proposals', icon: <ProposalsIcon />, badge: '2' },
    { id: 'history', label: 'History', icon: <HistoryIcon /> },
  ];

  const resourceItems = [
    { id: 'object-types', label: 'Object Types', icon: <ObjectTypesIcon /> },
    { id: 'properties', label: 'Properties', icon: <Category /> },
    { id: 'shared-properties', label: 'Shared Properties', icon: <DataObject /> },
    { id: 'link-types', label: 'Link Types', icon: <LinkTypesIcon /> },
    { id: 'functions', label: 'Functions', icon: <FunctionsIcon /> },
    { id: 'groups', label: 'Groups', icon: <GroupIcon /> },
  ];

  return (
    <Drawer
      variant="persistent"
      anchor="left"
      open={open}
      sx={{
        width: drawerWidth,
        flexShrink: 0,
        '& .MuiDrawer-paper': {
          width: drawerWidth,
          boxSizing: 'border-box',
          mt: '64px', // AppBar height
        },
      }}
    >
      <Box sx={{ p: 2 }}>
        <Typography variant="body2" color="text.secondary">
          현재 온톨로지
        </Typography>
        <Typography variant="h6">
          SPICE HARVESTER
        </Typography>
        <Chip label="main" size="small" sx={{ mt: 1 }} />
      </Box>
      
      <Divider />
      
      <List>
        {menuItems.map((item) => (
          <ListItemButton
            key={item.id}
            selected={selectedItem === item.id}
            onClick={() => onItemClick?.(item.id)}
          >
            <ListItemIcon>{item.icon}</ListItemIcon>
            <ListItemText primary={item.label} />
            {item.badge && (
              <Chip label={item.badge} size="small" color="primary" />
            )}
          </ListItemButton>
        ))}
      </List>
      
      <Divider />
      
      <List>
        <ListItemButton onClick={() => handleSectionClick('resources')}>
          <ListItemIcon>
            <ObjectTypesIcon />
          </ListItemIcon>
          <ListItemText primary="Resources" />
          {expandedSections.resources ? <ExpandLess /> : <ExpandMore />}
        </ListItemButton>
        
        <Collapse in={expandedSections.resources} timeout="auto" unmountOnExit>
          <List component="div" disablePadding>
            {resourceItems.map((item) => (
              <ListItemButton
                key={item.id}
                sx={{ pl: 4 }}
                selected={selectedItem === item.id}
                onClick={() => onItemClick?.(item.id)}
              >
                <ListItemIcon>{item.icon}</ListItemIcon>
                <ListItemText primary={item.label} />
              </ListItemButton>
            ))}
          </List>
        </Collapse>
      </List>
      
      <Box sx={{ flexGrow: 1 }} />
      
      <Divider />
      
      <List>
        <ListItemButton>
          <ListItemIcon>
            <SettingsIcon />
          </ListItemIcon>
          <ListItemText primary="Settings" />
        </ListItemButton>
      </List>
    </Drawer>
  );
};