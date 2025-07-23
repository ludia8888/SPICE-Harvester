import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  Tabs,
  Tab,
  Grid,
  Card,
  CardContent,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  IconButton,
  Button,
  Avatar,
  Tooltip,
} from '@mui/material';
import {
  Edit as EditIcon,
  Description as DescriptionIcon,
  Key as KeyIcon,
  Visibility as VisibilityIcon,
  VisibilityOff as VisibilityOffIcon,
  Add as AddIcon,
  Link as LinkIcon,
  AccountTree,
  Security,
  Storage,
  Transform,
} from '@mui/icons-material';
import { OntologyClass, Property, Relationship } from '../../types/ontology';

interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}

function TabPanel(props: TabPanelProps) {
  const { children, value, index, ...other } = props;

  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`tabpanel-${index}`}
      aria-labelledby={`tab-${index}`}
      {...other}
    >
      {value === index && <Box sx={{ p: 3 }}>{children}</Box>}
    </div>
  );
}

interface ObjectTypeViewProps {
  objectType: OntologyClass;
  onEdit?: () => void;
}

export const ObjectTypeView: React.FC<ObjectTypeViewProps> = ({ objectType, onEdit }) => {
  const [tabValue, setTabValue] = useState(0);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const renderPropertyStatus = (property: Property) => {
    const chips = [];
    
    if (property.required) {
      chips.push(
        <Chip 
          key="required" 
          label="Required" 
          size="small" 
          color="error" 
          sx={{ mr: 0.5 }}
        />
      );
    }
    
    if (property.name === 'id') { // Primary key example
      chips.push(
        <Chip 
          key="primary" 
          label="Primary Key" 
          size="small" 
          color="secondary" 
          icon={<KeyIcon />}
          sx={{ mr: 0.5 }}
        />
      );
    }
    
    return chips;
  };

  return (
    <Box sx={{ flexGrow: 1 }}>
      {/* Header */}
      <Paper sx={{ p: 3, mb: 2 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2 }}>
          <Avatar sx={{ bgcolor: 'primary.main', width: 56, height: 56 }}>
            <AccountTree />
          </Avatar>
          <Box sx={{ flexGrow: 1 }}>
            <Typography variant="h4">{objectType.label}</Typography>
            <Typography variant="body2" color="text.secondary">
              ID: {objectType.id}
            </Typography>
            {objectType.description && (
              <Typography variant="body1" sx={{ mt: 1 }}>
                {objectType.description}
              </Typography>
            )}
          </Box>
          <Button
            variant="contained"
            startIcon={<EditIcon />}
            onClick={onEdit}
          >
            편집
          </Button>
        </Box>
      </Paper>

      {/* Tabs */}
      <Paper sx={{ width: '100%' }}>
        <Tabs value={tabValue} onChange={handleTabChange} aria-label="object type tabs">
          <Tab label="Overview" />
          <Tab label="Properties" />
          <Tab label="Security" />
          <Tab label="Datasources" />
          <Tab label="Transforms" />
        </Tabs>

        <TabPanel value={tabValue} index={0}>
          <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 3, mb: 3 }}>
            {/* Metadata Card */}
            <Box>
              <Card>
                <CardContent>
                  <Typography variant="h6" gutterBottom>
                    메타데이터
                  </Typography>
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="body2" color="text.secondary">
                      Display Name
                    </Typography>
                    <Typography variant="body1">{objectType.label}</Typography>
                  </Box>
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="body2" color="text.secondary">
                      Parent Class
                    </Typography>
                    <Typography variant="body1">
                      {objectType.parent_class || 'None'}
                    </Typography>
                  </Box>
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="body2" color="text.secondary">
                      Abstract
                    </Typography>
                    <Typography variant="body1">
                      {objectType.abstract ? 'Yes' : 'No'}
                    </Typography>
                  </Box>
                </CardContent>
              </Card>
            </Box>

            {/* Properties Summary Card */}
            <Box>
              <Card>
                <CardContent>
                  <Typography variant="h6" gutterBottom>
                    Properties ({objectType.properties.length})
                  </Typography>
                  <TableContainer>
                    <Table size="small">
                      <TableHead>
                        <TableRow>
                          <TableCell>Name</TableCell>
                          <TableCell>Type</TableCell>
                          <TableCell>Status</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {objectType.properties.slice(0, 5).map((prop) => (
                          <TableRow key={prop.name}>
                            <TableCell>{prop.name}</TableCell>
                            <TableCell>{prop.type}</TableCell>
                            <TableCell>{renderPropertyStatus(prop)}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </TableContainer>
                  {objectType.properties.length > 5 && (
                    <Button size="small" sx={{ mt: 1 }}>
                      View all properties
                    </Button>
                  )}
                </CardContent>
              </Card>
            </Box>
          </Box>

          {/* Relationships Card */}
          <Box>
              <Card>
                <CardContent>
                  <Typography variant="h6" gutterBottom>
                    Relationships ({objectType.relationships.length})
                  </Typography>
                  {objectType.relationships.length === 0 ? (
                    <Typography variant="body2" color="text.secondary">
                      No relationships defined
                    </Typography>
                  ) : (
                    <TableContainer>
                      <Table size="small">
                        <TableHead>
                          <TableRow>
                            <TableCell>Predicate</TableCell>
                            <TableCell>Target</TableCell>
                            <TableCell>Cardinality</TableCell>
                            <TableCell>Label</TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {objectType.relationships.map((rel) => (
                            <TableRow key={rel.predicate}>
                              <TableCell>{rel.predicate}</TableCell>
                              <TableCell>
                                <Chip
                                  label={rel.target}
                                  size="small"
                                  icon={<LinkIcon />}
                                />
                              </TableCell>
                              <TableCell>{rel.cardinality}</TableCell>
                              <TableCell>{rel.label}</TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </TableContainer>
                  )}
                </CardContent>
              </Card>
          </Box>
        </TabPanel>

        <TabPanel value={tabValue} index={1}>
          {/* Properties Tab */}
          <Box sx={{ mb: 2 }}>
            <Button
              variant="contained"
              startIcon={<AddIcon />}
              size="small"
            >
              Add Property
            </Button>
          </Box>
          
          <TableContainer component={Paper}>
            <Table>
              <TableHead>
                <TableRow>
                  <TableCell>Name</TableCell>
                  <TableCell>Label</TableCell>
                  <TableCell>Type</TableCell>
                  <TableCell>Required</TableCell>
                  <TableCell>Status</TableCell>
                  <TableCell align="right">Actions</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {objectType.properties.map((property) => (
                  <TableRow key={property.name}>
                    <TableCell>{property.name}</TableCell>
                    <TableCell>{property.label}</TableCell>
                    <TableCell>
                      <Chip label={property.type} size="small" />
                    </TableCell>
                    <TableCell>
                      {property.required ? 'Yes' : 'No'}
                    </TableCell>
                    <TableCell>{renderPropertyStatus(property)}</TableCell>
                    <TableCell align="right">
                      <IconButton size="small">
                        <EditIcon />
                      </IconButton>
                      <IconButton size="small">
                        <VisibilityIcon />
                      </IconButton>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </TableContainer>
        </TabPanel>

        <TabPanel value={tabValue} index={2}>
          <Typography>Security settings will be displayed here</Typography>
        </TabPanel>

        <TabPanel value={tabValue} index={3}>
          <Typography>Datasources configuration will be displayed here</Typography>
        </TabPanel>

        <TabPanel value={tabValue} index={4}>
          <Typography>Transform settings will be displayed here</Typography>
        </TabPanel>
      </Paper>
    </Box>
  );
};