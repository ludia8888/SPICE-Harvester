import React, { useState } from 'react';
import { 
  Button, 
  ButtonGroup,
  Card, 
  Classes, 
  Icon, 
  InputGroup, 
  Intent, 
  Menu, 
  MenuItem, 
  Popover, 
  Position,
  Tag,
  Tooltip,
  HTMLTable
} from '@blueprintjs/core';
import clsx from 'clsx';
import { useOntologyStore, useFilteredLinkTypes } from '../../../stores/ontology.store';
import { LinkType } from '../../../stores/ontology.store';
import { LinkTypeEditor } from './LinkTypeEditor';

export const LinkTypePanel: React.FC = () => {
  const filteredLinkTypes = useFilteredLinkTypes();
  const { 
    selectedLinkType, 
    selectLinkType, 
    searchQuery, 
    setSearchQuery 
  } = useOntologyStore();

  const [isEditorOpen, setIsEditorOpen] = useState(false);
  const [editingLinkType, setEditingLinkType] = useState<LinkType | null>(null);

  const getCardinalityIcon = (cardinality: string) => {
    switch (cardinality) {
      case 'one_to_one': return 'minus';
      case 'one_to_many': return 'arrow-right';
      case 'many_to_one': return 'arrow-left';
      case 'many_to_many': return 'exchange';
      default: return 'link';
    }
  };

  const getCardinalityLabel = (cardinality: string) => {
    switch (cardinality) {
      case 'one_to_one': return '1:1';
      case 'one_to_many': return '1:N';
      case 'many_to_one': return 'N:1';
      case 'many_to_many': return 'N:N';
      default: return cardinality;
    }
  };

  const handleLinkTypeSelect = (linkType: LinkType) => {
    selectLinkType(linkType.id);
  };

  return (
    <div className="link-type-panel">
      <div className="panel-toolbar">
        <InputGroup
          leftIcon="search"
          placeholder="Search link types..."
          value={searchQuery}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSearchQuery(e.target.value)}
          small
          fill
        />
        
        <ButtonGroup minimal>
          <Tooltip content="Create new link type">
            <Button
              icon="add"
              intent={Intent.PRIMARY}
              small
              onClick={() => {
                setEditingLinkType(null);
                setIsEditorOpen(true);
              }}
            />
          </Tooltip>
          
          <Popover
            content={
              <Menu>
                <MenuItem text="Sort by Name" icon="sort-alphabetical" />
                <MenuItem text="Sort by Cardinality" icon="sort" />
                <MenuItem text="Sort by Modified" icon="sort-numerical" />
              </Menu>
            }
            position={Position.BOTTOM_RIGHT}
          >
            <Button icon="sort" small />
          </Popover>
        </ButtonGroup>
      </div>

      <div className="panel-content">
        {filteredLinkTypes.length === 0 ? (
          <div className="empty-state">
            <Icon icon="link" size={32} />
            <h4>No Link Types</h4>
            <p>
              {searchQuery 
                ? `No link types match "${searchQuery}"`
                : 'Create relationships between object types'
              }
            </p>
            <Button
              text="Create Link Type"
              icon="add"
              intent={Intent.PRIMARY}
              onClick={() => {
                setEditingLinkType(null);
                setIsEditorOpen(true);
              }}
            />
          </div>
        ) : (
          <div className="link-types-list">
            {filteredLinkTypes.map(linkType => (
              <Card 
                key={linkType.id}
                className={clsx('link-type-card', {
                  'selected': selectedLinkType === linkType.id
                })}
                onClick={() => handleLinkTypeSelect(linkType)}
                interactive
                elevation={selectedLinkType === linkType.id ? 2 : 0}
              >
                <div className="link-type-header">
                  <div className="link-info">
                    <Icon icon="link" size={16} />
                    <span className="link-label">{linkType.label}</span>
                  </div>
                  
                  <Tag
                    icon={getCardinalityIcon(linkType.cardinality)}
                    intent={Intent.NONE}
                    minimal
                    small
                  >
                    {getCardinalityLabel(linkType.cardinality)}
                  </Tag>
                </div>

                <div className="link-relationship">
                  <span className="from-class">{linkType.from_class}</span>
                  <Icon 
                    icon={getCardinalityIcon(linkType.cardinality)} 
                    size={12} 
                    className="relationship-icon"
                  />
                  <span className="to-class">{linkType.to_class}</span>
                </div>

                {linkType.description && (
                  <div className="link-description">
                    {linkType.description}
                  </div>
                )}

                {linkType.properties && linkType.properties.length > 0 && (
                  <div className="link-properties">
                    <Icon icon="properties" size={12} />
                    <span>{linkType.properties.length} properties</span>
                  </div>
                )}
              </Card>
            ))}
          </div>
        )}
      </div>

      {selectedLinkType && (
        <div className="selected-link-details">
          <Card className="details-card">
            <h5>Link Type Details</h5>
            {(() => {
              const linkType = filteredLinkTypes.find(lt => lt.id === selectedLinkType);
              if (!linkType) return null;

              return (
                <div className="link-details-content">
                  <HTMLTable small striped className="link-details-table">
                    <tbody>
                      <tr>
                        <td><strong>ID</strong></td>
                        <td>{linkType.id}</td>
                      </tr>
                      <tr>
                        <td><strong>Label</strong></td>
                        <td>{linkType.label}</td>
                      </tr>
                      <tr>
                        <td><strong>From</strong></td>
                        <td>{linkType.from_class}</td>
                      </tr>
                      <tr>
                        <td><strong>To</strong></td>
                        <td>{linkType.to_class}</td>
                      </tr>
                      <tr>
                        <td><strong>Cardinality</strong></td>
                        <td>
                          <Tag
                            icon={getCardinalityIcon(linkType.cardinality)}
                            intent={Intent.PRIMARY}
                            minimal
                            small
                          >
                            {getCardinalityLabel(linkType.cardinality)}
                          </Tag>
                        </td>
                      </tr>
                    </tbody>
                  </HTMLTable>

                  {linkType.properties && linkType.properties.length > 0 && (
                    <div className="link-properties-section">
                      <h6>Properties</h6>
                      {linkType.properties.map(property => (
                        <div key={property.name} className="property-item">
                          <Icon 
                            icon={property.required ? "key" : "property"} 
                            size={12}
                            intent={property.required ? Intent.WARNING : Intent.NONE}
                          />
                          <span>{property.label || property.name}</span>
                          <Tag minimal small>
                            {property.type.replace('xsd:', '').replace('custom:', '')}
                          </Tag>
                        </div>
                      ))}
                    </div>
                  )}

                  <ButtonGroup className="link-actions">
                    <Button 
                      text="Edit" 
                      icon="edit" 
                      small 
                      onClick={() => {
                        setEditingLinkType(linkType);
                        setIsEditorOpen(true);
                      }}
                    />
                    <Button 
                      text="Delete" 
                      icon="trash" 
                      small 
                      intent={Intent.DANGER}
                      onClick={() => {
                        if (confirm(`Are you sure you want to delete "${linkType.label}"?`)) {
                          // TODO: Implement delete
                          console.log('Delete link type:', linkType.id);
                        }
                      }}
                    />
                  </ButtonGroup>
                </div>
              );
            })()}
          </Card>
        </div>
      )}

      {/* Link Type Editor Dialog */}
      <LinkTypeEditor
        isOpen={isEditorOpen}
        onClose={() => {
          setIsEditorOpen(false);
          setEditingLinkType(null);
        }}
        linkType={editingLinkType}
      />
    </div>
  );
};