import React, { useState, useEffect } from 'react';
import { 
  Button, 
  Card, 
  Classes,
  Dialog,
  FormGroup,
  HTMLSelect,
  Icon, 
  InputGroup, 
  Intent, 
  Tab,
  Tabs,
  Tag,
  TextArea
} from '@blueprintjs/core';
import clsx from 'clsx';
import { useOntologyStore } from '../../../stores/ontology.store';
import { ontologyApi, OntologyApiError } from '../../../api/ontologyClient';
import { LinkType } from '../../../stores/ontology.store';

// Cardinality options for relationships
const CARDINALITY_OPTIONS = [
  { value: 'one_to_one', label: 'One to One (1:1)', description: 'Each entity relates to exactly one other entity' },
  { value: 'one_to_many', label: 'One to Many (1:N)', description: 'One entity can relate to many others' },
  { value: 'many_to_one', label: 'Many to One (N:1)', description: 'Many entities can relate to one other' },
  { value: 'many_to_many', label: 'Many to Many (N:N)', description: 'Many entities can relate to many others' },
];

interface LinkTypeEditorProps {
  isOpen: boolean;
  onClose: () => void;
  linkType?: LinkType | null; // null for create, LinkType for edit
}

export const LinkTypeEditor: React.FC<LinkTypeEditorProps> = ({
  isOpen,
  onClose,
  linkType
}) => {
  const {
    currentDatabase,
    objectTypes,
    addLinkType,
    updateLinkType,
    setError,
    clearError
  } = useOntologyStore();

  const [selectedTab, setSelectedTab] = useState<'general' | 'mapping' | 'validation'>('general');
  const [isSaving, setIsSaving] = useState(false);
  
  // Form state
  const [formData, setFormData] = useState<{
    id: string;
    label: string;
    description: string;
    from_class: string;
    to_class: string;
    cardinality: 'one_to_one' | 'one_to_many' | 'many_to_one' | 'many_to_many';
    properties: any[];
  }>({
    id: '',
    label: '',
    description: '',
    from_class: '',
    to_class: '',
    cardinality: 'one_to_many',
    properties: []
  });

  // Initialize form data
  useEffect(() => {
    if (linkType) {
      setFormData({
        id: linkType.id,
        label: linkType.label,
        description: linkType.description || '',
        from_class: linkType.from_class,
        to_class: linkType.to_class,
        cardinality: linkType.cardinality,
        properties: linkType.properties || []
      });
    } else {
      setFormData({
        id: '',
        label: '',
        description: '',
        from_class: '',
        to_class: '',
        cardinality: 'one_to_many',
        properties: []
      });
    }
    setSelectedTab('general');
    clearError('link-type-editor');
  }, [linkType, isOpen]);

  const isCreateMode = !linkType;

  const handleSave = async () => {
    if (!currentDatabase) return;

    // Validation
    if (!formData.id.trim() || !formData.label.trim()) {
      setError('link-type-editor', 'ID and Label are required');
      return;
    }

    if (!formData.from_class || !formData.to_class) {
      setError('link-type-editor', 'Both From and To classes must be selected');
      return;
    }

    if (formData.from_class === formData.to_class) {
      setError('link-type-editor', 'Self-referencing links are not supported yet');
      return;
    }

    setIsSaving(true);
    clearError('link-type-editor');

    try {
      const linkTypeData: LinkType = {
        id: formData.id,
        label: formData.label,
        description: formData.description,
        from_class: formData.from_class,
        to_class: formData.to_class,
        cardinality: formData.cardinality,
        properties: formData.properties
      };

      if (isCreateMode) {
        const created = await ontologyApi.linkType.create(currentDatabase, linkTypeData);
        addLinkType(created);
      } else {
        const updated = await ontologyApi.linkType.update(currentDatabase, formData.id, linkTypeData);
        updateLinkType(formData.id, updated);
      }

      onClose();
    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : `Failed to ${isCreateMode ? 'create' : 'update'} link type`;
      setError('link-type-editor', message);
    } finally {
      setIsSaving(false);
    }
  };

  const getCardinalityIcon = (cardinality: string) => {
    switch (cardinality) {
      case 'one_to_one': return 'minus';
      case 'one_to_many': return 'arrow-right';
      case 'many_to_one': return 'arrow-left';
      case 'many_to_many': return 'exchange';
      default: return 'link';
    }
  };

  const renderGeneralTab = () => (
    <div className="general-tab">
      <FormGroup label="Link Type ID *" labelFor="link-id">
        <InputGroup
          id="link-id"
          value={formData.id}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => 
            setFormData({...formData, id: e.target.value})
          }
          placeholder="Enter unique identifier..."
          disabled={!isCreateMode} // Can't change ID after creation
        />
      </FormGroup>

      <FormGroup label="Display Label *" labelFor="link-label">
        <InputGroup
          id="link-label"
          value={formData.label}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => 
            setFormData({...formData, label: e.target.value})
          }
          placeholder="Enter display name..."
        />
      </FormGroup>

      <FormGroup label="Description" labelFor="link-description">
        <TextArea
          id="link-description"
          value={formData.description}
          onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => 
            setFormData({...formData, description: e.target.value})
          }
          placeholder="Enter description..."
          rows={4}
          fill
        />
      </FormGroup>
    </div>
  );

  const renderMappingTab = () => (
    <div className="mapping-tab">
      <div className="relationship-builder">
        <Card className="relationship-card">
          <h5>Relationship Definition</h5>
          
          <div className="relationship-form">
            <FormGroup label="From Class *" labelFor="from-class">
              <HTMLSelect
                id="from-class"
                value={formData.from_class}
                onChange={(e: React.ChangeEvent<HTMLSelectElement>) => 
                  setFormData({...formData, from_class: e.target.value})
                }
                fill
              >
                <option value="">Select source class...</option>
                {objectTypes.map(obj => (
                  <option key={obj.id} value={obj.id}>
                    {obj.label} ({obj.id})
                  </option>
                ))}
              </HTMLSelect>
            </FormGroup>

            <div className="relationship-arrow">
              <Icon 
                icon={getCardinalityIcon(formData.cardinality)} 
                size={20} 
                intent={Intent.PRIMARY}
              />
              <span className="cardinality-label">
                {CARDINALITY_OPTIONS.find(c => c.value === formData.cardinality)?.label}
              </span>
            </div>

            <FormGroup label="To Class *" labelFor="to-class">
              <HTMLSelect
                id="to-class"
                value={formData.to_class}
                onChange={(e: React.ChangeEvent<HTMLSelectElement>) => 
                  setFormData({...formData, to_class: e.target.value})
                }
                fill
              >
                <option value="">Select target class...</option>
                {objectTypes.map(obj => (
                  <option key={obj.id} value={obj.id}>
                    {obj.label} ({obj.id})
                  </option>
                ))}
              </HTMLSelect>
            </FormGroup>
          </div>

          <FormGroup label="Cardinality" labelFor="cardinality">
            <HTMLSelect
              id="cardinality"
              value={formData.cardinality}
              onChange={(e: React.ChangeEvent<HTMLSelectElement>) => 
                setFormData({...formData, cardinality: e.target.value as any})
              }
              fill
            >
              {CARDINALITY_OPTIONS.map(option => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </HTMLSelect>
            <div className="cardinality-description">
              {CARDINALITY_OPTIONS.find(c => c.value === formData.cardinality)?.description}
            </div>
          </FormGroup>
        </Card>

        {/* Relationship Preview */}
        {formData.from_class && formData.to_class && (
          <Card className="relationship-preview">
            <h5>Relationship Preview</h5>
            <div className="preview-diagram">
              <div className="preview-node from-node">
                <Icon icon="cube" />
                <span>{objectTypes.find(obj => obj.id === formData.from_class)?.label}</span>
              </div>
              
              <div className="preview-connection">
                <Icon icon={getCardinalityIcon(formData.cardinality)} size={16} />
                <span className="connection-label">{formData.label}</span>
              </div>
              
              <div className="preview-node to-node">
                <Icon icon="cube" />
                <span>{objectTypes.find(obj => obj.id === formData.to_class)?.label}</span>
              </div>
            </div>
          </Card>
        )}
      </div>
    </div>
  );

  const renderValidationTab = () => (
    <div className="validation-tab">
      <Card>
        <h5>Validation Summary</h5>
        
        <div className="validation-checks">
          <div className={clsx('validation-item', {
            'valid': formData.id.trim() && formData.label.trim(),
            'invalid': !formData.id.trim() || !formData.label.trim()
          })}>
            <Icon icon={formData.id.trim() && formData.label.trim() ? "tick" : "cross"} />
            <span>Basic information complete</span>
          </div>

          <div className={clsx('validation-item', {
            'valid': formData.from_class && formData.to_class,
            'invalid': !formData.from_class || !formData.to_class
          })}>
            <Icon icon={formData.from_class && formData.to_class ? "tick" : "cross"} />
            <span>Classes selected</span>
          </div>

          <div className={clsx('validation-item', {
            'valid': formData.from_class !== formData.to_class,
            'invalid': formData.from_class === formData.to_class && formData.from_class
          })}>
            <Icon icon={formData.from_class !== formData.to_class ? "tick" : "cross"} />
            <span>No self-referencing (coming soon)</span>
          </div>

          <div className={clsx('validation-item', {
            'valid': objectTypes.some(obj => obj.id === formData.from_class),
            'warning': !objectTypes.some(obj => obj.id === formData.from_class) && formData.from_class
          })}>
            <Icon icon={objectTypes.some(obj => obj.id === formData.from_class) ? "tick" : "warning-sign"} />
            <span>From class exists</span>
          </div>

          <div className={clsx('validation-item', {
            'valid': objectTypes.some(obj => obj.id === formData.to_class),
            'warning': !objectTypes.some(obj => obj.id === formData.to_class) && formData.to_class
          })}>
            <Icon icon={objectTypes.some(obj => obj.id === formData.to_class) ? "tick" : "warning-sign"} />
            <span>To class exists</span>
          </div>
        </div>
      </Card>

      {/* Relationship Impact Analysis */}
      <Card className="impact-analysis">
        <h5>Impact Analysis</h5>
        <p>
          This relationship will connect{' '}
          <Tag minimal>
            {objectTypes.find(obj => obj.id === formData.from_class)?.label || formData.from_class}
          </Tag>
          {' '}to{' '}
          <Tag minimal>
            {objectTypes.find(obj => obj.id === formData.to_class)?.label || formData.to_class}
          </Tag>
          {' '}with{' '}
          <Tag intent={Intent.PRIMARY}>
            {CARDINALITY_OPTIONS.find(c => c.value === formData.cardinality)?.label}
          </Tag>
          {' '}cardinality.
        </p>
        
        {formData.cardinality === 'many_to_many' && (
          <div className="warning-note">
            <Icon icon="warning-sign" intent={Intent.WARNING} />
            <span>Many-to-many relationships may impact query performance.</span>
          </div>
        )}
      </Card>
    </div>
  );

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title={
        <div className="dialog-title">
          <Icon icon="link" />
          <span>{isCreateMode ? 'Create Link Type' : `Edit ${linkType?.label}`}</span>
        </div>
      }
      className="link-type-editor-dialog"
      style={{ width: '800px', maxHeight: '90vh' }}
    >
      <div className={Classes.DIALOG_BODY}>
        <Tabs
          id="link-type-tabs"
          selectedTabId={selectedTab}
          onChange={(tabId) => setSelectedTab(tabId as any)}
        >
          <Tab
            id="general"
            title="General"
            panel={renderGeneralTab()}
          />
          <Tab
            id="mapping"
            title="Relationship"
            panel={renderMappingTab()}
          />
          <Tab
            id="validation"
            title="Validation"
            panel={renderValidationTab()}
          />
        </Tabs>
      </div>
      
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Cancel" onClick={onClose} disabled={isSaving} />
          <Button
            text={isCreateMode ? 'Create Link Type' : 'Save Changes'}
            intent={Intent.PRIMARY}
            onClick={handleSave}
            disabled={
              !formData.id.trim() || 
              !formData.label.trim() || 
              !formData.from_class ||
              !formData.to_class ||
              formData.from_class === formData.to_class ||
              isSaving
            }
            loading={isSaving}
          />
        </div>
      </div>
    </Dialog>
  );
};