import React, { useState } from 'react';
import { 
  Button, 
  Card,
  Classes,
  Dialog,
  FormGroup,
  HTMLSelect,
  Icon, 
  Intent, 
  Tag,
  Alert,
  RadioGroup,
  Radio,
  TextArea
} from '@blueprintjs/core';
import { useOntologyStore } from '../../../stores/ontology.store';

interface MergeConflict {
  conflict_id: string;
  type: 'schema_conflict' | 'data_conflict' | 'relationship_conflict';
  description: string;
  source_branch_data: any;
  target_branch_data: any;
  suggested_resolution?: any;
}

interface MergePreview {
  source_branch: string;
  target_branch: string;
  mergeable: boolean;
  conflicts_detected: number;
  conflicts: MergeConflict[];
  statistics: {
    added: number;
    modified: number;
    deleted: number;
    conflicts: number;
  };
}

interface MergeConflictResolverProps {
  isOpen: boolean;
  onClose: () => void;
  onMergeComplete?: () => void;
}

export const MergeConflictResolver: React.FC<MergeConflictResolverProps> = ({
  isOpen,
  onClose,
  onMergeComplete
}) => {
  const { 
    currentDatabase,
    branches,
    currentBranch,
    setError,
    clearError
  } = useOntologyStore();

  const [sourceBranch, setSourceBranch] = useState('');
  const [targetBranch, setTargetBranch] = useState(currentBranch || 'main');
  const [mergeStrategy, setMergeStrategy] = useState<'merge' | 'rebase'>('merge');
  const [mergeMessage, setMergeMessage] = useState('');
  
  const [isSimulating, setIsSimulating] = useState(false);
  const [isResolving, setIsResolving] = useState(false);
  const [mergePreview, setMergePreview] = useState<MergePreview | null>(null);
  const [conflictResolutions, setConflictResolutions] = useState<Record<string, any>>({});

  const handleSimulateMerge = async () => {
    if (!currentDatabase || !sourceBranch || !targetBranch) return;

    setIsSimulating(true);
    clearError('merge-resolver');

    try {
      const response = await fetch(`http://localhost:8002/api/v1/database/${currentDatabase}/merge/simulate`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept-Language': 'ko',
        },
        body: JSON.stringify({
          source_branch: sourceBranch,
          target_branch: targetBranch,
          strategy: mergeStrategy
        }),
      });

      if (!response.ok) {
        throw new Error(`Merge simulation failed: ${response.statusText}`);
      }

      const result = await response.json();
      setMergePreview(result.data?.merge_preview || result);
      
      // Initialize conflict resolutions
      const initialResolutions: Record<string, any> = {};
      (result.data?.merge_preview?.conflicts || []).forEach((conflict: MergeConflict) => {
        initialResolutions[conflict.conflict_id] = conflict.suggested_resolution || 'source';
      });
      setConflictResolutions(initialResolutions);

    } catch (error) {
      const message = error instanceof Error ? error.message : 'Merge simulation failed';
      setError('merge-resolver', message);
    } finally {
      setIsSimulating(false);
    }
  };

  const handleResolveMerge = async () => {
    if (!currentDatabase || !mergePreview) return;

    setIsResolving(true);
    clearError('merge-resolver');

    try {
      const response = await fetch(`http://localhost:8002/api/v1/database/${currentDatabase}/merge/resolve`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept-Language': 'ko',
        },
        body: JSON.stringify({
          source_branch: sourceBranch,
          target_branch: targetBranch,
          strategy: mergeStrategy,
          message: mergeMessage || `Merge ${sourceBranch} into ${targetBranch}`,
          conflict_resolutions: conflictResolutions
        }),
      });

      if (!response.ok) {
        throw new Error(`Merge resolution failed: ${response.statusText}`);
      }

      await response.json();
      
      if (onMergeComplete) {
        onMergeComplete();
      }

      // Reset and close
      setMergePreview(null);
      setConflictResolutions({});
      setMergeMessage('');
      onClose();

    } catch (error) {
      const message = error instanceof Error ? error.message : 'Merge resolution failed';
      setError('merge-resolver', message);
    } finally {
      setIsResolving(false);
    }
  };

  const handleConflictResolution = (conflictId: string, resolution: any) => {
    setConflictResolutions(prev => ({
      ...prev,
      [conflictId]: resolution
    }));
  };

  const renderMergeForm = () => (
    <div className="merge-form">
      <FormGroup label="Source Branch" labelFor="source-branch">
        <HTMLSelect
          id="source-branch"
          value={sourceBranch}
          onChange={(e) => setSourceBranch(e.target.value)}
          options={[
            { value: '', label: 'Select source branch...' },
            ...branches
              .filter(b => b.name !== targetBranch)
              .map(branch => ({
                value: branch.name,
                label: branch.name + (branch.is_current ? ' (current)' : '')
              }))
          ]}
        />
      </FormGroup>

      <FormGroup label="Target Branch" labelFor="target-branch">
        <HTMLSelect
          id="target-branch"
          value={targetBranch}
          onChange={(e) => setTargetBranch(e.target.value)}
          options={branches.map(branch => ({
            value: branch.name,
            label: branch.name + (branch.is_current ? ' (current)' : '')
          }))}
        />
      </FormGroup>

      <FormGroup label="Merge Strategy" labelFor="merge-strategy">
        <RadioGroup
          selectedValue={mergeStrategy}
          onChange={(e) => setMergeStrategy(e.currentTarget.value as any)}
        >
          <Radio label="Merge (creates merge commit)" value="merge" />
          <Radio label="Rebase (linear history)" value="rebase" />
        </RadioGroup>
      </FormGroup>

      <FormGroup label="Merge Message" labelFor="merge-message">
        <TextArea
          id="merge-message"
          value={mergeMessage}
          onChange={(e) => setMergeMessage(e.target.value)}
          placeholder={`Merge ${sourceBranch} into ${targetBranch}`}
          rows={3}
          fill
        />
      </FormGroup>

      <div className="merge-actions">
        <Button
          text="Simulate Merge"
          icon="git-merge"
          intent={Intent.PRIMARY}
          onClick={handleSimulateMerge}
          disabled={!sourceBranch || !targetBranch || isSimulating}
          loading={isSimulating}
        />
      </div>
    </div>
  );

  const renderMergePreview = () => {
    if (!mergePreview) return null;

    return (
      <div className="merge-preview">
        <div className="preview-header">
          <h4>Merge Preview</h4>
          <div className="merge-info">
            <Tag minimal>{mergePreview.source_branch}</Tag>
            <Icon icon="arrow-right" />
            <Tag minimal>{mergePreview.target_branch}</Tag>
          </div>
        </div>

        <div className="merge-statistics">
          <div className="stats-row">
            <div className="stat-item success">
              <Icon icon="add" />
              <span>{mergePreview.statistics?.added || 0} Added</span>
            </div>
            <div className="stat-item info">
              <Icon icon="edit" />
              <span>{mergePreview.statistics?.modified || 0} Modified</span>
            </div>
            <div className="stat-item warning">
              <Icon icon="remove" />
              <span>{mergePreview.statistics?.deleted || 0} Deleted</span>
            </div>
            <div className="stat-item danger">
              <Icon icon="error" />
              <span>{mergePreview.conflicts_detected || 0} Conflicts</span>
            </div>
          </div>
        </div>

        {mergePreview.mergeable ? (
          <Alert isOpen={true} intent={Intent.SUCCESS} icon="tick">
            This merge can be completed automatically with no conflicts.
          </Alert>
        ) : (
          <Alert isOpen={true} intent={Intent.WARNING} icon="warning-sign">
            This merge has {mergePreview.conflicts_detected} conflicts that need to be resolved.
          </Alert>
        )}

        {mergePreview.conflicts && mergePreview.conflicts.length > 0 && (
          <div className="conflicts-section">
            <h5>Conflicts ({mergePreview.conflicts.length})</h5>
            {mergePreview.conflicts.map((conflict) => (
              <Card key={conflict.conflict_id} className="conflict-card">
                <div className="conflict-header">
                  <div className="conflict-info">
                    <Tag intent={Intent.DANGER}>{conflict.type}</Tag>
                    <span className="conflict-id">{conflict.conflict_id}</span>
                  </div>
                </div>
                
                <div className="conflict-description">
                  {conflict.description}
                </div>

                <div className="conflict-resolution">
                  <h6>Resolution</h6>
                  <RadioGroup
                    selectedValue={conflictResolutions[conflict.conflict_id] || 'source'}
                    onChange={(e) => handleConflictResolution(conflict.conflict_id, e.currentTarget.value)}
                  >
                    <Radio label="Keep source branch version" value="source" />
                    <Radio label="Keep target branch version" value="target" />
                    <Radio label="Merge both (manual)" value="merge" />
                  </RadioGroup>
                </div>
              </Card>
            ))}
          </div>
        )}

        <div className="merge-final-actions">
          <Button
            text={mergePreview.mergeable ? 'Complete Merge' : 'Resolve and Merge'}
            icon="git-merge"
            intent={Intent.SUCCESS}
            onClick={handleResolveMerge}
            disabled={isResolving}
            loading={isResolving}
            large
          />
        </div>
      </div>
    );
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title={
        <div className="dialog-title">
          <Icon icon="git-merge" />
          <span>Merge Branches</span>
        </div>
      }
      className="merge-conflict-resolver-dialog"
      style={{ width: '800px', maxHeight: '90vh' }}
    >
      <div className={Classes.DIALOG_BODY}>
        {renderMergeForm()}
        {renderMergePreview()}
      </div>
      
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Cancel" onClick={onClose} />
        </div>
      </div>
    </Dialog>
  );
};