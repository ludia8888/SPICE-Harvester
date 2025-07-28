import React, { useState } from 'react';
import { 
  Button, 
  ButtonGroup,
  Card, 
  FormGroup,
  Icon, 
  InputGroup, 
  Intent, 
  Menu, 
  MenuItem, 
  Popover, 
  Position,
  Tag,
  Dialog,
  Classes
} from '@blueprintjs/core';
import clsx from 'clsx';
import { useOntologyStore } from '../../../stores/ontology.store';
import { ontologyApi, OntologyApiError } from '../../../api/ontologyClient';

interface BranchPanelProps {
  onBranchChange: (branchName: string) => void;
}

export const BranchPanel: React.FC<BranchPanelProps> = ({ onBranchChange }) => {
  const { 
    currentDatabase,
    currentBranch,
    branches,
    setBranches,
    setLoading,
    setError
  } = useOntologyStore();

  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false);
  const [newBranchName, setNewBranchName] = useState('');
  const [isCreating, setIsCreating] = useState(false);

  const handleCreateBranch = async () => {
    if (!currentDatabase || !newBranchName.trim()) return;

    setIsCreating(true);
    try {
      const newBranch = await ontologyApi.branch.create(
        currentDatabase,
        newBranchName.trim(),
        currentBranch
      );
      
      // Refresh branches list
      const branchesData = await ontologyApi.branch.list(currentDatabase);
      setBranches(branchesData.branches);
      
      setIsCreateDialogOpen(false);
      setNewBranchName('');
    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : 'Failed to create branch';
      setError('branch-create', message);
    } finally {
      setIsCreating(false);
    }
  };

  const handleBranchSwitch = (branchName: string) => {
    onBranchChange(branchName);
  };

  const getBranchIcon = (branch: any) => {
    if (branch.is_current) return 'git-branch';
    if (branch.name === 'main') return 'crown';
    return 'git-branch';
  };

  const getBranchIntent = (branch: any) => {
    if (branch.is_current) return Intent.PRIMARY;
    if (branch.name === 'main') return Intent.SUCCESS;
    return Intent.NONE;
  };

  return (
    <div className="branch-panel">
      <div className="panel-toolbar">
        <div className="current-branch-info">
          <Icon icon="git-branch" intent={Intent.PRIMARY} />
          <span className="branch-name">{currentBranch}</span>
        </div>
        
        <Button
          icon="add"
          text="New Branch"
          intent={Intent.PRIMARY}
          small
          onClick={() => setIsCreateDialogOpen(true)}
          disabled={!currentDatabase}
        />
      </div>

      <div className="branches-list">
        {branches.length === 0 ? (
          <div className="empty-state">
            <Icon icon="git-branch" size={24} />
            <p>No branches available</p>
          </div>
        ) : (
          branches.map(branch => (
            <Card
              key={branch.name}
              className={clsx('branch-card', {
                'current-branch': branch.is_current
              })}
              interactive={!branch.is_current}
              onClick={() => !branch.is_current && handleBranchSwitch(branch.name)}
            >
              <div className="branch-header">
                <div className="branch-info">
                  <Icon 
                    icon={getBranchIcon(branch)} 
                    intent={getBranchIntent(branch)}
                    size={14}
                  />
                  <span className="branch-label">{branch.name}</span>
                  {branch.is_current && (
                    <Tag intent={Intent.PRIMARY} minimal small>
                      Current
                    </Tag>
                  )}
                </div>
                
                {!branch.is_current && (
                  <Popover
                    content={
                      <Menu>
                        <MenuItem
                          text="Switch to branch"
                          icon="git-branch"
                          onClick={() => handleBranchSwitch(branch.name)}
                        />
                        <MenuItem
                          text="Merge to main"
                          icon="git-merge"
                          disabled={branch.name === 'main'}
                        />
                        <MenuItem
                          text="Delete branch"
                          icon="trash"
                          intent={Intent.DANGER}
                          disabled={branch.name === 'main'}
                        />
                      </Menu>
                    }
                    position={Position.RIGHT_TOP}
                  >
                    <Button
                      icon="more"
                      minimal
                      small
                      onClick={(e) => e.stopPropagation()}
                    />
                  </Popover>
                )}
              </div>

              <div className="branch-stats">
                <div className="stat">
                  <Icon icon="git-commit" size={12} />
                  <span>{branch.commit_count || 0} commits</span>
                </div>
                
                {branch.last_commit && (
                  <div className="last-commit">
                    <Icon icon="time" size={12} />
                    <span className="commit-message">
                      {branch.last_commit.message.substring(0, 30)}
                      {branch.last_commit.message.length > 30 ? '...' : ''}
                    </span>
                  </div>
                )}
              </div>
            </Card>
          ))
        )}
      </div>

      {/* Create Branch Dialog */}
      <Dialog
        isOpen={isCreateDialogOpen}
        onClose={() => setIsCreateDialogOpen(false)}
        title="Create New Branch"
        className="create-branch-dialog"
      >
        <div className={Classes.DIALOG_BODY}>
          <FormGroup
            label="Branch Name"
            labelFor="branch-name-input"
            helperText={`New branch will be created from "${currentBranch}"`}
          >
            <InputGroup
              id="branch-name-input"
              value={newBranchName}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => 
                setNewBranchName(e.target.value)
              }
              placeholder="Enter branch name..."
              autoFocus
              onKeyPress={(e) => {
                if (e.key === 'Enter') {
                  handleCreateBranch();
                }
              }}
            />
          </FormGroup>
        </div>
        
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button
              text="Cancel"
              onClick={() => setIsCreateDialogOpen(false)}
              disabled={isCreating}
            />
            <Button
              text="Create Branch"
              intent={Intent.PRIMARY}
              onClick={handleCreateBranch}
              disabled={!newBranchName.trim() || isCreating}
              loading={isCreating}
            />
          </div>
        </div>
      </Dialog>
    </div>
  );
};