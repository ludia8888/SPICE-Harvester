import React, { useState } from 'react';
import { 
  Button, 
  Classes,
  Dialog,
  FormGroup,
  InputGroup, 
  Intent, 
  TextArea,
  Icon
} from '@blueprintjs/core';
import { useOntologyStore } from '../../../stores/ontology.store';
import { ontologyApi, OntologyApiError } from '../../../api/ontologyClient';

interface CommitDialogProps {
  isOpen: boolean;
  onClose: () => void;
  onSuccess?: (commitId: string) => void;
}

export const CommitDialog: React.FC<CommitDialogProps> = ({
  isOpen,
  onClose,
  onSuccess
}) => {
  const { 
    currentDatabase,
    currentBranch,
    setError,
    clearError
  } = useOntologyStore();

  const [commitMessage, setCommitMessage] = useState('');
  const [author, setAuthor] = useState('User');
  const [isCommitting, setIsCommitting] = useState(false);

  const handleCommit = async () => {
    if (!currentDatabase || !commitMessage.trim()) return;

    setIsCommitting(true);
    clearError('commit-dialog');

    try {
      // Call BFF commit API
      const response = await fetch(`http://localhost:8002/database/${currentDatabase}/commit`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept-Language': 'ko',
        },
        body: JSON.stringify({
          message: commitMessage.trim(),
          author: author.trim() || 'User'
        }),
      });

      if (!response.ok) {
        throw new Error(`Commit failed: ${response.statusText}`);
      }

      const result = await response.json();
      
      if (onSuccess && result.commit_id) {
        onSuccess(result.commit_id);
      }

      // Reset form and close
      setCommitMessage('');
      setAuthor('User');
      onClose();

    } catch (error) {
      const message = error instanceof Error ? error.message : 'Commit failed';
      setError('commit-dialog', message);
    } finally {
      setIsCommitting(false);
    }
  };

  const handleClose = () => {
    if (!isCommitting) {
      setCommitMessage('');
      setAuthor('User');
      clearError('commit-dialog');
      onClose();
    }
  };

  return (
    <Dialog
      isOpen={isOpen}
      onClose={handleClose}
      title={
        <div className="dialog-title">
          <Icon icon="git-commit" />
          <span>Commit Changes</span>
        </div>
      }
      className="commit-dialog"
      style={{ width: '500px' }}
    >
      <div className={Classes.DIALOG_BODY}>
        <div className="commit-info">
          <p>
            <strong>Branch:</strong> {currentBranch}
          </p>
          <p>
            <strong>Database:</strong> {currentDatabase}
          </p>
        </div>

        <FormGroup
          label="Commit Message"
          labelFor="commit-message"
          helperText="Describe the changes you've made"
        >
          <TextArea
            id="commit-message"
            value={commitMessage}
            onChange={(e) => setCommitMessage(e.target.value)}
            placeholder="Add new features, fix bugs, update documentation..."
            rows={4}
            fill
            autoFocus
          />
        </FormGroup>

        <FormGroup
          label="Author"
          labelFor="commit-author"
          helperText="Your name or identifier"
        >
          <InputGroup
            id="commit-author"
            value={author}
            onChange={(e) => setAuthor(e.target.value)}
            placeholder="Enter author name..."
          />
        </FormGroup>

        <div className="commit-preview">
          <h5>Preview</h5>
          <div className="preview-content">
            <div className="commit-meta">
              <Icon icon="person" size={14} />
              <span>{author || 'User'}</span>
              <Icon icon="git-branch" size={14} />
              <span>{currentBranch}</span>
            </div>
            <div className="commit-message-preview">
              {commitMessage || 'No commit message'}
            </div>
          </div>
        </div>
      </div>
      
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button 
            text="Cancel" 
            onClick={handleClose} 
            disabled={isCommitting}
          />
          <Button
            text="Commit Changes"
            intent={Intent.PRIMARY}
            icon="git-commit"
            onClick={handleCommit}
            disabled={!commitMessage.trim() || isCommitting}
            loading={isCommitting}
          />
        </div>
      </div>
    </Dialog>
  );
};