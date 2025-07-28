import React, { useState, useEffect } from 'react';
import { 
  Button, 
  Card, 
  Icon, 
  Intent, 
  Menu, 
  MenuItem, 
  Popover, 
  Position,
  Tag,
  Spinner
} from '@blueprintjs/core';
import { useOntologyStore } from '../../../stores/ontology.store';
import { ontologyApi, OntologyApiError } from '../../../api/ontologyClient';

interface CommitInfo {
  id: string;
  message: string;
  author: string;
  timestamp: string;
  branch: string;
}

export const HistoryPanel: React.FC = () => {
  const { 
    currentDatabase,
    currentBranch,
    setError
  } = useOntologyStore();

  const [commits, setCommits] = useState<CommitInfo[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [selectedCommit, setSelectedCommit] = useState<string | null>(null);

  useEffect(() => {
    if (currentDatabase) {
      loadHistory();
    }
  }, [currentDatabase, currentBranch]);

  const loadHistory = async () => {
    if (!currentDatabase) return;

    setIsLoading(true);
    try {
      const history = await ontologyApi.branch.history(currentDatabase, 50);
      setCommits(history);
    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : 'Failed to load history';
      setError('history', message);
    } finally {
      setIsLoading(false);
    }
  };

  const handleCommitClick = (commit: CommitInfo) => {
    setSelectedCommit(selectedCommit === commit.id ? null : commit.id);
  };

  const handleViewDiff = (commitId: string) => {
    // TODO: Implement diff view
    console.log('View diff for commit:', commitId);
  };

  const handleRevertCommit = (commitId: string) => {
    // TODO: Implement commit revert
    if (confirm('Are you sure you want to revert this commit?')) {
      console.log('Revert commit:', commitId);
    }
  };

  const formatDate = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
    
    if (diffDays === 0) {
      return date.toLocaleTimeString();
    } else if (diffDays === 1) {
      return 'Yesterday';
    } else if (diffDays < 7) {
      return `${diffDays} days ago`;
    } else {
      return date.toLocaleDateString();
    }
  };

  const getCommitIcon = (commit: CommitInfo) => {
    if (commit.branch === 'main') return 'git-commit';
    return 'git-branch';
  };

  if (isLoading) {
    return (
      <div className="history-panel">
        <div className="loading-state">
          <Spinner size={20} />
          <span>Loading history...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="history-panel">
      <div className="panel-toolbar">
        <div className="history-info">
          <Icon icon="history" />
          <span>Commit History</span>
          <Tag minimal>{commits.length}</Tag>
        </div>
        
        <Button
          icon="refresh"
          minimal
          small
          onClick={loadHistory}
          title="Refresh history"
        />
      </div>

      <div className="commits-list">
        {commits.length === 0 ? (
          <div className="empty-state">
            <Icon icon="history" size={24} />
            <h4>No Commits</h4>
            <p>No commit history available for this database.</p>
          </div>
        ) : (
          commits.map((commit, index) => (
            <Card
              key={commit.id}
              className={`commit-card ${selectedCommit === commit.id ? 'selected' : ''}`}
              interactive
              onClick={() => handleCommitClick(commit)}
            >
              <div className="commit-header">
                <div className="commit-info">
                  <Icon 
                    icon={getCommitIcon(commit)} 
                    size={14}
                    intent={commit.branch === 'main' ? Intent.SUCCESS : Intent.PRIMARY}
                  />
                  <span className="commit-id">
                    {commit.id.substring(0, 7)}
                  </span>
                  {commit.branch !== currentBranch && (
                    <Tag minimal small intent={Intent.WARNING}>
                      {commit.branch}
                    </Tag>
                  )}
                </div>
                
                <div className="commit-time">
                  {formatDate(commit.timestamp)}
                </div>
              </div>

              <div className="commit-message">
                {commit.message}
              </div>

              <div className="commit-author">
                <Icon icon="person" size={12} />
                <span>{commit.author}</span>
              </div>

              {selectedCommit === commit.id && (
                <div className="commit-actions">
                  <Button
                    text="View Changes"
                    icon="changes"
                    small
                    onClick={(e) => {
                      e.stopPropagation();
                      handleViewDiff(commit.id);
                    }}
                  />
                  
                  <Button
                    text="Revert"
                    icon="undo"
                    small
                    intent={Intent.DANGER}
                    onClick={(e) => {
                      e.stopPropagation();
                      handleRevertCommit(commit.id);
                    }}
                    disabled={index === 0} // Can't revert latest commit
                  />
                </div>
              )}
            </Card>
          ))
        )}
      </div>

      {commits.length > 0 && (
        <div className="history-footer">
          <Button
            text="Load More"
            icon="chevron-down"
            minimal
            small
            fill
            onClick={() => {
              // TODO: Load more commits with pagination
              console.log('Load more commits');
            }}
          />
        </div>
      )}
    </div>
  );
};