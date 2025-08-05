import React, { useEffect, useState } from 'react';
import { 
  Button, 
  ButtonGroup, 
  Card,
  Classes, 
  Dialog,
  Divider,
  FormGroup, 
  Icon, 
  InputGroup, 
  Intent, 
  Menu, 
  MenuItem, 
  Navbar, 
  NavbarDivider, 
  NavbarGroup, 
  NavbarHeading, 
  OverlayToaster,
  Popover, 
  Position, 
  Spinner, 
  Tag,
  TextArea,
  Tooltip
} from '@blueprintjs/core';
import clsx from 'clsx';
import { useOntologyStore } from '../../stores/ontology.store';
import { ontologyApi, OntologyApiError } from '../../api/ontologyClient';
import { extractDatabaseName } from '../../utils/database';
import './OntologyEditor.scss';

// Canvas-based components
import { OntologyCanvas } from './components/OntologyCanvas';
import { BranchPanel } from './components/BranchPanel';
import { HistoryPanel } from './components/HistoryPanel';
import { CommitDialog } from './components/CommitDialog';
import { MergeConflictResolver } from './components/MergeConflictResolver';

// Create toaster instance lazily to avoid React 18 warnings
let AppToaster: any = null;

const getToaster = () => {
  if (!AppToaster) {
    AppToaster = OverlayToaster.create({
      className: "ontology-toaster",
      position: Position.TOP_RIGHT,
    });
  }
  return AppToaster;
};

export const OntologyEditor: React.FC = () => {
  const {
    // State
    currentDatabase,
    currentBranch,
    isLoading,
    isSaving,
    showHistoryPanel,
    showBranchPanel,
    searchQuery,
    errors,
    
    // Actions
    setCurrentDatabase,
    setCurrentBranch,
    setLoading,
    setSaving,
    setDatabases,
    setBranches,
    setObjectTypes,
    setLinkTypes,
    toggleHistoryPanel,
    toggleBranchPanel,
    setSearchQuery,
    setError,
    clearAllErrors,
  } = useOntologyStore();

  const [availableDatabases, setAvailableDatabases] = useState<string[]>([]);
  const [isDatabasePopoverOpen, setIsDatabasePopoverOpen] = useState(false);
  const [isApiConnected, setIsApiConnected] = useState<boolean | null>(null); // null = not checked, true = connected, false = disconnected
  const [isCommitDialogOpen, setIsCommitDialogOpen] = useState(false);
  const [isMergeDialogOpen, setIsMergeDialogOpen] = useState(false);
  const [isCreateDbDialogOpen, setIsCreateDbDialogOpen] = useState(false);
  const [newDbName, setNewDbName] = useState('');
  const [newDbDescription, setNewDbDescription] = useState('');
  const [isDeleteDbDialogOpen, setIsDeleteDbDialogOpen] = useState(false);
  const [dbToDelete, setDbToDelete] = useState<string | null>(null);
  const [isDeleteAllDbsDialogOpen, setIsDeleteAllDbsDialogOpen] = useState(false);

  // Initialize workspace on component mount
  useEffect(() => {
    initializeWorkspace();
  }, []);

  // Load workspace data when database changes
  useEffect(() => {
    if (currentDatabase) {
      loadWorkspaceData();
    }
  }, [currentDatabase]);

  const initializeWorkspace = async () => {
    setLoading(true);
    clearAllErrors();
    
    try {
      // Load available databases  
      const databases = await ontologyApi.database.list();
      
      // Filter out invalid database names containing slashes (these are incorrectly created databases)
      const validDatabases = databases.filter(db => !db.name.includes('/'));
      
      setDatabases(validDatabases);
      const dbNames = validDatabases.map(db => db.name);
      setAvailableDatabases(dbNames);
      
      // Set default database to test_db if none selected
      if (!currentDatabase && validDatabases.length > 0) {
        const defaultDb = validDatabases.find(db => db.name === 'test_db') || validDatabases[0];
        setCurrentDatabase(defaultDb.name);
      }
      setIsApiConnected(true);
    } catch (error) {
      // Check if this is a network/connection error vs other API error
      if (error instanceof OntologyApiError && error.code === 'NETWORK_ERROR') {
        // Real connection failure - show backend not available message
        setIsApiConnected(false);
        setError('initialization', 'Backend API is not available. Please start the backend server.');
        showToast('Backend API is not running. Please start the backend server at http://localhost:8002', Intent.DANGER);
        setAvailableDatabases([]);
        setCurrentDatabase(null);
      } else {
        // API connected but other error occurred - still allow UI to work
        setIsApiConnected(true);
        setAvailableDatabases([]);
        setCurrentDatabase(null);
        showToast('Failed to load databases, but you can still create new ones', Intent.WARNING);
      }
    } finally {
      setLoading(false);
    }
  };

  const loadWorkspaceData = async () => {
    if (!currentDatabase) return;
    
    setLoading(true);
    clearAllErrors();
    
    try {
      const dbName = extractDatabaseName(currentDatabase);
      if (!dbName) return;
      const workspaceData = await ontologyApi.initializeWorkspace(dbName);
      
      
      setBranches(workspaceData.branches);
      setObjectTypes(workspaceData.objectTypes);
      setLinkTypes(workspaceData.linkTypes);
      
      // Set current branch from API response
      const currentBranchData = workspaceData.branches.find(b => b.is_current);
      if (currentBranchData && currentBranchData.name !== currentBranch) {
        setCurrentBranch(currentBranchData.name);
      }
      
      showToast(`Workspace loaded: ${currentDatabase}`, Intent.SUCCESS);
    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : 'Failed to load workspace data';
      
      // Show clear error for developers
      console.error('❌ Failed to load workspace data:', error);
      setError('workspace', 'Cannot load workspace. Backend API is not responding.');
      showToast('Failed to load workspace. Make sure backend is running on http://localhost:8002', Intent.DANGER);
      
      // Don't set fallback data - keep error state
      setBranches([]);
      setObjectTypes([]);
      setLinkTypes([]);
    } finally {
      setLoading(false);
    }
  };

  const handleDatabaseChange = (dbName: string) => {
    setCurrentDatabase(dbName);
  };

  const handleCreateDatabase = () => {
    setIsCreateDbDialogOpen(true);
  };

  const handleCreateDatabaseSubmit = async () => {
    // Validate inputs
    const sanitizedName = newDbName.trim();
    const sanitizedDescription = newDbDescription.trim() || 'No description provided';
    
    if (!sanitizedName) {
      showToast('Database name is required', Intent.WARNING);
      return;
    }
    
    // Validate database name format (alphanumeric, Korean including consonants/vowels, and underscore only)
    if (!/^[a-zA-Z0-9가-힣ㄱ-ㅎㅏ-ㅣ_]+$/.test(sanitizedName)) {
      showToast('Database name can only contain letters, Korean characters, numbers, and underscores', Intent.WARNING);
      return;
    }
    
    setSaving(true);
    try {
      await ontologyApi.database.create(sanitizedName, sanitizedDescription);
      
      // API is working if we got here
      setIsApiConnected(true);
      
      // Immediately update the UI state to show the new database
      setAvailableDatabases(prev => [...prev, sanitizedName]);
      setCurrentDatabase(sanitizedName);
      
      // Reset form and close dialog
      setNewDbName('');
      setNewDbDescription('');
      setIsCreateDbDialogOpen(false);
      
      showToast(`Database "${sanitizedName}" created and committed successfully`, Intent.SUCCESS);
      
      // Refresh workspace data in the background to ensure consistency
      setTimeout(() => {
        initializeWorkspace();
      }, 100);
      
    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : 'Failed to create database';
      showToast(message, Intent.DANGER);
    } finally {
      setSaving(false);
    }
  };

  const handleBranchChange = async (branchName: string) => {
    if (!currentDatabase) return;
    
    setSaving(true);
    try {
      const dbName = extractDatabaseName(currentDatabase);
      if (!dbName) return;
      await ontologyApi.branch.checkout(dbName, branchName);
      setCurrentBranch(branchName);
      await loadWorkspaceData(); // Refresh data for new branch
      showToast(`Switched to branch: ${branchName}`, Intent.SUCCESS);
    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : 'Failed to switch branch';
      showToast(message, Intent.DANGER);
    } finally {
      setSaving(false);
    }
  };

  const handleCommit = () => {
    setIsCommitDialogOpen(true);
  };

  const handleCommitSuccess = (commitId: string) => {
    showToast(`Changes committed: ${commitId}`, Intent.SUCCESS);
    loadWorkspaceData(); // Refresh to show new commit
  };

  const handleMergeComplete = () => {
    showToast('Merge completed successfully', Intent.SUCCESS);
    loadWorkspaceData(); // Refresh after merge
  };

  const handleDeleteDatabase = async (dbName: string) => {
    setDbToDelete(dbName);
    setIsDeleteDbDialogOpen(true);
  };

  const handleDeleteDatabaseConfirm = async () => {
    if (!dbToDelete) return;
    
    setSaving(true);
    try {
      const dbName = extractDatabaseName(dbToDelete);
      if (!dbName) return;
      await ontologyApi.database.delete(dbName);
      
      // Immediately update the UI state
      setAvailableDatabases(prev => prev.filter(db => db !== dbToDelete));
      
      // If we're deleting the current database, switch to another one
      if (dbToDelete === currentDatabase) {
        const remainingDbs = availableDatabases.filter(db => db !== dbToDelete);
        if (remainingDbs.length > 0) {
          setCurrentDatabase(remainingDbs[0]);
        } else {
          setCurrentDatabase(null);
        }
      }
      
      showToast(`Database "${dbToDelete}" deleted and committed successfully`, Intent.SUCCESS);
      
      // Refresh workspace data in the background to ensure consistency
      setTimeout(() => {
        initializeWorkspace();
      }, 100);
      
    } catch (error) {
      // If it's a 404 error, the database doesn't exist, so consider it deleted
      if (error instanceof OntologyApiError && error.code === 'NOT_FOUND') {
        // Immediately update the UI state (remove from list)
        setAvailableDatabases(prev => prev.filter(db => db !== dbToDelete));
        
        // If we're trying to delete the current database, switch to another one first
        if (dbToDelete === currentDatabase) {
          const remainingDbs = availableDatabases.filter(db => db !== dbToDelete);
          if (remainingDbs.length > 0) {
            setCurrentDatabase(remainingDbs[0]);
          } else {
            setCurrentDatabase(null);
          }
        }
        
        showToast(`Database "${dbToDelete}" was already deleted`, Intent.WARNING);
        
        // Refresh workspace data in the background to ensure consistency
        setTimeout(() => {
          initializeWorkspace();
        }, 100);
      } else {
        const message = error instanceof OntologyApiError 
          ? error.message 
          : 'Failed to delete database';
        showToast(message, Intent.DANGER);
      }
    } finally {
      setSaving(false);
      setIsDeleteDbDialogOpen(false);
      setDbToDelete(null);
    }
  };

  const handleDeleteAllDatabases = async () => {
    setIsDeleteAllDbsDialogOpen(true);
  };

  const handleDeleteAllDatabasesConfirm = async () => {
    // Get all databases except protected system databases
    const protectedDbs = ['_system', '_meta'];
    const databasesToDelete = availableDatabases.filter(dbName => !protectedDbs.includes(dbName));
    
    if (databasesToDelete.length === 0) {
      showToast('No databases to delete', Intent.WARNING);
      setIsDeleteAllDbsDialogOpen(false);
      return;
    }
    
    setSaving(true);
    let deletedCount = 0;
    let failedCount = 0;
    
    for (const dbName of databasesToDelete) {
      try {
        await ontologyApi.database.delete(dbName);
        deletedCount++;
      } catch (error) {
        // If it's a 404 error, the database doesn't exist, so consider it deleted
        if (error instanceof OntologyApiError && error.code === '404') {
          deletedCount++;
        } else {
          console.error(`Failed to delete ${dbName}:`, error);
          failedCount++;
        }
      }
    }
    
    // Immediately update the UI state (remove deleted databases)
    setAvailableDatabases(prev => prev.filter(db => protectedDbs.includes(db)));
    
    // Since all user databases are deleted, clear current database
    setCurrentDatabase(null);
    
    if (failedCount === 0) {
      showToast(`Successfully deleted ${deletedCount} databases`, Intent.SUCCESS);
    } else {
      showToast(`Deleted ${deletedCount} databases, ${failedCount} failed`, Intent.WARNING);
    }
    
    // Refresh workspace data in the background to ensure consistency
    setTimeout(() => {
      initializeWorkspace();
    }, 100);
    
    setSaving(false);
    setIsDeleteAllDbsDialogOpen(false);
  };

  const showToast = (message: string, intent: Intent = Intent.PRIMARY) => {
    const toaster = getToaster();
    toaster.show({
      message,
      intent,
      timeout: 3000,
    });
  };

  const renderTopbar = () => (
    <Navbar className="ontology-editor-topbar">
      <NavbarGroup align="left">
        <NavbarHeading className="ontology-brand">
          <Icon icon="graph" size={18} />
          <span>ONTOLOGY CANVAS</span>
        </NavbarHeading>
        
        <NavbarDivider />
        
        {/* Database Selector */}
        <Popover
          isOpen={isDatabasePopoverOpen}
          onInteraction={(nextOpenState) => setIsDatabasePopoverOpen(nextOpenState)}
          content={
            <Menu>
              {availableDatabases.map(dbName => {
                const isTestDb = dbName.includes('test') || dbName.includes('debug') || 
                                dbName.includes('load_') || dbName.includes('comm_');
                return (
                  <MenuItem
                    key={dbName}
                    text={dbName}
                    icon={dbName === currentDatabase ? "tick" : "database"}
                    onClick={() => {
                      handleDatabaseChange(dbName);
                      setIsDatabasePopoverOpen(false);
                    }}
                    active={dbName === currentDatabase}
                    label={
                      <Button
                        icon="trash"
                        minimal
                        small
                        intent={Intent.DANGER}
                        onClick={(e) => {
                          e.stopPropagation();
                          handleDeleteDatabase(dbName);
                        }}
                      />
                    }
                  />
                );
              })}
              <li className="bp5-menu-divider" />
              <MenuItem
                text="Create New Database"
                icon="add"
                onClick={() => {
                  handleCreateDatabase();
                  setIsDatabasePopoverOpen(false);
                }}
                disabled={isSaving}
              />
              <MenuItem
                text="Delete All Databases"
                icon="trash"
                intent={Intent.DANGER}
                onClick={() => {
                  handleDeleteAllDatabases();
                  setIsDatabasePopoverOpen(false);
                }}
                disabled={isSaving}
              />
            </Menu>
          }
          position={Position.BOTTOM_LEFT}
        >
          <Button
            text={currentDatabase || "Select Database"}
            rightIcon="caret-down"
            minimal
            className="database-selector"
            disabled={isLoading}
          />
        </Popover>
        
        <NavbarDivider />
        
        {/* Branch Info */}
        <Tag
          icon="git-branch"
          intent={currentBranch === 'main' ? Intent.PRIMARY : Intent.NONE}
          className="branch-tag"
        >
          {currentBranch}
        </Tag>
      </NavbarGroup>

      <NavbarGroup align="right">
        {/* Version Control Actions */}
        <ButtonGroup minimal>
          <Tooltip content="Manage branches">
            <Button
              icon="git-branch"
              onClick={toggleBranchPanel}
              active={showBranchPanel}
            />
          </Tooltip>
          
          <Tooltip content="Commit changes">
            <Button
              icon="git-commit"
              onClick={handleCommit}
              disabled={!currentDatabase}
            />
          </Tooltip>
          
          <Tooltip content="Merge branches">
            <Button
              icon="git-merge"
              onClick={() => setIsMergeDialogOpen(true)}
              disabled={!currentDatabase}
            />
          </Tooltip>
          
          <Divider />
          
          <Tooltip content="View history">
            <Button
              icon="history"
              onClick={toggleHistoryPanel}
              active={showHistoryPanel}
            />
          </Tooltip>
        </ButtonGroup>
      </NavbarGroup>
    </Navbar>
  );

  const renderContent = () => {
    if (isLoading) {
      return (
        <div className="ontology-loading">
          <Spinner size={40} />
          <p>Loading workspace...</p>
        </div>
      );
    }

    // Show error state only if API is actually disconnected
    if (isApiConnected === false) {
      return (
        <div className="ontology-error-state">
          <Card className="error-card" elevation={2}>
            <Icon icon="error" size={40} intent={Intent.DANGER} />
            <h2>Backend API Not Available</h2>
            <p>Cannot connect to the backend server.</p>
            <p className="error-details">
              Please ensure the backend is running on <code>http://localhost:8002</code>
            </p>
            <Button 
              text="Retry Connection" 
              intent={Intent.PRIMARY}
              onClick={initializeWorkspace}
              icon="refresh"
            />
          </Card>
        </div>
      );
    }

    // Show "no databases" state if API is connected but no databases exist
    if (isApiConnected === true && availableDatabases.length === 0) {
      return (
        <div className="ontology-error-state">
          <Card className="error-card" elevation={2}>
            <Icon icon="database" size={40} intent={Intent.PRIMARY} />
            <h2>No Databases Found</h2>
            <p>You haven't created any databases yet.</p>
            <p className="error-details">
              Create your first database to get started with the ontology editor.
            </p>
            <Button 
              text="Create Database" 
              intent={Intent.PRIMARY}
              onClick={handleCreateDatabase}
              icon="add"
            />
          </Card>
        </div>
      );
    }

    return (
      <div className="ontology-workspace">
        {/* Main Canvas */}
        <div className="ontology-canvas-container">
          <OntologyCanvas />
        </div>

        {/* Side Panels */}
        {showBranchPanel && (
          <div className="side-panel branch-panel">
            <div className="panel-header">
              <h4>Branches</h4>
              <Button
                icon="cross"
                minimal
                small
                onClick={toggleBranchPanel}
              />
            </div>
            <BranchPanel onBranchChange={handleBranchChange} />
          </div>
        )}
        
        {showHistoryPanel && (
          <div className="side-panel history-panel">
            <div className="panel-header">
              <h4>History</h4>
              <Button
                icon="cross"
                minimal
                small
                onClick={toggleHistoryPanel}
              />
            </div>
            <HistoryPanel />
          </div>
        )}
      </div>
    );
  };

  return (
    <div className={clsx('ontology-editor', Classes.DARK)}>
      {renderTopbar()}
      {renderContent()}
      
      {/* Version Control Dialogs */}
      <CommitDialog
        isOpen={isCommitDialogOpen}
        onClose={() => setIsCommitDialogOpen(false)}
        onSuccess={handleCommitSuccess}
      />
      
      <MergeConflictResolver
        isOpen={isMergeDialogOpen}
        onClose={() => setIsMergeDialogOpen(false)}
        onMergeComplete={handleMergeComplete}
      />
      
      {/* Create Database Dialog */}
      <Dialog
        isOpen={isCreateDbDialogOpen}
        onClose={() => {
          setIsCreateDbDialogOpen(false);
          setNewDbName('');
          setNewDbDescription('');
        }}
        title="Create New Database"
        icon="database"
        className={Classes.DARK}
        canEscapeKeyClose={true}
        canOutsideClickClose={true}
        enforceFocus={false}
        autoFocus={true}
      >
        <div className={Classes.DIALOG_BODY}>
          <FormGroup
            label="Database Name"
            labelFor="db-name"
            helperText="Use letters, Korean characters (including ㄱ-ㅎ, ㅏ-ㅣ), numbers, and underscores"
            intent={!/^[a-zA-Z0-9가-힣ㄱ-ㅎㅏ-ㅣ_]*$/.test(newDbName) && newDbName ? Intent.DANGER : Intent.NONE}
          >
            <InputGroup
              id="db-name"
              placeholder="예: customer_db, 고객관리, product_catalog"
              value={newDbName}
              onChange={(e) => setNewDbName(e.target.value)}
              onKeyDown={(e) => {
                // Prevent dialog from intercepting keyboard events
                e.stopPropagation();
                // Submit on Enter key
                if (e.key === 'Enter') {
                  e.preventDefault();
                  if (newDbName.trim() && /^[a-zA-Z0-9가-힣ㄱ-ㅎㅏ-ㅣ_]+$/.test(newDbName.trim()) && !isSaving) {
                    handleCreateDatabaseSubmit();
                  }
                }
              }}
              intent={!/^[a-zA-Z0-9가-힣ㄱ-ㅎㅏ-ㅣ_]*$/.test(newDbName) && newDbName ? Intent.DANGER : Intent.NONE}
              autoFocus
            />
          </FormGroup>
          
          <FormGroup
            label="Description"
            labelFor="db-description"
            helperText="Describe the purpose of this database (Korean supported)"
          >
            <TextArea
              id="db-description"
              placeholder="데이터베이스의 용도나 포함된 데이터를 설명하세요"
              value={newDbDescription}
              onChange={(e) => setNewDbDescription(e.target.value)}
              onKeyDown={(e) => {
                // Prevent dialog from intercepting keyboard events
                e.stopPropagation();
              }}
              rows={3}
              fill
            />
          </FormGroup>
        </div>
        
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button
              text="Cancel"
              onClick={() => {
                setIsCreateDbDialogOpen(false);
                setNewDbName('');
                setNewDbDescription('');
              }}
            />
            <Button
              text="Create Database"
              intent={Intent.PRIMARY}
              onClick={handleCreateDatabaseSubmit}
              disabled={
                !newDbName.trim() || 
                !/^[a-zA-Z0-9가-힣ㄱ-ㅎㅏ-ㅣ_]+$/.test(newDbName) ||
                isSaving
              }
              loading={isSaving}
            />
          </div>
        </div>
      </Dialog>
      
      {/* Delete Database Confirmation Dialog */}
      <Dialog
        isOpen={isDeleteDbDialogOpen}
        onClose={() => {
          setIsDeleteDbDialogOpen(false);
          setDbToDelete(null);
        }}
        title="Delete Database"
        icon="trash"
        className={Classes.DARK}
      >
        <div className={Classes.DIALOG_BODY}>
          <p>
            Are you sure you want to delete the database <strong>"{dbToDelete}"</strong>?
          </p>
          <p className="bp5-text-muted">
            This action cannot be undone. All data in this database will be permanently deleted.
          </p>
        </div>
        
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button
              text="Cancel"
              onClick={() => {
                setIsDeleteDbDialogOpen(false);
                setDbToDelete(null);
              }}
            />
            <Button
              text="Delete Database"
              intent={Intent.DANGER}
              onClick={handleDeleteDatabaseConfirm}
              loading={isSaving}
            />
          </div>
        </div>
      </Dialog>
      
      {/* Delete All Databases Confirmation Dialog */}
      <Dialog
        isOpen={isDeleteAllDbsDialogOpen}
        onClose={() => setIsDeleteAllDbsDialogOpen(false)}
        title="Delete All Databases"
        icon="trash"
        className={Classes.DARK}
      >
        <div className={Classes.DIALOG_BODY}>
          <p>
            This will delete all user databases including:
          </p>
          <ul>
            {availableDatabases
              .filter(dbName => !['_system', '_meta'].includes(dbName))
              .map(dbName => (
                <li key={dbName}><strong>{dbName}</strong></li>
              ))
            }
          </ul>
          <p className="bp5-text-muted">
            This action cannot be undone. All data in these databases will be permanently deleted.
          </p>
        </div>
        
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button
              text="Cancel"
              onClick={() => setIsDeleteAllDbsDialogOpen(false)}
            />
            <Button
              text="Delete All Databases"
              intent={Intent.DANGER}
              onClick={handleDeleteAllDatabasesConfirm}
              loading={isSaving}
            />
          </div>
        </div>
      </Dialog>
    </div>
  );
};