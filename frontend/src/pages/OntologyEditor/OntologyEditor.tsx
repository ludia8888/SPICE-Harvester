import React, { useEffect, useState } from 'react';
import { 
  Button, 
  ButtonGroup, 
  Classes, 
  Divider, 
  Icon, 
  InputGroup, 
  Intent, 
  Menu, 
  MenuDivider, 
  MenuItem, 
  Navbar, 
  NavbarDivider, 
  NavbarGroup, 
  NavbarHeading, 
  Popover, 
  Position, 
  Spinner, 
  Tab, 
  Tabs, 
  Tag,
  Toast,
  Toaster,
  Tooltip
} from '@blueprintjs/core';
import clsx from 'clsx';
import { useOntologyStore } from '../../stores/ontology.store';
import { ontologyApi, OntologyApiError } from '../../api/ontologyClient';
import './OntologyEditor.scss';

// Subcomponents (to be implemented)
import { ObjectTypePanel } from './components/ObjectTypePanel';
import { LinkTypePanel } from './components/LinkTypePanel';
import { PropertiesPanel } from './components/PropertiesPanel';
import { OntologyGraph } from './components/OntologyGraph';
import { BranchPanel } from './components/BranchPanel';
import { HistoryPanel } from './components/HistoryPanel';
import { TypeInferencePanel } from './components/TypeInferencePanel';

const AppToaster = Toaster.create({
  className: "ontology-toaster",
  position: Position.TOP_RIGHT,
});

export const OntologyEditor: React.FC = () => {
  const {
    // State
    currentDatabase,
    currentBranch,
    isLoading,
    isSaving,
    showPropertiesPanel,
    showRelationshipsPanel,
    showHistoryPanel,
    showBranchPanel,
    searchQuery,
    filterByType,
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
    togglePropertiesPanel,
    toggleRelationshipsPanel,
    toggleHistoryPanel,
    toggleBranchPanel,
    setSearchQuery,
    setFilterByType,
    setError,
    clearAllErrors,
  } = useOntologyStore();

  const [selectedTab, setSelectedTab] = useState<'discover' | 'objects' | 'links' | 'graph'>('discover');
  const [availableDatabases, setAvailableDatabases] = useState<string[]>([]);
  const [isTypeInferenceOpen, setIsTypeInferenceOpen] = useState(false);

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
      setDatabases(databases);
      setAvailableDatabases(databases.map(db => db.name));
      
      // Set default database if none selected
      if (!currentDatabase && databases.length > 0) {
        setCurrentDatabase(databases[0].name);
      }
    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : 'Failed to initialize workspace';
      setError('initialization', message);
      showToast(message, Intent.DANGER);
    } finally {
      setLoading(false);
    }
  };

  const loadWorkspaceData = async () => {
    if (!currentDatabase) return;
    
    setLoading(true);
    clearAllErrors();
    
    try {
      const workspaceData = await ontologyApi.initializeWorkspace(currentDatabase);
      
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
      setError('workspace', message);
      showToast(message, Intent.DANGER);
    } finally {
      setLoading(false);
    }
  };

  const handleDatabaseChange = (dbName: string) => {
    setCurrentDatabase(dbName);
  };

  const handleCreateDatabase = async () => {
    // This would typically open a dialog
    const dbName = prompt('Enter database name:');
    const dbDescription = prompt('Enter database description:');
    
    if (!dbName || !dbDescription) return;
    
    setSaving(true);
    try {
      await ontologyApi.database.create(dbName, dbDescription);
      await initializeWorkspace();
      setCurrentDatabase(dbName);
      showToast(`Database "${dbName}" created successfully`, Intent.SUCCESS);
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
      await ontologyApi.branch.checkout(currentDatabase, branchName);
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

  const handleCommit = async () => {
    if (!currentDatabase) return;
    
    const message = prompt('Enter commit message:');
    if (!message) return;
    
    setSaving(true);
    try {
      const result = await ontologyApi.branch.commit(currentDatabase, message, 'User'); // TODO: Get actual user
      showToast(`Changes committed: ${result.commit_id}`, Intent.SUCCESS);
      await loadWorkspaceData(); // Refresh to show new commit
    } catch (error) {
      const message = error instanceof OntologyApiError 
        ? error.message 
        : 'Failed to commit changes';
      showToast(message, Intent.DANGER);
    } finally {
      setSaving(false);
    }
  };

  const handleCreateNew = (type: 'object' | 'link') => {
    // This would typically open a creation dialog/form
    if (type === 'object') {
      // Switch to objects tab and trigger creation
      setSelectedTab('objects');
      // TODO: Trigger ObjectTypePanel creation mode
    } else {
      // Switch to links tab and trigger creation
      setSelectedTab('links');
      // TODO: Trigger LinkTypePanel creation mode
    }
  };

  const showToast = (message: string, intent: Intent = Intent.PRIMARY) => {
    AppToaster.show({
      message,
      intent,
      timeout: 3000,
    });
  };

  const renderTopbar = () => (
    <Navbar className="ontology-editor-topbar">
      <NavbarGroup align="left">
        <NavbarHeading className="ontology-brand">
          <Icon icon="predictive-analysis" size={18} />
          <span>ONTOLOGY EDITOR</span>
        </NavbarHeading>
        
        <NavbarDivider />
        
        {/* Database Selector */}
        <Popover
          content={
            <Menu>
              {availableDatabases.map(dbName => (
                <MenuItem
                  key={dbName}
                  text={dbName}
                  icon={dbName === currentDatabase ? "tick" : "database"}
                  onClick={() => handleDatabaseChange(dbName)}
                  active={dbName === currentDatabase}
                />
              ))}
              <MenuDivider />
              <MenuItem
                text="Create New Database"
                icon="add"
                onClick={handleCreateDatabase}
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
        
        {/* Branch Selector */}
        <Tag
          icon="git-branch"
          intent={currentBranch === 'main' ? Intent.PRIMARY : Intent.NONE}
          className="branch-tag"
        >
          {currentBranch}
        </Tag>
        
        <Tooltip content="Manage branches and version control">
          <Button
            icon="git-branch"
            minimal
            onClick={toggleBranchPanel}
            active={showBranchPanel}
          />
        </Tooltip>
      </NavbarGroup>

      <NavbarGroup className="navbar-center">
        {/* Global Search */}
        <InputGroup
          leftIcon="search"
          placeholder="Search ontology..."
          value={searchQuery}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSearchQuery(e.target.value)}
          className="ontology-search"
          small
        />
      </NavbarGroup>

      <NavbarGroup align="right">
        {/* Action Buttons */}
        <ButtonGroup minimal>
          <Popover
            content={
              <Menu>
                <MenuItem
                  text="Object Type"
                  icon="cube"
                  onClick={() => handleCreateNew('object')}
                />
                <MenuItem
                  text="Link Type"
                  icon="link"
                  onClick={() => handleCreateNew('link')}
                />
                <MenuDivider />
                <MenuItem
                  text="AI Type Inference"
                  icon="predictive-analysis"
                  onClick={() => setIsTypeInferenceOpen(true)}
                />
              </Menu>
            }
            position={Position.BOTTOM_RIGHT}
          >
            <Button
              text="New"
              icon="add"
              intent={Intent.PRIMARY}
              disabled={!currentDatabase || isLoading}
            />
          </Popover>
          
          <Button
            icon="git-commit"
            text="Commit"
            onClick={handleCommit}
            disabled={!currentDatabase || isSaving}
            loading={isSaving}
          />
          
          <Divider />
          
          <Tooltip content="View commit history">
            <Button
              icon="history"
              minimal
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

    if (!currentDatabase) {
      return (
        <div className="ontology-empty-state">
          <Icon icon="database" size={48} />
          <h3>No Database Selected</h3>
          <p>Select or create a database to start building your ontology.</p>
          <Button
            text="Create Database"
            icon="add"
            intent={Intent.PRIMARY}
            onClick={handleCreateDatabase}
            disabled={isSaving}
          />
        </div>
      );
    }

    return (
      <div className="ontology-workspace">
        <div className="ontology-main-content">
          <Tabs
            id="ontology-tabs"
            selectedTabId={selectedTab}
            onChange={(tabId) => setSelectedTab(tabId as any)}
            className="ontology-tabs"
          >
            <Tab
              id="discover"
              title={
                <span>
                  <Icon icon="search" />
                  Discover
                </span>
              }
              panel={
                <div className="ontology-discover-panel">
                  <h3>Welcome to Ontology Editor</h3>
                  <p>Your ontology workspace for {currentDatabase}</p>
                  {/* TODO: Add discover content - favorites, recent items, etc. */}
                </div>
              }
            />
            
            <Tab
              id="objects"
              title={
                <span>
                  <Icon icon="cube" />
                  Object Types
                </span>
              }
              panel={<ObjectTypePanel />}
            />
            
            <Tab
              id="links"
              title={
                <span>
                  <Icon icon="link" />
                  Link Types
                </span>
              }
              panel={<LinkTypePanel />}
            />
            
            <Tab
              id="graph"
              title={
                <span>
                  <Icon icon="graph" />
                  Graph View
                </span>
              }
              panel={<OntologyGraph />}
            />
          </Tabs>
        </div>

        {/* Side Panels */}
        <div className="ontology-side-panels">
          {showPropertiesPanel && (
            <div className="side-panel properties-panel">
              <div className="panel-header">
                <h4>Properties</h4>
                <Button
                  icon="cross"
                  minimal
                  small
                  onClick={togglePropertiesPanel}
                />
              </div>
              <PropertiesPanel />
            </div>
          )}
          
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
      </div>
    );
  };

  return (
    <div className={clsx('ontology-editor', Classes.DARK)}>
      {renderTopbar()}
      {renderContent()}
      
      {/* AI Type Inference Dialog */}
      <TypeInferencePanel
        isOpen={isTypeInferenceOpen}
        onClose={() => setIsTypeInferenceOpen(false)}
      />
    </div>
  );
};