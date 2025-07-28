import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { Home } from '../pages/Home';
import { OntologyEditor } from '../pages/OntologyEditor';

export const AppRouter: React.FC = () => {
  return (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route path="/home" element={<Home />} />
      <Route path="/editor" element={<OntologyEditor />} />
      <Route path="/ontology-editor" element={<OntologyEditor />} />
      <Route path="/analysis" element={<div className="page-placeholder">Analysis Page</div>} />
      <Route path="/actions" element={<div className="page-placeholder">Action Center Page</div>} />
      <Route path="/my" element={<div className="page-placeholder">My Page</div>} />
      <Route path="/datasets" element={<div className="page-placeholder">Datasets Page</div>} />
      <Route path="/pipelines" element={<div className="page-placeholder">Pipelines Page</div>} />
      <Route path="/models" element={<div className="page-placeholder">Models Page</div>} />
      <Route path="/monitoring" element={<div className="page-placeholder">Monitoring Page</div>} />
      <Route path="/settings" element={<div className="page-placeholder">Settings Page</div>} />
      {/* Catch all - redirect to home */}
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
};