import React from 'react';
import ReactDOM from 'react-dom/client';

// Import Blueprint styles and icons
import '@blueprintjs/core/lib/css/blueprint.css';
import '@blueprintjs/icons/lib/css/blueprint-icons.css';
import '@blueprintjs/datetime/lib/css/blueprint-datetime.css';
import '@blueprintjs/select/lib/css/blueprint-select.css';
import '@blueprintjs/table/lib/css/blueprint-table.css';

// Import our global styles after Blueprint
import './design-system/styles/global.scss';

import App from './App';

// Enable React Strict Mode for better development warnings
ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);