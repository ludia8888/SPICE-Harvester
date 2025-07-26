import { FocusStyleManager } from '@blueprintjs/core';
import { ThemeProvider } from './design-system/theme/ThemeProvider';

// Enable focus styles only when using keyboard navigation
FocusStyleManager.onlyShowFocusOnTabs();

function App(): JSX.Element {
  return (
    <ThemeProvider>
      <div className="bp5-focus-disabled">
        <h1>SPICE HARVESTER</h1>
        <p>Development Environment Ready</p>
      </div>
    </ThemeProvider>
  );
}

export default App;