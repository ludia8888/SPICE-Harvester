import type { Preview } from '@storybook/react';
import { BrowserRouter } from 'react-router-dom';
import React from 'react';

// Import Blueprint CSS
import 'normalize.css/normalize.css';
import '@blueprintjs/core/lib/css/blueprint.css';
import '@blueprintjs/icons/lib/css/blueprint-icons.css';
import '@blueprintjs/datetime/lib/css/blueprint-datetime.css';
import '@blueprintjs/select/lib/css/blueprint-select.css';
import '@blueprintjs/table/lib/css/blueprint-table.css';

// Import our styles
import '../src/design-system/styles/global.scss';
import '../src/App.scss';

const preview: Preview = {
  parameters: {
    actions: { argTypesRegex: '^on[A-Z].*' },
    controls: {
      matchers: {
        color: /(background|color)$/i,
        date: /Date$/,
      },
    },
    backgrounds: {
      default: 'light',
      values: [
        {
          name: 'light',
          value: '#ffffff',
        },
        {
          name: 'dark',
          value: '#1c1e21',
        },
      ],
    },
  },
  decorators: [
    (Story, context) => {
      // Apply dark theme class if needed
      React.useEffect(() => {
        const isDark = context.globals.theme === 'dark' || context.parameters.theme === 'dark';
        if (isDark) {
          document.documentElement.classList.add('bp5-dark');
        } else {
          document.documentElement.classList.remove('bp5-dark');
        }
      }, [context.globals.theme, context.parameters.theme]);

      return (
        <BrowserRouter>
          <Story />
        </BrowserRouter>
      );
    },
  ],
  globalTypes: {
    theme: {
      name: 'Theme',
      description: 'Global theme for components',
      defaultValue: 'light',
      toolbar: {
        icon: 'circlehollow',
        items: [
          { value: 'light', title: 'Light' },
          { value: 'dark', title: 'Dark' },
        ],
        showName: true,
      },
    },
  },
};

export default preview;