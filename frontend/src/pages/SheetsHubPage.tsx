import { Card, H5, Icon, Text } from '@blueprintjs/core'
import { PageHeader } from '../components/layout/PageHeader'
import { navigate } from '../state/pathname'
import { showAppToast } from '../app/AppToaster'
import { Intent } from '@blueprintjs/core'

type ConnectorOption = {
  id: string
  title: string
  description: string
  icon: string
  path?: string
  isMock?: boolean
}

export const SheetsHubPage = ({ dbName }: { dbName: string }) => {
  const connectors: ConnectorOption[] = [
    {
      id: 'google-sheets-live',
      title: 'Google Sheets (Live)',
      description: 'Connect and sync data directly from Google Sheets.',
      icon: 'document',
      path: `/db/${encodeURIComponent(dbName)}/data/sheets/google`,
    },
    {
      id: 'google-sheets-import',
      title: 'Google Sheets (Import)',
      description: 'One-time import from Google Sheets with schema mapping.',
      icon: 'cloud-download',
      path: `/db/${encodeURIComponent(dbName)}/data/import/sheets`,
    },
    {
      id: 'excel-import',
      title: 'Excel Import',
      description: 'Upload .xlsx files and map to ontology classes.',
      icon: 'import',
      path: `/db/${encodeURIComponent(dbName)}/data/import/excel`,
    },
    // Mocked Connectors
    {
      id: 'postgres',
      title: 'PostgreSQL',
      description: 'Connect to a remote PostgreSQL database.',
      icon: 'database',
      isMock: true,
    },
    {
      id: 'snowflake',
      title: 'Snowflake',
      description: 'Import large datasets from Snowflake.',
      icon: 'snowflake',
      isMock: true,
    },
    {
      id: 'salesforce',
      title: 'Salesforce',
      description: 'Sync CRM data entities.',
      icon: 'cloud',
      isMock: true,
    },
    {
      id: 'rest-api',
      title: 'REST API',
      description: 'Generic JSON data ingestion source.',
      icon: 'code',
      isMock: true,
    },
  ]

  const handleConnectorClick = (connector: ConnectorOption) => {
    if (connector.isMock) {
      void showAppToast({
        intent: Intent.PRIMARY,
        message: 'Coming Soon: This connector is under development.',
        icon: 'build',
      })
    } else if (connector.path) {
      navigate(connector.path)
    }
  }

  return (
    <div>
      <PageHeader
        title="Connectors"
        subtitle="Select a source to ingest data into your project."
      />

      <div className="db-grid">
        {connectors.map((connector) => (
          <Card
            key={connector.id}
            interactive
            onClick={() => handleConnectorClick(connector)}
            className="connector-card"
            style={{ display: 'flex', flexDirection: 'column', gap: '12px', minHeight: '160px' }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '8px' }}>
              <div
                style={{
                  width: '40px',
                  height: '40px',
                  borderRadius: '4px',
                  background: 'rgba(255, 255, 255, 0.05)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                }}
              >
                <Icon icon={connector.icon as any} size={20} color={connector.isMock ? '#5C7080' : '#48AFF0'} />
              </div>
              <H5 style={{ margin: 0, color: connector.isMock ? '#A7B6C2' : '#F5F8FA' }}>
                {connector.title}
              </H5>
            </div>
            <Text className={connector.isMock ? 'muted' : ''} style={{ fontSize: '0.85rem', lineHeight: '1.4' }}>
              {connector.description}
            </Text>
            {connector.isMock && (
              <div style={{ marginTop: 'auto', paddingTop: '12px' }}>
                <Text style={{ fontSize: '0.75rem', color: '#5C7080', fontStyle: 'italic' }}>
                  Coming Soon
                </Text>
              </div>
            )}
          </Card>
        ))}
      </div>
    </div>
  )
}
