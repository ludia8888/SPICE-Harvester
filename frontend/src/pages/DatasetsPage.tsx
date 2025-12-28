import { useQuery } from '@tanstack/react-query'
import { Card, H3, Spinner, Tag, Text } from '@blueprintjs/core'

type DatasetRecord = {
  id: string
  name: string
  source: string
  updatedAt: string
}

const fetchDatasets = async (): Promise<DatasetRecord[]> => {
  await new Promise((resolve) => setTimeout(resolve, 400))
  return [
    { id: 'ds-1', name: 'customer_profiles', source: 'csv_upload', updatedAt: 'just now' },
    { id: 'ds-2', name: 'transactions', source: 'google_sheets', updatedAt: '2 hours ago' },
  ]
}

export const DatasetsPage = () => {
  const { data, isLoading } = useQuery({ queryKey: ['datasets'], queryFn: fetchDatasets })

  return (
    <div className="page">
      <H3>Datasets</H3>
      {isLoading ? <Spinner size={24} /> : null}
      <div className="grid">
        {(data ?? []).map((dataset) => (
          <Card key={dataset.id} className="card">
            <Text className="card-title">{dataset.name}</Text>
            <Text className="card-meta">Source: {dataset.source}</Text>
            <Text className="card-meta">Updated: {dataset.updatedAt}</Text>
            <Tag minimal intent="primary">{dataset.name}</Tag>
          </Card>
        ))}
      </div>
    </div>
  )
}
