import { Button, Card, Callout, Intent } from '@blueprintjs/core'
import { useQuery } from '@tanstack/react-query'
import { getSummary } from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { qk } from '../query/queryKeys'
import { navigate } from '../state/pathname'
import { useAppStore } from '../store/useAppStore'

export const OverviewPage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const branch = useAppStore((state) => state.context.branch)

  const summaryQuery = useQuery({
    queryKey: qk.summary({ dbName, branch, language: requestContext.language }),
    queryFn: () => getSummary(requestContext, { dbName, branch }),
  })

  const base = `/db/${encodeURIComponent(dbName)}`

  return (
    <div>
      <PageHeader title="Overview" subtitle="Context summary and recommended next steps." />

      <div className="card-stack">
        <Card>
          <div className="card-title">Summary</div>
          {summaryQuery.isLoading ? <Callout>Loading summary...</Callout> : null}
          {summaryQuery.error ? (
            <Callout intent={Intent.DANGER}>Failed to load summary.</Callout>
          ) : null}
          <JsonViewer value={summaryQuery.data} />
        </Card>

        <Card>
          <div className="card-title">Next steps</div>
          <div className="form-row">
            <Button onClick={() => navigate(`${base}/ontology`)}>Define ontology</Button>
            <Button onClick={() => navigate(`${base}/data/sheets`)}>Preview sheets</Button>
            <Button onClick={() => navigate(`${base}/data/schema-suggestion`)}>Suggest schema</Button>
            <Button onClick={() => navigate(`${base}/data/import/sheets`)}>Import data</Button>
            <Button onClick={() => navigate(`${base}/explore/graph`)}>Graph explorer</Button>
          </div>
        </Card>
      </div>
    </div>
  )
}
