import { PageHeader } from '../components/layout/PageHeader'

export const OverviewPage = () => {
  // const requestContext = useRequestContext()
  // const branch = useAppStore((state) => state.context.branch)

  // const summaryQuery = useQuery({
  //   queryKey: qk.summary({ dbName, branch, language: requestContext.language }),
  //   queryFn: () => getSummary(requestContext, { dbName, branch }),
  // })

  // const base = `/db/${encodeURIComponent(dbName)}`

  return (
    <div>
      <PageHeader title="Home" subtitle="" />
      {/* Home page is intentionally empty for now */}
    </div>
  )
}
