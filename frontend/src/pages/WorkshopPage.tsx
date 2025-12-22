import { PageHeader } from '../components/layout/PageHeader'
import { useAppStore } from '../store/useAppStore'

export const WorkshopPage = () => {
  const language = useAppStore((state) => state.context.language)

  return (
    <div>
      <PageHeader
        title={language === 'ko' ? '워크숍' : 'Workshop'}
        subtitle={language === 'ko' ? '기능 개발 예정입니다.' : 'Coming soon.'}
      />
    </div>
  )
}
