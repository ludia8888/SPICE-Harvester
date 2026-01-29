import { Checkbox, Icon } from '@blueprintjs/core'
import type { OntologyClass } from '../../api/bff'

type ClassFilterProps = {
  classes: OntologyClass[]
  selectedClasses: string[]
  onSelectionChange: (selectedClasses: string[]) => void
  isLoading?: boolean
}

export const ClassFilter = ({
  classes,
  selectedClasses,
  onSelectionChange,
  isLoading = false,
}: ClassFilterProps) => {
  const handleToggle = (classId: string) => {
    if (selectedClasses.includes(classId)) {
      onSelectionChange(selectedClasses.filter((id) => id !== classId))
    } else {
      onSelectionChange([...selectedClasses, classId])
    }
  }

  const handleSelectAll = () => {
    if (selectedClasses.length === classes.length) {
      onSelectionChange([])
    } else {
      onSelectionChange(classes.map((c) => c.id))
    }
  }

  const isAllSelected = classes.length > 0 && selectedClasses.length === classes.length
  const isIndeterminate = selectedClasses.length > 0 && selectedClasses.length < classes.length

  return (
    <div className="class-filter">
      <div className="class-filter-header">
        <span className="class-filter-title">클래스 필터</span>
        {classes.length > 0 && (
          <Checkbox
            checked={isAllSelected}
            indeterminate={isIndeterminate}
            onChange={handleSelectAll}
            label="전체"
            className="class-filter-all"
          />
        )}
      </div>
      <div className="class-filter-list">
        {isLoading ? (
          <div className="class-filter-loading">
            <Icon icon="refresh" className="class-filter-loading-icon" />
            <span>불러오는 중...</span>
          </div>
        ) : classes.length === 0 ? (
          <div className="class-filter-empty">
            <Icon icon="cube" size={16} />
            <span>클래스가 없습니다</span>
          </div>
        ) : (
          classes.map((classItem) => (
            <Checkbox
              key={classItem.id}
              checked={selectedClasses.includes(classItem.id)}
              onChange={() => handleToggle(classItem.id)}
              className="class-filter-item"
            >
              <div className="class-filter-item-content">
                <span className="class-filter-item-name">{classItem.label}</span>
                {classItem.instanceCount !== undefined && (
                  <span className="class-filter-item-count">{classItem.instanceCount}</span>
                )}
              </div>
            </Checkbox>
          ))
        )}
      </div>
    </div>
  )
}
