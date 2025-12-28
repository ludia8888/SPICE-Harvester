import { Icon, Intent, Menu, MenuItem, ContextMenu } from '@blueprintjs/core'


export type ProjectFileIconProps = {
    name: string
    onClick: () => void
    onDelete?: () => void
    isAdmin?: boolean
}

export const ProjectFileIcon = ({ name, onClick, onDelete, isAdmin }: ProjectFileIconProps) => {
    const contextMenu = (
        <Menu>
            <MenuItem icon="folder-open" text="Open" onClick={onClick} />
            {isAdmin && onDelete && (
                <MenuItem icon="trash" text="Delete" intent={Intent.DANGER} onClick={onDelete} />
            )}
        </Menu>
    )

    return (
        <ContextMenu content={contextMenu}>
            <div
                className="project-file-icon"
                onClick={onClick}
                onDoubleClick={onClick}
                title={name}
                role="button"
                tabIndex={0}
            >
                <div className="icon-container">
                    <Icon icon="folder-close" size={48} className="folder-icon" color="#5C7080" />
                </div>
                <div className="filename-container">
                    <span className="filename">{name}</span>
                </div>
            </div>
        </ContextMenu>
    )
}
