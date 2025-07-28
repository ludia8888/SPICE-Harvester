import React from 'react';
import { Button, Icon, Classes } from '@blueprintjs/core';
import clsx from 'clsx';
import './ChatSidebar.scss';

interface ChatItem {
  id: string;
  title: string;
  timestamp: Date;
  preview?: string;
}

interface ChatSidebarProps {
  className?: string;
  chats?: ChatItem[];
  activeChat?: string;
  isCollapsed?: boolean;
  onToggleCollapse?: () => void;
  onNewChat?: () => void;
  onSelectChat?: (chatId: string) => void;
}

export const ChatSidebar: React.FC<ChatSidebarProps> = ({
  className,
  chats = [],
  activeChat,
  isCollapsed = false,
  onToggleCollapse,
  onNewChat,
  onSelectChat,
}) => {
  // Mock data for now
  const mockChats: ChatItem[] = [
    { id: '1', title: 'SPICE HARVESTER Platform Arc...', timestamp: new Date() },
    { id: '2', title: 'Event-Driven Microservices Archi...', timestamp: new Date() },
    { id: '3', title: 'AI-Driven Industrial Data Transfo...', timestamp: new Date() },
    { id: '4', title: 'Palantir Ontology Editor Mockup', timestamp: new Date() },
    { id: '5', title: 'Arrakis Project Code Base Improv...', timestamp: new Date() },
    { id: '6', title: "Startup Leadership: CEO's Multip...", timestamp: new Date() },
  ];

  const chatList = chats.length > 0 ? chats : mockChats;

  const formatDate = (date: Date) => {
    const now = new Date();
    const diff = now.getTime() - date.getTime();
    const days = Math.floor(diff / (1000 * 60 * 60 * 24));
    
    if (days === 0) return '오늘';
    if (days === 1) return '어제';
    if (days < 7) return `${days}일 전`;
    return date.toLocaleDateString('ko-KR');
  };

  return (
    <>
      <div className={clsx('chat-sidebar', className, {
        'chat-sidebar--collapsed': isCollapsed,
      })}>
        <div className="chat-sidebar-header">
        <div className="chat-sidebar-header-content">
          {isCollapsed ? (
            <Button
              icon="add"
              className="chat-sidebar-new-collapsed"
              intent="primary"
              onClick={onNewChat}
              minimal
              large
            />
          ) : (
            <>
              <h3 className="chat-sidebar-logo">
                <Icon icon="predictive-analysis" iconSize={20} />
                <span>MENTAT</span>
              </h3>
              <Button
                icon="add"
                text="새 채팅"
                className="chat-sidebar-new"
                onClick={onNewChat}
                fill
                large
              />
            </>
          )}
        </div>
      </div>

      {isCollapsed ? (
        <div className="chat-sidebar-collapsed-content">
          <Button
            icon="chat"
            className="chat-sidebar-icon-button"
            minimal
            large
            title="최근 채팅"
          />
          <Button
            icon="archive"
            className="chat-sidebar-icon-button"
            minimal
            large
            title="아카이브"
          />
        </div>
      ) : (
        <div className="chat-sidebar-sections">
          <div className="chat-sidebar-section">
            <h4 className="chat-sidebar-section-title">최근 항목</h4>
            <div className="chat-sidebar-list">
              {chatList.map((chat) => (
                <button
                  key={chat.id}
                  className={clsx('chat-sidebar-item', {
                    'chat-sidebar-item--active': activeChat === chat.id,
                  })}
                  onClick={() => onSelectChat?.(chat.id)}
                >
                  <Icon icon="chat" className="chat-sidebar-item-icon" />
                  <span className="chat-sidebar-item-title">{chat.title}</span>
                </button>
              ))}
            </div>
          </div>

          <div className="chat-sidebar-section">
            <h4 className="chat-sidebar-section-title">아카이브</h4>
            <div className="chat-sidebar-list">
              <button className="chat-sidebar-item">
                <Icon icon="folder-close" className="chat-sidebar-item-icon" />
                <span className="chat-sidebar-item-title">아티팩트</span>
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
    
    <Button
      icon={isCollapsed ? 'chevron-right' : 'chevron-left'}
      className="chat-sidebar-toggle"
      minimal
      onClick={onToggleCollapse}
      title={isCollapsed ? '사이드바 펼치기' : '사이드바 접기'}
    />
    </>
  );
};