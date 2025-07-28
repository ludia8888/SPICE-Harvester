import React from 'react';
import { Icon, Classes } from '@blueprintjs/core';
import clsx from 'clsx';
import './ChatMessage.scss';

interface Message {
  id: string;
  content: string;
  role: 'user' | 'assistant';
  timestamp: Date;
}

interface ChatMessageProps {
  message: Message;
}

export const ChatMessage: React.FC<ChatMessageProps> = ({ message }) => {
  const formatTime = (date: Date) => {
    return date.toLocaleTimeString('en-US', {
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  return (
    <div className={clsx('chat-message', `chat-message--${message.role}`)}>
      <div className="chat-message-avatar">
        <Icon 
          icon={message.role === 'user' ? 'user' : 'predictive-analysis'} 
          iconSize={20}
        />
      </div>
      <div className="chat-message-content">
        <div className="chat-message-header">
          <span className="chat-message-role">
            {message.role === 'user' ? '나' : 'MENTAT'}
          </span>
          <span className={clsx('chat-message-time', Classes.TEXT_MUTED)}>
            {formatTime(message.timestamp)}
          </span>
        </div>
        <div className="chat-message-text">
          {message.content}
        </div>
      </div>
    </div>
  );
};