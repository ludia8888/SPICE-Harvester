import React, { forwardRef } from 'react';
import { Button, TextArea, Classes } from '@blueprintjs/core';
import clsx from 'clsx';
import './ChatInput.scss';

interface ChatInputProps {
  value: string;
  onChange: (value: string) => void;
  onSend: () => void;
  onKeyPress: (e: React.KeyboardEvent) => void;
  isLoading: boolean;
}

export const ChatInput = forwardRef<HTMLTextAreaElement, ChatInputProps>(({
  value,
  onChange,
  onSend,
  onKeyPress,
  isLoading,
}, ref) => {
  return (
    <div className="chat-input-container">
      <div className="chat-input-wrapper">
        <TextArea
          ref={ref}
          className="chat-input-textarea"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onKeyPress={onKeyPress}
          placeholder="오늘 어떤 도움을 드릴까요?"
          rows={1}
          growVertically={true}
          large={true}
          disabled={isLoading}
        />
        <Button
          className="chat-input-button"
          icon="send-message"
          intent="primary"
          onClick={onSend}
          disabled={!value.trim() || isLoading}
          loading={isLoading}
        />
      </div>
      <div className={clsx('chat-input-hint', Classes.TEXT_MUTED)}>
        Enter로 전송, Shift+Enter로 줄바꿈
      </div>
    </div>
  );
});