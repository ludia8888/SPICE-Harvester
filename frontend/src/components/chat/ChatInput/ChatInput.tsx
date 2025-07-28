import React, { forwardRef, useImperativeHandle, useRef, useEffect } from 'react';
import { Button, TextArea, Classes } from '@blueprintjs/core';
import clsx from 'clsx';
import './ChatInput.scss';

interface ChatInputProps {
  value: string;
  onChange: (value: string) => void;
  onSend: () => void;
  onKeyPress: (e: React.KeyboardEvent) => void;
  isLoading: boolean;
  placeholder?: string;
}

export interface ChatInputRef {
  focus: () => void;
  resetHeight: () => void;
}

export const ChatInput = forwardRef<ChatInputRef, ChatInputProps>(({
  value,
  onChange,
  onSend,
  onKeyPress,
  isLoading,
  placeholder = "",
}, ref) => {
  const textAreaRef = useRef<HTMLTextAreaElement>(null);
  
  const resetTextAreaHeight = () => {
    if (textAreaRef.current) {
      // Reset to single row height
      textAreaRef.current.style.height = 'auto';
      textAreaRef.current.rows = 1;
    }
  };
  
  useImperativeHandle(ref, () => ({
    focus: () => {
      textAreaRef.current?.focus();
    },
    resetHeight: resetTextAreaHeight
  }));
  
  // Reset height when value becomes empty
  useEffect(() => {
    if (value === '') {
      resetTextAreaHeight();
    }
  }, [value]);
  
  // Auto-focus on mount and when disabled state changes
  useEffect(() => {
    if (!isLoading && textAreaRef.current) {
      textAreaRef.current.focus();
    }
  }, [isLoading]);
  return (
    <div className="chat-input-container">
      <div className="chat-input-wrapper">
        <TextArea
          inputRef={textAreaRef}
          className="chat-input-textarea"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          onKeyPress={onKeyPress}
          onBlur={(e) => {
            // Refocus after a short delay unless clicking the send button
            if (!isLoading && !(e.relatedTarget as HTMLElement)?.classList.contains('chat-input-button')) {
              setTimeout(() => {
                textAreaRef.current?.focus();
              }, 50);
            }
          }}
          placeholder={placeholder}
          rows={1}
          growVertically={true}
          large={true}
          disabled={isLoading}
          style={{
            maxHeight: '200px',
            overflowY: 'auto'
          }}
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