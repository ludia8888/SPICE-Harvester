import React, { useState, useRef, useEffect } from 'react';
import { Button, TextArea, Classes, Icon } from '@blueprintjs/core';
import clsx from 'clsx';
import { ChatMessage } from '../../components/chat/ChatMessage';
import { ChatInput, ChatInputRef } from '../../components/chat/ChatInput';
import { ChatSidebar } from '../../components/layout/ChatSidebar';
import './Home.scss';

interface Message {
  id: string;
  content: string;
  role: 'user' | 'assistant';
  timestamp: Date;
}

export const Home: React.FC = () => {
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isChatSidebarCollapsed, setIsChatSidebarCollapsed] = useState(true); // Default to collapsed
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const chatInputRef = useRef<ChatInputRef>(null);
  
  // Mock user data - replace with actual user data from auth/context
  const userName = 'sihuyhn';

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
    
    // Auto-focus input when chat interface appears
    if (messages.length > 0 && chatInputRef.current && typeof chatInputRef.current.focus === 'function') {
      setTimeout(() => {
        chatInputRef.current?.focus();
      }, 100);
    }
  }, [messages]);

  const handleSend = async () => {
    if (!inputValue.trim() || isLoading) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      content: inputValue,
      role: 'user',
      timestamp: new Date(),
    };

    setMessages(prev => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);
    
    // Keep focus on input
    setTimeout(() => {
      chatInputRef.current?.focus();
    }, 50);

    // Simulate AI response
    setTimeout(() => {
      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        content: 'This is a simulated response. In a real implementation, this would connect to an AI service.',
        role: 'assistant',
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, assistantMessage]);
      setIsLoading(false);
      
      // Auto-focus input after response
      setTimeout(() => {
        chatInputRef.current?.focus();
      }, 100);
    }, 1000);
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleNewChat = () => {
    setMessages([]);
    setInputValue('');
  };

  return (
    <div className="home-wrapper">
      <ChatSidebar 
        isCollapsed={isChatSidebarCollapsed}
        onToggleCollapse={() => setIsChatSidebarCollapsed(!isChatSidebarCollapsed)}
        onNewChat={handleNewChat}
      />
      <div className={clsx('home-container', {
        'home-container--sidebar-collapsed': isChatSidebarCollapsed,
      })}>
        <div className="chat-container">

        <div className="chat-messages">
          {messages.length === 0 ? (
            <div className="chat-welcome">
              <div className="mentat-hero">
                <div className="mentat-logo-container">
                  <div className="mentat-logo-glow" />
                  <Icon icon="predictive-analysis" iconSize={48} className="mentat-logo-icon" />
                </div>
                <h1 className="mentat-brand">MENTAT</h1>
                <p className="mentat-tagline">Human-Computer Synthesis for Enterprise Intelligence</p>
              </div>
              
              <div className="chat-prompt-example">
                <input
                  type="text"
                  className="chat-prompt-input"
                  placeholder="오늘 어떤 도움을 드릴까요?"
                  value={inputValue}
                  onChange={(e) => setInputValue(e.target.value)}
                  onKeyPress={(e) => {
                    if (e.key === 'Enter') {
                      e.preventDefault();
                      handleSend();
                    }
                  }}
                  autoFocus
                />
                <div className="chat-prompt-actions">
                  <Button icon="add" minimal className="chat-prompt-add" />
                  <Button 
                    text="Run" 
                    icon="play" 
                    className="chat-prompt-run"
                    onClick={handleSend}
                    disabled={!inputValue.trim() || isLoading}
                  />
                </div>
              </div>

              <div className="chat-features">
                <h3>What's new</h3>
                <div className="chat-feature-grid">
                  <div className="chat-feature-card">
                    <div className="chat-feature-icon">
                      <Icon icon="link" iconSize={24} />
                    </div>
                    <div className="chat-feature-content">
                      <h4>URL context tool</h4>
                      <p>Fetch information from web links</p>
                    </div>
                  </div>
                  
                  <div className="chat-feature-card">
                    <div className="chat-feature-icon">
                      <Icon icon="volume-up" iconSize={24} />
                    </div>
                    <div className="chat-feature-content">
                      <h4>Native speech generation</h4>
                      <p>Generate high quality text to speech with Gemini</p>
                    </div>
                  </div>
                  
                  <div className="chat-feature-card">
                    <div className="chat-feature-icon">
                      <Icon icon="chat" iconSize={24} />
                    </div>
                    <div className="chat-feature-content">
                      <h4>Live audio-to-audio dialog</h4>
                      <p>Try Gemini's natural, real-time dialog with audio and video inputs</p>
                    </div>
                  </div>
                  
                  <div className="chat-feature-card">
                    <div className="chat-feature-icon">
                      <Icon icon="media" iconSize={24} />
                    </div>
                    <div className="chat-feature-content">
                      <h4>Native image generation</h4>
                      <p>Interleaved text-and-image generation with Gemini 2.0 Flash</p>
                    </div>
                  </div>
                </div>
                
                <div className="chat-token-notice">
                  <p>Let the model decide how many thinking tokens to use or choose your own value</p>
                </div>
              </div>
            </div>
          ) : (
            <>
              {messages.map((message) => (
                <ChatMessage key={message.id} message={message} />
              ))}
              {isLoading && (
                <div className="chat-loading">
                  <div className="chat-loading-dot" />
                  <div className="chat-loading-dot" />
                  <div className="chat-loading-dot" />
                </div>
              )}
              <div ref={messagesEndRef} />
            </>
          )}
        </div>

        {messages.length > 0 && (
          <ChatInput
            ref={chatInputRef}
            value={inputValue}
            onChange={setInputValue}
            onSend={handleSend}
            onKeyPress={handleKeyPress}
            isLoading={isLoading}
            placeholder=""
          />
        )}
      </div>
    </div>
    </div>
  );
};