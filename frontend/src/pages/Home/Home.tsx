import React, { useState, useRef, useEffect } from 'react';
import { Button, Icon } from '@blueprintjs/core';
import { useLocation, useNavigate } from 'react-router-dom';
import clsx from 'clsx';
import { ChatMessage } from '../../components/chat/ChatMessage';
import { ChatInput, ChatInputRef } from '../../components/chat/ChatInput';
import { ChatSidebar } from '../../components/layout/ChatSidebar';
import { InsightPanel, InsightStatus } from '../../components/layout/InsightPanel';
import './Home.scss';

interface Message {
  id: string;
  content: string;
  role: 'user' | 'assistant';
  timestamp: Date;
}

interface Insight {
  id: string;
  type: 'warning' | 'insight' | 'alert' | 'success';
  icon: string;
  title: string;
  subtitle: string;
  description: string;
  action: string;
}

export const Home: React.FC = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isChatSidebarCollapsed, setIsChatSidebarCollapsed] = useState(true); // Default to collapsed
  const [currentInsightIndex, setCurrentInsightIndex] = useState(0);
  const [isTransitioning, setIsTransitioning] = useState(false);
  const [showChat, setShowChat] = useState(false);
  const [activeInsightId, setActiveInsightId] = useState<string | null>(null);
  const [insightStatuses, setInsightStatuses] = useState<InsightStatus[]>([]);
  const [isInsightPanelVisible, setIsInsightPanelVisible] = useState(true);
  const [completedInsightIds, setCompletedInsightIds] = useState<Set<string>>(new Set());
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const chatInputRef = useRef<ChatInputRef>(null);
  const chatMessagesRef = useRef<HTMLDivElement>(null);
  const [isUserScrolling, setIsUserScrolling] = useState(false);
  const [hasNewMessages, setHasNewMessages] = useState(false);
  const prevMessageCountRef = useRef(0);
  
  // Mock user data - replace with actual user data from auth/context
  // const userName = 'sihuyhn';

  // Define base insights with unique IDs - MOVED UP BEFORE FUNCTIONS
  const baseInsights: Insight[] = [
    {
      id: 'pvc-shortage',
      type: 'warning',
      icon: 'warning-sign',
      title: '재고 부족 예상',
      subtitle: 'PVC 원자재',
      description: '현재 소비율 기준 3일 내 재고 소진 예상',
      action: '긴급 발주 권장',
    },
    {
      id: 'production-efficiency',
      type: 'insight',
      icon: 'trending-up',
      title: '생산 효율성',
      subtitle: '개선 기회',
      description: '라인 B 야간 생산성 23% 저하',
      action: '조명 개선으로 15% 향상 가능',
    },
    {
      id: 'quality-alert',
      type: 'alert',
      icon: 'timeline-line-chart',
      title: '품질 이상',
      subtitle: '패턴 감지',
      description: '금형 A-12 불량률 8.3% 상승',
      action: '예방 정비 권장',
    },
    {
      id: 'cost-saving',
      type: 'success',
      icon: 'dollar',
      title: '비용 절감',
      subtitle: '기회 발견',
      description: '전력 사용 시간대 조정 가능',
      action: '월 340만원 절감 예상',
    },
  ];
  
  // Filter out completed insights using their IDs
  const insights = baseInsights.filter(insight => !completedInsightIds.has(insight.id));

  // Track location key changes (navigation events)
  const prevLocationKeyRef = useRef(location.key);
  
  // Reset to welcome page when home navigation is detected
  useEffect(() => {
    // If we're on /home and the location key changed (navigation event) and in chat mode
    if (location.pathname === '/home' && 
        location.key !== prevLocationKeyRef.current && 
        showChat) {
      // Reset to welcome page from navigation
      setShowChat(false);
      setMessages([]);
      setInsightStatuses([]);
      setActiveInsightId(null);
      setInputValue('');
      setHasNewMessages(false);
      setIsUserScrolling(false);
      prevMessageCountRef.current = 0;
      setIsTransitioning(false);
    }
    
    // Update previous location key
    prevLocationKeyRef.current = location.key;
  }, [location.key, location.pathname, showChat]);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  const handleScrollToBottom = () => {
    setIsUserScrolling(false);
    setHasNewMessages(false);
    scrollToBottom();
  };

  // Check if user is scrolling to view previous messages
  useEffect(() => {
    const handleScroll = () => {
      if (!chatMessagesRef.current) return;
      
      const { scrollTop, scrollHeight, clientHeight } = chatMessagesRef.current;
      // If user scrolled up more than 100px from bottom, they're viewing previous messages
      const isAtBottom = scrollHeight - scrollTop - clientHeight < 100;
      setIsUserScrolling(!isAtBottom);
      
    };

    const messagesElement = chatMessagesRef.current;
    if (messagesElement) {
      messagesElement.addEventListener('scroll', handleScroll);
      return () => messagesElement.removeEventListener('scroll', handleScroll);
    }
  }, [showChat]);

  useEffect(() => {
    // Check if a new message was added
    const isNewMessage = messages.length > prevMessageCountRef.current;
    const lastMessage = messages.length > 0 ? messages[messages.length - 1] : null;
    
    // Update previous message count
    prevMessageCountRef.current = messages.length;
    
    // Only auto-scroll if user is not viewing previous messages
    if (!isUserScrolling) {
      scrollToBottom();
      setHasNewMessages(false);
    } else if (isNewMessage && lastMessage?.role === 'assistant') {
      // Show new message indicator ONLY when:
      // 1. A new message was actually added (not just re-render)
      // 2. The new message is from assistant
      // 3. User is currently scrolling/viewing previous messages
      setHasNewMessages(true);
    }
    
    // Auto-focus input when chat interface appears
    if (messages.length > 0 && chatInputRef.current && typeof chatInputRef.current.focus === 'function') {
      setTimeout(() => {
        chatInputRef.current?.focus();
      }, 100);
    }
  }, [messages, isUserScrolling]);

  const handleSend = async () => {
    if (!inputValue.trim() || isLoading) return;

    // Start transition if this is the first message
    if (messages.length === 0 && !showChat) {
      setIsTransitioning(true);
      
      // Initialize insight statuses if there are incomplete insights
      if (insights.length > 0 && insightStatuses.length === 0) {
        const statuses: InsightStatus[] = insights.map((ins) => ({
          id: ins.id,
          type: ins.type,
          icon: ins.icon,
          title: ins.title,
          subtitle: ins.subtitle,
          status: 'pending',
          progress: undefined,
        }));
        setInsightStatuses(statuses);
        setIsInsightPanelVisible(false); // Keep panel closed for normal chat start
      }
      
      setTimeout(() => {
        setShowChat(true);
        setIsTransitioning(false);
      }, 300);
    }

    const userMessage: Message = {
      id: Date.now().toString(),
      content: inputValue,
      role: 'user',
      timestamp: new Date(),
    };

    setMessages(prev => [...prev, userMessage]);
    setInputValue('');
    setIsLoading(true);
    setIsUserScrolling(false); // Reset scrolling state when sending a message
    
    // Reset input height and keep focus
    setTimeout(() => {
      chatInputRef.current?.resetHeight();
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
      
      // Reset height and auto-focus input after response
      setTimeout(() => {
        chatInputRef.current?.resetHeight();
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
    // Keep chat mode if already in chat, otherwise stay in welcome
    // Don't change showChat state - let user input trigger the transition
    setIsTransitioning(false);
    setInsightStatuses([]);
    setActiveInsightId(null);
    setHasNewMessages(false);
    prevMessageCountRef.current = 0;
    setIsUserScrolling(false);
    // Don't reset completedInsightIds when starting new chat
    
    // Focus input after clearing
    if (showChat) {
      setTimeout(() => {
        chatInputRef.current?.focus();
      }, 100);
    }
  };

  const handlePrevInsight = () => {
    if (insights.length === 0) return;
    setCurrentInsightIndex((prev) => (prev === 0 ? insights.length - 1 : prev - 1));
  };

  const handleNextInsight = () => {
    if (insights.length === 0) return;
    setCurrentInsightIndex((prev) => (prev === insights.length - 1 ? 0 : prev + 1));
  };
  
  // Adjust current index when insights are removed
  useEffect(() => {
    if (currentInsightIndex >= insights.length && insights.length > 0) {
      setCurrentInsightIndex(insights.length - 1);
    } else if (insights.length === 0) {
      setCurrentInsightIndex(0);
    }
  }, [insights.length, currentInsightIndex]);

  const handleInsightClick = async (insight: Insight) => {
    // Start transition animation
    setIsTransitioning(true);
    
    // Initialize insight statuses with actual insight IDs
    const statuses: InsightStatus[] = insights.map((ins) => ({
      id: ins.id, // Use actual insight ID instead of index
      type: ins.type,
      icon: ins.icon,
      title: ins.title,
      subtitle: ins.subtitle,
      status: ins.id === insight.id ? 'active' : 'pending',
      progress: ins.id === insight.id ? 10 : undefined,
    }));
    setInsightStatuses(statuses);
    setActiveInsightId(insight.id);
    setIsInsightPanelVisible(true); // Show panel when insight is clicked
    
    setTimeout(() => {
      // Create user message with insight card content and common prompt
      const commonPrompt = '위 사항에 대해 철저하게 분석해주세요. 그 근거와 실제 데이터를 기반으로 설명하고, 구체적인 실행 계획을 제시해주세요.';
      
      const cardContent = `[${insight.title} - ${insight.subtitle}]\n\n` +
                         `현황: ${insight.description}\n` +
                         `권장사항: ${insight.action}\n\n` +
                         `────────────────────\n\n` +
                         `${commonPrompt}`;
      
      const userMessage: Message = {
        id: Date.now().toString(),
        content: cardContent,
        role: 'user',
        timestamp: new Date(),
      };

      // Add message and trigger loading state
      setMessages([userMessage]);
      setInputValue('');
      setIsLoading(true);
      setShowChat(true);
      
      setTimeout(() => {
        setIsTransitioning(false);
      }, 300);
      
      // Start progress and AI response together
      simulateInsightWithResponse(insight.id, insight.type, insight.title, insight.id);
    }, 300);
  };

  const getInsightResponse = (type: string, title: string): string => {
    const responses: Record<string, string> = {
      warning: `${title} 관련 분석을 시작하겠습니다.\n\n현재 상황:
- PVC 원자재 현 재고: 2.3톤 (평균 소비량의 3.2일분)
- 주문 리드타임: 평균 7-10일
- 주요 공급업체: ABC케미칼(국내), XYZ폴리머(중국)
\n긴급 대응 방안:
1. ABC케미칼 긴급 발주 - 5톤 가능 (72시간 내 납품)
2. 대체 원자재(PP) 검토 - 품질 테스트 필요
\n추가 분석이 필요하신가요?`,
      insight: `${title} 분석 결과를 보고드리겠습니다.\n\n현황 진단:
- 라인 B 야간 생산성: 78개/시간 (주간: 101개/시간)
- 주요 원인: 조명 부족(40%), 작업자 피로도(35%), 온도 편차(25%)
\n개선 방안:
1. LED 조명 교체 - ROI 8개월, 예상 개선율 12%
2. 야간 근무 인센티브 - 월 150만원, 예상 개선율 8%
3. 공조 시스템 개선 - 예상 개선율 5%
\n우선순위를 정하시겠습니까?`,
      alert: `${title} 상황을 상세히 분석해드리겠습니다.\n\n금형 A-12 불량 분석:
- 현재 불량률: 4.8% (전주 대비 +1.7%p)
- 불량 유형: 버(Burr) 45%, 크랙 30%, 변형 25%
- 누적 생산량: 245,000개
\n예방 정비 계획:
1. 긴급 점검 - 48시간 내 실시
2. 코어 핀 교체 및 청소
3. 온도 센서 교정
\n유사 금형 점검도 필요하신가요?`,
      success: `${title} 기회를 구체적으로 분석해드리겠습니다.\n\n전력 사용 패턴 분석:
- 현재 피크 시간: 14:00-16:00 (요금 150원/kWh)
- 오전 저렴 시간: 09:00-11:00 (요금 80원/kWh)
- 평균 사용량: 2,400kWh/일
\n실행 계획:
1. 비필수 공정 시간 조정 가능
2. ESS(에너지저장장치) 도입 검토
3. 태양광 설치 타당성 분석
\n상세 실행 계획을 수립하시겠습니까?`,
    };
    return responses[type] || 'This is a simulated response. In a real implementation, this would connect to an AI service.';
  };

  const simulateInsightWithResponse = (insightId: string, type: string, title: string, insightUniqueId: string) => {
    // insightId is now the actual unique ID, not an index
    // Start progress animation (0 -> 70% during AI processing)
    let progress = 10;
    const progressInterval = setInterval(() => {
      progress += 20;
      if (progress > 70) {
        progress = 70; // Hold at 70% until AI response completes
        clearInterval(progressInterval);
      }
      setInsightStatuses(prev => prev.map(ins => 
        ins.id === insightId ? { ...ins, progress } : ins
      ));
    }, 300);

    // Simulate AI response processing
    setTimeout(() => {
      // Generate AI response
      const contextualResponse = getInsightResponse(type, title);
      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        content: contextualResponse,
        role: 'assistant',
        timestamp: new Date(),
      };
      
      // Add AI response
      setMessages(prev => [...prev, assistantMessage]);
      setIsLoading(false);
      
      // Complete progress animation (70 -> 100%) when AI responds
      let finalProgress = 70;
      const completeInterval = setInterval(() => {
        finalProgress += 10;
        if (finalProgress >= 100) {
          finalProgress = 100;
          clearInterval(completeInterval);
          
          // Mark insight as completed only after AI response is delivered
          setInsightStatuses(prev => prev.map(ins => 
            ins.id === insightId ? { ...ins, status: 'completed', progress: 100 } : ins
          ));
          
          // Add to completed insights using unique ID
          setCompletedInsightIds(prev => {
            const newSet = new Set(prev);
            newSet.add(insightUniqueId);
            return newSet;
          });
          
          // Check if all insights are completed
          setTimeout(() => {
            checkAllInsightsCompleted();
          }, 500);
        } else {
          setInsightStatuses(prev => prev.map(ins => 
            ins.id === insightId ? { ...ins, progress: finalProgress } : ins
          ));
        }
      }, 100);
      
      // Reset height and auto-focus input
      setTimeout(() => {
        chatInputRef.current?.resetHeight();
        chatInputRef.current?.focus();
      }, 100);
    }, 1500); // AI response takes 1.5 seconds
  };

  const handleInsightPanelClick = (insightId: string) => {
    // Find insight by ID instead of index
    const insight = baseInsights.find(ins => ins.id === insightId);
    if (!insight || completedInsightIds.has(insightId)) return;
    
    // Update active insight
    setActiveInsightId(insightId);
    
    // Update statuses
    setInsightStatuses(prev => prev.map(ins => ({
      ...ins,
      status: ins.id === insightId ? 'active' : 
              ins.status === 'completed' ? 'completed' : 'pending',
      progress: ins.id === insightId && ins.status !== 'completed' ? 10 : ins.progress,
    })));
    
    // Send new message with insight card content and common prompt
    const commonPrompt = '위 사항에 대해 철저하게 분석해주세요. 그 근거와 실제 데이터를 기반으로 설명하고, 구체적인 실행 계획을 제시해주세요.';
    
    const cardContent = `[${insight.title} - ${insight.subtitle}]\n\n` +
                       `현황: ${insight.description}\n` +
                       `권장사항: ${insight.action}\n\n` +
                       `────────────────────\n\n` +
                       `${commonPrompt}`;
    
    const userMessage: Message = {
      id: Date.now().toString(),
      content: cardContent,
      role: 'user',
      timestamp: new Date(),
    };
    
    setMessages(prev => [...prev, userMessage]);
    setIsLoading(true);
    
    // Use unified function for progress and AI response
    simulateInsightWithResponse(insight.id, insight.type, insight.title, insight.id);
  };

  const checkAllInsightsCompleted = () => {
    const allCompleted = insightStatuses.every(ins => ins.status === 'completed');
    if (allCompleted && insightStatuses.length > 0) {
      // All insights completed - show completion state
      // Panel will show "홈으로 돌아가기" button
    }
  };

  const handleReturnHome = () => {
    setShowChat(false);
    setMessages([]);
    setInsightStatuses([]);
    setActiveInsightId(null);
    setInputValue('');
    setHasNewMessages(false);
    prevMessageCountRef.current = 0;
    // Keep completedInsightIds to maintain completed state
    
    // Adjust carousel index if needed
    if (currentInsightIndex >= insights.length && insights.length > 0) {
      setCurrentInsightIndex(0);
    }
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
        'home-container--with-insights': showChat && insightStatuses.length > 0 && isInsightPanelVisible,
      })}>
        <div className="chat-container">
          <div className="chat-messages" ref={chatMessagesRef}>
          {!showChat ? (
            <div className={clsx('chat-welcome', {
              'chat-welcome--transitioning': isTransitioning,
            })}>
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

              {insights.length > 0 ? (
                <div className="chat-insights">
                  <div className="chat-insights-header">
                    <span className="chat-insights-title">INSIGHTS</span>
                    <div className="chat-insights-nav">
                      <Button 
                        icon="chevron-left" 
                        minimal 
                        small
                        className="chat-insights-nav-btn"
                        onClick={handlePrevInsight}
                        disabled={insights.length <= 1}
                      />
                      <span className="chat-insights-counter">
                        {currentInsightIndex + 1} / {insights.length}
                      </span>
                      <Button 
                        icon="chevron-right" 
                        minimal 
                        small
                        className="chat-insights-nav-btn"
                        onClick={handleNextInsight}
                        disabled={insights.length <= 1}
                      />
                    </div>
                  </div>
                
                <div className="chat-insights-carousel">
                  <div 
                    className="chat-insights-track"
                    style={{ transform: `translateX(-${currentInsightIndex * 100}%)` }}
                  >
                    {insights.map((insight, index) => (
                      <div key={index} className="chat-insight-slide">
                        <div 
                          className={`chat-insight-card chat-insight-card--${insight.type}`}
                          onClick={() => handleInsightClick(insight)}
                        >
                          <div className="chat-insight-icon">
                            <Icon icon={insight.icon as any} iconSize={20} />
                          </div>
                          <div className="chat-insight-content">
                            <div className="chat-insight-header">
                              <h4 className="chat-insight-title">{insight.title}</h4>
                              <span className="chat-insight-subtitle">{insight.subtitle}</span>
                            </div>
                            <p className="chat-insight-description">{insight.description}</p>
                            <p className="chat-insight-action">{insight.action}</p>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
                
                <div className="chat-insights-dots">
                  {insights.map((_, index) => (
                    <button
                      key={index}
                      className={clsx('chat-insights-dot', {
                        'chat-insights-dot--active': index === currentInsightIndex,
                      })}
                      onClick={() => setCurrentInsightIndex(index)}
                    />
                  ))}
                  </div>
                </div>
              ) : null}
            </div>
          ) : (
            <div className={clsx('chat-conversation', {
              'chat-conversation--entering': showChat && messages.length > 0,
            })}>
              {messages.length === 0 && !isLoading ? (
                <div className="chat-empty-state">
                  <div className="chat-empty-icon-wrapper">
                    <div className="chat-empty-icon-glow" />
                    <Icon icon="chat" iconSize={32} className="chat-empty-icon" />
                  </div>
                  <h3 className="chat-empty-title">START A CONVERSATION</h3>
                  <p className="chat-empty-subtitle">Ask anything about your operations</p>
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
                </>
              )}
              <div ref={messagesEndRef} />
            </div>
          )}
          </div>

          {showChat && (
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
        
        {showChat && hasNewMessages && isUserScrolling && (
          <Button
            icon="arrow-down"
            text="새 메시지"
            className="new-message-indicator"
            onClick={handleScrollToBottom}
          />
        )}
      </div>
    {showChat && (insights.length > 0 || insightStatuses.length > 0) && !isInsightPanelVisible && (
      <Button
        icon="timeline-events"
        minimal
        className="insight-panel-toggle"
        onClick={() => setIsInsightPanelVisible(true)}
        title="인사이트 패널 열기"
      />
    )}
    <InsightPanel
      insights={insightStatuses}
      activeInsightId={activeInsightId}
      onInsightClick={handleInsightPanelClick}
      onClose={handleReturnHome}
      onToggle={() => setIsInsightPanelVisible(!isInsightPanelVisible)}
      isVisible={showChat && (insights.length > 0 || insightStatuses.length > 0) && isInsightPanelVisible}
    />
    </div>
  );
};