import React from 'react';
import { Button, Icon, ProgressBar } from '@blueprintjs/core';
import clsx from 'clsx';
import './InsightPanel.scss';

export interface InsightStatus {
  id: string;
  type: 'warning' | 'insight' | 'alert' | 'success';
  icon: string;
  title: string;
  subtitle: string;
  status: 'active' | 'pending' | 'completed';
  progress?: number;
}

interface InsightPanelProps {
  insights: InsightStatus[];
  activeInsightId: string | null;
  onInsightClick: (insightId: string) => void;
  onClose: () => void;
  onToggle: () => void;
  isVisible: boolean;
}

export const InsightPanel: React.FC<InsightPanelProps> = ({
  insights,
  activeInsightId,
  onInsightClick,
  onClose,
  onToggle,
  isVisible,
}) => {
  const hasActiveInsights = insights.some(i => i.status !== 'completed');
  
  return (
    <div className={clsx('insight-panel', {
      'insight-panel--visible': isVisible,
    })}>
      <div className="insight-panel-header">
        <h3 className="insight-panel-title">
          <Icon icon="timeline-events" iconSize={16} />
          <span>분석 중인 인사이트</span>
        </h3>
        <Button
          icon="cross"
          minimal
          small
          className="insight-panel-close"
          onClick={onToggle}
        />
      </div>
      
      <div className="insight-panel-content">
        {insights.map((insight) => (
          <div
            key={insight.id}
            className={clsx('insight-card-mini', `insight-card-mini--${insight.type}`, {
              'insight-card-mini--active': insight.id === activeInsightId,
              'insight-card-mini--completed': insight.status === 'completed',
            })}
            onClick={() => insight.status !== 'completed' && onInsightClick(insight.id)}
          >
            <div className="insight-card-mini-indicator" />
            
            <div className="insight-card-mini-content">
              <div className="insight-card-mini-header">
                <Icon 
                  icon={insight.icon as any} 
                  iconSize={14} 
                  className="insight-card-mini-icon"
                />
                <h4 className="insight-card-mini-title">{insight.title}</h4>
              </div>
              
              <p className="insight-card-mini-subtitle">{insight.subtitle}</p>
              
              {insight.status === 'active' && insight.progress !== undefined && (
                <div className="insight-card-mini-progress">
                  <ProgressBar
                    value={insight.progress / 100}
                    intent={insight.type === 'warning' ? 'warning' : 'primary'}
                    animate={false}
                    stripes={false}
                  />
                  <span className="insight-card-mini-progress-text">{insight.progress}%</span>
                </div>
              )}
              
              {insight.status === 'completed' && (
                <div className="insight-card-mini-status">
                  <Icon icon="tick" iconSize={12} />
                  <span>분석 완료</span>
                </div>
              )}
              
              {insight.status === 'pending' && (
                <div className="insight-card-mini-status insight-card-mini-status--pending">
                  <span>대기 중</span>
                </div>
              )}
            </div>
          </div>
        ))}
      </div>
      
      {!hasActiveInsights && (
        <div className="insight-panel-footer">
          <p className="insight-panel-complete-message">
            모든 인사이트 분석이 완료되었습니다
          </p>
          <Button
            text="홈으로 돌아가기"
            icon="home"
            intent="primary"
            small
            onClick={onClose}
            className="insight-panel-home-button"
          />
        </div>
      )}
    </div>
  );
};