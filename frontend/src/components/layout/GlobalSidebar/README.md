# GlobalSidebar Component

## 사용 예시

```tsx
import React from 'react';
import { BrowserRouter } from 'react-router-dom';
import { GlobalSidebar } from '@components/layout';

function App() {
  return (
    <BrowserRouter>
      <div className="app-container">
        <GlobalSidebar />
        <main className="app-main">
          {/* Your routes here */}
        </main>
      </div>
    </BrowserRouter>
  );
}
```

## Props

| Prop | Type | Default | Description |
|------|------|---------|-------------|
| className | string | - | 추가 CSS 클래스 |
| collapsed | boolean | false | 사이드바 접힘 상태 |
| onCollapsedChange | (collapsed: boolean) => void | - | 접힘 상태 변경 콜백 |

## 특징

- **Blueprint.js 아이콘**: 미니멀하고 현대적인 아이콘 사용
- **Tooltip 지원**: Collapse 모드에서 hover 시 라벨 표시
- **다크 테마**: Palantir 스타일의 다크 테마 기본 적용
- **반응형**: 모바일에서는 숨김/표시 토글 가능
- **접근성**: ARIA 속성과 키보드 네비게이션 완벽 지원
  - `aria-expanded`: collapse 상태 표시
  - `aria-hidden`: 숨겨진 요소 표시
  - `aria-current`: 현재 페이지 표시
  - ARIA roles: navigation, complementary
- **상태 관리**: Zustand와 React Router 연동
  - URL path와 자동 동기화
  - 새로고침 시에도 상태 유지
- **스크린리더 최적화**: collapse 모드에서도 완전한 네비게이션 가능

## 아이콘 목록

- Home: `home`
- Ontology Editor: `wrench` (스패너/도구)
- Analysis: `chart`
- Action Center: `flash` (번개)
- My Page: `user`

## 스타일 커스터마이징

SCSS 변수를 통해 쉽게 커스터마이징 가능:

```scss
// 사이드바 너비 변경
$sidebar-width: 280px;

// 색상 변경
$sidebar-bg: $dark-gray2;
```

## 테스트

컴포넌트는 완전한 테스트 커버리지를 제공합니다:

```bash
npm run test GlobalSidebar.test.tsx
```

테스트 항목:
- 모든 네비게이션 아이템 렌더링
- 현재 경로 활성화 상태
- Collapse 토글 기능
- Tooltip 표시 (collapse 모드)
- 접근성 속성 검증
- URL 동기화
- 스크린리더 대응