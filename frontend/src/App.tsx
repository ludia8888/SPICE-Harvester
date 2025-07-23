import { useState } from 'react';
import { ThemeProvider } from '@mui/material/styles';
import { CssBaseline, Box } from '@mui/material';
import { theme } from './themes/theme';
import { TopBar } from './components/layout/TopBar';
import { Sidebar } from './components/layout/Sidebar';
import { DiscoverView } from './components/ontology/DiscoverView';
import { ObjectTypeView } from './components/ontology/ObjectTypeView';
import { PropertyEditor } from './components/ontology/PropertyEditor';
import { OntologyCanvas } from './components/ontology/OntologyCanvas';
import { OntologyClass, Property } from './types/ontology';

// Manufacturing Company Ontology - Samsung Electronics 모델링
const mockClasses: OntologyClass[] = [
  {
    id: 'product',
    label: 'Product',
    description: '제조되는 제품 (스마트폰, 반도체, 디스플레이 등)',
    abstract: false,
    created_at: '2024-01-15T10:00:00Z',
    updated_at: '2024-01-20T15:30:00Z',
    properties: [
      {
        name: 'product_id',
        type: 'xsd:string',
        label: '제품 ID',
        required: true,
        description: '제품 고유 식별자 (SM-S921N, 등)',
      },
      {
        name: 'model_name',
        type: 'xsd:string',
        label: '모델명',
        required: true,
        description: '제품 모델명 (Galaxy S24 Ultra)',
      },
      {
        name: 'product_category',
        type: 'xsd:string',
        label: '제품 카테고리',
        required: true,
        description: '제품 분류 (Mobile, Semiconductor, Display)',
      },
      {
        name: 'manufacturing_cost',
        type: 'xsd:decimal',
        label: '제조원가',
        required: false,
        description: '단위당 제조원가 (USD)',
      },
      {
        name: 'release_date',
        type: 'xsd:date',
        label: '출시일',
        required: false,
        description: '제품 출시일',
      },
      {
        name: 'specifications',
        type: 'xsd:string',
        label: '제품 사양',
        required: false,
        description: 'JSON 형태의 제품 상세 사양',
      },
    ],
    relationships: [
      {
        predicate: 'manufacturedBy',
        target: 'production_line',
        label: '생산라인',
        cardinality: 'n:1',
        description: '제품을 생산하는 라인',
      },
      {
        predicate: 'usesComponent',
        target: 'component',
        label: '사용부품',
        cardinality: 'n:n',
        description: '제품에 사용되는 부품들',
      },
      {
        predicate: 'hasQualityCheck',
        target: 'quality_inspection',
        label: '품질검사',
        cardinality: '1:n',
        description: '제품의 품질검사 기록',
      },
    ],
  },
  {
    id: 'component',
    label: 'Component',
    description: '제품을 구성하는 부품/소재 (AP, 메모리, 디스플레이 패널 등)',
    abstract: false,
    created_at: '2024-01-10T09:00:00Z',
    updated_at: '2024-01-18T14:20:00Z',
    properties: [
      {
        name: 'component_id',
        type: 'xsd:string',
        label: '부품 ID',
        required: true,
        description: '부품 고유 식별자',
      },
      {
        name: 'component_name',
        type: 'xsd:string',
        label: '부품명',
        required: true,
        description: '부품 이름 (Exynos 2400, LPDDR5X RAM)',
      },
      {
        name: 'supplier',
        type: 'xsd:string',
        label: '공급업체',
        required: true,
        description: '부품 공급업체명',
      },
      {
        name: 'unit_cost',
        type: 'xsd:decimal',
        label: '단가',
        required: false,
        description: '부품 단가 (USD)',
      },
      {
        name: 'inventory_level',
        type: 'xsd:integer',
        label: '재고수준',
        required: false,
        description: '현재 재고 수량',
      },
      {
        name: 'lead_time_days',
        type: 'xsd:integer',
        label: '리드타임',
        required: false,
        description: '주문부터 입고까지 소요일수',
      },
    ],
    relationships: [
      {
        predicate: 'suppliedBy',
        target: 'supplier',
        label: '공급업체',
        cardinality: 'n:1',
        description: '부품을 공급하는 업체',
      },
      {
        predicate: 'hasQualitySpec',
        target: 'quality_specification',
        label: '품질규격',
        cardinality: '1:1',
        description: '부품의 품질 규격',
      },
    ],
  },
  {
    id: 'production_line',
    label: 'Production Line',
    description: '제조라인 (스마트폰 조립라인, 반도체 팹 라인 등)',
    abstract: false,
    created_at: '2024-01-12T11:00:00Z',
    updated_at: '2024-01-19T16:45:00Z',
    properties: [
      {
        name: 'line_id',
        type: 'xsd:string',
        label: '라인 ID',
        required: true,
        description: '생산라인 고유 식별자',
      },
      {
        name: 'line_name',
        type: 'xsd:string',
        label: '라인명',
        required: true,
        description: '생산라인 이름 (Galaxy Assembly Line A)',
      },
      {
        name: 'capacity_per_hour',
        type: 'xsd:integer',
        label: '시간당 생산능력',
        required: false,
        description: '시간당 최대 생산 가능 수량',
      },
      {
        name: 'efficiency_rate',
        type: 'xsd:decimal',
        label: '가동효율',
        required: false,
        description: '라인 가동 효율 (0.0 ~ 1.0)',
      },
      {
        name: 'location',
        type: 'xsd:string',
        label: '위치',
        required: false,
        description: '생산라인 위치 (구미, 화성, 평택 등)',
      },
    ],
    relationships: [
      {
        predicate: 'belongsToFactory',
        target: 'factory',
        label: '소속공장',
        cardinality: 'n:1',
        description: '라인이 속한 공장',
      },
      {
        predicate: 'operatedBy',
        target: 'employee',
        label: '운영담당자',
        cardinality: 'n:n',
        description: '라인을 운영하는 직원들',
      },
      {
        predicate: 'usesMachine',
        target: 'machine',
        label: '사용장비',
        cardinality: '1:n',
        description: '라인에서 사용하는 장비들',
      },
    ],
  },
  {
    id: 'machine',
    label: 'Machine',
    description: '생산장비 (SMT 장비, 테스트 장비, 패키징 장비 등)',
    abstract: false,
    created_at: '2024-01-14T13:00:00Z',
    updated_at: '2024-01-21T10:15:00Z',
    properties: [
      {
        name: 'machine_id',
        type: 'xsd:string',
        label: '장비 ID',
        required: true,
        description: '장비 고유 식별자',
      },
      {
        name: 'machine_name',
        type: 'xsd:string',
        label: '장비명',
        required: true,
        description: '장비 이름',
      },
      {
        name: 'machine_type',
        type: 'xsd:string',
        label: '장비타입',
        required: true,
        description: '장비 분류 (SMT, Assembly, Test, Packaging)',
      },
      {
        name: 'manufacturer',
        type: 'xsd:string',
        label: '제조사',
        required: false,
        description: '장비 제조사',
      },
      {
        name: 'installation_date',
        type: 'xsd:date',
        label: '설치일',
        required: false,
        description: '장비 설치일',
      },
      {
        name: 'status',
        type: 'xsd:string',
        label: '가동상태',
        required: false,
        description: '장비 현재 상태 (Running, Idle, Maintenance, Down)',
      },
    ],
    relationships: [
      {
        predicate: 'maintainedBy',
        target: 'employee',
        label: '유지보수담당자',
        cardinality: 'n:n',
        description: '장비를 유지보수하는 직원들',
      },
      {
        predicate: 'hasMaintenanceRecord',
        target: 'maintenance_record',
        label: '유지보수기록',
        cardinality: '1:n',
        description: '장비의 유지보수 이력',
      },
    ],
  },
  {
    id: 'employee',
    label: 'Employee',
    description: '삼성전자 직원 (엔지니어, 오퍼레이터, 품질관리자 등)',
    abstract: false,
    created_at: '2024-01-13T14:30:00Z',
    updated_at: '2024-01-22T09:45:00Z',
    properties: [
      {
        name: 'employee_id',
        type: 'xsd:string',
        label: '사번',
        required: true,
        description: '직원 고유 사번',
      },
      {
        name: 'name',
        type: 'xsd:string',
        label: '이름',
        required: true,
        description: '직원 이름',
      },
      {
        name: 'department',
        type: 'xsd:string',
        label: '부서',
        required: true,
        description: '소속 부서 (DX, DS, Harman 등)',
      },
      {
        name: 'position',
        type: 'xsd:string',
        label: '직급',
        required: false,
        description: '직급 (사원, 주임, 선임, 수석, 책임 등)',
      },
      {
        name: 'skill_level',
        type: 'xsd:string',
        label: '기술등급',
        required: false,
        description: '기술 숙련도 (Junior, Senior, Expert)',
      },
      {
        name: 'hire_date',
        type: 'xsd:date',
        label: '입사일',
        required: false,
        description: '입사일',
      },
    ],
    relationships: [
      {
        predicate: 'worksInFactory',
        target: 'factory',
        label: '근무공장',
        cardinality: 'n:1',
        description: '근무하는 공장',
      },
      {
        predicate: 'hasCertification',
        target: 'certification',
        label: '보유자격',
        cardinality: 'n:n',
        description: '보유한 자격증/인증',
      },
    ],
  },
  {
    id: 'factory',
    label: 'Factory',
    description: '삼성전자 제조공장 (구미, 화성, 평택, 천안 등)',
    abstract: false,
    created_at: '2024-01-11T08:00:00Z',
    updated_at: '2024-01-20T17:30:00Z',
    properties: [
      {
        name: 'factory_id',
        type: 'xsd:string',
        label: '공장 ID',
        required: true,
        description: '공장 고유 식별자',
      },
      {
        name: 'factory_name',
        type: 'xsd:string',
        label: '공장명',
        required: true,
        description: '공장 이름 (구미사업장, 화성캠퍼스 등)',
      },
      {
        name: 'location',
        type: 'xsd:string',
        label: '위치',
        required: true,
        description: '공장 소재지',
      },
      {
        name: 'total_area',
        type: 'xsd:decimal',
        label: '총면적',
        required: false,
        description: '공장 총 면적 (m²)',
      },
      {
        name: 'established_date',
        type: 'xsd:date',
        label: '설립일',
        required: false,
        description: '공장 설립일',
      },
      {
        name: 'main_products',
        type: 'xsd:string',
        label: '주요제품',
        required: false,
        description: '공장의 주요 생산 제품군',
      },
    ],
    relationships: [
      {
        predicate: 'hasEnvironmentalCert',
        target: 'certification',
        label: '환경인증',
        cardinality: '1:n',
        description: '보유한 환경 관련 인증',
      },
    ],
  },
  {
    id: 'quality_inspection',
    label: 'Quality Inspection',
    description: '품질검사 기록 (입고검사, 공정검사, 출하검사)',
    abstract: false,
    created_at: '2024-01-16T11:30:00Z',
    updated_at: '2024-01-23T14:20:00Z',
    properties: [
      {
        name: 'inspection_id',
        type: 'xsd:string',
        label: '검사 ID',
        required: true,
        description: '품질검사 고유 식별자',
      },
      {
        name: 'inspection_type',
        type: 'xsd:string',
        label: '검사타입',
        required: true,
        description: '검사 종류 (Incoming, In-Process, Final)',
      },
      {
        name: 'inspection_date',
        type: 'xsd:dateTime',
        label: '검사일시',
        required: true,
        description: '검사 수행 일시',
      },
      {
        name: 'result',
        type: 'xsd:string',
        label: '검사결과',
        required: true,
        description: '검사 결과 (Pass, Fail, Conditional)',
      },
      {
        name: 'defect_rate',
        type: 'xsd:decimal',
        label: '불량률',
        required: false,
        description: '측정된 불량률 (%)',
      },
    ],
    relationships: [
      {
        predicate: 'inspectedBy',
        target: 'employee',
        label: '검사자',
        cardinality: 'n:1',
        description: '검사를 수행한 직원',
      },
    ],
  },
];

const drawerWidth = 240;

function App() {
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [selectedView, setSelectedView] = useState('discover');
  const [selectedClass, setSelectedClass] = useState<OntologyClass | undefined>();
  const [propertyEditorOpen, setPropertyEditorOpen] = useState(false);
  const [editingProperty] = useState<Property | undefined>();
  const [favoriteClasses, setFavoriteClasses] = useState<string[]>(['product', 'production_line']);

  const handleMenuClick = () => {
    setSidebarOpen(!sidebarOpen);
  };

  const handleSidebarItemClick = (item: string) => {
    setSelectedView(item);
    if (item === 'object-types' && mockClasses.length > 0) {
      setSelectedClass(mockClasses[0]);
    }
  };

  const handleClassClick = (classId: string) => {
    const cls = mockClasses.find(c => c.id === classId);
    if (cls) {
      setSelectedClass(cls);
      setSelectedView('object-types');
    }
  };

  const handleToggleFavorite = (classId: string) => {
    setFavoriteClasses(prev => {
      if (prev.includes(classId)) {
        return prev.filter(id => id !== classId);
      }
      return [...prev, classId];
    });
  };

  const handlePropertySave = (property: Property) => {
    console.log('Saving property:', property);
    // TODO: Implement property save logic
    setPropertyEditorOpen(false);
  };

  const renderContent = () => {
    switch (selectedView) {
      case 'discover':
        return (
          <DiscoverView
            favoriteClasses={mockClasses.filter(c => favoriteClasses.includes(c.id))}
            recentClasses={mockClasses.slice(0, 2)}
            allClasses={mockClasses}
            onClassClick={handleClassClick}
            onToggleFavorite={handleToggleFavorite}
          />
        );
      case 'canvas':
        return (
          <OntologyCanvas
            classes={mockClasses}
            onNodeClick={(node) => {
              const cls = mockClasses.find(c => c.id === node.id);
              if (cls) {
                setSelectedClass(cls);
                setSelectedView('object-types');
              }
            }}
            onEdgeClick={(edge) => console.log('Edge clicked:', edge)}
            onCanvasClick={() => console.log('Canvas clicked')}
          />
        );
      case 'object-types':
        return selectedClass ? (
          <ObjectTypeView
            objectType={selectedClass}
            onEdit={() => console.log('Edit clicked')}
          />
        ) : (
          <div>Select an object type</div>
        );
      default:
        return <div>{selectedView} view - Coming soon</div>;
    }
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Box sx={{ display: 'flex' }}>
        <TopBar onMenuClick={handleMenuClick} currentBranch="main" />
        
        <Sidebar
          open={sidebarOpen}
          selectedItem={selectedView}
          onItemClick={handleSidebarItemClick}
        />
        
        <Box
          component="main"
          sx={{
            flexGrow: 1,
            p: 3,
            mt: 8, // AppBar height
            ml: sidebarOpen ? `${drawerWidth}px` : 0,
            transition: theme.transitions.create(['margin'], {
              easing: theme.transitions.easing.sharp,
              duration: theme.transitions.duration.leavingScreen,
            }),
          }}
        >
          {renderContent()}
        </Box>

        <PropertyEditor
          open={propertyEditorOpen}
          onClose={() => setPropertyEditorOpen(false)}
          property={editingProperty}
          onSave={handlePropertySave}
        />
      </Box>
    </ThemeProvider>
  );
}

export default App;