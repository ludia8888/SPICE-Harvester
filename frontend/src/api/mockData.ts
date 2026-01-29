/**
 * Mock Data for Frontend Development
 * VITE_MOCK_API=true 일 때 사용됩니다.
 */

import type {
  DatabaseRecord,
  DatasetRecord,
  PipelineRecord,
  PipelineDetailRecord,
  PipelineArtifactRecord,
  ActionType,
  ActionTypeDetail,
  ActionLog,
  OntologyClass,
  LinkType,
  UdfRecord,
} from './bff'

// === Databases ===
export const mockDatabases: DatabaseRecord[] = [
  {
    db_name: 'enterprise-analytics',
    name: 'Enterprise Analytics',
    description: 'Palantir Foundry 스타일 엔터프라이즈 분석 프로젝트',
    dataset_count: 4,
    created_at: '2024-01-01T00:00:00Z',
  },
  {
    db_name: 'supply-chain-mgmt',
    name: 'Supply Chain Management',
    description: '글로벌 공급망 관리 및 최적화',
    dataset_count: 5,
    created_at: '2024-01-15T00:00:00Z',
  },
  {
    db_name: 'fraud-detection',
    name: 'Fraud Detection',
    description: '금융 사기 탐지 및 AML 시스템',
    dataset_count: 4,
    created_at: '2024-02-01T00:00:00Z',
  },
]

// === Datasets (Palantir Foundry 스타일 - CSV 파일 기반) ===
export const mockDatasets: DatasetRecord[] = [
  // Enterprise Analytics 프로젝트
  {
    dataset_id: 'ds-customers',
    db_name: 'enterprise-analytics',
    name: 'Customers',
    description: '글로벌 고객 마스터 데이터 (KYC, 리스크 스코어 포함)',
    source_type: 'csv',
    branch: 'main',
    row_count: 15,
    created_at: '2024-01-10T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'customer_id', type: 'string' },
        { name: 'name', type: 'string' },
        { name: 'email', type: 'string' },
        { name: 'phone', type: 'string' },
        { name: 'tier', type: 'string' },
        { name: 'country', type: 'string' },
        { name: 'city', type: 'string' },
        { name: 'registration_date', type: 'date' },
        { name: 'last_activity', type: 'date' },
        { name: 'total_spent', type: 'number' },
        { name: 'risk_score', type: 'number' },
      ],
    },
    sample_json: {
      rows: [
        { customer_id: 'CUST-001', name: '삼성전자', email: 'samsung@company.com', phone: '02-1234-5678', tier: 'Enterprise', country: 'Korea', city: 'Seoul', registration_date: '2018-03-15', last_activity: '2024-02-20', total_spent: 285000000000, risk_score: 15 },
        { customer_id: 'CUST-002', name: '현대자동차', email: 'hyundai@company.com', phone: '02-2345-6789', tier: 'Enterprise', country: 'Korea', city: 'Seoul', registration_date: '2017-06-01', last_activity: '2024-02-21', total_spent: 195000000000, risk_score: 18 },
        { customer_id: 'CUST-003', name: 'LG화학', email: 'lgchem@company.com', phone: '02-3456-7890', tier: 'Enterprise', country: 'Korea', city: 'Seoul', registration_date: '2019-01-10', last_activity: '2024-02-19', total_spent: 156000000000, risk_score: 22 },
        { customer_id: 'CUST-004', name: 'SK하이닉스', email: 'skhynix@company.com', phone: '031-456-7890', tier: 'Enterprise', country: 'Korea', city: 'Icheon', registration_date: '2016-09-20', last_activity: '2024-02-21', total_spent: 178000000000, risk_score: 12 },
        { customer_id: 'CUST-005', name: 'CATL', email: 'catl@company.cn', phone: '+86-591-1234', tier: 'Enterprise', country: 'China', city: 'Ningde', registration_date: '2020-03-01', last_activity: '2024-02-18', total_spent: 89000000000, risk_score: 35 },
      ],
    },
  },
  {
    dataset_id: 'ds-accounts',
    db_name: 'enterprise-analytics',
    name: 'Accounts',
    description: '금융 계좌 마스터 데이터',
    source_type: 'csv',
    branch: 'main',
    row_count: 20,
    created_at: '2024-01-12T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'account_id', type: 'string' },
        { name: 'customer_id', type: 'string' },
        { name: 'account_type', type: 'string' },
        { name: 'currency', type: 'string' },
        { name: 'balance', type: 'number' },
        { name: 'credit_limit', type: 'number' },
        { name: 'status', type: 'string' },
        { name: 'opened_date', type: 'date' },
        { name: 'branch_code', type: 'string' },
      ],
    },
    sample_json: {
      rows: [
        { account_id: 'ACC-10001', customer_id: 'CUST-001', account_type: 'Operating', currency: 'KRW', balance: 15000000000, credit_limit: 50000000000, status: 'Active', opened_date: '2018-03-20', branch_code: 'SEL-001' },
        { account_id: 'ACC-10002', customer_id: 'CUST-001', account_type: 'Settlement', currency: 'USD', balance: 25000000, credit_limit: 100000000, status: 'Active', opened_date: '2018-03-20', branch_code: 'SEL-001' },
        { account_id: 'ACC-10003', customer_id: 'CUST-002', account_type: 'Operating', currency: 'KRW', balance: 8500000000, credit_limit: 30000000000, status: 'Active', opened_date: '2017-06-15', branch_code: 'SEL-002' },
        { account_id: 'ACC-10004', customer_id: 'CUST-003', account_type: 'Operating', currency: 'KRW', balance: 12000000000, credit_limit: 40000000000, status: 'Active', opened_date: '2019-01-15', branch_code: 'SEL-003' },
        { account_id: 'ACC-10005', customer_id: 'CUST-004', account_type: 'Operating', currency: 'KRW', balance: 9200000000, credit_limit: 35000000000, status: 'Active', opened_date: '2016-10-01', branch_code: 'ICH-001' },
      ],
    },
  },
  {
    dataset_id: 'ds-transactions',
    db_name: 'enterprise-analytics',
    name: 'Transactions',
    description: '금융 거래 내역 (이상 탐지 플래그 포함)',
    source_type: 'csv',
    branch: 'main',
    row_count: 25,
    created_at: '2024-01-15T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'transaction_id', type: 'string' },
        { name: 'from_account', type: 'string' },
        { name: 'to_account', type: 'string' },
        { name: 'amount', type: 'number' },
        { name: 'currency', type: 'string' },
        { name: 'transaction_type', type: 'string' },
        { name: 'status', type: 'string' },
        { name: 'timestamp', type: 'datetime' },
        { name: 'description', type: 'string' },
        { name: 'risk_flag', type: 'boolean' },
        { name: 'category', type: 'string' },
      ],
    },
    sample_json: {
      rows: [
        { transaction_id: 'TXN-000001', from_account: 'ACC-10001', to_account: 'ACC-10015', amount: 125000000, currency: 'KRW', transaction_type: 'Wire Transfer', status: 'Completed', timestamp: '2024-02-21T14:30:00Z', description: '원자재 대금', risk_flag: false, category: 'Supplier Payment' },
        { transaction_id: 'TXN-000002', from_account: 'ACC-10003', to_account: 'ACC-10008', amount: 85000000, currency: 'KRW', transaction_type: 'Wire Transfer', status: 'Completed', timestamp: '2024-02-21T10:15:00Z', description: '부품 대금', risk_flag: false, category: 'Supplier Payment' },
        { transaction_id: 'TXN-000003', from_account: 'ACC-10004', to_account: 'ACC-10001', amount: 250000000, currency: 'KRW', transaction_type: 'Wire Transfer', status: 'Completed', timestamp: '2024-02-20T16:00:00Z', description: '반도체 납품대금', risk_flag: false, category: 'Customer Payment' },
        { transaction_id: 'TXN-000004', from_account: 'ACC-10002', to_account: 'ACC-EXT-001', amount: 5000000, currency: 'USD', transaction_type: 'International Wire', status: 'Pending', timestamp: '2024-02-21T09:00:00Z', description: '해외 설비 구매', risk_flag: true, category: 'Equipment' },
        { transaction_id: 'TXN-000005', from_account: 'ACC-10005', to_account: 'ACC-10012', amount: 180000000, currency: 'KRW', transaction_type: 'Wire Transfer', status: 'Completed', timestamp: '2024-02-19T11:30:00Z', description: '배터리 소재 대금', risk_flag: false, category: 'Supplier Payment' },
      ],
    },
  },
  {
    dataset_id: 'ds-employees',
    db_name: 'enterprise-analytics',
    name: 'Employees',
    description: '직원 마스터 데이터 (조직 구조 포함)',
    source_type: 'csv',
    branch: 'main',
    row_count: 20,
    created_at: '2024-01-20T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'employee_id', type: 'string' },
        { name: 'name', type: 'string' },
        { name: 'email', type: 'string' },
        { name: 'department', type: 'string' },
        { name: 'title', type: 'string' },
        { name: 'facility_id', type: 'string' },
        { name: 'manager_id', type: 'string' },
        { name: 'hire_date', type: 'date' },
        { name: 'clearance_level', type: 'string' },
      ],
    },
  },
  // Supply Chain Management 프로젝트
  {
    dataset_id: 'ds-vendors',
    db_name: 'supply-chain-mgmt',
    name: 'Vendors',
    description: '글로벌 공급업체 마스터 데이터',
    source_type: 'csv',
    branch: 'main',
    row_count: 15,
    created_at: '2024-01-25T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'vendor_id', type: 'string' },
        { name: 'name', type: 'string' },
        { name: 'country', type: 'string' },
        { name: 'city', type: 'string' },
        { name: 'category', type: 'string' },
        { name: 'contact_email', type: 'string' },
        { name: 'rating', type: 'number' },
        { name: 'contract_start', type: 'date' },
        { name: 'contract_end', type: 'date' },
        { name: 'risk_tier', type: 'string' },
        { name: 'annual_volume', type: 'number' },
      ],
    },
    sample_json: {
      rows: [
        { vendor_id: 'VND-001', name: 'POSCO', country: 'Korea', city: 'Pohang', category: 'Steel', contact_email: 'sales@posco.com', rating: 4.8, contract_start: '2020-01-01', contract_end: '2025-12-31', risk_tier: 'Low', annual_volume: 850000000000 },
        { vendor_id: 'VND-002', name: 'Albemarle', country: 'USA', city: 'Charlotte', category: 'Chemicals', contact_email: 'sales@albemarle.com', rating: 4.5, contract_start: '2021-03-01', contract_end: '2024-02-28', risk_tier: 'Medium', annual_volume: 125000000 },
        { vendor_id: 'VND-003', name: 'Ganfeng Lithium', country: 'China', city: 'Xinyu', category: 'Battery Materials', contact_email: 'export@ganfenglithium.com', rating: 4.2, contract_start: '2022-06-01', contract_end: '2025-05-31', risk_tier: 'Medium', annual_volume: 95000000 },
        { vendor_id: 'VND-004', name: 'Contemporary Amperex', country: 'China', city: 'Ningde', category: 'Battery Cells', contact_email: 'b2b@catl.com', rating: 4.6, contract_start: '2023-01-01', contract_end: '2025-12-31', risk_tier: 'Low', annual_volume: 280000000 },
        { vendor_id: 'VND-005', name: 'Umicore', country: 'Belgium', city: 'Brussels', category: 'Cathode Materials', contact_email: 'sales@umicore.com', rating: 4.7, contract_start: '2021-09-01', contract_end: '2024-08-31', risk_tier: 'Low', annual_volume: 180000000 },
      ],
    },
  },
  {
    dataset_id: 'ds-facilities',
    db_name: 'supply-chain-mgmt',
    name: 'Facilities',
    description: '글로벌 시설/창고/공장 마스터 데이터',
    source_type: 'csv',
    branch: 'main',
    row_count: 15,
    created_at: '2024-01-28T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'facility_id', type: 'string' },
        { name: 'name', type: 'string' },
        { name: 'type', type: 'string' },
        { name: 'country', type: 'string' },
        { name: 'city', type: 'string' },
        { name: 'address', type: 'string' },
        { name: 'capacity_units', type: 'number' },
        { name: 'current_utilization', type: 'number' },
        { name: 'manager', type: 'string' },
        { name: 'status', type: 'string' },
      ],
    },
  },
  {
    dataset_id: 'ds-products',
    db_name: 'supply-chain-mgmt',
    name: 'Products',
    description: '제품/부품 카탈로그 (재고, 리드타임 포함)',
    source_type: 'csv',
    branch: 'main',
    row_count: 20,
    created_at: '2024-02-01T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'product_id', type: 'string' },
        { name: 'name', type: 'string' },
        { name: 'category', type: 'string' },
        { name: 'subcategory', type: 'string' },
        { name: 'unit_price', type: 'number' },
        { name: 'unit_cost', type: 'number' },
        { name: 'inventory_qty', type: 'number' },
        { name: 'reorder_point', type: 'number' },
        { name: 'lead_time_days', type: 'number' },
        { name: 'vendor_id', type: 'string' },
        { name: 'status', type: 'string' },
      ],
    },
  },
  {
    dataset_id: 'ds-orders',
    db_name: 'supply-chain-mgmt',
    name: 'Orders',
    description: 'B2B 주문 데이터',
    source_type: 'csv',
    branch: 'main',
    row_count: 15,
    created_at: '2024-02-05T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'order_id', type: 'string' },
        { name: 'customer_id', type: 'string' },
        { name: 'product_id', type: 'string' },
        { name: 'quantity', type: 'number' },
        { name: 'total_amount', type: 'number' },
        { name: 'order_date', type: 'date' },
        { name: 'required_date', type: 'date' },
        { name: 'status', type: 'string' },
        { name: 'facility_id', type: 'string' },
        { name: 'priority', type: 'string' },
      ],
    },
  },
  {
    dataset_id: 'ds-shipments',
    db_name: 'supply-chain-mgmt',
    name: 'Shipments',
    description: '배송/물류 추적 데이터',
    source_type: 'csv',
    branch: 'main',
    row_count: 10,
    created_at: '2024-02-08T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'shipment_id', type: 'string' },
        { name: 'order_id', type: 'string' },
        { name: 'origin_facility', type: 'string' },
        { name: 'destination_facility', type: 'string' },
        { name: 'carrier', type: 'string' },
        { name: 'tracking_number', type: 'string' },
        { name: 'ship_date', type: 'date' },
        { name: 'estimated_arrival', type: 'date' },
        { name: 'status', type: 'string' },
        { name: 'weight_kg', type: 'number' },
      ],
    },
  },
  // Fraud Detection 프로젝트
  {
    dataset_id: 'ds-alerts',
    db_name: 'fraud-detection',
    name: 'Alerts',
    description: '이상 탐지 알림 및 위험 이벤트',
    source_type: 'csv',
    branch: 'main',
    row_count: 15,
    created_at: '2024-02-10T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'alert_id', type: 'string' },
        { name: 'alert_type', type: 'string' },
        { name: 'severity', type: 'string' },
        { name: 'entity_type', type: 'string' },
        { name: 'entity_id', type: 'string' },
        { name: 'title', type: 'string' },
        { name: 'description', type: 'string' },
        { name: 'created_at', type: 'datetime' },
        { name: 'status', type: 'string' },
        { name: 'assigned_to', type: 'string' },
      ],
    },
    sample_json: {
      rows: [
        { alert_id: 'ALT-2024-001', alert_type: 'Fraud Detection', severity: 'Critical', entity_type: 'Transaction', entity_id: 'TXN-000019', title: 'Suspicious Wire Transfer', description: 'Large wire transfer to offshore account detected', created_at: '2024-02-21T02:35:00Z', status: 'Open', assigned_to: 'AML Team' },
        { alert_id: 'ALT-2024-002', alert_type: 'Inventory', severity: 'High', entity_type: 'Product', entity_id: 'PRD-016', title: 'Low Stock Alert', description: 'Lithium Carbonate inventory below reorder point', created_at: '2024-02-20T08:00:00Z', status: 'In Progress', assigned_to: 'Supply Chain' },
        { alert_id: 'ALT-2024-003', alert_type: 'Credit Risk', severity: 'High', entity_type: 'Account', entity_id: 'ACC-10020', title: 'Credit Limit Breach', description: 'Account suspended due to credit violations', created_at: '2024-01-30T16:05:00Z', status: 'Resolved', assigned_to: 'Credit Team' },
        { alert_id: 'ALT-2024-010', alert_type: 'Security', severity: 'Critical', entity_type: 'Facility', entity_id: 'FAC-005', title: 'Access Anomaly', description: 'Unusual after-hours access detected at R&D Center', created_at: '2024-02-21T03:15:00Z', status: 'In Progress', assigned_to: 'Security' },
        { alert_id: 'ALT-2024-011', alert_type: 'Financial', severity: 'High', entity_type: 'Customer', entity_id: 'CUST-003', title: 'Payment Overdue', description: 'Invoice payment overdue by 45 days', created_at: '2024-02-18T09:00:00Z', status: 'Open', assigned_to: 'Collections' },
      ],
    },
  },
  {
    dataset_id: 'ds-suspicious-txn',
    db_name: 'fraud-detection',
    name: 'Suspicious Transactions',
    description: '의심 거래 목록 (ML 모델 탐지)',
    source_type: 'parquet',
    branch: 'main',
    row_count: 1250,
    created_at: '2024-02-12T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'transaction_id', type: 'string' },
        { name: 'account_id', type: 'string' },
        { name: 'amount', type: 'number' },
        { name: 'risk_score', type: 'number' },
        { name: 'detection_reason', type: 'string' },
        { name: 'detected_at', type: 'datetime' },
        { name: 'model_version', type: 'string' },
      ],
    },
  },
  {
    dataset_id: 'ds-risk-profiles',
    db_name: 'fraud-detection',
    name: 'Customer Risk Profiles',
    description: '고객별 리스크 프로파일 및 행동 패턴',
    source_type: 'parquet',
    branch: 'main',
    row_count: 5000,
    created_at: '2024-02-08T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'customer_id', type: 'string' },
        { name: 'risk_tier', type: 'string' },
        { name: 'risk_score', type: 'number' },
        { name: 'avg_transaction_amount', type: 'number' },
        { name: 'transaction_frequency', type: 'number' },
        { name: 'unusual_pattern_count', type: 'number' },
        { name: 'last_reviewed', type: 'datetime' },
      ],
    },
  },
  {
    dataset_id: 'ds-investigations',
    db_name: 'fraud-detection',
    name: 'Case Investigations',
    description: '사기 조사 케이스 및 결과',
    source_type: 'csv',
    branch: 'main',
    row_count: 320,
    created_at: '2024-02-15T00:00:00Z',
    schema_json: {
      columns: [
        { name: 'case_id', type: 'string' },
        { name: 'alert_ids', type: 'string' },
        { name: 'assigned_analyst', type: 'string' },
        { name: 'status', type: 'string' },
        { name: 'priority', type: 'string' },
        { name: 'opened_at', type: 'datetime' },
        { name: 'closed_at', type: 'datetime' },
        { name: 'outcome', type: 'string' },
        { name: 'notes', type: 'string' },
      ],
    },
  },
]

// === Pipelines ===
export const mockPipelines: PipelineRecord[] = [
  // Enterprise Analytics 프로젝트
  {
    pipeline_id: 'pl-001',
    db_name: 'enterprise-analytics',
    name: 'Customer Risk Scoring',
    description: '고객 거래 패턴 분석 및 리스크 스코어 산출',
    pipeline_type: 'batch',
    status: 'active',
    updated_at: '2024-02-15T00:00:00Z',
  },
  {
    pipeline_id: 'pl-002',
    db_name: 'enterprise-analytics',
    name: 'Transaction Aggregation',
    description: '거래 데이터 일별/월별 집계',
    pipeline_type: 'batch',
    status: 'active',
    updated_at: '2024-02-20T00:00:00Z',
  },
  {
    pipeline_id: 'pl-003',
    db_name: 'enterprise-analytics',
    name: 'Real-time Transaction Monitor',
    description: '실시간 거래 모니터링 및 이상 탐지',
    pipeline_type: 'streaming',
    status: 'active',
    updated_at: '2024-02-25T00:00:00Z',
  },
  // Supply Chain Management 프로젝트
  {
    pipeline_id: 'pl-004',
    db_name: 'supply-chain-mgmt',
    name: 'Inventory Optimization',
    description: '재고 수준 최적화 및 재주문점 계산',
    pipeline_type: 'batch',
    status: 'active',
    updated_at: '2024-02-18T00:00:00Z',
  },
  {
    pipeline_id: 'pl-005',
    db_name: 'supply-chain-mgmt',
    name: 'Shipment Tracking ETL',
    description: '배송 추적 데이터 ETL 및 상태 업데이트',
    pipeline_type: 'batch',
    status: 'active',
    updated_at: '2024-02-22T00:00:00Z',
  },
  {
    pipeline_id: 'pl-006',
    db_name: 'supply-chain-mgmt',
    name: 'Vendor Performance Analysis',
    description: '공급업체 성과 분석 및 등급 산정',
    pipeline_type: 'batch',
    status: 'active',
    updated_at: '2024-02-24T00:00:00Z',
  },
  // Fraud Detection 프로젝트
  {
    pipeline_id: 'pl-007',
    db_name: 'fraud-detection',
    name: 'Anomaly Detection Pipeline',
    description: '거래 이상 패턴 탐지 및 알림 생성',
    pipeline_type: 'streaming',
    status: 'active',
    updated_at: '2024-02-26T00:00:00Z',
  },
  {
    pipeline_id: 'pl-008',
    db_name: 'fraud-detection',
    name: 'Alert Enrichment',
    description: '탐지된 알림에 컨텍스트 정보 추가',
    pipeline_type: 'batch',
    status: 'active',
    updated_at: '2024-02-27T00:00:00Z',
  },
]

export const mockPipelineDetail: PipelineDetailRecord = {
  pipeline_id: 'pl-001',
  db_name: 'enterprise-analytics',
  name: 'Customer Risk Scoring',
  description: '고객 거래 패턴 분석 및 리스크 스코어 산출',
  pipeline_type: 'batch',
  status: 'active',
  updated_at: '2024-02-15T00:00:00Z',
  definition_json: {
    nodes: [
      { id: 'input-1', type: 'input', metadata: { datasetName: 'Customers', datasetId: 'ds-customers' } },
      { id: 'input-2', type: 'input', metadata: { datasetName: 'Transactions', datasetId: 'ds-transactions' } },
      { id: 'transform-1', type: 'transform', metadata: { operation: 'join', leftKey: 'customer_id', rightKey: 'from_account' } },
      { id: 'transform-2', type: 'transform', metadata: { operation: 'groupBy', groupBy: ['customer_id'], aggregations: [{ column: 'amount', function: 'sum' }, { column: 'risk_flag', function: 'count' }] } },
      { id: 'transform-3', type: 'transform', metadata: { operation: 'compute', expression: 'risk_score = risk_flag_count / total_amount * 100' } },
      { id: 'output-1', type: 'output', metadata: { datasetName: 'Customer Risk Scores' } },
    ],
    edges: [
      { id: 'e1', source: 'input-1', target: 'transform-1' },
      { id: 'e2', source: 'input-2', target: 'transform-1' },
      { id: 'e3', source: 'transform-1', target: 'transform-2' },
      { id: 'e4', source: 'transform-2', target: 'transform-3' },
      { id: 'e5', source: 'transform-3', target: 'output-1' },
    ],
  },
}

export const mockPipelineArtifacts: PipelineArtifactRecord[] = [
  {
    artifact_id: 'art-001',
    pipeline_id: 'pl-001',
    job_id: 'job-001',
    mode: 'build',
    status: 'completed',
    created_at: '2024-02-15T10:00:00Z',
  },
  {
    artifact_id: 'art-002',
    pipeline_id: 'pl-001',
    job_id: 'job-002',
    mode: 'deploy',
    status: 'completed',
    created_at: '2024-02-15T11:00:00Z',
  },
]

// === Ontology - Action Types ===
export const mockActionTypes: ActionType[] = [
  // Financial Actions
  {
    id: 'action-transfer',
    name: '계좌 이체',
    description: '한 계좌에서 다른 계좌로 자금을 이체합니다',
    targetClass: 'Account',
    parameters: [
      { name: 'fromAccount', label: '출금 계좌', type: 'instance', required: true, instanceClassId: 'class-account' },
      { name: 'toAccount', label: '입금 계좌', type: 'instance', required: true, instanceClassId: 'class-account' },
      { name: 'amount', label: '이체 금액', type: 'number', required: true },
      { name: 'currency', label: '통화', type: 'select', required: true, options: [{ value: 'KRW', label: '원화 (KRW)' }, { value: 'USD', label: '달러 (USD)' }, { value: 'EUR', label: '유로 (EUR)' }] },
    ],
  },
  {
    id: 'action-freeze-account',
    name: '계좌 동결',
    description: '의심 계좌를 동결하여 거래를 차단합니다',
    targetClass: 'Account',
    parameters: [
      { name: 'accountId', label: '대상 계좌', type: 'instance', required: true, instanceClassId: 'class-account' },
      { name: 'reason', label: '동결 사유', type: 'string', required: true },
    ],
  },
  // Status Actions
  {
    id: 'action-status-change',
    name: '상태 변경',
    description: '객체의 상태를 변경합니다',
    parameters: [
      { name: 'entityId', label: '대상 엔티티', type: 'string', required: true },
      { name: 'newStatus', label: '새 상태', type: 'select', required: true, options: [{ value: 'active', label: '활성' }, { value: 'inactive', label: '비활성' }, { value: 'suspended', label: '정지' }] },
    ],
  },
  {
    id: 'action-update-risk-score',
    name: '리스크 스코어 업데이트',
    description: '고객/거래의 리스크 스코어를 수동 조정합니다',
    targetClass: 'Customer',
    parameters: [
      { name: 'customerId', label: '대상 고객', type: 'instance', required: true, instanceClassId: 'class-customer' },
      { name: 'newScore', label: '새 리스크 스코어', type: 'number', required: true },
      { name: 'reason', label: '변경 사유', type: 'string', required: true },
    ],
  },
  // Relationship Actions
  {
    id: 'action-link-create',
    name: '관계 생성',
    description: '두 객체 간의 관계를 생성합니다',
    parameters: [
      { name: 'sourceId', label: '소스 엔티티', type: 'string', required: true },
      { name: 'targetId', label: '타겟 엔티티', type: 'string', required: true },
      { name: 'linkType', label: '관계 유형', type: 'string', required: true },
    ],
  },
  {
    id: 'action-link-delete',
    name: '관계 삭제',
    description: '두 객체 간의 관계를 삭제합니다',
    parameters: [
      { name: 'linkId', label: '관계 ID', type: 'string', required: true },
    ],
  },
  // Alert Actions
  {
    id: 'action-assign-alert',
    name: '알림 할당',
    description: '알림을 담당자에게 할당합니다',
    targetClass: 'Alert',
    parameters: [
      { name: 'alertId', label: '알림', type: 'instance', required: true, instanceClassId: 'class-alert' },
      { name: 'assignee', label: '담당자', type: 'instance', required: true, instanceClassId: 'class-employee' },
    ],
  },
  {
    id: 'action-resolve-alert',
    name: '알림 해결',
    description: '알림을 해결됨으로 표시합니다',
    targetClass: 'Alert',
    parameters: [
      { name: 'alertId', label: '알림', type: 'instance', required: true, instanceClassId: 'class-alert' },
      { name: 'resolution', label: '해결 내용', type: 'string', required: true },
    ],
  },
  {
    id: 'action-escalate-alert',
    name: '알림 상향 보고',
    description: '알림을 상위 담당자에게 에스컬레이션합니다',
    targetClass: 'Alert',
    parameters: [
      { name: 'alertId', label: '알림', type: 'instance', required: true, instanceClassId: 'class-alert' },
      { name: 'escalateTo', label: '상향 보고 대상', type: 'string', required: true },
      { name: 'priority', label: '우선순위', type: 'select', required: true, options: [{ value: 'High', label: '높음' }, { value: 'Critical', label: '긴급' }] },
    ],
  },
  // Supply Chain Actions
  {
    id: 'action-update-inventory',
    name: '재고 업데이트',
    description: '제품 재고 수량을 수동으로 조정합니다',
    targetClass: 'Product',
    parameters: [
      { name: 'productId', label: '제품', type: 'instance', required: true, instanceClassId: 'class-product' },
      { name: 'adjustment', label: '조정 수량', type: 'number', required: true },
      { name: 'reason', label: '조정 사유', type: 'string', required: true },
    ],
  },
  {
    id: 'action-update-shipment-status',
    name: '배송 상태 업데이트',
    description: '배송 상태를 수동으로 변경합니다',
    targetClass: 'Shipment',
    parameters: [
      { name: 'shipmentId', label: '배송', type: 'instance', required: true, instanceClassId: 'class-shipment' },
      { name: 'newStatus', label: '새 상태', type: 'select', required: true, options: [{ value: 'Pending', label: '대기' }, { value: 'In Transit', label: '배송중' }, { value: 'Delivered', label: '배송완료' }, { value: 'Delayed', label: '지연' }] },
    ],
  },
  // Notification Actions
  {
    id: 'action-notify',
    name: '알림 발송',
    description: '사용자에게 알림을 발송합니다',
    parameters: [
      { name: 'recipient', label: '수신자', type: 'instance', required: true, instanceClassId: 'class-employee' },
      { name: 'message', label: '메시지', type: 'string', required: true },
      { name: 'channel', label: '채널', type: 'select', required: false, options: [{ value: 'email', label: '이메일' }, { value: 'slack', label: 'Slack' }, { value: 'sms', label: 'SMS' }], defaultValue: 'email' },
    ],
  },
]

export const mockActionTypeDetail: ActionTypeDetail = {
  id: 'action-transfer',
  name: '계좌 이체',
  description: '한 계좌에서 다른 계좌로 자금을 이체합니다',
  targetClass: 'Account',
  parameters: [
    { name: 'fromAccount', label: '출금 계좌', type: 'instance', required: true, instanceClassId: 'class-account', description: '자금이 출금될 계좌' },
    { name: 'toAccount', label: '입금 계좌', type: 'instance', required: true, instanceClassId: 'class-account', description: '자금이 입금될 계좌' },
    { name: 'amount', label: '이체 금액', type: 'number', required: true, description: '이체할 금액' },
    { name: 'currency', label: '통화', type: 'select', required: true, options: [{ value: 'KRW', label: '원화 (KRW)' }, { value: 'USD', label: '달러 (USD)' }, { value: 'EUR', label: '유로 (EUR)' }], description: '통화 단위' },
    { name: 'memo', label: '메모', type: 'string', required: false, description: '이체 관련 메모' },
  ],
  examples: [
    { description: '원자재 대금 이체', params: { fromAccount: 'ACC-10001', toAccount: 'ACC-10002', amount: 50000000, currency: 'KRW', memo: '원자재 대금' } },
  ],
}

export const mockActionLogs: ActionLog[] = [
  {
    id: 'log-001',
    actionTypeId: 'action-transfer',
    actionTypeName: '계좌 이체',
    status: 'success',
    executedAt: '2024-02-21T14:30:00Z',
    executedBy: '김재무',
    params: { fromAccount: 'ACC-10001', toAccount: 'ACC-10015', amount: 125000000, currency: 'KRW' },
  },
  {
    id: 'log-002',
    actionTypeId: 'action-assign-alert',
    actionTypeName: '알림 할당',
    status: 'success',
    executedAt: '2024-02-21T10:15:00Z',
    executedBy: '이준수',
    params: { alertId: 'ALT-2024-001', assignee: 'AML Team' },
  },
  {
    id: 'log-003',
    actionTypeId: 'action-freeze-account',
    actionTypeName: '계좌 동결',
    status: 'success',
    executedAt: '2024-02-21T03:00:00Z',
    executedBy: '김보안',
    params: { accountId: 'ACC-10019', reason: 'Suspicious offshore transfer' },
  },
  {
    id: 'log-004',
    actionTypeId: 'action-update-inventory',
    actionTypeName: '재고 업데이트',
    status: 'success',
    executedAt: '2024-02-20T16:45:00Z',
    executedBy: '박배터리',
    params: { productId: 'PRD-002', adjustment: -50, reason: '불량품 폐기' },
  },
  {
    id: 'log-005',
    actionTypeId: 'action-transfer',
    actionTypeName: '계좌 이체',
    status: 'failed',
    executedAt: '2024-02-20T11:00:00Z',
    executedBy: '정구매',
    params: { fromAccount: 'ACC-10020', toAccount: 'ACC-10008', amount: 500000000, currency: 'KRW' },
    result: { error: '신용 한도 초과 - 계좌 ACC-10020 일시 정지 상태' },
  },
  {
    id: 'log-006',
    actionTypeId: 'action-escalate-alert',
    actionTypeName: '알림 상향 보고',
    status: 'success',
    executedAt: '2024-02-21T09:30:00Z',
    executedBy: 'AML Team',
    params: { alertId: 'ALT-2024-010', escalateTo: 'Security', priority: 'Critical' },
  },
]

// === Ontology - Object Types (CSV 스키마 기반) ===
export const mockOntologyClasses: OntologyClass[] = [
  // Enterprise Analytics
  {
    id: 'class-customer',
    label: 'Customer',
    description: '글로벌 고객 마스터 (KYC, 리스크 스코어 포함)',
    propertyCount: 8,
    instanceCount: 15,
  },
  {
    id: 'class-account',
    label: 'Account',
    description: '금융 계좌 정보',
    propertyCount: 7,
    instanceCount: 20,
  },
  {
    id: 'class-transaction',
    label: 'Transaction',
    description: '금융 거래 내역 (이상 탐지 플래그 포함)',
    propertyCount: 8,
    instanceCount: 25,
  },
  {
    id: 'class-employee',
    label: 'Employee',
    description: '직원 마스터 (조직 구조 포함)',
    propertyCount: 8,
    instanceCount: 20,
  },
  // Supply Chain
  {
    id: 'class-vendor',
    label: 'Vendor',
    description: '글로벌 공급업체',
    propertyCount: 7,
    instanceCount: 15,
  },
  {
    id: 'class-facility',
    label: 'Facility',
    description: '글로벌 시설/창고/공장',
    propertyCount: 7,
    instanceCount: 15,
  },
  {
    id: 'class-product',
    label: 'Product',
    description: '제품/부품 카탈로그',
    propertyCount: 8,
    instanceCount: 20,
  },
  {
    id: 'class-order',
    label: 'Order',
    description: 'B2B 주문',
    propertyCount: 7,
    instanceCount: 15,
  },
  {
    id: 'class-shipment',
    label: 'Shipment',
    description: '배송/물류 추적',
    propertyCount: 7,
    instanceCount: 10,
  },
  // Fraud Detection
  {
    id: 'class-alert',
    label: 'Alert',
    description: '이상 탐지 알림 및 위험 이벤트',
    propertyCount: 8,
    instanceCount: 15,
  },
]

// === Ontology - Link Types ===
export const mockLinkTypes: LinkType[] = [
  // Customer-Account 관계
  {
    id: 'link-owns-account',
    name: 'owns_account',
    predicate: 'owns',
    sourceClassId: 'class-customer',
    sourceClassName: 'Customer',
    targetClassId: 'class-account',
    targetClassName: 'Account',
    cardinality: 'one-to-many',
    description: '고객이 계좌를 소유',
  },
  // Transaction 관계
  {
    id: 'link-transfers-from',
    name: 'transfers_from',
    predicate: 'from',
    sourceClassId: 'class-transaction',
    sourceClassName: 'Transaction',
    targetClassId: 'class-account',
    targetClassName: 'Account',
    cardinality: 'many-to-many',
    description: '계좌에서 출금',
  },
  {
    id: 'link-transfers-to',
    name: 'transfers_to',
    predicate: 'to',
    sourceClassId: 'class-transaction',
    sourceClassName: 'Transaction',
    targetClassId: 'class-account',
    targetClassName: 'Account',
    cardinality: 'many-to-many',
    description: '계좌로 입금',
  },
  // Employee 관계
  {
    id: 'link-manages',
    name: 'manages',
    predicate: 'manages',
    sourceClassId: 'class-employee',
    sourceClassName: 'Employee',
    targetClassId: 'class-employee',
    targetClassName: 'Employee',
    cardinality: 'one-to-many',
    description: '직원이 다른 직원을 관리',
  },
  {
    id: 'link-works-at',
    name: 'works_at',
    predicate: 'worksAt',
    sourceClassId: 'class-employee',
    sourceClassName: 'Employee',
    targetClassId: 'class-facility',
    targetClassName: 'Facility',
    cardinality: 'many-to-many',
    description: '직원이 시설에서 근무',
  },
  // Supply Chain 관계
  {
    id: 'link-supplies',
    name: 'supplies',
    predicate: 'supplies',
    sourceClassId: 'class-vendor',
    sourceClassName: 'Vendor',
    targetClassId: 'class-product',
    targetClassName: 'Product',
    cardinality: 'one-to-many',
    description: '공급업체가 제품을 공급',
  },
  {
    id: 'link-orders',
    name: 'orders',
    predicate: 'placedBy',
    sourceClassId: 'class-order',
    sourceClassName: 'Order',
    targetClassId: 'class-customer',
    targetClassName: 'Customer',
    cardinality: 'many-to-many',
    description: '고객이 제품을 주문',
  },
  {
    id: 'link-contains-product',
    name: 'contains_product',
    predicate: 'contains',
    sourceClassId: 'class-order',
    sourceClassName: 'Order',
    targetClassId: 'class-product',
    targetClassName: 'Product',
    cardinality: 'many-to-many',
    description: '주문에 제품이 포함',
  },
  {
    id: 'link-ships',
    name: 'ships',
    predicate: 'fulfills',
    sourceClassId: 'class-shipment',
    sourceClassName: 'Shipment',
    targetClassId: 'class-order',
    targetClassName: 'Order',
    cardinality: 'one-to-one',
    description: '주문이 배송됨',
  },
  {
    id: 'link-origin',
    name: 'ships_from',
    predicate: 'originatesFrom',
    sourceClassId: 'class-shipment',
    sourceClassName: 'Shipment',
    targetClassId: 'class-facility',
    targetClassName: 'Facility',
    cardinality: 'many-to-many',
    description: '배송이 시설에서 출발',
  },
  {
    id: 'link-destination',
    name: 'ships_to',
    predicate: 'destinedTo',
    sourceClassId: 'class-shipment',
    sourceClassName: 'Shipment',
    targetClassId: 'class-facility',
    targetClassName: 'Facility',
    cardinality: 'many-to-many',
    description: '배송이 시설로 도착',
  },
  // Alert 관계
  {
    id: 'link-alert-entity',
    name: 'alerts_on',
    predicate: 'alertsOn',
    sourceClassId: 'class-alert',
    sourceClassName: 'Alert',
    targetClassId: 'class-transaction',
    targetClassName: 'Transaction',
    cardinality: 'many-to-many',
    description: '알림이 엔티티에 대해 발생',
  },
  {
    id: 'link-assigned-to',
    name: 'assigned_to',
    predicate: 'assignedTo',
    sourceClassId: 'class-alert',
    sourceClassName: 'Alert',
    targetClassId: 'class-employee',
    targetClassName: 'Employee',
    cardinality: 'many-to-many',
    description: '알림이 직원에게 할당',
  },
]

// === UDF ===
export const mockUdfs: UdfRecord[] = [
  // Enterprise Analytics UDFs
  {
    udf_id: 'udf-001',
    db_name: 'enterprise-analytics',
    name: 'calculate_risk_score',
    description: '거래 패턴 기반 리스크 스코어 계산',
    latest_version: 3,
    created_at: '2024-01-15T00:00:00Z',
    updated_at: '2024-02-10T00:00:00Z',
    code: `def transform(row):
    amount = float(row.get("amount") or 0)
    risk_flag = row.get("risk_flag", False)
    base_score = min(amount / 10000, 50)
    flag_penalty = 30 if risk_flag else 0
    return {**row, "risk_score": base_score + flag_penalty}`,
  },
  {
    udf_id: 'udf-002',
    db_name: 'enterprise-analytics',
    name: 'normalize_currency',
    description: '다중 통화를 USD 기준으로 정규화',
    latest_version: 2,
    created_at: '2024-02-01T00:00:00Z',
    updated_at: '2024-02-15T00:00:00Z',
    code: `def transform(row):
    rates = {"KRW": 0.00075, "EUR": 1.08, "CNY": 0.14, "JPY": 0.0067, "USD": 1.0}
    amount = float(row.get("amount") or 0)
    currency = row.get("currency", "USD")
    usd_amount = amount * rates.get(currency, 1.0)
    return {**row, "amount_usd": usd_amount}`,
  },
  // Supply Chain UDFs
  {
    udf_id: 'udf-003',
    db_name: 'supply-chain-mgmt',
    name: 'calculate_reorder_qty',
    description: '안전재고 기반 재주문 수량 계산',
    latest_version: 1,
    created_at: '2024-02-05T00:00:00Z',
    updated_at: '2024-02-05T00:00:00Z',
    code: `def transform(row):
    inventory = int(row.get("inventory_qty") or 0)
    reorder_point = int(row.get("reorder_point") or 0)
    lead_time = int(row.get("lead_time_days") or 7)
    safety_stock = reorder_point * 0.2
    reorder_qty = max(0, (reorder_point + safety_stock) - inventory) * (1 + lead_time / 30)
    return {**row, "suggested_reorder_qty": int(reorder_qty)}`,
  },
  {
    udf_id: 'udf-004',
    db_name: 'supply-chain-mgmt',
    name: 'classify_vendor_tier',
    description: '공급업체 등급 자동 분류',
    latest_version: 1,
    created_at: '2024-02-08T00:00:00Z',
    updated_at: '2024-02-08T00:00:00Z',
    code: `def transform(row):
    rating = float(row.get("rating") or 0)
    volume = float(row.get("annual_volume") or 0)
    if rating >= 4.5 and volume >= 1000000:
        tier = "Strategic"
    elif rating >= 4.0 and volume >= 500000:
        tier = "Preferred"
    elif rating >= 3.5:
        tier = "Approved"
    else:
        tier = "Conditional"
    return {**row, "vendor_tier": tier}`,
  },
  // Fraud Detection UDFs
  {
    udf_id: 'udf-005',
    db_name: 'fraud-detection',
    name: 'detect_velocity_anomaly',
    description: '거래 속도 이상 탐지',
    latest_version: 2,
    created_at: '2024-02-10T00:00:00Z',
    updated_at: '2024-02-20T00:00:00Z',
    code: `def transform(row):
    txn_count_1h = int(row.get("txn_count_1h") or 0)
    avg_txn_count = float(row.get("avg_txn_count_1h") or 5)
    velocity_ratio = txn_count_1h / max(avg_txn_count, 1)
    is_anomaly = velocity_ratio > 3.0
    return {**row, "velocity_ratio": velocity_ratio, "velocity_anomaly": is_anomaly}`,
  },
]

// === Preview Data (Sample - Customers) ===
export const mockPreviewData = {
  columns: [
    { name: 'customer_id', type: 'string' },
    { name: 'name', type: 'string' },
    { name: 'tier', type: 'string' },
    { name: 'country', type: 'string' },
    { name: 'risk_score', type: 'number' },
    { name: 'total_spent', type: 'number' },
  ],
  rows: [
    { customer_id: 'CUST-001', name: '삼성전자', tier: 'Enterprise', country: 'Korea', risk_score: 15, total_spent: 285000000000 },
    { customer_id: 'CUST-002', name: '현대자동차', tier: 'Enterprise', country: 'Korea', risk_score: 18, total_spent: 195000000000 },
    { customer_id: 'CUST-003', name: 'LG화학', tier: 'Enterprise', country: 'Korea', risk_score: 22, total_spent: 156000000000 },
    { customer_id: 'CUST-004', name: 'SK하이닉스', tier: 'Enterprise', country: 'Korea', risk_score: 12, total_spent: 178000000000 },
    { customer_id: 'CUST-005', name: 'CATL', tier: 'Enterprise', country: 'China', risk_score: 35, total_spent: 89000000000 },
  ],
}

// === Simulation Result ===
export const mockSimulationResult = {
  success: true,
  changes: [
    { entityId: 'ACC-10001', entityLabel: '운영 계좌 (KRW)', field: 'balance', before: 15000000000, after: 14875000000 },
    { entityId: 'ACC-10015', entityLabel: '공급업체 결제 계좌', field: 'balance', before: 2500000000, after: 2625000000 },
    { entityId: 'TXN-NEW', entityLabel: '신규 거래', field: 'status', before: null, after: 'pending' },
  ],
  warnings: [
    '출금 계좌(ACC-10001)의 일일 이체 한도 85% 도달',
  ],
  errors: [],
}

// === Helper: Delay for realistic feel ===
export const mockDelay = (ms: number = 300) => new Promise(resolve => setTimeout(resolve, ms))

// === Mock Raw File Content ===
const mockRawContentMap: Record<string, string> = {
  'ds-customers': `customer_id,name,email,phone,tier,country,city,registration_date,last_activity,total_spent,risk_score
CUST-001,삼성전자,samsung@company.com,02-1234-5678,Enterprise,Korea,Seoul,2018-03-15,2024-02-20,285000000000,15
CUST-002,현대자동차,hyundai@company.com,02-2345-6789,Enterprise,Korea,Seoul,2017-06-01,2024-02-21,195000000000,18
CUST-003,LG화학,lgchem@company.com,02-3456-7890,Enterprise,Korea,Seoul,2019-01-10,2024-02-19,156000000000,22
CUST-004,SK하이닉스,skhynix@company.com,031-456-7890,Enterprise,Korea,Icheon,2016-09-20,2024-02-21,178000000000,12
CUST-005,CATL,catl@company.cn,+86-591-1234,Enterprise,China,Ningde,2020-03-01,2024-02-18,89000000000,35`,

  'ds-accounts': `account_id,customer_id,account_type,currency,balance,credit_limit,status,opened_date,branch_code
ACC-10001,CUST-001,Operating,KRW,15000000000,50000000000,Active,2018-03-20,SEL-001
ACC-10002,CUST-001,Settlement,USD,25000000,100000000,Active,2018-03-20,SEL-001
ACC-10003,CUST-002,Operating,KRW,8500000000,30000000000,Active,2017-06-15,SEL-002
ACC-10004,CUST-003,Operating,KRW,12000000000,40000000000,Active,2019-01-15,SEL-003
ACC-10005,CUST-004,Operating,KRW,9200000000,35000000000,Active,2016-10-01,ICH-001`,

  'ds-transactions': `transaction_id,from_account,to_account,amount,currency,transaction_type,status,timestamp,description,risk_flag,category
TXN-000001,ACC-10001,ACC-10015,125000000,KRW,Wire Transfer,Completed,2024-02-21T14:30:00Z,원자재 대금,false,Supplier Payment
TXN-000002,ACC-10003,ACC-10008,85000000,KRW,Wire Transfer,Completed,2024-02-21T10:15:00Z,부품 대금,false,Supplier Payment
TXN-000003,ACC-10004,ACC-10001,250000000,KRW,Wire Transfer,Completed,2024-02-20T16:00:00Z,반도체 납품대금,false,Customer Payment
TXN-000004,ACC-10002,ACC-EXT-001,5000000,USD,International Wire,Pending,2024-02-21T09:00:00Z,해외 설비 구매,true,Equipment
TXN-000005,ACC-10005,ACC-10012,180000000,KRW,Wire Transfer,Completed,2024-02-19T11:30:00Z,배터리 소재 대금,false,Supplier Payment`,

  'ds-employees': `employee_id,name,email,department,title,facility_id,manager_id,hire_date,salary,currency,status,clearance_level
EMP-001,김영수,kim.ys@company.com,Operations,Plant Manager,FAC-001,EMP-010,2015-03-15,125000000,KRW,Active,Level 4
EMP-002,박철호,park.ch@company.com,Operations,Mill Supervisor,FAC-002,EMP-010,2012-06-01,98000000,KRW,Active,Level 4
EMP-003,이민정,lee.mj@company.com,Logistics,Distribution Manager,FAC-003,EMP-011,2019-09-10,85000000,KRW,Active,Level 3
EMP-004,최항만,choi.hm@company.com,Logistics,Terminal Director,FAC-004,EMP-011,2010-01-01,115000000,KRW,Active,Level 4
EMP-005,정연구,jung.yg@company.com,R&D,Research Director,FAC-005,EMP-012,2018-11-20,135000000,KRW,Active,Level 5`,

  'ds-vendors': `vendor_id,name,country,city,category,contact_email,contact_phone,rating,contract_start,contract_end,payment_terms,risk_tier,annual_volume
VND-001,POSCO,Korea,Pohang,Steel,sales@posco.com,054-123-4567,4.8,2020-01-01,2025-12-31,Net 30,Low,850000000000
VND-002,Albemarle,USA,Charlotte,Chemicals,sales@albemarle.com,+1-704-123-4567,4.5,2021-03-01,2024-02-28,Net 45,Medium,125000000
VND-003,Ganfeng Lithium,China,Xinyu,Battery Materials,export@ganfenglithium.com,+86-790-123-4567,4.2,2022-06-01,2025-05-31,Net 60,Medium,95000000
VND-004,Contemporary Amperex,China,Ningde,Battery Cells,b2b@catl.com,+86-593-123-4567,4.6,2023-01-01,2025-12-31,Net 30,Low,280000000
VND-005,Umicore,Belgium,Brussels,Cathode Materials,sales@umicore.com,+32-2-123-4567,4.7,2021-09-01,2024-08-31,Net 45,Low,180000000`,

  'ds-facilities': `facility_id,name,type,country,city,address,capacity_units,current_utilization,manager,status,opened_date
FAC-001,포항제철소,Manufacturing,Korea,Pohang,포항시 남구 동해안로 6261,12000000,0.87,김영수,Operational,1973-07-03
FAC-002,광양제철소,Manufacturing,Korea,Gwangyang,광양시 금호동 700,15000000,0.82,박철호,Operational,1987-03-01
FAC-003,인천물류센터,Distribution,Korea,Incheon,인천시 중구 공항동로 295,500000,0.71,이민정,Operational,2015-06-15
FAC-004,부산신항터미널,Port Terminal,Korea,Busan,부산시 강서구 성북동 1234,2000000,0.65,최항만,Operational,2006-01-19
FAC-005,대전연구소,R&D Center,Korea,Daejeon,대전시 유성구 과학로 169,50000,0.45,정연구,Operational,2010-03-01`,

  'ds-products': `product_id,name,category,subcategory,unit_price,unit_cost,currency,inventory_qty,reorder_point,lead_time_days,vendor_id,status
PRD-001,리튬이온 배터리 셀,Battery,Cell,45000,32000,KRW,125000,50000,14,VND-004,Active
PRD-002,전기차 배터리팩,Battery,Pack,8500000,6200000,KRW,2500,1000,21,VND-004,Active
PRD-003,열연강판,Steel,Hot Rolled,890000,720000,KRW,45000,15000,7,VND-001,Active
PRD-004,냉연강판,Steel,Cold Rolled,1250000,980000,KRW,32000,10000,7,VND-001,Active
PRD-005,자동차용 강판,Steel,Automotive,2450000,1850000,KRW,18000,5000,10,VND-001,Active`,

  'ds-orders': `order_id,customer_id,product_id,quantity,unit_price,total_amount,currency,order_date,required_date,status,facility_id,priority
ORD-2024-0001,CUST-006,PRD-001,50000,45000,2250000000,KRW,2024-02-01,2024-03-15,Processing,FAC-007,High
ORD-2024-0002,CUST-007,PRD-002,500,8500000,4250000000,KRW,2024-02-03,2024-03-20,Confirmed,FAC-007,Critical
ORD-2024-0003,CUST-009,PRD-004,100000,125000,12500000000,KRW,2024-02-05,2024-04-01,Processing,FAC-001,High
ORD-2024-0004,CUST-013,PRD-005,2000,2450000,4900000000,KRW,2024-02-08,2024-03-25,Shipped,FAC-006,Normal
ORD-2024-0005,CUST-006,PRD-006,5000,680000,3400000000,KRW,2024-02-10,2024-03-10,Delivered,FAC-010,Normal`,

  'ds-shipments': `shipment_id,order_id,origin_facility,destination_facility,carrier,tracking_number,ship_date,estimated_arrival,actual_arrival,status,weight_kg,volume_cbm
SHP-2024-0001,ORD-2024-0004,FAC-006,FAC-010,Maersk,MAEU1234567,2024-02-20,2024-03-05,null,In Transit,45000,125
SHP-2024-0002,ORD-2024-0005,FAC-010,FAC-003,CJ Logistics,CJ9876543210,2024-02-25,2024-02-28,2024-02-27,Delivered,12500,45
SHP-2024-0003,ORD-2024-0008,FAC-002,FAC-011,DB Schenker,DB2024021501,2024-02-22,2024-03-08,null,In Transit,85000,280
SHP-2024-0004,ORD-2024-0001,FAC-007,FAC-003,Hanjin,HJ2024020101,2024-02-28,2024-03-02,null,Pending,22000,68
SHP-2024-0005,ORD-2024-0003,FAC-001,FAC-008,COSCO,COSU2024020501,2024-03-01,2024-03-15,null,Pending,35000,95`,

  'ds-alerts': `alert_id,alert_type,severity,entity_type,entity_id,title,description,created_at,status,assigned_to,resolved_at,resolution_notes
ALT-2024-001,Fraud Detection,Critical,Transaction,TXN-000019,Suspicious Wire Transfer,Large wire transfer to offshore account detected,2024-02-21T02:35:00Z,Open,AML Team,null,null
ALT-2024-002,Inventory,High,Product,PRD-016,Low Stock Alert,Lithium Carbonate inventory below reorder point,2024-02-20T08:00:00Z,In Progress,Supply Chain,null,null
ALT-2024-003,Credit Risk,High,Account,ACC-10020,Credit Limit Breach,Account suspended due to credit violations,2024-01-30T16:05:00Z,Resolved,Credit Team,2024-02-05T10:00:00Z,Payment plan established
ALT-2024-004,Supply Chain,Medium,Vendor,VND-004,Contract Expiring,Vendor contract expires in 7 days,2024-02-21T00:00:00Z,Open,Procurement,null,null
ALT-2024-005,Quality,High,Facility,FAC-007,Capacity Warning,Battery plant operating at 94% capacity,2024-02-19T14:00:00Z,In Progress,Operations,null,null`,

  'ds-suspicious-txn': `transaction_id,account_id,amount,risk_score,detection_reason,detected_at,model_version
TXN-S-001,ACC-10019,850000000,92,Unusual offshore destination + large amount,2024-02-21T02:30:00Z,v2.3.1
TXN-S-002,ACC-10008,125000000,78,Velocity anomaly - 5 transactions in 10 minutes,2024-02-20T16:35:00Z,v2.3.1
TXN-S-003,ACC-EXT-005,95000000,85,First-time recipient + round amount,2024-02-19T14:22:00Z,v2.3.1
TXN-S-004,ACC-10003,250000000,71,After-hours transaction + new beneficiary,2024-02-18T23:45:00Z,v2.3.1
TXN-S-005,ACC-10015,180000000,68,Geographic anomaly - unusual country,2024-02-17T09:15:00Z,v2.3.1`,

  'ds-risk-profiles': `customer_id,risk_tier,risk_score,avg_transaction_amount,transaction_frequency,unusual_pattern_count,last_reviewed
CUST-001,Low,15,125000000,45,0,2024-02-15T10:00:00Z
CUST-002,Low,18,85000000,38,1,2024-02-14T14:30:00Z
CUST-003,Medium,22,95000000,52,2,2024-02-10T09:00:00Z
CUST-004,Low,12,180000000,28,0,2024-02-12T16:00:00Z
CUST-005,High,35,250000000,15,5,2024-02-20T11:00:00Z`,

  'ds-investigations': `case_id,alert_ids,assigned_analyst,status,priority,opened_at,closed_at,outcome,notes
CASE-2024-001,"ALT-2024-001,ALT-2024-007",이준수,Open,Critical,2024-02-21T03:00:00Z,null,null,Offshore transfer investigation in progress
CASE-2024-002,ALT-2024-003,박신용,Closed,High,2024-01-31T09:00:00Z,2024-02-05T10:00:00Z,False Positive,Credit issue resolved with payment plan
CASE-2024-003,ALT-2024-010,김보안,Open,Critical,2024-02-21T03:30:00Z,null,null,R&D Center security breach under investigation
CASE-2024-004,"ALT-2024-011,ALT-2024-013",최규정,In Review,High,2024-02-18T10:00:00Z,null,null,Enhanced due diligence in progress
CASE-2024-005,ALT-2024-006,이준수,Closed,Medium,2024-02-15T10:00:00Z,2024-02-18T15:00:00Z,Escalated,KYC documents received and verified`,
}

export const getMockRawContent = (datasetId: string): string => {
  return mockRawContentMap[datasetId] || 'No data available for this dataset'
}
