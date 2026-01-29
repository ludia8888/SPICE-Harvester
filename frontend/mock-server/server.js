const express = require('express')
const cors = require('cors')
const fs = require('fs')
const path = require('path')
const { parse } = require('csv-parse/sync')

const app = express()
const DEFAULT_PORT = 3001
const PORT = (() => {
  const parsed = Number.parseInt(process.env.PORT || '', 10)
  return Number.isFinite(parsed) ? parsed : DEFAULT_PORT
})()

app.use(cors())
app.use(express.json())

// CSV 파일 로드 및 파싱 (raw 또는 clean)
const loadCSV = (filename, isClean = false) => {
  const subDir = isClean ? 'clean' : ''
  const filePath = path.join(__dirname, '..', 'mock-data', subDir, filename)
  if (!fs.existsSync(filePath)) {
    console.log(`[WARN] CSV file not found: ${filePath}`)
    return []
  }
  try {
    const content = fs.readFileSync(filePath, 'utf-8')
    return parse(content, {
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
      trim: true,
    })
  } catch (err) {
    console.error(`[ERROR] Failed to parse ${filename}:`, err.message)
    return []
  }
}

// Raw 데이터 로드
const customersRaw = loadCSV('customers.csv')
const accountsRaw = loadCSV('accounts.csv')
const transactionsRaw = loadCSV('transactions.csv')
const employeesRaw = loadCSV('employees.csv')
const vendorsRaw = loadCSV('vendors.csv')
const facilitiesRaw = loadCSV('facilities.csv')
const productsRaw = loadCSV('products.csv')
const ordersRaw = loadCSV('orders.csv')
const shipmentsRaw = loadCSV('shipments.csv')
const alertsRaw = loadCSV('alerts.csv')

// Clean 데이터 로드
const customersClean = loadCSV('customers.csv', true)
const accountsClean = loadCSV('accounts.csv', true)
const transactionsClean = loadCSV('transactions.csv', true)
const employeesClean = loadCSV('employees.csv', true)
const vendorsClean = loadCSV('vendors.csv', true)
const facilitiesClean = loadCSV('facilities.csv', true)
const productsClean = loadCSV('products.csv', true)
const ordersClean = loadCSV('orders.csv', true)
const shipmentsClean = loadCSV('shipments.csv', true)
const alertsClean = loadCSV('alerts.csv', true)

// === Databases ===
const databases = [
  {
    db_name: 'enterprise-analytics',
    name: 'Enterprise Analytics',
    description: 'Palantir Foundry 스타일 엔터프라이즈 분석 프로젝트',
    dataset_count: 8, // raw + clean
    created_at: '2024-01-01T00:00:00Z',
  },
  {
    db_name: 'supply-chain-mgmt',
    name: 'Supply Chain Management',
    description: '글로벌 공급망 관리 및 최적화',
    dataset_count: 10,
    created_at: '2024-01-15T00:00:00Z',
  },
  {
    db_name: 'fraud-detection',
    name: 'Fraud Detection',
    description: '금융 사기 탐지 및 AML 시스템',
    dataset_count: 2,
    created_at: '2024-02-01T00:00:00Z',
  },
]

// === Datasets (Raw + Clean) ===
const datasets = [
  // Enterprise Analytics - Raw
  { dataset_id: 'ds-customers', db_name: 'enterprise-analytics', name: 'Customers', description: '글로벌 고객 마스터 데이터 (원본)', source_type: 'csv', stage: 'raw', row_count: customersRaw.length, created_at: '2024-01-10T00:00:00Z' },
  { dataset_id: 'ds-accounts', db_name: 'enterprise-analytics', name: 'Accounts', description: '금융 계좌 마스터 데이터 (원본)', source_type: 'csv', stage: 'raw', row_count: accountsRaw.length, created_at: '2024-01-12T00:00:00Z' },
  { dataset_id: 'ds-transactions', db_name: 'enterprise-analytics', name: 'Transactions', description: '금융 거래 내역 (원본)', source_type: 'csv', stage: 'raw', row_count: transactionsRaw.length, created_at: '2024-01-15T00:00:00Z' },
  { dataset_id: 'ds-employees', db_name: 'enterprise-analytics', name: 'Employees', description: '직원 마스터 데이터 (원본)', source_type: 'csv', stage: 'raw', row_count: employeesRaw.length, created_at: '2024-01-20T00:00:00Z' },

  // Enterprise Analytics - Clean (cleansed)
  { dataset_id: 'ds-customers-clean', db_name: 'enterprise-analytics', name: 'Customers', description: '글로벌 고객 마스터 데이터 (정제됨)', source_type: 'csv', stage: 'clean', row_count: customersClean.length, created_at: '2024-01-11T00:00:00Z' },
  { dataset_id: 'ds-accounts-clean', db_name: 'enterprise-analytics', name: 'Accounts', description: '금융 계좌 마스터 데이터 (정제됨)', source_type: 'csv', stage: 'clean', row_count: accountsClean.length, created_at: '2024-01-13T00:00:00Z' },
  { dataset_id: 'ds-transactions-clean', db_name: 'enterprise-analytics', name: 'Transactions', description: '금융 거래 내역 (정제됨)', source_type: 'csv', stage: 'clean', row_count: transactionsClean.length, created_at: '2024-01-16T00:00:00Z' },
  { dataset_id: 'ds-employees-clean', db_name: 'enterprise-analytics', name: 'Employees', description: '직원 마스터 데이터 (정제됨)', source_type: 'csv', stage: 'clean', row_count: employeesClean.length, created_at: '2024-01-21T00:00:00Z' },

  // Supply Chain - Raw
  { dataset_id: 'ds-vendors', db_name: 'supply-chain-mgmt', name: 'Vendors', description: '글로벌 공급업체 마스터 데이터 (원본)', source_type: 'csv', stage: 'raw', row_count: vendorsRaw.length, created_at: '2024-01-25T00:00:00Z' },
  { dataset_id: 'ds-facilities', db_name: 'supply-chain-mgmt', name: 'Facilities', description: '글로벌 시설/창고/공장 마스터 데이터 (원본)', source_type: 'csv', stage: 'raw', row_count: facilitiesRaw.length, created_at: '2024-01-28T00:00:00Z' },
  { dataset_id: 'ds-products', db_name: 'supply-chain-mgmt', name: 'Products', description: '제품/부품 카탈로그 (원본)', source_type: 'csv', stage: 'raw', row_count: productsRaw.length, created_at: '2024-02-01T00:00:00Z' },
  { dataset_id: 'ds-orders', db_name: 'supply-chain-mgmt', name: 'Orders', description: 'B2B 주문 데이터 (원본)', source_type: 'csv', stage: 'raw', row_count: ordersRaw.length, created_at: '2024-02-05T00:00:00Z' },
  { dataset_id: 'ds-shipments', db_name: 'supply-chain-mgmt', name: 'Shipments', description: '배송/물류 추적 데이터 (원본)', source_type: 'csv', stage: 'raw', row_count: shipmentsRaw.length, created_at: '2024-02-08T00:00:00Z' },

  // Supply Chain - Clean
  { dataset_id: 'ds-vendors-clean', db_name: 'supply-chain-mgmt', name: 'Vendors', description: '글로벌 공급업체 마스터 데이터 (정제됨)', source_type: 'csv', stage: 'clean', row_count: vendorsClean.length, created_at: '2024-01-26T00:00:00Z' },
  { dataset_id: 'ds-facilities-clean', db_name: 'supply-chain-mgmt', name: 'Facilities', description: '글로벌 시설/창고/공장 마스터 데이터 (정제됨)', source_type: 'csv', stage: 'clean', row_count: facilitiesClean.length, created_at: '2024-01-29T00:00:00Z' },
  { dataset_id: 'ds-products-clean', db_name: 'supply-chain-mgmt', name: 'Products', description: '제품/부품 카탈로그 (정제됨)', source_type: 'csv', stage: 'clean', row_count: productsClean.length, created_at: '2024-02-02T00:00:00Z' },
  { dataset_id: 'ds-orders-clean', db_name: 'supply-chain-mgmt', name: 'Orders', description: 'B2B 주문 데이터 (정제됨)', source_type: 'csv', stage: 'clean', row_count: ordersClean.length, created_at: '2024-02-06T00:00:00Z' },
  { dataset_id: 'ds-shipments-clean', db_name: 'supply-chain-mgmt', name: 'Shipments', description: '배송/물류 추적 데이터 (정제됨)', source_type: 'csv', stage: 'clean', row_count: shipmentsClean.length, created_at: '2024-02-09T00:00:00Z' },

  // Fraud Detection
  { dataset_id: 'ds-alerts', db_name: 'fraud-detection', name: 'Alerts', description: '이상 탐지 알림 및 위험 이벤트 (원본)', source_type: 'csv', stage: 'raw', row_count: alertsRaw.length, created_at: '2024-02-10T00:00:00Z' },
  { dataset_id: 'ds-alerts-clean', db_name: 'fraud-detection', name: 'Alerts', description: '이상 탐지 알림 및 위험 이벤트 (정제됨)', source_type: 'csv', stage: 'clean', row_count: alertsClean.length, created_at: '2024-02-11T00:00:00Z' },
]

// 데이터셋 ID와 실제 데이터 매핑 (Raw)
const datasetDataMap = {
  // Raw
  'ds-customers': customersRaw,
  'ds-accounts': accountsRaw,
  'ds-transactions': transactionsRaw,
  'ds-employees': employeesRaw,
  'ds-vendors': vendorsRaw,
  'ds-facilities': facilitiesRaw,
  'ds-products': productsRaw,
  'ds-orders': ordersRaw,
  'ds-shipments': shipmentsRaw,
  'ds-alerts': alertsRaw,
  // Clean
  'ds-customers-clean': customersClean,
  'ds-accounts-clean': accountsClean,
  'ds-transactions-clean': transactionsClean,
  'ds-employees-clean': employeesClean,
  'ds-vendors-clean': vendorsClean,
  'ds-facilities-clean': facilitiesClean,
  'ds-products-clean': productsClean,
  'ds-orders-clean': ordersClean,
  'ds-shipments-clean': shipmentsClean,
  'ds-alerts-clean': alertsClean,
}

// Clean 데이터셋용 정확한 스키마 정의
const cleanSchemas = {
  'ds-customers-clean': {
    columns: [
      { name: 'customer_id', type: 'id' },
      { name: 'name', type: 'string' },
      { name: 'email', type: 'email' },
      { name: 'phone', type: 'phone' },
      { name: 'tier', type: 'enum' },
      { name: 'country', type: 'string' },
      { name: 'city', type: 'string' },
      { name: 'registration_date', type: 'date' },
      { name: 'last_activity', type: 'date' },
      { name: 'total_spent', type: 'integer' },
      { name: 'risk_score', type: 'float' },
    ]
  },
  'ds-accounts-clean': {
    columns: [
      { name: 'account_id', type: 'id' },
      { name: 'customer_id', type: 'foreign_key' },
      { name: 'account_type', type: 'enum' },
      { name: 'currency', type: 'string' },
      { name: 'balance', type: 'integer' },
      { name: 'credit_limit', type: 'integer' },
      { name: 'status', type: 'enum' },
      { name: 'opened_date', type: 'date' },
      { name: 'last_transaction_date', type: 'date' },
      { name: 'branch_code', type: 'string' },
    ]
  },
  'ds-transactions-clean': {
    columns: [
      { name: 'transaction_id', type: 'id' },
      { name: 'from_account', type: 'foreign_key' },
      { name: 'to_account', type: 'foreign_key' },
      { name: 'amount', type: 'integer' },
      { name: 'currency', type: 'string' },
      { name: 'transaction_type', type: 'enum' },
      { name: 'status', type: 'enum' },
      { name: 'timestamp', type: 'datetime' },
      { name: 'description', type: 'string' },
      { name: 'risk_flag', type: 'boolean' },
      { name: 'category', type: 'enum' },
    ]
  },
  'ds-employees-clean': {
    columns: [
      { name: 'employee_id', type: 'id' },
      { name: 'name', type: 'string' },
      { name: 'email', type: 'email' },
      { name: 'department', type: 'enum' },
      { name: 'position', type: 'string' },
      { name: 'hire_date', type: 'date' },
      { name: 'salary', type: 'integer' },
      { name: 'manager_id', type: 'foreign_key' },
      { name: 'status', type: 'enum' },
    ]
  },
  'ds-vendors-clean': {
    columns: [
      { name: 'vendor_id', type: 'id' },
      { name: 'name', type: 'string' },
      { name: 'country', type: 'string' },
      { name: 'contact_email', type: 'email' },
      { name: 'contact_phone', type: 'phone' },
      { name: 'category', type: 'enum' },
      { name: 'rating', type: 'float' },
      { name: 'contract_start', type: 'date' },
      { name: 'contract_end', type: 'date' },
      { name: 'status', type: 'enum' },
    ]
  },
  'ds-products-clean': {
    columns: [
      { name: 'product_id', type: 'id' },
      { name: 'name', type: 'string' },
      { name: 'category', type: 'enum' },
      { name: 'price', type: 'float' },
      { name: 'cost', type: 'float' },
      { name: 'stock_quantity', type: 'integer' },
      { name: 'vendor_id', type: 'foreign_key' },
      { name: 'created_at', type: 'datetime' },
      { name: 'updated_at', type: 'datetime' },
      { name: 'is_active', type: 'boolean' },
    ]
  },
  'ds-orders-clean': {
    columns: [
      { name: 'order_id', type: 'id' },
      { name: 'customer_id', type: 'foreign_key' },
      { name: 'order_date', type: 'datetime' },
      { name: 'total_amount', type: 'float' },
      { name: 'currency', type: 'string' },
      { name: 'status', type: 'enum' },
      { name: 'shipping_address', type: 'string' },
      { name: 'payment_method', type: 'enum' },
      { name: 'fulfilled_at', type: 'datetime' },
    ]
  },
  'ds-shipments-clean': {
    columns: [
      { name: 'shipment_id', type: 'id' },
      { name: 'order_id', type: 'foreign_key' },
      { name: 'carrier', type: 'string' },
      { name: 'tracking_number', type: 'string' },
      { name: 'shipped_date', type: 'datetime' },
      { name: 'delivered_date', type: 'datetime' },
      { name: 'status', type: 'enum' },
      { name: 'weight_kg', type: 'float' },
    ]
  },
  'ds-facilities-clean': {
    columns: [
      { name: 'facility_id', type: 'id' },
      { name: 'name', type: 'string' },
      { name: 'type', type: 'enum' },
      { name: 'country', type: 'string' },
      { name: 'city', type: 'string' },
      { name: 'address', type: 'string' },
      { name: 'capacity', type: 'integer' },
      { name: 'utilization_rate', type: 'float' },
      { name: 'manager_id', type: 'foreign_key' },
      { name: 'is_operational', type: 'boolean' },
    ]
  },
  'ds-alerts-clean': {
    columns: [
      { name: 'alert_id', type: 'id' },
      { name: 'alert_type', type: 'enum' },
      { name: 'severity', type: 'enum' },
      { name: 'source_entity', type: 'string' },
      { name: 'source_id', type: 'foreign_key' },
      { name: 'message', type: 'string' },
      { name: 'created_at', type: 'datetime' },
      { name: 'resolved_at', type: 'datetime' },
      { name: 'is_resolved', type: 'boolean' },
      { name: 'assigned_to', type: 'foreign_key' },
    ]
  },
}

// 스키마 자동 생성 (Raw 데이터용)
const inferSchema = (rows) => {
  if (rows.length === 0) return { columns: [] }
  const firstRow = rows[0]
  const columns = Object.keys(firstRow).map(name => {
    const value = firstRow[name]
    let type = 'string'
    if (!isNaN(Number(value)) && value !== '') type = 'number'
    else if (value === 'true' || value === 'false') type = 'boolean'
    else if (/^\d{4}-\d{2}-\d{2}/.test(value)) type = 'date'
    return { name, type }
  })
  return { columns }
}

// =====================================================
// Transform Pipeline Preview Data Generators
// =====================================================

// 유틸: 문자열 정규화 (trim, empty to null 시뮬레이션)
const normalizeValue = (val) => {
  if (val === null || val === undefined || val === '') return null
  if (typeof val === 'string') return val.trim()
  return val
}

// 유틸: 이메일 소문자 변환
const normalizeEmail = (val) => {
  if (!val || typeof val !== 'string') return val
  return val.toLowerCase().trim()
}

// 유틸: 전화번호 E.164 형식
const normalizePhone = (val) => {
  if (!val || typeof val !== 'string') return val
  const digits = val.replace(/\D/g, '')
  if (digits.length === 10) return `+1${digits}`
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`
  return `+${digits}`
}

// 유틸: 안전한 날짜 변환
const safeToDateString = (val) => {
  if (!val) return null
  try {
    const date = new Date(val)
    if (isNaN(date.getTime())) return val // 유효하지 않은 날짜는 원본 반환
    return date.toISOString().split('T')[0]
  } catch {
    return val
  }
}

const safeToISOString = (val) => {
  if (!val) return null
  try {
    const date = new Date(val)
    if (isNaN(date.getTime())) return val
    return date.toISOString()
  } catch {
    return val
  }
}

// ===== ACCOUNTS PIPELINE TRANSFORMS =====
const accountsTransforms = {
  // Step 1: Normalize
  'ds-accounts-normalize': () => {
    return accountsRaw.map(row => ({
      ...row,
      account_type: normalizeValue(row.account_type),
      currency: normalizeValue(row.currency)?.toUpperCase(),
      status: normalizeValue(row.status),
      branch_code: normalizeValue(row.branch_code)?.toUpperCase(),
    }))
  },
  // Step 2: Cast Types
  'ds-accounts-cast': () => {
    const normalized = accountsTransforms['ds-accounts-normalize']()
    return normalized.map(row => ({
      ...row,
      balance: parseFloat(row.balance) || 0,
      credit_limit: parseInt(row.credit_limit) || 0,
      opened_date: safeToDateString(row.opened_date),
    }))
  },
  // Step 3: Filter (Active Only)
  'ds-accounts-filter': () => {
    const casted = accountsTransforms['ds-accounts-cast']()
    return casted.filter(row => row.status === 'Active')
  },
  // Step 4: Select Columns
  'ds-accounts-select': () => {
    const filtered = accountsTransforms['ds-accounts-filter']()
    return filtered.map(({ account_id, customer_id, account_type, balance, status, currency }) => ({
      account_id, customer_id, account_type, balance, status, currency
    }))
  },
  // Step 5: Canonical Output
  'ds-accounts-canonical': () => accountsTransforms['ds-accounts-select'](),
}

// ===== CUSTOMERS PIPELINE TRANSFORMS =====
const customersTransforms = {
  // Step 1: Normalize
  'ds-customers-normalize': () => {
    return customersRaw.map(row => ({
      ...row,
      name: normalizeValue(row.name),
      email: normalizeEmail(row.email),
      phone: normalizePhone(row.phone),
      tier: normalizeValue(row.tier),
      country: normalizeValue(row.country),
      city: normalizeValue(row.city),
    }))
  },
  // Step 2: Dedupe (by customer_id, keep first)
  'ds-customers-dedupe': () => {
    const normalized = customersTransforms['ds-customers-normalize']()
    const seen = new Set()
    return normalized.filter(row => {
      if (seen.has(row.customer_id)) return false
      seen.add(row.customer_id)
      return true
    })
  },
  // Step 3: Cast Types
  'ds-customers-cast': () => {
    const deduped = customersTransforms['ds-customers-dedupe']()
    return deduped.map(row => ({
      ...row,
      tier: ['Bronze', 'Silver', 'Gold', 'Platinum'].includes(row.tier) ? row.tier : 'Bronze',
      registration_date: safeToDateString(row.registration_date),
      total_spent: parseInt(row.total_spent) || 0,
      risk_score: parseFloat(row.risk_score) || 0.0,
    }))
  },
  // Step 4: Select Columns
  'ds-customers-select': () => {
    const casted = customersTransforms['ds-customers-cast']()
    return casted.map(({ customer_id, name, email, phone, tier, country }) => ({
      customer_id, name, email, phone, tier, country
    }))
  },
  // Step 5: Canonical Output
  'ds-customers-canonical': () => customersTransforms['ds-customers-select'](),
}

// ===== PRODUCTS PIPELINE TRANSFORMS =====
const productsTransforms = {
  // Step 1: Normalize
  'ds-products-normalize': () => {
    return productsRaw.map(row => ({
      ...row,
      name: normalizeValue(row.name),
      category: normalizeValue(row.category)?.replace(/^\w/, c => c.toUpperCase()),
      sku: normalizeValue(row.sku)?.toUpperCase(),
    }))
  },
  // Step 2: Cast Types
  'ds-products-cast': () => {
    const normalized = productsTransforms['ds-products-normalize']()
    return normalized.map(row => ({
      ...row,
      price: parseFloat(row.price)?.toFixed(2) || '0.00',
      cost: parseFloat(row.cost)?.toFixed(2) || '0.00',
      stock_quantity: parseInt(row.stock_quantity) || 0,
    }))
  },
  // Step 3: Filter (In Stock)
  'ds-products-filter': () => {
    const casted = productsTransforms['ds-products-cast']()
    return casted.filter(row => parseInt(row.stock_quantity) > 0)
  },
  // Step 4: Select Columns
  'ds-products-select': () => {
    const filtered = productsTransforms['ds-products-filter']()
    return filtered.map(({ product_id, name, category, price, stock_quantity }) => ({
      product_id, name, category, price, stock_quantity
    }))
  },
  // Step 5: Canonical Output
  'ds-products-canonical': () => productsTransforms['ds-products-select'](),
}

// ===== ORDERS PIPELINE TRANSFORMS =====
const ordersTransforms = {
  // Step 1: Filter (Valid Orders)
  'ds-orders-filter': () => {
    return ordersRaw.filter(row => row.status !== 'Cancelled')
  },
  // Step 2: Cast Types
  'ds-orders-cast': () => {
    const filtered = ordersTransforms['ds-orders-filter']()
    return filtered.map(row => ({
      ...row,
      order_date: safeToISOString(row.order_date),
      total_amount: parseFloat(row.total_amount)?.toFixed(2) || '0.00',
      quantity: parseInt(row.quantity) || 1,
      unit_price: parseFloat(row.unit_price)?.toFixed(2) || '0.00',
    }))
  },
  // Step 3: Compute (line_total)
  'ds-orders-compute': () => {
    const casted = ordersTransforms['ds-orders-cast']()
    return casted.map(row => ({
      ...row,
      line_total: (parseFloat(row.quantity) * parseFloat(row.unit_price)).toFixed(2),
    }))
  },
  // Step 4: Select Columns
  'ds-orders-select': () => {
    const computed = ordersTransforms['ds-orders-compute']()
    return computed.map(({ order_id, customer_id, product_id, total_amount, status, line_total }) => ({
      order_id, customer_id, product_id, total_amount, status, line_total
    }))
  },
  // Step 5: Canonical Output
  'ds-orders-canonical': () => ordersTransforms['ds-orders-select'](),
}

// ===== TRANSACTIONS PIPELINE TRANSFORMS =====
const transactionsTransforms = {
  // Step 1: Filter (Completed Only)
  'ds-transactions-filter': () => {
    return transactionsRaw.filter(row => row.status === 'Completed')
  },
  // Step 2: Normalize
  'ds-transactions-normalize': () => {
    const filtered = transactionsTransforms['ds-transactions-filter']()
    return filtered.map(row => ({
      ...row,
      currency: normalizeValue(row.currency)?.toUpperCase(),
      transaction_type: normalizeValue(row.transaction_type),
      description: normalizeValue(row.description),
    }))
  },
  // Step 3: Cast Types
  'ds-transactions-cast': () => {
    const normalized = transactionsTransforms['ds-transactions-normalize']()
    return normalized.map(row => ({
      ...row,
      amount: parseFloat(row.amount)?.toFixed(2) || '0.00',
      timestamp: safeToISOString(row.timestamp),
      risk_flag: row.risk_flag === 'true' || row.risk_flag === true,
    }))
  },
  // Step 4: Select Columns
  'ds-transactions-select': () => {
    const casted = transactionsTransforms['ds-transactions-cast']()
    return casted.map(({ transaction_id, from_account, to_account, amount, transaction_type, timestamp }) => ({
      transaction_id, from_account, to_account, amount, transaction_type, timestamp
    }))
  },
  // Step 5: Canonical Output
  'ds-transactions-canonical': () => transactionsTransforms['ds-transactions-select'](),
}

// ===== JOIN PIPELINE TRANSFORMS =====
const joinTransforms = {
  // Customer 360: Join Customers + Accounts
  'join-customer-360': () => {
    const customers = customersTransforms['ds-customers-canonical']()
    const accounts = accountsTransforms['ds-accounts-canonical']()
    return customers.map(cust => {
      const custAccounts = accounts.filter(acc => acc.customer_id === cust.customer_id)
      return {
        ...cust,
        account_count: custAccounts.length,
        total_balance: custAccounts.reduce((sum, acc) => sum + (parseFloat(acc.balance) || 0), 0).toFixed(2),
        account_types: [...new Set(custAccounts.map(a => a.account_type))].join(', '),
      }
    })
  },
  // Customer 360: Aggregate
  'agg-customer-360': () => {
    const joined = joinTransforms['join-customer-360']()
    return joined.map(row => ({
      customer_id: row.customer_id,
      name: row.name,
      email: row.email,
      tier: row.tier,
      account_count: row.account_count,
      total_balance: row.total_balance,
    }))
  },
  // Customer 360: Output
  'out-customer-360': () => joinTransforms['agg-customer-360'](),

  // Order Details: Join Orders + Products
  'join-order-details': () => {
    const orders = ordersTransforms['ds-orders-canonical']()
    const products = productsTransforms['ds-products-canonical']()
    return orders.map(ord => {
      const product = products.find(p => p.product_id === ord.product_id) || {}
      return {
        ...ord,
        product_name: product.name || 'Unknown',
        category: product.category || 'Unknown',
        unit_price: product.price || '0.00',
      }
    })
  },
  // Order Details: Compute (enrich)
  'compute-order-details': () => {
    const joined = joinTransforms['join-order-details']()
    return joined.map(row => ({
      order_id: row.order_id,
      customer_id: row.customer_id,
      product_name: row.product_name,
      category: row.category,
      total_amount: row.total_amount,
      status: row.status,
    }))
  },
  // Order Details: Output
  'out-order-details': () => joinTransforms['compute-order-details'](),

  // Transaction Ledger: Join Transactions + Accounts
  'join-txn-ledger': () => {
    const transactions = transactionsTransforms['ds-transactions-canonical']()
    const accounts = accountsTransforms['ds-accounts-canonical']()
    return transactions.map(txn => {
      const account = accounts.find(a => a.account_id === txn.from_account) || {}
      return {
        ...txn,
        account_type: account.account_type || 'Unknown',
        account_status: account.status || 'Unknown',
      }
    })
  },
  // Transaction Ledger: Aggregate
  'agg-txn-ledger': () => {
    const joined = joinTransforms['join-txn-ledger']()
    // Group by from_account
    const grouped = {}
    joined.forEach(txn => {
      if (!grouped[txn.from_account]) {
        grouped[txn.from_account] = {
          account_id: txn.from_account,
          account_type: txn.account_type,
          txn_count: 0,
          total_volume: 0,
        }
      }
      grouped[txn.from_account].txn_count++
      grouped[txn.from_account].total_volume += parseFloat(txn.amount) || 0
    })
    return Object.values(grouped).map(g => ({
      ...g,
      total_volume: g.total_volume.toFixed(2),
    }))
  },
  // Transaction Ledger: Output
  'out-txn-ledger': () => joinTransforms['agg-txn-ledger'](),
}

// Transform 노드 ID → 데이터 매핑
const getTransformPreviewData = (nodeId) => {
  // Accounts transforms
  if (accountsTransforms[nodeId]) return accountsTransforms[nodeId]()
  // Customers transforms
  if (customersTransforms[nodeId]) return customersTransforms[nodeId]()
  // Products transforms
  if (productsTransforms[nodeId]) return productsTransforms[nodeId]()
  // Orders transforms
  if (ordersTransforms[nodeId]) return ordersTransforms[nodeId]()
  // Transactions transforms
  if (transactionsTransforms[nodeId]) return transactionsTransforms[nodeId]()
  // Join transforms
  if (joinTransforms[nodeId]) return joinTransforms[nodeId]()
  return null
}

// Transform 노드별 스키마 정의
const transformSchemas = {
  // Accounts pipeline
  'ds-accounts-normalize': { columns: [
    { name: 'account_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'account_type', type: 'string' }, { name: 'currency', type: 'string' },
    { name: 'balance', type: 'string' }, { name: 'credit_limit', type: 'string' },
    { name: 'status', type: 'string' }, { name: 'opened_date', type: 'string' }, { name: 'branch_code', type: 'string' }
  ]},
  'ds-accounts-cast': { columns: [
    { name: 'account_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'account_type', type: 'string' }, { name: 'currency', type: 'string' },
    { name: 'balance', type: 'decimal' }, { name: 'credit_limit', type: 'integer' },
    { name: 'status', type: 'string' }, { name: 'opened_date', type: 'date' }, { name: 'branch_code', type: 'string' }
  ]},
  'ds-accounts-filter': { columns: [
    { name: 'account_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'account_type', type: 'string' }, { name: 'currency', type: 'string' },
    { name: 'balance', type: 'decimal' }, { name: 'credit_limit', type: 'integer' },
    { name: 'status', type: 'enum' }, { name: 'opened_date', type: 'date' }, { name: 'branch_code', type: 'string' }
  ]},
  'ds-accounts-select': { columns: [
    { name: 'account_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'account_type', type: 'enum' }, { name: 'balance', type: 'decimal' },
    { name: 'status', type: 'enum' }, { name: 'currency', type: 'string' }
  ]},
  'ds-accounts-canonical': { columns: [
    { name: 'account_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'account_type', type: 'enum' }, { name: 'balance', type: 'decimal' },
    { name: 'status', type: 'enum' }, { name: 'currency', type: 'string' }
  ]},

  // Customers pipeline
  'ds-customers-normalize': { columns: [
    { name: 'customer_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'email', type: 'email' }, { name: 'phone', type: 'phone' },
    { name: 'tier', type: 'string' }, { name: 'country', type: 'string' }, { name: 'city', type: 'string' }
  ]},
  'ds-customers-dedupe': { columns: [
    { name: 'customer_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'email', type: 'email' }, { name: 'phone', type: 'phone' },
    { name: 'tier', type: 'string' }, { name: 'country', type: 'string' }, { name: 'city', type: 'string' }
  ]},
  'ds-customers-cast': { columns: [
    { name: 'customer_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'email', type: 'email' }, { name: 'phone', type: 'phone' },
    { name: 'tier', type: 'enum' }, { name: 'country', type: 'string' },
    { name: 'registration_date', type: 'date' }, { name: 'total_spent', type: 'integer' }, { name: 'risk_score', type: 'float' }
  ]},
  'ds-customers-select': { columns: [
    { name: 'customer_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'email', type: 'email' }, { name: 'phone', type: 'phone' },
    { name: 'tier', type: 'enum' }, { name: 'country', type: 'string' }
  ]},
  'ds-customers-canonical': { columns: [
    { name: 'customer_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'email', type: 'email' }, { name: 'phone', type: 'phone' },
    { name: 'tier', type: 'enum' }, { name: 'country', type: 'string' }
  ]},

  // Products pipeline
  'ds-products-normalize': { columns: [
    { name: 'product_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'category', type: 'string' }, { name: 'sku', type: 'string' },
    { name: 'price', type: 'string' }, { name: 'stock_quantity', type: 'string' }
  ]},
  'ds-products-cast': { columns: [
    { name: 'product_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'category', type: 'string' }, { name: 'sku', type: 'string' },
    { name: 'price', type: 'decimal' }, { name: 'cost', type: 'decimal' }, { name: 'stock_quantity', type: 'integer' }
  ]},
  'ds-products-filter': { columns: [
    { name: 'product_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'category', type: 'string' }, { name: 'price', type: 'decimal' }, { name: 'stock_quantity', type: 'integer' }
  ]},
  'ds-products-select': { columns: [
    { name: 'product_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'category', type: 'enum' }, { name: 'price', type: 'decimal' }, { name: 'stock_quantity', type: 'integer' }
  ]},
  'ds-products-canonical': { columns: [
    { name: 'product_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'category', type: 'enum' }, { name: 'price', type: 'decimal' }, { name: 'stock_quantity', type: 'integer' }
  ]},

  // Orders pipeline
  'ds-orders-filter': { columns: [
    { name: 'order_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'product_id', type: 'foreign_key' }, { name: 'order_date', type: 'string' },
    { name: 'total_amount', type: 'string' }, { name: 'status', type: 'string' }
  ]},
  'ds-orders-cast': { columns: [
    { name: 'order_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'product_id', type: 'foreign_key' }, { name: 'order_date', type: 'datetime' },
    { name: 'total_amount', type: 'decimal' }, { name: 'quantity', type: 'integer' }, { name: 'unit_price', type: 'decimal' }, { name: 'status', type: 'string' }
  ]},
  'ds-orders-compute': { columns: [
    { name: 'order_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'product_id', type: 'foreign_key' }, { name: 'order_date', type: 'datetime' },
    { name: 'total_amount', type: 'decimal' }, { name: 'quantity', type: 'integer' },
    { name: 'unit_price', type: 'decimal' }, { name: 'status', type: 'string' }, { name: 'line_total', type: 'decimal' }
  ]},
  'ds-orders-select': { columns: [
    { name: 'order_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'product_id', type: 'foreign_key' }, { name: 'total_amount', type: 'decimal' },
    { name: 'status', type: 'enum' }, { name: 'line_total', type: 'decimal' }
  ]},
  'ds-orders-canonical': { columns: [
    { name: 'order_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'product_id', type: 'foreign_key' }, { name: 'total_amount', type: 'decimal' },
    { name: 'status', type: 'enum' }, { name: 'line_total', type: 'decimal' }
  ]},

  // Transactions pipeline
  'ds-transactions-filter': { columns: [
    { name: 'transaction_id', type: 'id' }, { name: 'from_account', type: 'foreign_key' },
    { name: 'to_account', type: 'foreign_key' }, { name: 'amount', type: 'string' },
    { name: 'currency', type: 'string' }, { name: 'transaction_type', type: 'string' }, { name: 'status', type: 'enum' }
  ]},
  'ds-transactions-normalize': { columns: [
    { name: 'transaction_id', type: 'id' }, { name: 'from_account', type: 'foreign_key' },
    { name: 'to_account', type: 'foreign_key' }, { name: 'amount', type: 'string' },
    { name: 'currency', type: 'string' }, { name: 'transaction_type', type: 'string' }, { name: 'status', type: 'enum' }
  ]},
  'ds-transactions-cast': { columns: [
    { name: 'transaction_id', type: 'id' }, { name: 'from_account', type: 'foreign_key' },
    { name: 'to_account', type: 'foreign_key' }, { name: 'amount', type: 'decimal' },
    { name: 'currency', type: 'string' }, { name: 'transaction_type', type: 'string' },
    { name: 'timestamp', type: 'datetime' }, { name: 'risk_flag', type: 'boolean' }
  ]},
  'ds-transactions-select': { columns: [
    { name: 'transaction_id', type: 'id' }, { name: 'from_account', type: 'foreign_key' },
    { name: 'to_account', type: 'foreign_key' }, { name: 'amount', type: 'decimal' },
    { name: 'transaction_type', type: 'enum' }, { name: 'timestamp', type: 'datetime' }
  ]},
  'ds-transactions-canonical': { columns: [
    { name: 'transaction_id', type: 'id' }, { name: 'from_account', type: 'foreign_key' },
    { name: 'to_account', type: 'foreign_key' }, { name: 'amount', type: 'decimal' },
    { name: 'transaction_type', type: 'enum' }, { name: 'timestamp', type: 'datetime' }
  ]},

  // Join pipelines
  'join-customer-360': { columns: [
    { name: 'customer_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'email', type: 'email' }, { name: 'tier', type: 'enum' },
    { name: 'account_count', type: 'integer' }, { name: 'total_balance', type: 'decimal' }, { name: 'account_types', type: 'string' }
  ]},
  'agg-customer-360': { columns: [
    { name: 'customer_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'email', type: 'email' }, { name: 'tier', type: 'enum' },
    { name: 'account_count', type: 'integer' }, { name: 'total_balance', type: 'decimal' }
  ]},
  'out-customer-360': { columns: [
    { name: 'customer_id', type: 'id' }, { name: 'name', type: 'string' },
    { name: 'email', type: 'email' }, { name: 'tier', type: 'enum' },
    { name: 'account_count', type: 'integer' }, { name: 'total_balance', type: 'decimal' }
  ]},
  'join-order-details': { columns: [
    { name: 'order_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'product_id', type: 'foreign_key' }, { name: 'total_amount', type: 'decimal' },
    { name: 'status', type: 'enum' }, { name: 'product_name', type: 'string' }, { name: 'category', type: 'string' }
  ]},
  'compute-order-details': { columns: [
    { name: 'order_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'product_name', type: 'string' }, { name: 'category', type: 'enum' },
    { name: 'total_amount', type: 'decimal' }, { name: 'status', type: 'enum' }
  ]},
  'out-order-details': { columns: [
    { name: 'order_id', type: 'id' }, { name: 'customer_id', type: 'foreign_key' },
    { name: 'product_name', type: 'string' }, { name: 'category', type: 'enum' },
    { name: 'total_amount', type: 'decimal' }, { name: 'status', type: 'enum' }
  ]},
  'join-txn-ledger': { columns: [
    { name: 'transaction_id', type: 'id' }, { name: 'from_account', type: 'foreign_key' },
    { name: 'to_account', type: 'foreign_key' }, { name: 'amount', type: 'decimal' },
    { name: 'transaction_type', type: 'enum' }, { name: 'account_type', type: 'string' }
  ]},
  'agg-txn-ledger': { columns: [
    { name: 'account_id', type: 'id' }, { name: 'account_type', type: 'enum' },
    { name: 'txn_count', type: 'integer' }, { name: 'total_volume', type: 'decimal' }
  ]},
  'out-txn-ledger': { columns: [
    { name: 'account_id', type: 'id' }, { name: 'account_type', type: 'enum' },
    { name: 'txn_count', type: 'integer' }, { name: 'total_volume', type: 'decimal' }
  ]},
}

// 데이터셋 ID에 따라 스키마 반환 (Clean은 정확한 스키마, Raw는 추론)
const getSchemaForDataset = (datasetId, rows) => {
  if (cleanSchemas[datasetId]) {
    return cleanSchemas[datasetId]
  }
  return inferSchema(rows)
}

// === Pipelines ===
const pipelines = [
  { pipeline_id: 'pl-001', db_name: 'enterprise-analytics', name: 'Customer Risk Scoring', description: '고객 거래 패턴 분석 및 리스크 스코어 산출', pipeline_type: 'batch', status: 'active', updated_at: '2024-02-15T00:00:00Z' },
  { pipeline_id: 'pl-002', db_name: 'enterprise-analytics', name: 'Transaction Aggregation', description: '거래 데이터 일별/월별 집계', pipeline_type: 'batch', status: 'active', updated_at: '2024-02-20T00:00:00Z' },
  { pipeline_id: 'pl-003', db_name: 'supply-chain-mgmt', name: 'Inventory Optimization', description: '재고 수준 최적화 및 재주문점 계산', pipeline_type: 'batch', status: 'active', updated_at: '2024-02-18T00:00:00Z' },
  { pipeline_id: 'pl-004', db_name: 'fraud-detection', name: 'Anomaly Detection', description: '거래 이상 패턴 탐지', pipeline_type: 'streaming', status: 'active', updated_at: '2024-02-26T00:00:00Z' },
]

// === API Routes ===

// Databases
app.get('/api/v1/databases', (req, res) => {
  res.json({ databases })
})

app.get('/api/v1/databases/:dbName', (req, res) => {
  const db = databases.find(d => d.db_name === req.params.dbName)
  if (!db) return res.status(404).json({ error: 'Database not found' })
  res.json(db)
})

// Datasets
app.get('/api/v1/pipelines/datasets', (req, res) => {
  const dbName = req.query.db_name || req.headers['x-db-name']
  const stage = req.query.stage // 'raw' or 'clean' filter
  let result = datasets
  if (dbName) {
    result = datasets.filter(d => d.db_name === dbName)
  }
  if (stage) {
    result = result.filter(d => d.stage === stage)
  }
  // 스키마와 샘플 데이터 추가
  result = result.map(ds => {
    const data = datasetDataMap[ds.dataset_id] || []
    return {
      ...ds,
      schema_json: getSchemaForDataset(ds.dataset_id, data),
      sample_json: { rows: data.slice(0, 10) },
    }
  })
  res.json({ datasets: result })
})

app.get('/api/v1/pipelines/datasets/:datasetId', (req, res) => {
  const ds = datasets.find(d => d.dataset_id === req.params.datasetId)
  if (!ds) return res.status(404).json({ error: 'Dataset not found' })
  const data = datasetDataMap[ds.dataset_id] || []
  res.json({
    ...ds,
    schema_json: getSchemaForDataset(ds.dataset_id, data),
    sample_json: { rows: data.slice(0, 100) },
  })
})

app.get('/api/v1/pipelines/datasets/:datasetId/raw-file', (req, res) => {
  const ds = datasets.find(d => d.dataset_id === req.params.datasetId)
  if (!ds) return res.status(404).json({ error: 'Dataset not found' })

  // Clean 데이터셋인지 확인
  const isClean = ds.stage === 'clean'
  const baseFilename = ds.dataset_id.replace('ds-', '').replace('-clean', '') + '.csv'
  const subDir = isClean ? 'clean' : ''
  const filePath = path.join(__dirname, '..', 'mock-data', subDir, baseFilename)

  let content = ''
  if (fs.existsSync(filePath)) {
    content = fs.readFileSync(filePath, 'utf-8')
  }

  res.json({
    file: {
      dataset_id: ds.dataset_id,
      filename: `${ds.name}${isClean ? '_clean' : ''}.csv`,
      content_type: 'text/csv',
      encoding: 'utf-8',
      content,
    }
  })
})

// Transform Node Preview API
app.get('/api/v1/pipelines/transform-preview/:nodeId', (req, res) => {
  const nodeId = req.params.nodeId
  const data = getTransformPreviewData(nodeId)

  if (!data) {
    return res.status(404).json({ error: 'Transform node not found', nodeId })
  }

  const schema = transformSchemas[nodeId] || inferSchema(data)

  res.json({
    node_id: nodeId,
    schema_json: schema,
    sample_json: { rows: data.slice(0, 100) },
    row_count: data.length,
  })
})

// Pipelines
app.get('/api/v1/pipelines', (req, res) => {
  const dbName = req.query.db_name || req.headers['x-db-name']
  let result = pipelines
  if (dbName) {
    result = pipelines.filter(p => p.db_name === dbName)
  }
  res.json({ pipelines: result })
})

app.get('/api/v1/pipelines/:pipelineId', (req, res) => {
  const pl = pipelines.find(p => p.pipeline_id === req.params.pipelineId)
  if (!pl) return res.status(404).json({ error: 'Pipeline not found' })
  res.json({
    ...pl,
    definition_json: {
      nodes: [],
      edges: [],
    }
  })
})

app.put('/api/v1/pipelines/:pipelineId', (req, res) => {
  const pl = pipelines.find(p => p.pipeline_id === req.params.pipelineId)
  if (!pl) return res.status(404).json({ error: 'Pipeline not found' })
  Object.assign(pl, req.body)
  res.json(pl)
})

app.get('/api/v1/pipelines/:pipelineId/artifacts', (req, res) => {
  res.json({ artifacts: [] })
})

app.post('/api/v1/pipelines/:pipelineId/build', (req, res) => {
  res.json({ job_id: `job-${Date.now()}`, status: 'queued' })
})

app.post('/api/v1/pipelines/:pipelineId/deploy', (req, res) => {
  res.json({ job_id: `job-${Date.now()}`, status: 'deployed' })
})

// UDFs
app.get('/api/v1/pipelines/udfs', (req, res) => {
  res.json({ udfs: [] })
})

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() })
})

// Start server
app.listen(PORT, () => {
  console.log(`
╔════════════════════════════════════════════════════════╗
║                                                        ║
║   Mock API Server running on http://localhost:${PORT}     ║
║                                                        ║
║   Available endpoints:                                 ║
║   - GET  /api/v1/databases                            ║
║   - GET  /api/v1/pipelines/datasets?db_name=xxx       ║
║   - GET  /api/v1/pipelines/datasets?stage=raw|clean   ║
║   - GET  /api/v1/pipelines/datasets/:id               ║
║   - GET  /api/v1/pipelines/datasets/:id/raw-file      ║
║   - GET  /api/v1/pipelines                            ║
║   - GET  /health                                       ║
║                                                        ║
║   Data Quality:                                        ║
║   - Raw data: Contains dirty/inconsistent data        ║
║   - Clean data: Cleansed & standardized               ║
║                                                        ║
╚════════════════════════════════════════════════════════╝
  `)
})
