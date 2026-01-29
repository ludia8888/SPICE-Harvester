import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, screen, within } from '@testing-library/react'
import { OntologyPage } from '../../src/pages/OntologyPage'
import { renderWithClient, resetAppStore } from '../testUtils'
import { useAppStore } from '../../src/state/store'

const makeResponse = (payload: unknown, ok = true) => ({
  ok,
  statusText: ok ? 'OK' : 'Bad Request',
  json: () => Promise.resolve(payload),
})

describe('OntologyPage', () => {
  beforeEach(() => {
    resetAppStore()
    useAppStore.setState({
      pipelineContext: { folderId: 'demo-project', folderName: 'Demo Project' },
    })

    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL) => {
        const url = typeof input === 'string' ? input : input.toString()

        if (url.includes('/api/v1/ontology/demo-project/classes')) {
          return Promise.resolve(
            makeResponse({
              status: 'success',
              data: {
                classes: [
                  { id: 'Facility', label: 'Facility', description: '시설' },
                  { id: 'Vendor', label: 'Vendor', description: '협력사' },
                ],
              },
            }),
          )
        }

        if (url.includes('/api/v1/ontology/demo-project/link-types')) {
          return Promise.resolve(
            makeResponse({
              status: 'success',
              data: {
                linkTypes: [
                  {
                    id: 'facility_vendor',
                    name: 'Facility ↔ Vendor',
                    predicate: 'hasVendor',
                    sourceClassName: 'Facility',
                    targetClassName: 'Vendor',
                    description: '시설과 협력사 관계',
                  },
                ],
              },
            }),
          )
        }

        if (url.includes('/api/v1/ontology/demo-project/action-types')) {
          return Promise.resolve(
            makeResponse({
              status: 'success',
              data: {
                actionTypes: [
                  { id: 'normalize_address', name: 'Normalize Address', description: '주소 정규화' },
                ],
              },
            }),
          )
        }

        throw new Error(`Unexpected fetch: ${url}`)
      }) as never,
    )
  })

  afterEach(() => {
    vi.unstubAllGlobals()
    vi.resetAllMocks()
  })

  it('shows the default discover content', () => {
    renderWithClient(<OntologyPage />)
    expect(screen.getByText('Ontology Manager')).toBeInTheDocument()
    const title = document.querySelector('.ontology-content-title')
    expect(title).not.toBeNull()
    expect(title).toHaveTextContent('Discover')
  })

  it('filters search results and selects a resource', async () => {
    renderWithClient(<OntologyPage />)

    const input = screen.getByLabelText('Search ontology resources')
    fireEvent.focus(input)
    fireEvent.change(input, { target: { value: 'Facility' } })

    const listbox = await screen.findByRole('listbox')
    const options = await within(listbox).findAllByRole('option')
    const objectTypeOption = options.find((option) => option.textContent?.includes('Object type'))
    expect(objectTypeOption).toBeDefined()
    fireEvent.click(objectTypeOption!)

    expect((input as HTMLInputElement).value).toContain('Facility')
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument()
  })

  it('switches navigation sections', () => {
    renderWithClient(<OntologyPage />)
    const functionsButton = screen.getByRole('button', { name: /Functions/i })
    fireEvent.click(functionsButton)

    expect(screen.getByText(/Object types/i)).toBeInTheDocument()
    const title = document.querySelector('.ontology-content-title')
    expect(title).not.toBeNull()
    expect(title).toHaveTextContent('Functions')
  })
})
