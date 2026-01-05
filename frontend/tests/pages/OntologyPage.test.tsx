import { describe, expect, it } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { OntologyPage } from '../../src/pages/OntologyPage'

describe('OntologyPage', () => {
  it('shows the default discover content', () => {
    render(<OntologyPage />)
    expect(screen.getByText('Ontology Manager')).toBeInTheDocument()
    const title = document.querySelector('.ontology-content-title')
    expect(title).not.toBeNull()
    expect(title).toHaveTextContent('Discover')
  })

  it('filters search results and selects a resource', () => {
    render(<OntologyPage />)

    const input = screen.getByLabelText('Search ontology resources')
    fireEvent.focus(input)
    fireEvent.change(input, { target: { value: 'Facility' } })

    const result = screen.getAllByRole('option', { name: /Facility/i })[0]
    fireEvent.click(result)

    expect((input as HTMLInputElement).value).toContain('Facility')
    expect(screen.queryByRole('listbox')).not.toBeInTheDocument()
  })

  it('switches navigation sections', () => {
    render(<OntologyPage />)
    const functionsButton = screen.getByRole('button', { name: /Functions/i })
    fireEvent.click(functionsButton)

    expect(screen.getByText(/Object types/i)).toBeInTheDocument()
    expect(screen.getByText('Functions (147)')).toBeInTheDocument()
  })
})
