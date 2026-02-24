import os from 'node:os'
import path from 'node:path'
import { mkdtemp, mkdir, readFile, readdir, writeFile } from 'node:fs/promises'
import { QABugCollector } from '../e2e/foundryQaUtils'

const makeBug = (id: string) => ({
  severity: 'P1' as const,
  phase: 'QA',
  repro_steps: [`step-${id}`],
  expected: 'expected',
  actual: 'actual',
  endpoint: '/api/test',
  ui_path: '/ui/test',
  evidence: { id },
  hypothesis: 'hypothesis',
})

describe.sequential('QABugCollector', () => {
  const originalCwd = process.cwd()
  let tempRoot = ''
  let frontendDir = ''
  let qaBugFilePath = ''

  beforeEach(async () => {
    tempRoot = await mkdtemp(path.join(os.tmpdir(), 'qa-bugs-'))
    frontendDir = path.join(tempRoot, 'frontend')
    qaBugFilePath = path.join(tempRoot, 'qa_bugs.json')
    await mkdir(frontendDir, { recursive: true })
    process.chdir(frontendDir)
  })

  afterEach(() => {
    process.chdir(originalCwd)
  })

  it('archives previous run bugs and writes only current run payload', async () => {
    await writeFile(qaBugFilePath, `${JSON.stringify([{ id: 'old-bug' }], null, 2)}\n`, 'utf-8')

    const collector = new QABugCollector()
    collector.add(makeBug('new'))
    await collector.flush()

    const currentRaw = await readFile(qaBugFilePath, 'utf-8')
    const current = JSON.parse(currentRaw) as Array<Record<string, unknown>>
    expect(current).toHaveLength(1)
    expect(current[0]).toMatchObject({
      source: 'frontend_e2e',
      phase: 'QA',
      repro_steps: ['step-new'],
    })

    const archiveDir = path.join(tempRoot, 'qa_bugs_archive')
    const archives = await readdir(archiveDir)
    expect(archives).toHaveLength(1)

    const archivedRaw = await readFile(path.join(archiveDir, archives[0]), 'utf-8')
    const archived = JSON.parse(archivedRaw) as Array<Record<string, unknown>>
    expect(archived).toEqual([{ id: 'old-bug' }])
  })

  it('archives once per run and appends on subsequent flushes in same run', async () => {
    await writeFile(qaBugFilePath, `${JSON.stringify([{ id: 'stale-bug' }], null, 2)}\n`, 'utf-8')

    const first = new QABugCollector()
    first.add(makeBug('one'))
    await first.flush()

    const second = new QABugCollector()
    second.add(makeBug('two'))
    await second.flush()

    const currentRaw = await readFile(qaBugFilePath, 'utf-8')
    const current = JSON.parse(currentRaw) as Array<Record<string, unknown>>
    expect(current).toHaveLength(2)
    expect(current.map((row) => row.repro_steps)).toEqual([['step-one'], ['step-two']])

    const archiveDir = path.join(tempRoot, 'qa_bugs_archive')
    const archives = await readdir(archiveDir)
    expect(archives).toHaveLength(1)
  })
})
