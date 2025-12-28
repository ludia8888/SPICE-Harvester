import { useEffect, useMemo, useState } from 'react'
import CytoscapeComponent from 'react-cytoscapejs'
import { Button, Card, H3, InputGroup, Radio, RadioGroup, Text } from '@blueprintjs/core'
import { useAppStore, type PipelineDataset, type PipelineType } from '../state/store'

const style = [
  {
    selector: 'node',
    style: {
      label: 'data(label)',
      backgroundColor: '#137cbd',
      color: '#f5f8fa',
      textOutlineColor: '#137cbd',
      textOutlineWidth: 2,
      fontSize: 10,
    },
  },
  {
    selector: 'node.pipeline-node',
    style: {
      backgroundColor: '#0f9960',
      textOutlineColor: '#0f9960',
    },
  },
  {
    selector: 'edge',
    style: {
      width: 2,
      lineColor: '#5c7080',
      targetArrowColor: '#5c7080',
      targetArrowShape: 'triangle',
      curveStyle: 'bezier',
    },
  },
]

const defaultLocation = '/Bigdata Doctor-f5dbc1/1st BDD'

const buildPipelineElements = (datasets: PipelineDataset[]) => {
  const pipelineNode = {
    data: {
      id: 'pipeline',
      label: 'Pipeline',
    },
    classes: 'pipeline-node',
  }

  if (datasets.length === 0) {
    return [pipelineNode]
  }

  const nodes = datasets.map((dataset) => ({
    data: {
      id: dataset.id,
      label: dataset.datasetName || dataset.name,
    },
  }))
  const edges = datasets.map((dataset) => ({
    data: {
      id: `edge-${dataset.id}`,
      source: dataset.id,
      target: 'pipeline',
    },
  }))

  return [...nodes, pipelineNode, ...edges]
}

export const GraphPage = () => {
  const pipelineContext = useAppStore((state) => state.pipelineContext)
  const pipelinesByFolder = useAppStore((state) => state.pipelinesByFolder)
  const createPipeline = useAppStore((state) => state.createPipeline)
  const setActiveNav = useAppStore((state) => state.setActiveNav)
  const [pipelineName, setPipelineName] = useState('Pipeline Builder')
  const [pipelineType, setPipelineType] = useState<PipelineType>('batch')
  const pipeline = pipelineContext ? pipelinesByFolder[pipelineContext.folderId] : null
  const datasets = pipelineContext?.datasets ?? []
  const graphElements = useMemo(() => buildPipelineElements(datasets), [datasets])
  const canCreate = pipelineName.trim().length > 0 && !!pipelineContext
  const locationLabel = pipelineContext ? `/${pipelineContext.folderName}` : defaultLocation

  useEffect(() => {
    if (pipeline) {
      setPipelineName(pipeline.name)
      setPipelineType(pipeline.type)
      return
    }
    if (pipelineContext) {
      setPipelineName(`Pipeline Builder ${pipelineContext.folderName}`)
      setPipelineType('batch')
      return
    }
    setPipelineName('Pipeline Builder')
    setPipelineType('batch')
  }, [pipeline, pipelineContext])

  const handleCreatePipeline = () => {
    if (!canCreate || !pipelineContext) {
      return
    }
    createPipeline({
      id: `pipeline-${Date.now()}`,
      folderId: pipelineContext.folderId,
      name: pipelineName.trim(),
      type: pipelineType,
    })
  }

  if (!pipelineContext) {
    return (
      <div className="page">
        <Card className="card">
          <Text>Select a folder in Files to start a pipeline.</Text>
        </Card>
      </div>
    )
  }

  if (!pipeline) {
    return (
      <div className="page">
        <div className="pipeline-setup-wrapper">
          <Card className="pipeline-setup bp5-dark">
            <div className="pipeline-setup-header">
              <Text className="pipeline-setup-title">Create new pipeline</Text>
            </div>
            <div className="pipeline-setup-body">
              <div className="pipeline-form-group">
                <Text className="pipeline-label">Pipeline name and location</Text>
                <div className="pipeline-name-row">
                  <InputGroup
                    value={pipelineName}
                    onChange={(event) => setPipelineName(event.currentTarget.value)}
                  />
                  <Button icon="folder-close" text="Edit location" disabled />
                </div>
                <Text className="pipeline-location">{locationLabel}</Text>
              </div>
              <div className="pipeline-form-group">
                <Text className="pipeline-label">Pipeline type</Text>
                <RadioGroup
                  className="pipeline-type-options"
                  selectedValue={pipelineType}
                  onChange={(event) => setPipelineType(event.currentTarget.value as 'batch' | 'streaming')}
                >
                  <Radio
                    value="batch"
                    labelElement={
                      <div className="pipeline-type-card">
                        <Text className="pipeline-type-title">Batch pipeline</Text>
                        <Text className="pipeline-type-description">
                          Builds and transforms entire datasets on each deploy. Use for data ingested periodically.
                        </Text>
                      </div>
                    }
                  />
                  <Radio
                    value="streaming"
                    disabled
                    labelElement={
                      <div className="pipeline-type-card">
                        <Text className="pipeline-type-title">Streaming pipeline</Text>
                        <Text className="pipeline-type-description">
                          Transforms data continuously as new data is made available. Use for high-frequency ingestion.
                        </Text>
                      </div>
                    }
                  />
                </RadioGroup>
              </div>
              <div className="pipeline-setup-footer">
                <Button minimal icon="arrow-left" text="Back" onClick={() => setActiveNav('datasets')} />
                <Button
                  intent="success"
                  rightIcon="arrow-right"
                  text="Create pipeline"
                  disabled={!canCreate}
                  onClick={handleCreatePipeline}
                />
              </div>
            </div>
          </Card>
        </div>
      </div>
    )
  }

  return (
    <div className="page">
      <H3>{pipelineName.trim() || 'Pipeline Graph'}</H3>
      <Card className="card">
        <Text>This is a minimal Cytoscape canvas wired into the UI.</Text>
        <div className="graph-frame">
          <CytoscapeComponent elements={graphElements} stylesheet={style} style={{ width: '100%', height: '100%' }} />
        </div>
      </Card>
    </div>
  )
}
