import { useState, useRef, useCallback, useEffect } from 'react'
import { Icon } from '@blueprintjs/core'

/* ── Types ─────────────────────────────────────────── */
export type LegendColor = {
  id: string
  label: string
  color: string
}

type Props = {
  colors: LegendColor[]
  onAddColor: (label: string, color: string) => void
  onRemoveColor?: (id: string) => void
  /** Map of color-id → count of nodes using it */
  colorCounts?: Record<string, number>
}

/* ── Preset swatches ─────────────────────────────── */
const PRESETS = [
  '#E76A35', '#2B95D6', '#15B371', '#0D8050',
  '#D9822B', '#00B3A4', '#F5498B', '#7157D9', '#B6D94C',
  '#9F45B0', '#29A634', '#D68D00', '#8A9BA8',
]

/* ── Helpers ──────────────────────────────────────── */
function hexToRgb(hex: string) {
  const h = hex.replace('#', '')
  return {
    r: parseInt(h.substring(0, 2), 16) || 0,
    g: parseInt(h.substring(2, 4), 16) || 0,
    b: parseInt(h.substring(4, 6), 16) || 0,
  }
}

function rgbToHex(r: number, g: number, b: number) {
  const clamp = (v: number) => Math.max(0, Math.min(255, Math.round(v)))
  return '#' + [clamp(r), clamp(g), clamp(b)].map((v) => v.toString(16).padStart(2, '0')).join('').toUpperCase()
}

function hsvToRgb(h: number, s: number, v: number) {
  const c = v * s
  const x = c * (1 - Math.abs(((h / 60) % 2) - 1))
  const m = v - c
  let r = 0, g = 0, b = 0
  if (h < 60) { r = c; g = x }
  else if (h < 120) { r = x; g = c }
  else if (h < 180) { g = c; b = x }
  else if (h < 240) { g = x; b = c }
  else if (h < 300) { r = x; b = c }
  else { r = c; b = x }
  return {
    r: Math.round((r + m) * 255),
    g: Math.round((g + m) * 255),
    b: Math.round((b + m) * 255),
  }
}

function rgbToHsv(r: number, g: number, b: number) {
  r /= 255; g /= 255; b /= 255
  const max = Math.max(r, g, b), min = Math.min(r, g, b)
  const d = max - min
  let h = 0
  if (d !== 0) {
    if (max === r) h = 60 * (((g - b) / d) % 6)
    else if (max === g) h = 60 * ((b - r) / d + 2)
    else h = 60 * ((r - g) / d + 4)
  }
  if (h < 0) h += 360
  const s = max === 0 ? 0 : d / max
  return { h, s, v: max }
}

/* ── Component ───────────────────────────────────── */
export const PipelineLegend = ({
  colors,
  onAddColor,
  onRemoveColor,
  colorCounts = {},
}: Props) => {
  const [collapsed, setCollapsed] = useState(false)
  const [pickerOpen, setPickerOpen] = useState(false)

  /* Picker state */
  const [pickerLabel, setPickerLabel] = useState('New color')
  const [hue, setHue] = useState(135)
  const [sat, setSat] = useState(0.75)
  const [val, setVal] = useState(0.65)
  const [hexInput, setHexInput] = useState('29A634')

  const pickerRef = useRef<HTMLDivElement>(null)
  const gradientRef = useRef<HTMLDivElement>(null)
  const hueRef = useRef<HTMLDivElement>(null)

  /* Close color picker on click-away */
  useEffect(() => {
    if (!pickerOpen) return
    const handler = (e: MouseEvent) => {
      if (pickerRef.current && !pickerRef.current.contains(e.target as Node)) {
        const target = e.target as Element | null
        // Don't close if clicking ReactFlow node (avoids mid-click re-render)
        if (target?.closest('.react-flow__node')) return
        // Don't close if clicking legend header or add-color button
        if (target?.closest('.pipeline-legend')) return
        setPickerOpen(false)
      }
    }
    const timer = setTimeout(() => document.addEventListener('mousedown', handler), 80)
    return () => {
      clearTimeout(timer)
      document.removeEventListener('mousedown', handler)
    }
  }, [pickerOpen])

  const rgb = hsvToRgb(hue, sat, val)
  const currentHex = rgbToHex(rgb.r, rgb.g, rgb.b)

  /* Sync hex input → HSV */
  const applyHex = useCallback((hex: string) => {
    const clean = hex.replace('#', '')
    if (clean.length === 6) {
      const { r, g, b } = hexToRgb(clean)
      const hsv = rgbToHsv(r, g, b)
      setHue(hsv.h)
      setSat(hsv.s)
      setVal(hsv.v)
    }
  }, [])

  /* Gradient (saturation-brightness) drag */
  const onGradientPointer = useCallback((e: React.PointerEvent<HTMLDivElement>) => {
    const rect = gradientRef.current?.getBoundingClientRect()
    if (!rect) return
    const update = (ev: PointerEvent | React.PointerEvent) => {
      const x = Math.max(0, Math.min(1, (ev.clientX - rect.left) / rect.width))
      const y = Math.max(0, Math.min(1, (ev.clientY - rect.top) / rect.height))
      setSat(x)
      setVal(1 - y)
      const newRgb = hsvToRgb(hue, x, 1 - y)
      setHexInput(rgbToHex(newRgb.r, newRgb.g, newRgb.b).replace('#', ''))
    }
    update(e)
    const onMove = (ev: PointerEvent) => update(ev)
    const onUp = () => {
      document.removeEventListener('pointermove', onMove)
      document.removeEventListener('pointerup', onUp)
    }
    document.addEventListener('pointermove', onMove)
    document.addEventListener('pointerup', onUp)
  }, [hue])

  /* Hue slider drag */
  const onHuePointer = useCallback((e: React.PointerEvent<HTMLDivElement>) => {
    const rect = hueRef.current?.getBoundingClientRect()
    if (!rect) return
    const update = (ev: PointerEvent | React.PointerEvent) => {
      const x = Math.max(0, Math.min(1, (ev.clientX - rect.left) / rect.width))
      const newHue = x * 360
      setHue(newHue)
      const newRgb = hsvToRgb(newHue, sat, val)
      setHexInput(rgbToHex(newRgb.r, newRgb.g, newRgb.b).replace('#', ''))
    }
    update(e)
    const onMove = (ev: PointerEvent) => update(ev)
    const onUp = () => {
      document.removeEventListener('pointermove', onMove)
      document.removeEventListener('pointerup', onUp)
    }
    document.addEventListener('pointermove', onMove)
    document.addEventListener('pointerup', onUp)
  }, [sat, val])

  const handleSave = () => {
    if (pickerLabel.trim()) {
      onAddColor(pickerLabel.trim(), currentHex)
    }
    setPickerOpen(false)
    setPickerLabel('New color')
  }

  const hueColor = `hsl(${hue}, 100%, 50%)`

  return (
    <div className="pipeline-legend">
      {/* Header */}
      <div className="pipeline-legend-header" onClick={() => setCollapsed((c) => !c)}>
        <span className="pipeline-legend-title">Legend</span>
        <Icon icon={collapsed ? 'chevron-down' : 'chevron-up'} size={12} />
      </div>

      {!collapsed && (
        <div className="pipeline-legend-body">
          {/* Color entries */}
          {colors.length > 0 && (
            <div className="pipeline-legend-items">
              {colors.map((c) => (
                <div key={c.id} className="pipeline-legend-item">
                  <span className="pipeline-legend-swatch" style={{ background: c.color }} />
                  <span className="pipeline-legend-item-label">{c.label}</span>
                  <span className="pipeline-legend-item-count">
                    ({colorCounts[c.id] ?? 0})
                  </span>
                  {onRemoveColor && (
                    <button
                      className="pipeline-legend-item-remove"
                      onClick={() => onRemoveColor(c.id)}
                      title="Remove color"
                    >
                      <Icon icon="small-cross" size={12} />
                    </button>
                  )}
                </div>
              ))}
            </div>
          )}

          {/* Add color button */}
          <button
            className="pipeline-legend-add"
            onClick={() => setPickerOpen((o) => !o)}
          >
            <Icon icon="plus" size={12} />
            <span>Add color</span>
          </button>

          {/* Color picker popover */}
          {pickerOpen && (
            <div className="pipeline-color-picker" ref={pickerRef}>
              {/* Name input */}
              <input
                className="pipeline-cp-name"
                value={pickerLabel}
                onChange={(e) => setPickerLabel(e.target.value)}
                placeholder="Color name"
                autoFocus
              />

              {/* Gradient area */}
              <div
                className="pipeline-cp-gradient"
                ref={gradientRef}
                onPointerDown={onGradientPointer}
                style={{ background: hueColor }}
              >
                <div className="pipeline-cp-gradient-white" />
                <div className="pipeline-cp-gradient-black" />
                <div
                  className="pipeline-cp-cursor"
                  style={{
                    left: `${sat * 100}%`,
                    top: `${(1 - val) * 100}%`,
                  }}
                />
              </div>

              {/* Hue slider */}
              <div className="pipeline-cp-hue-row">
                <div
                  className="pipeline-cp-hue"
                  ref={hueRef}
                  onPointerDown={onHuePointer}
                >
                  <div
                    className="pipeline-cp-hue-thumb"
                    style={{ left: `${(hue / 360) * 100}%` }}
                  />
                </div>
                <div
                  className="pipeline-cp-preview"
                  style={{ background: currentHex }}
                />
              </div>

              {/* Hex / RGB inputs */}
              <div className="pipeline-cp-inputs">
                <div className="pipeline-cp-field">
                  <input
                    value={hexInput}
                    onChange={(e) => {
                      setHexInput(e.target.value)
                      applyHex(e.target.value)
                    }}
                  />
                  <span>Hex</span>
                </div>
                <div className="pipeline-cp-field pipeline-cp-field-sm">
                  <input
                    type="number"
                    min={0}
                    max={255}
                    value={rgb.r}
                    onChange={(e) => {
                      const r = Number(e.target.value)
                      const hsv = rgbToHsv(r, rgb.g, rgb.b)
                      setHue(hsv.h); setSat(hsv.s); setVal(hsv.v)
                      setHexInput(rgbToHex(r, rgb.g, rgb.b).replace('#', ''))
                    }}
                  />
                  <span>R</span>
                </div>
                <div className="pipeline-cp-field pipeline-cp-field-sm">
                  <input
                    type="number"
                    min={0}
                    max={255}
                    value={rgb.g}
                    onChange={(e) => {
                      const g = Number(e.target.value)
                      const hsv = rgbToHsv(rgb.r, g, rgb.b)
                      setHue(hsv.h); setSat(hsv.s); setVal(hsv.v)
                      setHexInput(rgbToHex(rgb.r, g, rgb.b).replace('#', ''))
                    }}
                  />
                  <span>G</span>
                </div>
                <div className="pipeline-cp-field pipeline-cp-field-sm">
                  <input
                    type="number"
                    min={0}
                    max={255}
                    value={rgb.b}
                    onChange={(e) => {
                      const b = Number(e.target.value)
                      const hsv = rgbToHsv(rgb.r, rgb.g, b)
                      setHue(hsv.h); setSat(hsv.s); setVal(hsv.v)
                      setHexInput(rgbToHex(rgb.r, rgb.g, b).replace('#', ''))
                    }}
                  />
                  <span>B</span>
                </div>
              </div>

              {/* Preset swatches */}
              <div className="pipeline-cp-presets">
                {PRESETS.map((c) => (
                  <button
                    key={c}
                    className="pipeline-cp-preset"
                    style={{ background: c }}
                    onClick={() => {
                      applyHex(c)
                      setHexInput(c.replace('#', ''))
                    }}
                    title={c}
                  />
                ))}
              </div>

              {/* Actions */}
              <div className="pipeline-cp-actions">
                <button className="pipeline-cp-save" onClick={handleSave}>
                  Save
                </button>
                <button
                  className="pipeline-cp-cancel"
                  onClick={() => setPickerOpen(false)}
                >
                  Cancel
                </button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
