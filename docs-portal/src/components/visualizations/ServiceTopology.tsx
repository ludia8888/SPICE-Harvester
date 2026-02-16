import { useRef, useEffect, useCallback, useState, useMemo } from "react";
import * as d3 from "d3";
import styles from "./ServiceTopology.module.css";

/* ------------------------------------------------------------------ */
/* Types                                                               */
/* ------------------------------------------------------------------ */
export interface ServiceNode {
  id: string;
  label: string;
  category: "api" | "worker" | "data" | "observability" | "bootstrap";
  port?: number;
  description?: string;
}

export interface ServiceEdge {
  source: string;
  target: string;
  label?: string;
}

interface ServiceTopologyProps {
  nodes?: ServiceNode[];
  edges?: ServiceEdge[];
  width?: number;
  height?: number;
  onNodeClick?: (node: ServiceNode) => void;
}

/* ------------------------------------------------------------------ */
/* Category color palette                                              */
/* ------------------------------------------------------------------ */
const CATEGORY_COLORS: Record<string, string> = {
  api: "#2D72D2",
  worker: "#0D9373",
  data: "#C87619",
  observability: "#7C3AED",
  bootstrap: "#8a9ba8",
};

const CATEGORY_LABELS: Record<string, string> = {
  api: "API Services",
  worker: "Workers",
  data: "Data Stores",
  observability: "Observability",
  bootstrap: "Bootstrap",
};

/* ------------------------------------------------------------------ */
/* Default data — SPICE Harvester service topology                     */
/* ------------------------------------------------------------------ */
const DEFAULT_NODES: ServiceNode[] = [
  // API services
  { id: "bff", label: "BFF", category: "api", port: 8002, description: "Backend for Frontend — API Gateway" },
  { id: "oms", label: "OMS", category: "api", port: 8000, description: "Ontology Management Service" },
  { id: "funnel", label: "Funnel", category: "api", port: 8003, description: "Type Inference Service" },
  { id: "agent", label: "Agent", category: "api", port: 8004, description: "AI Agent Service" },
  // Workers
  { id: "action-worker", label: "Action Worker", category: "worker", description: "Executes action templates" },
  { id: "instance-worker", label: "Instance Worker", category: "worker", description: "Processes instance events" },
  { id: "pipeline-worker", label: "Pipeline Worker", category: "worker", description: "Runs data pipelines" },
  { id: "projection-worker", label: "Projection Worker", category: "worker", description: "Materializes projections" },
  { id: "objectify-worker", label: "Objectify Worker", category: "worker", description: "Object instantiation" },
  { id: "connector-sync", label: "Connector Sync", category: "worker", description: "External data sync" },
  { id: "message-relay", label: "Message Relay", category: "worker", description: "Event routing" },
  { id: "writeback", label: "Writeback", category: "worker", description: "Write materialization" },
  { id: "ingest-reconciler", label: "Ingest Reconciler", category: "worker", description: "Data reconciliation" },
  // Data stores
  { id: "postgres", label: "PostgreSQL", category: "data", port: 5433, description: "Primary database" },
  { id: "elasticsearch", label: "Elasticsearch", category: "data", port: 9200, description: "Search & read model" },
  { id: "redis", label: "Redis", category: "data", port: 6379, description: "Cache layer" },
  { id: "kafka", label: "Kafka", category: "data", port: 39092, description: "Event streaming" },
  { id: "minio", label: "MinIO", category: "data", port: 9000, description: "S3-compatible event store" },
  { id: "lakefs", label: "LakeFS", category: "data", port: 48080, description: "Data versioning" },
  // Observability
  { id: "jaeger", label: "Jaeger", category: "observability", port: 16686, description: "Distributed tracing" },
  { id: "prometheus", label: "Prometheus", category: "observability", port: 19090, description: "Metrics" },
  { id: "grafana", label: "Grafana", category: "observability", port: 13000, description: "Dashboards" },
  { id: "otel", label: "OTEL Collector", category: "observability", description: "Telemetry pipeline" },
  { id: "alertmanager", label: "AlertManager", category: "observability", port: 19093, description: "Alerts" },
];

const DEFAULT_EDGES: ServiceEdge[] = [
  // BFF connections
  { source: "bff", target: "oms", label: "REST" },
  { source: "bff", target: "funnel", label: "REST" },
  { source: "bff", target: "postgres", label: "SQL" },
  { source: "bff", target: "redis", label: "Cache" },
  { source: "bff", target: "elasticsearch", label: "Query" },
  { source: "bff", target: "kafka", label: "Events" },
  // OMS connections
  { source: "oms", target: "postgres", label: "SQL" },
  { source: "oms", target: "elasticsearch", label: "Index" },
  { source: "oms", target: "kafka", label: "Events" },
  { source: "oms", target: "minio", label: "Events" },
  // Workers
  { source: "action-worker", target: "kafka" },
  { source: "action-worker", target: "postgres" },
  { source: "instance-worker", target: "kafka" },
  { source: "instance-worker", target: "postgres" },
  { source: "pipeline-worker", target: "kafka" },
  { source: "pipeline-worker", target: "postgres" },
  { source: "projection-worker", target: "kafka" },
  { source: "projection-worker", target: "elasticsearch" },
  { source: "projection-worker", target: "redis" },
  { source: "objectify-worker", target: "kafka" },
  { source: "objectify-worker", target: "postgres" },
  { source: "connector-sync", target: "kafka" },
  { source: "message-relay", target: "kafka" },
  { source: "writeback", target: "kafka" },
  { source: "writeback", target: "postgres" },
  { source: "ingest-reconciler", target: "kafka" },
  // Observability
  { source: "otel", target: "jaeger" },
  { source: "otel", target: "prometheus" },
  { source: "prometheus", target: "grafana" },
  { source: "prometheus", target: "alertmanager" },
  // Funnel
  { source: "funnel", target: "postgres" },
];

/* ------------------------------------------------------------------ */
/* Component                                                           */
/* ------------------------------------------------------------------ */
export default function ServiceTopology({
  nodes = DEFAULT_NODES,
  edges = DEFAULT_EDGES,
  width = 960,
  height = 640,
  onNodeClick,
}: ServiceTopologyProps) {
  const svgRef = useRef<SVGSVGElement>(null);
  const zoomRef = useRef<d3.ZoomBehavior<SVGSVGElement, unknown> | null>(null);
  const [hoveredNode, setHoveredNode] = useState<ServiceNode | null>(null);
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });

  const categories = useMemo(
    () => Array.from(new Set(nodes.map((n) => n.category))),
    [nodes],
  );

  const handleZoomIn = useCallback(() => {
    if (!svgRef.current || !zoomRef.current) return;
    d3.select(svgRef.current)
      .transition()
      .duration(300)
      .call(zoomRef.current.scaleBy as never, 1.3);
  }, []);

  const handleZoomOut = useCallback(() => {
    if (!svgRef.current || !zoomRef.current) return;
    d3.select(svgRef.current)
      .transition()
      .duration(300)
      .call(zoomRef.current.scaleBy as never, 0.7);
  }, []);

  const handleFitView = useCallback(() => {
    if (!svgRef.current || !zoomRef.current) return;
    d3.select(svgRef.current)
      .transition()
      .duration(500)
      .call(
        zoomRef.current.transform as never,
        d3.zoomIdentity.translate(width / 2, height / 2).scale(0.7),
      );
  }, [width, height]);

  useEffect(() => {
    if (!svgRef.current || nodes.length === 0) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const g = svg.append("g").attr("class", "topology-container");

    // Zoom
    const zoom = d3
      .zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.2, 4])
      .on("zoom", (event) => g.attr("transform", event.transform));
    zoomRef.current = zoom;
    svg.call(zoom);
    svg.call(
      zoom.transform,
      d3.zoomIdentity.translate(width / 2, height / 2).scale(0.7),
    );

    // Build degree map
    const degreeMap = new Map<string, number>();
    edges.forEach((e) => {
      degreeMap.set(e.source, (degreeMap.get(e.source) || 0) + 1);
      degreeMap.set(e.target, (degreeMap.get(e.target) || 0) + 1);
    });

    const simNodes = nodes.map((n) => ({
      ...n,
      degree: degreeMap.get(n.id) || 0,
    }));
    const simEdges = edges.map((e) => ({ ...e }));

    // Force simulation
    const simulation = d3
      .forceSimulation(simNodes as d3.SimulationNodeDatum[])
      .force(
        "link",
        d3
          .forceLink(
            simEdges as d3.SimulationLinkDatum<d3.SimulationNodeDatum>[],
          )
          .id((d: unknown) => (d as ServiceNode).id)
          .distance(120),
      )
      .force("charge", d3.forceManyBody().strength(-400))
      .force("center", d3.forceCenter(0, 0))
      .force("collision", d3.forceCollide().radius(55));

    // Arrow marker
    const defs = svg.append("defs");
    defs
      .append("marker")
      .attr("id", "topo-arrow")
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 28)
      .attr("refY", 0)
      .attr("orient", "auto")
      .attr("markerWidth", 5)
      .attr("markerHeight", 5)
      .append("path")
      .attr("d", "M0,-5L10,0L0,5")
      .attr("fill", "#5c7080");

    // Glow filter
    const filter = defs.append("filter").attr("id", "glow");
    filter
      .append("feGaussianBlur")
      .attr("stdDeviation", "3")
      .attr("result", "blur");
    const feMerge = filter.append("feMerge");
    feMerge.append("feMergeNode").attr("in", "blur");
    feMerge.append("feMergeNode").attr("in", "SourceGraphic");

    // Links
    const link = g
      .append("g")
      .selectAll("line")
      .data(simEdges)
      .enter()
      .append("line")
      .attr("stroke", "#404854")
      .attr("stroke-width", 1.2)
      .attr("stroke-opacity", 0.6)
      .attr("marker-end", "url(#topo-arrow)");

    // Nodes
    const node = g
      .append("g")
      .selectAll<SVGGElement, (typeof simNodes)[0]>("g")
      .data(simNodes)
      .enter()
      .append("g")
      .style("cursor", "pointer")
      .call(
        d3
          .drag<SVGGElement, (typeof simNodes)[0]>()
          .on("start", (event, d) => {
            if (!event.active) simulation.alphaTarget(0.3).restart();
            (d as any).fx = (d as any).x;
            (d as any).fy = (d as any).y;
          })
          .on("drag", (event, d) => {
            (d as any).fx = event.x;
            (d as any).fy = event.y;
          })
          .on("end", (event, d) => {
            if (!event.active) simulation.alphaTarget(0);
            (d as any).fx = null;
            (d as any).fy = null;
          }) as never,
      );

    // Node circles
    node
      .append("circle")
      .attr("r", (d) => Math.min(28, Math.max(16, 14 + Math.sqrt(d.degree) * 3)))
      .attr("fill", (d) => CATEGORY_COLORS[d.category] || "#8a9ba8")
      .attr("stroke", "#1c2127")
      .attr("stroke-width", 2)
      .attr("opacity", 0.9);

    // Port labels (inside node)
    node
      .filter((d) => !!d.port)
      .append("text")
      .attr("text-anchor", "middle")
      .attr("dy", "0.35em")
      .attr("fill", "#fff")
      .attr("font-size", "9px")
      .attr("font-weight", "600")
      .text((d) => String(d.port));

    // Node labels (below)
    node
      .append("text")
      .attr("text-anchor", "middle")
      .attr("dy", (d) =>
        Math.min(28, Math.max(16, 14 + Math.sqrt(d.degree) * 3)) + 14,
      )
      .attr("fill", "#bfccd6")
      .attr("font-size", "10px")
      .attr("font-weight", "500")
      .text((d) => d.label);

    // Hover interactions
    node
      .on("mouseenter", function (event, d) {
        d3.select(this)
          .select("circle")
          .transition()
          .duration(200)
          .attr("filter", "url(#glow)")
          .attr("opacity", 1);

        link
          .attr("stroke-opacity", (l: any) =>
            l.source?.id === d.id || l.target?.id === d.id ? 1 : 0.15,
          )
          .attr("stroke-width", (l: any) =>
            l.source?.id === d.id || l.target?.id === d.id ? 2.5 : 1.2,
          )
          .attr("stroke", (l: any) =>
            l.source?.id === d.id || l.target?.id === d.id
              ? CATEGORY_COLORS[d.category]
              : "#404854",
          );

        setHoveredNode(d);
        setTooltipPos({ x: event.clientX, y: event.clientY });
      })
      .on("mousemove", (event) => {
        setTooltipPos({ x: event.clientX, y: event.clientY });
      })
      .on("mouseleave", function () {
        d3.select(this)
          .select("circle")
          .transition()
          .duration(200)
          .attr("filter", null)
          .attr("opacity", 0.9);

        link
          .attr("stroke-opacity", 0.6)
          .attr("stroke-width", 1.2)
          .attr("stroke", "#404854");

        setHoveredNode(null);
      })
      .on("click", (_, d) => onNodeClick?.(d));

    // Tick
    simulation.on("tick", () => {
      link
        .attr("x1", (d: any) => d.source.x || 0)
        .attr("y1", (d: any) => d.source.y || 0)
        .attr("x2", (d: any) => d.target.x || 0)
        .attr("y2", (d: any) => d.target.y || 0);

      node.attr("transform", (d: any) => `translate(${d.x || 0},${d.y || 0})`);
    });

    return () => {
      simulation.stop();
      zoomRef.current = null;
    };
  }, [nodes, edges, width, height, onNodeClick]);

  return (
    <div className={styles.container} style={{ width, height }}>
      {/* Legend */}
      <div className={styles.legend}>
        {categories.map((cat) => (
          <div key={cat} className={styles.legendItem}>
            <span
              className={styles.legendDot}
              style={{ background: CATEGORY_COLORS[cat] }}
            />
            <span className={styles.legendLabel}>
              {CATEGORY_LABELS[cat] || cat}
            </span>
          </div>
        ))}
      </div>

      {/* Controls */}
      <div className={styles.controls}>
        <button className={styles.controlBtn} onClick={handleZoomIn} title="Zoom In">
          +
        </button>
        <button className={styles.controlBtn} onClick={handleZoomOut} title="Zoom Out">
          -
        </button>
        <button className={styles.controlBtn} onClick={handleFitView} title="Fit View">
          &#x2922;
        </button>
      </div>

      {/* SVG */}
      <svg ref={svgRef} width="100%" height="100%" />

      {/* Tooltip */}
      {hoveredNode && (
        <div
          className={styles.tooltip}
          style={{
            left: tooltipPos.x + 12,
            top: tooltipPos.y - 8,
          }}
        >
          <div className={styles.tooltipTitle}>{hoveredNode.label}</div>
          {hoveredNode.port && (
            <div className={styles.tooltipPort}>Port {hoveredNode.port}</div>
          )}
          {hoveredNode.description && (
            <div className={styles.tooltipDesc}>{hoveredNode.description}</div>
          )}
          <div
            className={styles.tooltipCategory}
            style={{ color: CATEGORY_COLORS[hoveredNode.category] }}
          >
            {CATEGORY_LABELS[hoveredNode.category]}
          </div>
        </div>
      )}
    </div>
  );
}
