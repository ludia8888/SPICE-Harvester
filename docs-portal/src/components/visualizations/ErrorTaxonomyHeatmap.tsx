import { useRef, useEffect, useState, useMemo, useCallback } from "react";
import * as d3 from "d3";
import styles from "./ErrorTaxonomyHeatmap.module.css";

/* ------------------------------------------------------------------ */
/* Types                                                               */
/* ------------------------------------------------------------------ */
export interface ErrorSpec {
  code: string;
  domain: string;
  errorClass: string;
  severity: "INFO" | "WARNING" | "ERROR" | "CRITICAL";
  title: string;
  retryable: boolean;
  retryPolicy: string;
  httpStatus: number;
  subsystem: string;
}

interface ErrorTaxonomyHeatmapProps {
  errors?: ErrorSpec[];
  width?: number;
  height?: number;
}

/* ------------------------------------------------------------------ */
/* Color scales                                                        */
/* ------------------------------------------------------------------ */
const SEVERITY_COLORS: Record<string, string> = {
  INFO: "#4C90F0",
  WARNING: "#C87619",
  ERROR: "#CD4246",
  CRITICAL: "#8F0E12",
};

const SEVERITY_BG: Record<string, string> = {
  INFO: "rgba(76, 144, 240, 0.15)",
  WARNING: "rgba(200, 118, 25, 0.15)",
  ERROR: "rgba(205, 66, 70, 0.15)",
  CRITICAL: "rgba(143, 14, 18, 0.2)",
};

/* ------------------------------------------------------------------ */
/* Default data — representative SPICE error catalog                   */
/* ------------------------------------------------------------------ */
const DEFAULT_ERRORS: ErrorSpec[] = [
  { code: "SHV-BFF-INP-VAL-0001", domain: "INPUT", errorClass: "VALIDATION", severity: "WARNING", title: "Invalid request body", retryable: false, retryPolicy: "NONE", httpStatus: 400, subsystem: "BFF" },
  { code: "SHV-BFF-INP-VAL-0002", domain: "INPUT", errorClass: "VALIDATION", severity: "WARNING", title: "Missing required field", retryable: false, retryPolicy: "NONE", httpStatus: 400, subsystem: "BFF" },
  { code: "SHV-BFF-ACC-SEC-0001", domain: "ACCESS", errorClass: "SECURITY", severity: "ERROR", title: "Authentication required", retryable: false, retryPolicy: "AFTER_REFRESH", httpStatus: 401, subsystem: "BFF" },
  { code: "SHV-BFF-ACC-PRM-0001", domain: "ACCESS", errorClass: "PERMISSION", severity: "ERROR", title: "Insufficient permissions", retryable: false, retryPolicy: "NONE", httpStatus: 403, subsystem: "BFF" },
  { code: "SHV-OMS-RES-NF-0001", domain: "RESOURCE", errorClass: "NOT_FOUND", severity: "WARNING", title: "Object type not found", retryable: false, retryPolicy: "NONE", httpStatus: 404, subsystem: "OMS" },
  { code: "SHV-OMS-RES-NF-0002", domain: "RESOURCE", errorClass: "NOT_FOUND", severity: "WARNING", title: "Instance not found", retryable: false, retryPolicy: "NONE", httpStatus: 404, subsystem: "OMS" },
  { code: "SHV-OMS-CON-CON-0001", domain: "CONFLICT", errorClass: "CONFLICT", severity: "ERROR", title: "Concurrent modification", retryable: true, retryPolicy: "BACKOFF", httpStatus: 409, subsystem: "OMS" },
  { code: "SHV-BFF-RL-LIM-0001", domain: "RATE_LIMIT", errorClass: "LIMIT", severity: "WARNING", title: "Rate limit exceeded", retryable: true, retryPolicy: "BACKOFF", httpStatus: 429, subsystem: "BFF" },
  { code: "SHV-OMS-UPS-TMO-0001", domain: "UPSTREAM", errorClass: "TIMEOUT", severity: "ERROR", title: "Elasticsearch timeout", retryable: true, retryPolicy: "BACKOFF", httpStatus: 504, subsystem: "OMS" },
  { code: "SHV-OMS-UPS-UNA-0001", domain: "UPSTREAM", errorClass: "UNAVAILABLE", severity: "CRITICAL", title: "Kafka unavailable", retryable: true, retryPolicy: "BACKOFF", httpStatus: 503, subsystem: "OMS" },
  { code: "SHV-SHR-DB-INT-0001", domain: "DATABASE", errorClass: "INTERNAL", severity: "CRITICAL", title: "Database connection lost", retryable: true, retryPolicy: "BACKOFF", httpStatus: 503, subsystem: "SHR" },
  { code: "SHV-SHR-SYS-INT-0001", domain: "SYSTEM", errorClass: "INTERNAL", severity: "CRITICAL", title: "Unhandled exception", retryable: false, retryPolicy: "NONE", httpStatus: 500, subsystem: "SHR" },
  { code: "SHV-SHR-DAT-VAL-0001", domain: "DATA", errorClass: "VALIDATION", severity: "ERROR", title: "Schema mismatch", retryable: false, retryPolicy: "NONE", httpStatus: 422, subsystem: "SHR" },
  { code: "SHV-BFF-MAP-INT-0001", domain: "MAPPING", errorClass: "INTEGRATION", severity: "ERROR", title: "Label mapping failed", retryable: true, retryPolicy: "IMMEDIATE", httpStatus: 502, subsystem: "BFF" },
  { code: "SHV-PIP-PIP-STA-0001", domain: "PIPELINE", errorClass: "STATE", severity: "ERROR", title: "Pipeline stuck", retryable: true, retryPolicy: "BACKOFF", httpStatus: 500, subsystem: "PIP" },
  { code: "SHV-PIP-PIP-VAL-0001", domain: "PIPELINE", errorClass: "VALIDATION", severity: "WARNING", title: "Invalid transform config", retryable: false, retryPolicy: "NONE", httpStatus: 400, subsystem: "PIP" },
  { code: "SHV-OBJ-ONT-VAL-0001", domain: "ONTOLOGY", errorClass: "VALIDATION", severity: "WARNING", title: "Property type mismatch", retryable: false, retryPolicy: "NONE", httpStatus: 400, subsystem: "OBJ" },
  { code: "SHV-OBJ-OBJ-INT-0001", domain: "OBJECTIFY", errorClass: "INTERNAL", severity: "ERROR", title: "Objectify batch failure", retryable: true, retryPolicy: "BACKOFF", httpStatus: 500, subsystem: "OBJ" },
  { code: "SHV-ACT-INP-VAL-0001", domain: "INPUT", errorClass: "VALIDATION", severity: "WARNING", title: "Invalid action params", retryable: false, retryPolicy: "NONE", httpStatus: 400, subsystem: "ACT" },
  { code: "SHV-CON-UPS-TMO-0001", domain: "UPSTREAM", errorClass: "TIMEOUT", severity: "ERROR", title: "Connector timeout", retryable: true, retryPolicy: "BACKOFF", httpStatus: 504, subsystem: "CON" },
];

/* ------------------------------------------------------------------ */
/* Component                                                           */
/* ------------------------------------------------------------------ */
export default function ErrorTaxonomyHeatmap({
  errors = DEFAULT_ERRORS,
  width = 960,
  height = 480,
}: ErrorTaxonomyHeatmapProps) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [selectedError, setSelectedError] = useState<ErrorSpec | null>(null);
  const [filter, setFilter] = useState<string>("all");

  const filteredErrors = useMemo(
    () => filter === "all" ? errors : errors.filter((e) => e.severity === filter),
    [errors, filter],
  );

  const { domains, classes } = useMemo(() => {
    const d = Array.from(new Set(errors.map((e) => e.domain))).sort();
    const c = Array.from(new Set(errors.map((e) => e.errorClass))).sort();
    return { domains: d, classes: c };
  }, [errors]);

  // Build heatmap grid
  const grid = useMemo(() => {
    const cells: Array<{
      domain: string;
      errorClass: string;
      count: number;
      errors: ErrorSpec[];
      maxSeverity: string;
    }> = [];

    for (const domain of domains) {
      for (const cls of classes) {
        const matching = filteredErrors.filter(
          (e) => e.domain === domain && e.errorClass === cls,
        );
        const severityOrder = ["INFO", "WARNING", "ERROR", "CRITICAL"];
        const maxSev =
          matching.reduce((max, e) => {
            const idx = severityOrder.indexOf(e.severity);
            const maxIdx = severityOrder.indexOf(max);
            return idx > maxIdx ? e.severity : max;
          }, "INFO") || "INFO";

        cells.push({
          domain,
          errorClass: cls,
          count: matching.length,
          errors: matching,
          maxSeverity: maxSev,
        });
      }
    }
    return cells;
  }, [domains, classes, filteredErrors]);

  useEffect(() => {
    if (!svgRef.current) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const margin = { top: 60, right: 20, bottom: 20, left: 120 };
    const innerW = width - margin.left - margin.right;
    const innerH = height - margin.top - margin.bottom;

    const g = svg
      .append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    const xScale = d3
      .scaleBand()
      .domain(classes)
      .range([0, innerW])
      .padding(0.08);

    const yScale = d3
      .scaleBand()
      .domain(domains)
      .range([0, innerH])
      .padding(0.08);

    // X axis (top)
    g.append("g")
      .call(d3.axisTop(xScale).tickSize(0))
      .call((g) => g.select(".domain").remove())
      .selectAll("text")
      .attr("fill", "var(--ifm-font-color-secondary)")
      .attr("font-size", "10px")
      .attr("font-weight", "600")
      .attr("transform", "rotate(-35)")
      .attr("text-anchor", "start");

    // Y axis (left)
    g.append("g")
      .call(d3.axisLeft(yScale).tickSize(0))
      .call((g) => g.select(".domain").remove())
      .selectAll("text")
      .attr("fill", "var(--ifm-font-color-secondary)")
      .attr("font-size", "11px")
      .attr("font-weight", "500");

    // Cells
    const cells = g
      .selectAll("rect")
      .data(grid)
      .enter()
      .append("rect")
      .attr("x", (d) => xScale(d.errorClass)!)
      .attr("y", (d) => yScale(d.domain)!)
      .attr("width", xScale.bandwidth())
      .attr("height", yScale.bandwidth())
      .attr("rx", 4)
      .attr("fill", (d) =>
        d.count > 0
          ? SEVERITY_BG[d.maxSeverity] || SEVERITY_BG.INFO
          : "var(--portal-surface-overlay)",
      )
      .attr("stroke", (d) =>
        d.count > 0
          ? SEVERITY_COLORS[d.maxSeverity] || SEVERITY_COLORS.INFO
          : "transparent",
      )
      .attr("stroke-width", (d) => (d.count > 0 ? 1.5 : 0))
      .attr("opacity", (d) => (d.count > 0 ? 1 : 0.3))
      .style("cursor", (d) => (d.count > 0 ? "pointer" : "default"));

    // Count labels
    g.selectAll(".cell-count")
      .data(grid.filter((d) => d.count > 0))
      .enter()
      .append("text")
      .attr("class", "cell-count")
      .attr("x", (d) => xScale(d.errorClass)! + xScale.bandwidth() / 2)
      .attr("y", (d) => yScale(d.domain)! + yScale.bandwidth() / 2)
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "central")
      .attr("fill", (d) => SEVERITY_COLORS[d.maxSeverity])
      .attr("font-size", "13px")
      .attr("font-weight", "700")
      .text((d) => d.count);

    // Hover
    cells
      .on("mouseenter", function (_, d) {
        if (d.count === 0) return;
        d3.select(this)
          .transition()
          .duration(150)
          .attr("stroke-width", 3)
          .attr("opacity", 1);
      })
      .on("mouseleave", function (_, d) {
        d3.select(this)
          .transition()
          .duration(150)
          .attr("stroke-width", d.count > 0 ? 1.5 : 0)
          .attr("opacity", d.count > 0 ? 1 : 0.3);
      })
      .on("click", (_, d) => {
        if (d.count > 0) setSelectedError(d.errors[0]);
      });
  }, [grid, domains, classes, width, height]);

  const severityCounts = useMemo(() => {
    const counts: Record<string, number> = { INFO: 0, WARNING: 0, ERROR: 0, CRITICAL: 0 };
    errors.forEach((e) => counts[e.severity]++);
    return counts;
  }, [errors]);

  return (
    <div className={styles.container}>
      {/* Header */}
      <div className={styles.header}>
        <div className={styles.stats}>
          {Object.entries(severityCounts).map(([sev, count]) => (
            <button
              key={sev}
              className={`${styles.statBadge} ${filter === sev ? styles.statBadgeActive : ""}`}
              style={{
                borderColor: SEVERITY_COLORS[sev],
                color: filter === sev ? "#fff" : SEVERITY_COLORS[sev],
                background: filter === sev ? SEVERITY_COLORS[sev] : "transparent",
              }}
              onClick={() => setFilter(filter === sev ? "all" : sev)}
            >
              {sev} ({count})
            </button>
          ))}
          {filter !== "all" && (
            <button
              className={styles.clearFilter}
              onClick={() => setFilter("all")}
            >
              Clear filter
            </button>
          )}
        </div>
      </div>

      {/* Heatmap */}
      <svg ref={svgRef} width={width} height={height} />

      {/* Detail panel */}
      {selectedError && (
        <div className={styles.detailPanel}>
          <div className={styles.detailHeader}>
            <code className={styles.detailCode}>{selectedError.code}</code>
            <button
              className={styles.detailClose}
              onClick={() => setSelectedError(null)}
            >
              &times;
            </button>
          </div>
          <h4 className={styles.detailTitle}>{selectedError.title}</h4>
          <div className={styles.detailGrid}>
            <div className={styles.detailItem}>
              <span className={styles.detailLabel}>Severity</span>
              <span
                className={styles.detailValue}
                style={{ color: SEVERITY_COLORS[selectedError.severity] }}
              >
                {selectedError.severity}
              </span>
            </div>
            <div className={styles.detailItem}>
              <span className={styles.detailLabel}>HTTP Status</span>
              <span className={styles.detailValue}>{selectedError.httpStatus}</span>
            </div>
            <div className={styles.detailItem}>
              <span className={styles.detailLabel}>Retryable</span>
              <span className={styles.detailValue}>
                {selectedError.retryable ? "Yes" : "No"}
              </span>
            </div>
            <div className={styles.detailItem}>
              <span className={styles.detailLabel}>Retry Policy</span>
              <span className={styles.detailValue}>
                {selectedError.retryPolicy}
              </span>
            </div>
            <div className={styles.detailItem}>
              <span className={styles.detailLabel}>Subsystem</span>
              <span className={styles.detailValue}>
                {selectedError.subsystem}
              </span>
            </div>
            <div className={styles.detailItem}>
              <span className={styles.detailLabel}>Domain</span>
              <span className={styles.detailValue}>{selectedError.domain}</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
