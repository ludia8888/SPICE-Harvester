import { useRef, useState, useCallback } from "react";
import styles from "./MermaidEnhanced.module.css";

interface MermaidEnhancedProps {
  children: React.ReactNode;
  title?: string;
}

/**
 * Enhanced Mermaid diagram wrapper with:
 * - Fullscreen toggle (modal overlay)
 * - SVG download button
 * - Title label
 *
 * Usage in MDX:
 *   <MermaidEnhanced title="Data Flow">
 *     ```mermaid
 *     graph TB
 *       A --> B
 *     ```
 *   </MermaidEnhanced>
 */
export default function MermaidEnhanced({
  children,
  title,
}: MermaidEnhancedProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [isFullscreen, setIsFullscreen] = useState(false);

  const handleDownloadSvg = useCallback(() => {
    const container = containerRef.current;
    if (!container) return;

    const svgEl = container.querySelector("svg");
    if (!svgEl) return;

    const serializer = new XMLSerializer();
    const svgString = serializer.serializeToString(svgEl);
    const blob = new Blob([svgString], { type: "image/svg+xml;charset=utf-8" });
    const url = URL.createObjectURL(blob);

    const link = document.createElement("a");
    link.href = url;
    link.download = `${title?.replace(/\s+/g, "-").toLowerCase() || "diagram"}.svg`;
    link.click();
    URL.revokeObjectURL(url);
  }, [title]);

  const handleFullscreen = useCallback(() => {
    setIsFullscreen((prev) => !prev);
  }, []);

  return (
    <>
      <div
        ref={containerRef}
        className={`${styles.wrapper} ${isFullscreen ? styles.fullscreen : ""}`}
      >
        {/* Header bar */}
        <div className={styles.header}>
          {title && <span className={styles.title}>{title}</span>}
          <div className={styles.actions}>
            <button
              className={styles.actionBtn}
              onClick={handleDownloadSvg}
              title="Download SVG"
              aria-label="Download diagram as SVG"
            >
              <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
                <path
                  d="M8 1v9M4 6l4 4 4-4M2 12v2h12v-2"
                  stroke="currentColor"
                  strokeWidth="1.5"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </button>
            <button
              className={styles.actionBtn}
              onClick={handleFullscreen}
              title={isFullscreen ? "Exit Fullscreen" : "Fullscreen"}
              aria-label={isFullscreen ? "Exit fullscreen" : "View fullscreen"}
            >
              {isFullscreen ? (
                <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
                  <path
                    d="M5 1v4H1M11 1v4h4M5 15v-4H1M11 15v-4h4"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              ) : (
                <svg width="14" height="14" viewBox="0 0 16 16" fill="none">
                  <path
                    d="M1 5V1h4M15 5V1h-4M1 11v4h4M15 11v4h-4"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              )}
            </button>
          </div>
        </div>

        {/* Diagram content */}
        <div className={styles.content}>{children}</div>
      </div>

      {/* Fullscreen overlay backdrop */}
      {isFullscreen && (
        <div
          className={styles.backdrop}
          onClick={handleFullscreen}
          role="button"
          tabIndex={0}
          aria-label="Close fullscreen"
        />
      )}
    </>
  );
}
