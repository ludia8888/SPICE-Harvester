import styles from "./VersionBadge.module.css";

interface VersionBadgeProps {
  version: "v1" | "v2";
  deprecated?: boolean;
}

export default function VersionBadge({
  version,
  deprecated = false,
}: VersionBadgeProps) {
  return (
    <span
      className={`${styles.badge} ${deprecated ? styles.deprecated : ""}`}
    >
      {version.toUpperCase()}
      {deprecated && <span className={styles.deprecatedLabel}>Deprecated</span>}
    </span>
  );
}
