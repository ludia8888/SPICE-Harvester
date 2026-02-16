import styles from "./PersonaBadge.module.css";

type Persona =
  | "data-engineer"
  | "platform-admin"
  | "integration-dev"
  | "business-analyst"
  | "all";

const PERSONA_CONFIG: Record<Persona, { label: string; color: string }> = {
  "data-engineer": { label: "Data Engineer", color: "#2D72D2" },
  "platform-admin": { label: "Platform Admin", color: "#7C3AED" },
  "integration-dev": { label: "Integration Dev", color: "#0D9373" },
  "business-analyst": { label: "Business Analyst", color: "#C87619" },
  all: { label: "All Roles", color: "#8a9ba8" },
};

interface PersonaBadgeProps {
  persona: Persona;
}

export default function PersonaBadge({ persona }: PersonaBadgeProps) {
  const config = PERSONA_CONFIG[persona] || PERSONA_CONFIG.all;
  return (
    <span
      className={styles.badge}
      style={{
        borderColor: config.color,
        color: config.color,
      }}
    >
      {config.label}
    </span>
  );
}
