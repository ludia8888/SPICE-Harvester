import React from "react";
import Link from "@docusaurus/Link";
import styles from "./HeroSection.module.css";

interface HeroAction {
  label: string;
  href: string;
}

interface HeroSectionProps {
  title: string;
  tagline: string;
  primaryAction: HeroAction;
  secondaryAction?: HeroAction;
  children?: React.ReactNode;
}

export default function HeroSection({
  title,
  tagline,
  primaryAction,
  secondaryAction,
  children,
}: HeroSectionProps): React.ReactElement {
  return (
    <section className={styles.hero}>
      <div className={styles.heroInner}>
        <h1 className={styles.title}>{title}</h1>
        <p className={styles.tagline}>{tagline}</p>
        <div className={styles.actions}>
          <Link className={styles.primaryBtn} to={primaryAction.href}>
            {primaryAction.label}
            <span aria-hidden="true">&rarr;</span>
          </Link>
          {secondaryAction && (
            <Link className={styles.secondaryBtn} to={secondaryAction.href}>
              {secondaryAction.label}
            </Link>
          )}
        </div>
      </div>
      {children && <div className={styles.childrenSlot}>{children}</div>}
    </section>
  );
}
