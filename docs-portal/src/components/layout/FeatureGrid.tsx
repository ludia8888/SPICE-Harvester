import React from "react";
import Link from "@docusaurus/Link";
import styles from "./FeatureGrid.module.css";

export interface Feature {
  icon: string;
  title: string;
  description: string;
  link: string;
}

interface FeatureGridProps {
  features: Feature[];
}

export default function FeatureGrid({
  features,
}: FeatureGridProps): React.ReactElement {
  return (
    <div className={styles.grid}>
      {features.map((feature) => (
        <Link key={feature.title} className={styles.card} to={feature.link}>
          <div className={styles.iconWrapper} aria-hidden="true">
            {feature.icon}
          </div>
          <h3 className={styles.cardTitle}>{feature.title}</h3>
          <p className={styles.cardDescription}>{feature.description}</p>
          <span className={styles.cardArrow}>
            Learn more <span aria-hidden="true">&rarr;</span>
          </span>
        </Link>
      ))}
    </div>
  );
}
