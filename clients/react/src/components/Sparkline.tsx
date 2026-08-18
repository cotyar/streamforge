/**
 * Sparkline -- dependency-free inline SVG trend line, distilled from
 * `web/src/components/Sparkline.tsx` (the console's version): same point/path math, minus the
 * `--spark-stroke` CSS-variable default and the area fill. `stroke` defaults to `currentColor` so
 * the line inherits the host's text color with zero setup -- the one styling knob this component
 * needs, given as a prop rather than a CSS hook because SVG `stroke` cannot be set via a bare class
 * name the way the table's colors can.
 *
 * ponytail: no gradient/area fill under the line (the console's version has one) -- add only if a
 * consumer actually asks; the bare line already satisfies every acceptance case here.
 */
import type { ReactElement } from "react";

export interface SparklineProps {
  values: readonly number[];
  width?: number;
  height?: number;
  className?: string;
  stroke?: string;
}

export function Sparkline(props: SparklineProps): ReactElement {
  const { values, width = 120, height = 28, className, stroke = "currentColor" } = props;
  const rootClass = className ? `sf-sparkline ${className}` : "sf-sparkline";

  if (values.length === 0) {
    return <svg className={rootClass} width={width} height={height} viewBox={`0 0 ${width} ${height}`} aria-hidden="true" />;
  }

  if (values.length === 1) {
    // One point has no direction to draw -- render a flat mid-height line rather than nothing, so
    // the caller's layout doesn't jump when a second value arrives.
    const y = height / 2;
    return (
      <svg
        className={rootClass}
        width={width}
        height={height}
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        aria-hidden="true"
      >
        <line x1={0} y1={y} x2={width} y2={y} stroke={stroke} strokeWidth={1.5} />
      </svg>
    );
  }

  const max = Math.max(...values);
  const min = Math.min(...values);
  const range = max - min || 1; // all values equal -- flat line, guards the /range below from a divide-by-zero
  const stepX = width / (values.length - 1);

  const path = values
    .map((v, i) => {
      const x = i * stepX;
      const y = height - ((v - min) / range) * (height - 4) - 2;
      return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <svg
      className={rootClass}
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="none"
      aria-hidden="true"
    >
      <path d={path} fill="none" stroke={stroke} strokeWidth={1.5} strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  );
}
