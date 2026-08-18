/**
 * StreamView -- the height-capped, auto-scrolling box every live view in the console re-typed
 * inline with Tailwind classes (`web/src/components/ResultsTable.tsx`'s `max-h-full overflow-auto`
 * + sticky header, `TableDetailPage.tsx`'s `max-h-[28rem] overflow-auto`, `SourcesPage.tsx`'s fixed
 * `h-32` ScrollArea over `useSourceTape`'s newest-first tape). The useful behaviour was never the
 * height cap itself -- it's staying pinned to the newest edge while content streams in, WITHOUT
 * yanking the view out from under someone who scrolled up to read something. That's the entire
 * point of this component; composition (what goes inside) is the caller's job, not this one's --
 * no sticky header, no jump-to-latest button, no chrome. Put a `<LiveTableView>` or your own markup
 * inside.
 *
 * Auto-follow mechanics:
 *  - "At the newest edge" is a tolerance check (EDGE_TOLERANCE_PX), not an exact `=== 0` /
 *    `=== scrollHeight - clientHeight` comparison -- sub-pixel layout rounding and browser zoom
 *    make an exact comparison flaky (a user sitting exactly at the edge can read back as 0.4px off
 *    after a zoom change, which would wrongly read as "scrolled away").
 *  - Distinguishing OUR scroll (from following) from the USER's scroll (from a wheel/touch/scrollbar
 *    drag) matters because a native 'scroll' event carries no origin -- both look identical to the
 *    handler. `programmaticRef` is set immediately before this component assigns `el.scrollTop`
 *    itself, and consumed (cleared) by the very next 'scroll' event the browser fires for that
 *    assignment, so that event is not mistaken for the user scrolling away. It is only armed when
 *    the assignment actually changes the value -- if the box is already sitting at the target
 *    offset (nothing to scroll), the browser never fires a 'scroll' event at all, and an armed flag
 *    with no event to consume it would wrongly swallow the user's NEXT real scroll.
 *  - Content growth is observed two ways: a `ResizeObserver` on the content wrapper (fires on any
 *    box-size change, not just a React commit -- e.g. an image inside `children` finishing its own
 *    async load) and a layout effect that runs after every render regardless. happy-dom (this
 *    package's test DOM) does not implement `ResizeObserver`, so it's feature-detected and skipped
 *    there -- the render-triggered layout effect is what covers growth in tests, same as it covers
 *    the general case of "children changed" in real usage.
 *  - The scroll adjustment itself runs in `useLayoutEffect` (SSR-shimmed to `useEffect`, per React's
 *    own recommended pattern, since `useLayoutEffect` warns when it runs with no DOM) so the browser
 *    paints the post-scroll frame directly -- an effect scheduled after paint would flash the
 *    pre-scroll position for one frame.
 */
import { useEffect, useLayoutEffect, useRef, useState } from "react";
import type { ReactElement, ReactNode } from "react";

const useIsomorphicLayoutEffect = typeof window === "undefined" ? useEffect : useLayoutEffect;

// A few px of slack for "am I at the newest edge" -- see the file doc comment. Named so the
// tolerance is a single, explained knob rather than a magic number repeated at each call site.
const EDGE_TOLERANCE_PX = 4;

function isAtEdge(el: HTMLElement, newest: "top" | "bottom"): boolean {
  return newest === "top" ? el.scrollTop <= EDGE_TOLERANCE_PX : el.scrollHeight - el.clientHeight - el.scrollTop <= EDGE_TOLERANCE_PX;
}

// Mutates el.scrollTop toward the pinned edge, and arms programmaticRef ONLY when that actually
// moves the value -- see the file doc comment for why a no-op assignment must not arm it.
function scrollToEdge(el: HTMLElement, newest: "top" | "bottom", programmaticRef: { current: boolean }): void {
  const target = newest === "top" ? 0 : el.scrollHeight - el.clientHeight;
  if (el.scrollTop === target) return;
  programmaticRef.current = true;
  el.scrollTop = target;
}

export interface StreamViewProps {
  children: ReactNode;
  /** Height cap for the scroll box. Number = px. Default: 320. */
  maxHeight?: number | string;
  /** Keep the newest edge in view as content grows, unless the user has scrolled away. Default: true. */
  follow?: boolean;
  /** Which edge is "newest": "bottom" (log-style, default) or "top" (newest-first tapes like the console's source tape). */
  newest?: "top" | "bottom";
  className?: string;
}

export function StreamView(props: StreamViewProps): ReactElement {
  const { children, maxHeight = 320, follow = true, newest = "bottom", className } = props;

  const scrollRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const programmaticRef = useRef(false);
  const [following, setFollowing] = useState(follow);

  // follow flipping to false at runtime cuts auto-scroll immediately, not just for the next append.
  useEffect(() => {
    if (!follow) setFollowing(false);
  }, [follow]);

  useIsomorphicLayoutEffect(() => {
    if (!follow || !following) return;
    const el = scrollRef.current;
    if (el) scrollToEdge(el, newest, programmaticRef);
  });

  useEffect(() => {
    if (!follow) return;
    if (typeof ResizeObserver === "undefined") return; // ponytail: no polyfill -- the layout effect above already re-checks after every render, so only a same-render, no-prop-change growth (e.g. an async image inside children) needs the observer; harmless to skip where it's unavailable (happy-dom), a real browser always has it.
    const content = contentRef.current;
    const scrollEl = scrollRef.current;
    if (!content || !scrollEl) return;
    const ro = new ResizeObserver(() => {
      if (following) scrollToEdge(scrollEl, newest, programmaticRef);
    });
    ro.observe(content);
    return () => ro.disconnect();
  }, [follow, following, newest]);

  function handleScroll(): void {
    if (programmaticRef.current) {
      programmaticRef.current = false;
      return;
    }
    if (!follow) return;
    const el = scrollRef.current;
    if (el) setFollowing(isAtEdge(el, newest));
  }

  const rootClass = ["sf-stream", following && "sf-stream--following", className].filter(Boolean).join(" ");

  return (
    <div
      ref={scrollRef}
      className={rootClass}
      style={{ maxHeight: typeof maxHeight === "number" ? `${maxHeight}px` : maxHeight, overflow: "auto" }}
      onScroll={handleScroll}
    >
      <div ref={contentRef}>{children}</div>
    </div>
  );
}
