/**
 * The grid features layered onto LiveTableView -- sort, fuzzy/column filter, reorder, virtualize --
 * one runnable check per behavior (see react.test.tsx's own doc comment for the house style this
 * mirrors). Pure rendering only: no LiveTable/Transport involved, same as LiveTableView's own suite
 * in react.test.tsx.
 *
 * happy-dom implements no layout engine, so `offsetHeight`/`offsetWidth` are 0 on every element and
 * (per its own module doc comment) it has no ResizeObserver either. TanStack Virtual only remeasures
 * a scroll element when the element's IDENTITY changes (see virtual-core's `_willUpdate`), so mocking
 * metrics AFTER mount -- the way stream-view.test.tsx does for StreamView's own scrollTop logic --
 * would be too late here: there is no observer to pick the change up. `withMockedOffsetHeight` below
 * instead overrides `HTMLElement.prototype.offsetHeight` BEFORE the component mounts, so the
 * virtualizer's one-shot initial measurement already sees a real height. That earns an honest
 * "renders fewer rows than exist" assertion; without it, the only honest claim would be "renders
 * without crashing", which the test also makes, explicitly, alongside the windowed-subset one.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { canonicalKey } from "@streamsforge/client";
import { LiveTableView } from "../src/index.js";

afterEach(cleanup);

function withMockedOffsetHeight<T>(height: number, fn: () => T): T {
  const original = Object.getOwnPropertyDescriptor(HTMLElement.prototype, "offsetHeight");
  Object.defineProperty(HTMLElement.prototype, "offsetHeight", { configurable: true, get: () => height });
  try {
    return fn();
  } finally {
    if (original) Object.defineProperty(HTMLElement.prototype, "offsetHeight", original);
    else delete (HTMLElement.prototype as unknown as Record<string, unknown>).offsetHeight;
  }
}

function cellTexts(container: HTMLElement): string[] {
  return Array.from(container.querySelectorAll("td.sf-table__cell")).map((td) => td.textContent ?? "");
}

describe("sortable", () => {
  test("clicking a header cycles asc -> desc -> none and sets aria-sort", () => {
    const rows = [{ n: 3 }, { n: 1 }, { n: 2 }];
    const { container } = render(<LiveTableView rows={rows} sortable />);
    const th = container.querySelector("th")!;
    const button = screen.getByRole("button", { name: "n" });

    // Unsorted but sortable -- aria-sort is "none", not omitted (the column IS sortable).
    expect(th.getAttribute("aria-sort")).toBe("none");
    expect(cellTexts(container)).toEqual(["3", "1", "2"]);

    fireEvent.click(button);
    expect(th.getAttribute("aria-sort")).toBe("ascending");
    expect(th.className).toContain("sf-table__head--sorted-asc");
    expect(cellTexts(container)).toEqual(["1", "2", "3"]);

    fireEvent.click(button);
    expect(th.getAttribute("aria-sort")).toBe("descending");
    expect(cellTexts(container)).toEqual(["3", "2", "1"]);

    fireEvent.click(button);
    expect(th.getAttribute("aria-sort")).toBe("none");
    expect(cellTexts(container)).toEqual(["3", "1", "2"]); // back to the original, unsorted order
  });
});

describe("globalFilter", () => {
  test("narrows rows and ranks a better match above a worse one", () => {
    const rows = [{ name: "banana" }, { name: "apple" }, { name: "grape" }];
    const { container, rerender } = render(<LiveTableView rows={rows} />);
    expect(cellTexts(container)).toEqual(["banana", "apple", "grape"]);

    rerender(<LiveTableView rows={rows} globalFilter="ap" />);
    // "banana" has no "ap" substring at all -- excluded. "apple" STARTS_WITH "ap" (a better rank
    // than "grape", which only CONTAINS it) -- ranked first.
    expect(cellTexts(container)).toEqual(["apple", "grape"]);
  });
});

describe("columnFilters", () => {
  test("a per-column filter narrows independently of the others", () => {
    const rows = [
      { a: "foo", b: "x" },
      { a: "bar", b: "y" },
    ];
    const { container } = render(<LiveTableView rows={rows} columnFilters />);
    expect(cellTexts(container)).toEqual(["foo", "x", "bar", "y"]);

    fireEvent.change(screen.getByLabelText("Filter a"), { target: { value: "fo" } });
    expect(cellTexts(container)).toEqual(["foo", "x"]); // column b's filter stayed empty, untouched
  });
});

describe("column visibility + onColumnStateChange", () => {
  test("initialHiddenColumns hides a column, and the callback reports full order + hidden", () => {
    const rows = [{ a: 1, b: 2 }];
    const calls: Array<{ order: string[]; hidden: string[] }> = [];
    const { container } = render(
      <LiveTableView rows={rows} initialHiddenColumns={["b"]} onColumnStateChange={(s) => calls.push(s)} />,
    );

    const headers = Array.from(container.querySelectorAll("th")).map((th) => th.textContent);
    expect(headers).toEqual(["a"]); // b hidden from the DOM entirely, not just visually

    expect(calls.length).toBeGreaterThan(0);
    const last = calls[calls.length - 1];
    expect(last?.order).toEqual(["a", "b"]); // order lists every real column, hidden or not
    expect(last?.hidden).toEqual(["b"]);
  });
});

describe("reorderable", () => {
  test("a drop event reorders columns and fires onColumnStateChange", () => {
    const rows = [{ a: 1, b: 2 }];
    const calls: Array<{ order: string[]; hidden: string[] }> = [];
    const { container } = render(<LiveTableView rows={rows} reorderable onColumnStateChange={(s) => calls.push(s)} />);

    const ths = () => Array.from(container.querySelectorAll("th"));
    expect(ths().map((th) => th.textContent)).toEqual(["a", "b"]);

    const [thA, thB] = ths();
    fireEvent.dragStart(thB!);
    expect(thB!.className).toContain("sf-table__head--dragging");
    fireEvent.dragOver(thA!);
    fireEvent.drop(thA!);

    expect(ths().map((th) => th.textContent)).toEqual(["b", "a"]);
    const last = calls[calls.length - 1];
    expect(last?.order).toEqual(["b", "a"]);
  });
});

describe("virtual", () => {
  test("renders a windowed subset without crashing", () => {
    const rows = Array.from({ length: 1000 }, (_, i) => ({ id: i, val: `row-${i}` }));

    withMockedOffsetHeight(200, () => {
      const { container } = render(<LiveTableView rows={rows} virtual maxHeight={200} />);

      const scrollBox = container.querySelector(".sf-table__scroll") as HTMLElement | null;
      expect(scrollBox).toBeTruthy();
      expect(scrollBox?.style.maxHeight).toBe("200px");
      expect(scrollBox?.style.overflow).toBe("auto");

      // With a mocked 200px viewport and the ~28px row-height default, this is a real windowed
      // subset, not merely "rendered something" -- see the file doc comment for why the mock has to
      // be in place before mount for this assertion to be honest under happy-dom.
      const renderedRows = container.querySelectorAll("tr.sf-table__row").length;
      expect(renderedRows).toBeGreaterThan(0);
      expect(renderedRows).toBeLessThan(1000);
    });

    // UNVERIFIED under happy-dom: real-browser scroll behavior (that scrolling the box swaps which
    // rows are in the window) is not exercised here -- happy-dom has no scroll/layout loop to drive
    // it, and StreamView's own suite documents the same limitation for its scroll-pinning logic.
  });
});

describe("flashKeys", () => {
  // The wiring that matters: `flashKeys` carries the Z-set's CANONICAL keys (what useLiveTable's
  // flashKeys and LiveTable's `touched` both speak), not the React reconciliation key rowKey()
  // builds. This asserts LiveTableView resolves one against the other via canonicalKey(), which is
  // the only reason a host's CSS animation lands on the row that actually changed.
  test("marks only the rows whose canonical key is flashed", () => {
    const rows = [{ sym: "AAPL", px: 1 }, { sym: "MSFT", px: 2 }];
    const { container } = render(<LiveTableView rows={rows} flashKeys={new Set([canonicalKey(rows[1]!)])} />);

    const flashed = Array.from(container.querySelectorAll("tr.sf-table__row--flash"));
    expect(flashed).toHaveLength(1);
    expect(flashed[0]?.textContent).toContain("MSFT");
    expect(container.querySelectorAll("tr.sf-table__row")).toHaveLength(2);
  });

  test("an empty or absent flash set marks nothing", () => {
    const rows = [{ sym: "AAPL" }];
    const { container, rerender } = render(<LiveTableView rows={rows} flashKeys={new Set()} />);
    expect(container.querySelectorAll("tr.sf-table__row--flash")).toHaveLength(0);
    rerender(<LiveTableView rows={rows} />);
    expect(container.querySelectorAll("tr.sf-table__row--flash")).toHaveLength(0);
  });
});
