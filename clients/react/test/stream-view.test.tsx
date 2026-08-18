/**
 * StreamView -- one runnable check per piece of non-trivial behavior (this is a ponytail repo, see
 * react.test.tsx's own doc comment for the house style this mirrors).
 *
 * happy-dom does not run a real layout engine, so `scrollHeight`/`clientHeight`/`scrollTop` are
 * inert on a plain element (no box model, no scrolling). `mockMetrics` below overrides them with
 * `Object.defineProperty(..., { writable: true })` so the component's own `el.scrollTop = ...`
 * assignments behave like a real scrollable box would -- this SIMULATES layout, it does not measure
 * anything happy-dom itself computed.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { act, cleanup, render } from "@testing-library/react";
import { StreamView } from "../src/index.js";

afterEach(cleanup);

function mockMetrics(el: HTMLElement, metrics: { scrollHeight: number; clientHeight: number; scrollTop: number }): void {
  Object.defineProperty(el, "scrollHeight", { value: metrics.scrollHeight, configurable: true, writable: true });
  Object.defineProperty(el, "clientHeight", { value: metrics.clientHeight, configurable: true, writable: true });
  Object.defineProperty(el, "scrollTop", { value: metrics.scrollTop, configurable: true, writable: true });
}

function scrollBox(container: HTMLElement): HTMLDivElement {
  const el = container.querySelector(".sf-stream");
  if (!el) throw new Error("no .sf-stream root rendered");
  return el as HTMLDivElement;
}

describe("StreamView", () => {
  test("renders children inside a scroll box carrying the height cap", () => {
    const { container, getByText } = render(
      <StreamView maxHeight={200}>
        <p>hello</p>
      </StreamView>,
    );
    const el = scrollBox(container);
    expect(el.style.maxHeight).toBe("200px");
    expect(el.style.overflow).toBe("auto");
    expect(getByText("hello")).toBeTruthy();
  });

  test("root class is sf-stream, following class is added while pinned, and a custom className is appended not replaced", () => {
    const { container } = render(<StreamView className="my-box">a</StreamView>);
    const el = scrollBox(container);
    expect(el.classList.contains("sf-stream")).toBe(true);
    expect(el.classList.contains("sf-stream--following")).toBe(true); // follow defaults true, so mount starts pinned
    expect(el.classList.contains("my-box")).toBe(true);
  });

  test("appending content while pinned at the newest edge keeps the scroll pinned to that edge", () => {
    const { container, rerender } = render(
      <StreamView maxHeight={100}>
        <div>a</div>
      </StreamView>,
    );
    const el = scrollBox(container);
    // Content exactly fills the box -- already "at edge" (scrollHeight - clientHeight - scrollTop = 0).
    mockMetrics(el, { scrollHeight: 100, clientHeight: 100, scrollTop: 0 });
    rerender(
      <StreamView maxHeight={100}>
        <div>a</div>
        <div>b</div>
      </StreamView>,
    );

    // Simulate the box growing past its cap as content streams in.
    el.scrollHeight = 250;
    rerender(
      <StreamView maxHeight={100}>
        <div>a</div>
        <div>b</div>
        <div>c</div>
      </StreamView>,
    );

    expect(el.scrollTop).toBe(150); // scrollHeight(250) - clientHeight(100) -- pinned to the new bottom edge
  });

  test("after the user scrolls away from the edge, appending content does not move the scroll position", () => {
    const { container, rerender } = render(
      <StreamView maxHeight={100}>
        <div>a</div>
      </StreamView>,
    );
    const el = scrollBox(container);
    mockMetrics(el, { scrollHeight: 250, clientHeight: 100, scrollTop: 150 }); // starts pinned at the bottom edge

    el.scrollTop = 40; // user scrolls up, well outside EDGE_TOLERANCE_PX
    // A bare property assignment fires no 'scroll' event on its own -- this is the browser event a
    // real drag/wheel gesture would produce. It triggers a setState (following -> false), so it
    // needs act() same as any other state-updating event in a test.
    act(() => {
      el.dispatchEvent(new Event("scroll"));
    });

    el.scrollHeight = 400; // more content arrives
    rerender(
      <StreamView maxHeight={100}>
        <div>a</div>
        <div>b</div>
      </StreamView>,
    );

    expect(el.scrollTop).toBe(40); // following stopped -- the append must not have touched scroll position
  });

  test('newest: "top" pins the scroll position at scrollTop 0', () => {
    const { container, rerender } = render(
      <StreamView maxHeight={100} newest="top">
        <div>a</div>
      </StreamView>,
    );
    const el = scrollBox(container);
    mockMetrics(el, { scrollHeight: 100, clientHeight: 100, scrollTop: 0 }); // fits exactly -- at edge

    el.scrollHeight = 300; // a new item prepended, box grew
    el.scrollTop = 50; // simulate the browser having left old content in view before we correct it
    rerender(
      <StreamView maxHeight={100} newest="top">
        <div>b</div>
        <div>a</div>
      </StreamView>,
    );

    expect(el.scrollTop).toBe(0);
  });
});
