// @vitest-environment jsdom
/* eslint-disable react/react-in-jsx-scope */

import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, it } from "vitest";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "./tabs";

(
  globalThis as { IS_REACT_ACT_ENVIRONMENT?: boolean }
).IS_REACT_ACT_ENVIRONMENT = true;

describe("Tabs", () => {
  let root: Root | undefined;

  afterEach(() => act(() => root?.unmount()));

  it("links controls to their content and changes selection with the keyboard", () => {
    const container = document.createElement("div");
    root = createRoot(container);

    act(() =>
      root!.render(
        <Tabs defaultValue="overview">
          <TabsList aria-label="Database views">
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="metrics">Metrics</TabsTrigger>
          </TabsList>
          <TabsContent value="overview">Overview content</TabsContent>
          <TabsContent value="metrics">Metrics content</TabsContent>
        </Tabs>,
      ),
    );

    const [overview, metrics] = Array.from(
      container.querySelectorAll<HTMLButtonElement>('[role="tab"]'),
    );
    expect(overview.getAttribute("aria-selected")).toBe("true");
    expect(container.textContent).toContain("Overview content");
    expect(container.textContent).not.toContain("Metrics content");

    act(() =>
      overview.dispatchEvent(
        new KeyboardEvent("keydown", { bubbles: true, key: "ArrowRight" }),
      ),
    );

    expect(metrics.getAttribute("aria-selected")).toBe("true");
    expect(metrics.getAttribute("aria-controls")).toBeTruthy();
    expect(container.textContent).toContain("Metrics content");
    expect(container.textContent).not.toContain("Overview content");
  });
});
