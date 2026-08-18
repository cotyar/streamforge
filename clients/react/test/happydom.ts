/**
 * Registers happy-dom's DOM globals and flips on the React "act" test-environment flag before any
 * test file (or the React/ReactDOM/@testing-library/react modules it imports) loads.
 *
 * Order matters and is the whole reason this lives in a preload rather than at the top of
 * react.test.tsx: ES module imports are hoisted and evaluated before the importing module's own
 * top-level statements run, so `import { GlobalRegistrator } ...; GlobalRegistrator.register();`
 * placed at the top of the test file would still run AFTER "@testing-library/react" (and the
 * react-dom it pulls in, which needs `document` to exist) had already been imported and initialized
 * without a DOM. A bun test preload (see ../bunfig.toml) runs this file as its own module turn
 * before the test file is loaded at all, so both the DOM globals and the act-environment flag are
 * in place before React ever sees them.
 */
import { GlobalRegistrator } from "@happy-dom/global-registrator";

GlobalRegistrator.register();

// @testing-library/react (v14+) reads this global to know it's safe to auto-wrap effects/updates
// in act() -- without it, every state update from an effect (which is most of what this package's
// hooks do) prints a "not wrapped in act(...)" warning.
(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT?: boolean }).IS_REACT_ACT_ENVIRONMENT = true;
