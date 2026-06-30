import App from "./App.svelte";
import "./styles.css";
import { mount } from "svelte";
import { initDesktop } from "./lib/desktop";

const app = mount(App, {
  target: document.getElementById("app")!,
});

// Attach to the Electrobun host if we're running natively (no-op in a browser).
void initDesktop();

export default app;
