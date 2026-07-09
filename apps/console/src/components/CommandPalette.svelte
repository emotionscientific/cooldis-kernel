<script lang="ts">
  import { Command, Dialog } from "bits-ui";
  import { app } from "../lib/app.svelte";
  import { MODES } from "../lib/schema";
  import Icon from "./Icon.svelte";

  function run(fn: () => void) {
    fn();
    app.paletteOpen = false;
  }
</script>

<Dialog.Root bind:open={app.paletteOpen}>
  <Dialog.Portal>
    <Dialog.Overlay class="cmdk-overlay" />
    <Dialog.Content class="cmdk" aria-label="Command palette">
      <Dialog.Title class="sr-only">Command palette</Dialog.Title>
      <Dialog.Description class="sr-only">Search and run commands</Dialog.Description>
      <Command.Root loop>
        <div class="cmdk-input-row">
          <Icon name="Search" size={16} />
          <Command.Input placeholder="Jump to a thread, tool, or agent…" />
          <span class="kbd">esc</span>
        </div>
        <Command.List>
          <Command.Empty>No matches.</Command.Empty>

          <Command.Group>
            <Command.GroupHeading>Navigate</Command.GroupHeading>
            <Command.GroupItems>
              {#each MODES as m}
                <Command.Item value={`go ${m.label}`} keywords={[m.label]} onSelect={() => run(() => app.setMode(m.id))}>
                  <Icon name={m.icon} size={15} />
                  <span class="ci-label">{m.label}</span>
                  {#if m.key}<span class="ci-meta">⌘{m.key}</span>{/if}
                </Command.Item>
              {/each}
            </Command.GroupItems>
          </Command.Group>

          <Command.Group>
            <Command.GroupHeading>Threads</Command.GroupHeading>
            <Command.GroupItems>
              {#each app.threads as t}
                <Command.Item
                  value={`thread ${t.title} ${t.id}`}
                  keywords={[t.model, t.provider]}
                  onSelect={() => run(() => app.openThread(t))}
                >
                  <Icon name="MessagesSquare" size={15} />
                  <span class="ci-label">{t.title.replace(/^↳\s*/, "")}</span>
                  <span class="ci-meta">{t.model}</span>
                </Command.Item>
              {/each}
            </Command.GroupItems>
          </Command.Group>

          <Command.Group>
            <Command.GroupHeading>Registry</Command.GroupHeading>
            <Command.GroupItems>
              {#each app.tools as t}
                <Command.Item
                  value={`tool ${t.name}`}
                  keywords={[t.summary]}
                  onSelect={() =>
                    run(() => {
                      app.setMode("registry");
                      app.selectedEntity = { kind: "tool", id: t.id };
                    })}
                >
                  <Icon name="Wrench" size={15} />
                  <span class="ci-label">{t.name}</span>
                </Command.Item>
              {/each}
              {#each app.manifests as m}
                <Command.Item
                  value={`agent ${m.name} ${m.id}`}
                  keywords={[m.name, m.summary, m.id]}
                  onSelect={() =>
                    run(() => {
                      app.setMode("registry");
                      app.selectedEntity = { kind: "manifest", id: m.id };
                    })}
                >
                  <Icon name="Bot" size={15} />
                  <span class="ci-label">{m.name}</span>
                </Command.Item>
              {/each}
            </Command.GroupItems>
          </Command.Group>

          <Command.Group>
            <Command.GroupHeading>Actions</Command.GroupHeading>
            <Command.GroupItems>
              <Command.Item value="new thread" onSelect={() => run(() => void app.newThreadFromDefault())}>
                <Icon name="Plus" size={15} />
                <span class="ci-label">New thread</span>
              </Command.Item>
              {#each app.manifests as m}
                <Command.Item
                  value={`new thread with ${m.name} ${m.id}`}
                  keywords={[m.name, m.summary, m.id]}
                  onSelect={() => run(() => void app.newThread(m.id))}
                >
                  <Icon name="Bot" size={15} />
                  <span class="ci-label">New thread with {m.name}</span>
                  <span class="ci-meta">v{m.version}</span>
                </Command.Item>
              {/each}
              <Command.Item value="toggle connection" onSelect={() => run(() => app.toggleConnection())}>
                <Icon name={app.connected ? "Power" : "Plug"} size={15} />
                <span class="ci-label">{app.connected ? "Disconnect" : "Connect"} app-server</span>
              </Command.Item>
            </Command.GroupItems>
          </Command.Group>
        </Command.List>
      </Command.Root>
    </Dialog.Content>
  </Dialog.Portal>
</Dialog.Root>
