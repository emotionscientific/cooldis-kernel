<script lang="ts">
  import { app } from "../../lib/app.svelte";
  import Icon from "../Icon.svelte";

  const atRoot = $derived(!app.browsePath || app.browsePath === app.resourceRoot);
  const crumb = $derived(
    app.browsePath && app.resourceRoot && app.browsePath !== app.resourceRoot
      ? app.browsePath.slice(app.resourceRoot.length).replace(/^\//, "")
      : "",
  );
</script>

<div class="view">
  <div class="view-toolbar">
    <h1>Workspace</h1>
    <span class="sub mono">{app.resourceRoot ?? "no workspace root reported"}{crumb ? ` / ${crumb}` : ""}</span>
    <div style="flex:1"></div>
    {#if !atRoot}
      <button class="btn" onclick={app.browseUp}><Icon name="CornerLeftUp" size={13} /> Up</button>
    {/if}
  </div>
  <div class="view-scroll">
    <table class="table">
      <thead>
        <tr><th>Name</th><th>Kind</th></tr>
      </thead>
      <tbody>
        {#each app.resources as r (r.path)}
          <tr
            onclick={() => (r.kind === "file" ? app.openFile(r.path) : app.browse(r.path))}
            style="cursor:pointer"
          >
            <td>
              <span style="display:inline-flex;gap:7px;align-items:center">
                <Icon name={r.kind === "dir" ? "Folder" : "FileCode"} size={14} />
                <span class="strong mono">{r.name}</span>
              </span>
            </td>
            <td><span class="chip muted">{r.kind}</span></td>
          </tr>
        {:else}
          <tr>
            <td colspan="2">
              <div class="empty small">
                <span class="ic"><Icon name="FolderTree" size={18} /></span>
                <p>
                  {#if !app.connected}
                    Connect to a Cooldis app-server to browse its workspace files.
                  {:else if app.loadErrors.resources}
                    {app.loadErrors.resources}
                  {:else if !app.resourceRoot}
                    The runtime did not report a workspace root.
                  {:else}
                    This directory is empty.
                  {/if}
                </p>
              </div>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  </div>
</div>
