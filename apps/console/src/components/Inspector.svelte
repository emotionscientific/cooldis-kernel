<script lang="ts">
  import { untrack } from "svelte";
  import { app } from "../lib/app.svelte";
  import type {
    ThreadApprovalEntry,
    ThreadDebugExportResponse,
    ThreadEnvelope,
    ThreadEnvelopeBinding,
  } from "../lib/schema";
  import AgentManifestEditor from "./AgentManifestEditor.svelte";
  import Icon from "./Icon.svelte";

  const sel = $derived(app.selectedEntity);
  const thread = $derived(
    app.mode === "chat" && app.activeTab?.kind === "chat"
      ? app.threads.find((t) => t.id === app.activeTab?.threadId)
      : undefined,
  );
  // The bind receipt is the witness for "what can this thread actually do" —
  // rendered from the event, never inferred from the manifest.
  const threadId = $derived(thread?.id);
  const envelope = $derived(threadId ? app.threadEnvelopes[threadId] : undefined);
  const envelopeLoaded = $derived(threadId ? threadId in app.threadEnvelopes : false);
  const envelopeError = $derived(
    threadId && app.threadEnvelopeErrorThreadId === threadId ? app.loadErrors.threadEnvelope : undefined,
  );
  const couplings = $derived(threadId ? app.threadCouplings[threadId] : undefined);
  const approvals = $derived(threadId ? app.threadApprovals[threadId] : undefined);
  const waiting = $derived(threadId ? app.threadWaiting[threadId] : undefined);
  const debugExport = $derived(threadId ? app.threadDebugExports[threadId] : undefined);
  $effect(() => {
    const id = threadId;
    if (!id || !app.connected) return;
    untrack(() => {
      void app.ensureThreadEnvelope(id);
      void app.loadThreadControlSurfaces(id);
    });
  });
  let resolvingApproval = $state("");
  let copiedDebugExport = $state(false);
  function shortHash(hash: string) {
    const normalized = hash.replace(/^sha256:/, "");
    return normalized ? normalized.slice(0, 12) : "—";
  }
  function shortId(value: string | null | undefined) {
    if (!value) return "—";
    return value.length > 12 ? value.slice(0, 12) : value;
  }
  function prettyJson(value: unknown) {
    if (typeof value === "string") return value;
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }
  async function decideApproval(entry: ThreadApprovalEntry, decision: "approved" | "denied") {
    if (!threadId) return;
    resolvingApproval = `${entry.approvalId}:${decision}`;
    try {
      await app.resolveApproval(threadId, entry.approvalId, decision, "console");
    } finally {
      resolvingApproval = "";
    }
  }
  async function copyDebugExport(bundle: ThreadDebugExportResponse | undefined) {
    if (!bundle || typeof navigator === "undefined" || !navigator.clipboard) return;
    await navigator.clipboard.writeText(prettyJson(bundle));
    copiedDebugExport = true;
    window.setTimeout(() => {
      copiedDebugExport = false;
    }, 1200);
  }
  function modelProfileLabel(value: ThreadEnvelope) {
    const model = value.modelId && value.providerId ? `${value.modelId}@${value.providerId}` : value.modelId || value.providerId;
    return [value.modelProfileId, model].filter(Boolean).join(" · ") || "—";
  }
  function bindingName(binding: ThreadEnvelopeBinding) {
    return binding.name || "unnamed binding";
  }
  function bindingTitle(binding: ThreadEnvelopeBinding) {
    return binding.artifactHash ? `${bindingName(binding)}@${binding.artifactHash}` : bindingName(binding);
  }
  function directToolTitle(direct: { toolName: string; operation: string }) {
    return direct.operation ? `direct tool for ${direct.operation}` : "direct tool";
  }
  const tool = $derived(
    app.mode === "registry" && sel?.kind === "tool" ? app.tools.find((t) => t.id === sel.id) : undefined,
  );
  const manifest = $derived(
    app.mode === "registry" && sel?.kind === "manifest" ? app.manifests.find((m) => m.id === sel.id) : undefined,
  );
  const editableAgentRef = $derived(thread && envelope ? envelope.refUri : manifest?.id ?? "");
  let agentEditorEditing = $state(false);
  let agentEditorCancelSignal = $state(0);
  let activeAgentRef = $state("");
  $effect(() => {
    const ref = editableAgentRef;
    if (ref === activeAgentRef) return;
    activeAgentRef = ref;
    agentEditorEditing = false;
  });
  function toggleAgentEditor() {
    if (agentEditorEditing) {
      agentEditorCancelSignal += 1;
    } else {
      agentEditorEditing = true;
    }
  }
</script>

<div class="pane-fill inspector">
  <div class="insp-head">
    <Icon name="CircleDot" size={13} />
    <h2>Inspector</h2>
    {#if editableAgentRef}
      <button
        class="btn insp-edit-btn"
        class:is-editing={agentEditorEditing}
        title={agentEditorEditing ? "Cancel editing agent definition" : "Edit agent definition"}
        onclick={toggleAgentEditor}
      >
        <Icon name={agentEditorEditing ? "X" : "FileText"} size={13} />
        {agentEditorEditing ? "Cancel" : "Edit"}
      </button>
    {/if}
  </div>
  <div class="insp-scroll">
    {#if thread}
      {#if envelope}
        <div class="insp-section agent-definition-section">
          <div class="insp-section-head">
            <h4>Agent Definition</h4>
            {#if agentEditorEditing}<span class="insp-section-state">editing</span>{/if}
          </div>
          <AgentManifestEditor
            agentRef={envelope.refUri}
            threadId={thread.id}
            allowContinue={true}
            threadBusy={thread.status !== "idle" || Boolean(app.activeTab?.busy)}
            chrome="inline"
            bind:editing={agentEditorEditing}
            cancelSignal={agentEditorCancelSignal}
          />
        </div>
      {/if}
      <div class="insp-section">
        <h4>Thread</h4>
        <dl class="kv">
          <dt>ID</dt><dd class="mono">{thread.id}</dd>
          <dt>Model</dt><dd class="mono">{thread.model}</dd>
          <dt>Provider</dt><dd>{thread.provider}</dd>
          <dt>Status</dt><dd>{thread.status}</dd>
          {#if thread.thinking}<dt>Thinking</dt><dd class="mono">{thread.thinking}</dd>{/if}
          {#if thread.parentId}<dt>Parent</dt><dd class="mono">{thread.parentId}</dd>{/if}
        </dl>
      </div>
      <div class="insp-section">
        <h4>Envelope</h4>
        {#if envelope}
          <dl class="kv">
            <dt>Manifest</dt><dd class="mono" title={envelope.manifestHash}>{shortHash(envelope.manifestHash)}</dd>
            <dt>Profile</dt><dd class="mono" title={modelProfileLabel(envelope)}>{modelProfileLabel(envelope)}</dd>
            <dt>Cwd</dt>
            <dd class="mono env-cwd" title={envelope.effectiveCwd || undefined}>
              <span class="env-cwd-path">{envelope.effectiveCwd || "—"}</span>{#if envelope.overriddenKeys.includes("default_cwd")}
                <span class="chip muted" title="Overridden on thread/start">override</span>{/if}
            </dd>
            {#if envelope.turnTimeoutMs !== undefined}<dt>Turn limit</dt><dd>{envelope.turnTimeoutMs}ms</dd>{/if}
          </dl>
        {:else if envelopeError}
          <p class="env-note danger">{envelopeError}</p>
        {:else if envelopeLoaded}
          <p class="env-note">No bind receipt — this thread predates manifest lineage.</p>
        {:else if app.connected}
          <p class="env-note">Loading bind receipt…</p>
        {:else}
          <p class="env-note">Connect to load the bind receipt.</p>
        {/if}
      </div>
      {#if envelope}
        <div class="insp-section">
          <h4>Tools ({envelope.operationBindings.length ? envelope.operationBindings.length : "none"})</h4>
          {#each envelope.operationBindings as binding, index (index)}
            <div class="env-binding">
              <div class="mono env-binding-name" title={bindingTitle(binding)}>
                {bindingName(binding)}{#if binding.artifactHash}<span class="env-hash">@{shortHash(binding.artifactHash)}</span>{/if}
              </div>
              <div class="env-pills">
                {#each binding.operations.length ? binding.operations : ["whole record"] as op}
                  <span class="pill mono" title={op}>{op}</span>
                {/each}
                {#each binding.directTools as direct}
                  <span class="pill mono" title={directToolTitle(direct)}>{direct.toolName || "unnamed direct tool"}</span>
                {/each}
              </div>
            </div>
          {:else}
            <p class="env-note">No operation-backed tool rows bound.</p>
          {/each}
        </div>
      {/if}
      <div class="insp-section">
        <div class="insp-section-head">
          <h4>Control Plane</h4>
          {#if threadId}
            <button class="btn ghost mini" onclick={() => void app.loadThreadControlSurfaces(threadId)}>
              <Icon name="RefreshCcw" size={12} />
              Refresh
            </button>
          {/if}
        </div>

        <div class="control-card">
          <div class="control-card-head">
            <span><Icon name="Boxes" size={13} /> Couplings</span>
            <span class="chip muted">{couplings?.data.length ?? 0}</span>
          </div>
          {#if app.loadErrors.threadCouplings}
            <p class="env-note danger">{app.loadErrors.threadCouplings}</p>
          {:else if app.connected && !couplings}
            <p class="env-note">Loading couplings...</p>
          {:else}
            {#if couplings?.agentRef || couplings?.manifestHash}
              <dl class="kv compact">
                <dt>Agent</dt><dd class="mono">{couplings.agentRef ?? "—"}</dd>
                <dt>Manifest</dt><dd class="mono">{couplings.manifestHash ? shortHash(couplings.manifestHash) : "—"}</dd>
                <dt>Bind</dt><dd class="mono">{shortId(couplings.bindEventId)}</dd>
              </dl>
            {/if}
            {#each couplings?.data ?? [] as row (row.id)}
              <details class="control-row">
                <summary>
                  <span class="mono" title={row.id}>{row.id}</span>
                  <span class="chip muted">{row.role}</span>
                </summary>
                <dl class="kv compact">
                  <dt>Trigger</dt><dd class="mono">{row.triggerKind}</dd>
                  <dt>Function</dt><dd class="mono">{row.operationName ?? row.functionRef}</dd>
                  <dt>Artifact</dt><dd class="mono">{shortHash(row.artifactHash)}</dd>
                </dl>
                {#if row.sourceStreams.length}
                  <div class="env-pills">{#each row.sourceStreams as stream}<span class="pill mono">{stream}</span>{/each}</div>
                {/if}
                {#if row.sourceKinds.length}
                  <div class="env-pills">{#each row.sourceKinds as kind}<span class="pill mono">{kind}</span>{/each}</div>
                {/if}
                <pre class="control-json mono">{prettyJson(row)}</pre>
              </details>
            {:else}
              <p class="env-note">No bound couplings.</p>
            {/each}
          {/if}
        </div>

        <div class="control-card">
          <div class="control-card-head">
            <span><Icon name="KeyRound" size={13} /> Approvals</span>
            <span class="chip muted">{approvals?.data.length ?? 0}</span>
          </div>
          {#if app.loadErrors.threadApprovals}
            <p class="env-note danger">{app.loadErrors.threadApprovals}</p>
          {:else if app.connected && !approvals}
            <p class="env-note">Loading approvals...</p>
          {:else}
            {#each approvals?.data ?? [] as entry (entry.approvalId)}
              <div class="control-row flat">
                <div class="control-row-title">
                  <span class="mono" title={entry.approvalId}>{entry.approvalId}</span>
                  <span class="chip muted">{entry.status}</span>
                </div>
                <dl class="kv compact">
                  <dt>Turn</dt><dd class="mono">{shortId(entry.turnId)}</dd>
                  <dt>Call</dt><dd class="mono">{shortId(entry.callId)}</dd>
                  {#if entry.reason}<dt>Reason</dt><dd>{entry.reason}</dd>{/if}
                </dl>
                <div class="control-actions">
                  <button class="btn mini primary" disabled={Boolean(resolvingApproval)} onclick={() => void decideApproval(entry, "approved")}>
                    {resolvingApproval === `${entry.approvalId}:approved` ? "Approving..." : "Approve"}
                  </button>
                  <button class="btn mini danger" disabled={Boolean(resolvingApproval)} onclick={() => void decideApproval(entry, "denied")}>
                    {resolvingApproval === `${entry.approvalId}:denied` ? "Denying..." : "Deny"}
                  </button>
                </div>
              </div>
            {:else}
              <p class="env-note">No pending approvals.</p>
            {/each}
          {/if}
        </div>

        <div class="control-card">
          <div class="control-card-head">
            <span><Icon name="CirclePause" size={13} /> Waiting</span>
            <span class="chip muted">{waiting?.data.length ?? 0}</span>
          </div>
          {#if app.loadErrors.threadWaiting}
            <p class="env-note danger">{app.loadErrors.threadWaiting}</p>
          {:else if app.connected && !waiting}
            <p class="env-note">Loading waiting state...</p>
          {:else}
            {#each waiting?.data ?? [] as entry, index (entry.eventId || `${entry.kind}-${index}`)}
              <details class="control-row">
                <summary>
                  <span class="mono">{entry.kind}</span>
                  <span class="chip muted">{entry.continuation ?? "waiting"}</span>
                </summary>
                <dl class="kv compact">
                  <dt>Event</dt><dd class="mono">{shortId(entry.eventId)}</dd>
                  <dt>Turn</dt><dd class="mono">{shortId(entry.turnId)}</dd>
                  <dt>Call</dt><dd class="mono">{shortId(entry.callId)}</dd>
                  {#if entry.approvalId}<dt>Approval</dt><dd class="mono">{shortId(entry.approvalId)}</dd>{/if}
                  {#if entry.reason}<dt>Reason</dt><dd>{entry.reason}</dd>{/if}
                </dl>
                <pre class="control-json mono">{prettyJson(entry)}</pre>
              </details>
            {:else}
              <p class="env-note">No waiting turns.</p>
            {/each}
          {/if}
        </div>
      </div>

      <div class="insp-section">
        <div class="insp-section-head">
          <h4>Debug Export</h4>
          <div class="control-actions">
            {#if threadId}
              <button class="btn ghost mini" onclick={() => void app.loadThreadDebugExport(threadId)}>
                <Icon name="RefreshCcw" size={12} />
                Refresh
              </button>
            {/if}
            <button class="btn ghost mini" disabled={!debugExport} onclick={() => void copyDebugExport(debugExport)}>
              {copiedDebugExport ? "Copied" : "Copy"}
            </button>
          </div>
        </div>
        {#if app.loadErrors.threadDebugExport}
          <p class="env-note danger">{app.loadErrors.threadDebugExport}</p>
        {:else if app.connected && !debugExport}
          <p class="env-note">Loading export bundle...</p>
        {:else if debugExport}
          <dl class="kv compact">
            <dt>Schema</dt><dd class="mono">{debugExport.schema}</dd>
            <dt>Streams</dt><dd>{debugExport.streams.length}</dd>
            <dt>Receipts</dt><dd>{debugExport.receipts.length}</dd>
            <dt>Backend</dt><dd class="mono">{typeof debugExport.backend.kind === "string" ? debugExport.backend.kind : "—"}</dd>
          </dl>
          {#each debugExport.streams as stream (stream.streamId)}
            <div class="control-row flat">
              <div class="control-row-title">
                <span class="mono" title={stream.streamId}>{stream.selector}</span>
                <span class:danger={stream.truncated} class="chip muted">{stream.eventCount}</span>
              </div>
              <dl class="kv compact">
                <dt>Stream</dt><dd class="mono">{stream.streamId}</dd>
                {#if stream.streamCursor}<dt>Cursor</dt><dd class="mono">{shortId(stream.streamCursor.event_id)}</dd>{/if}
              </dl>
            </div>
          {/each}
          <details class="control-row">
            <summary>
              <span>Bundle JSON</span>
              <span class="chip muted">redacted</span>
            </summary>
            <pre class="control-json mono">{prettyJson(debugExport)}</pre>
          </details>
        {:else}
          <p class="env-note">No export bundle loaded.</p>
        {/if}
      </div>

      <div class="insp-section">
        <h4>Subthreads</h4>
        {#each app.threads.filter((t) => t.parentId === thread.id) as c}
          <button class="row-item" onclick={() => app.openThread(c)}>
            <Icon name="GitBranch" size={13} />
            <span class="title">{c.title.replace(/^↳\s*/, "")}</span>
          </button>
        {:else}
          <p style="color:var(--tx-faint);font-size:12px;margin:0">No subthreads.</p>
        {/each}
      </div>
    {:else if tool}
      <div class="insp-section">
        <h4>Tool</h4>
        <dl class="kv">
          <dt>Name</dt><dd class="mono">{tool.name}</dd>
          <dt>Artifact</dt><dd class="mono">{tool.version || "—"}</dd>
          <dt>ABI power</dt><dd>{tool.power}</dd>
          <dt>Operations</dt><dd>{tool.inputs.join(", ")}</dd>
          <dt>Source</dt><dd class="mono">{tool.source}</dd>
        </dl>
      </div>
    {:else if manifest}
      <div class="insp-section agent-definition-section">
        <div class="insp-section-head">
          <h4>Agent Definition</h4>
          {#if agentEditorEditing}<span class="insp-section-state">editing</span>{/if}
        </div>
        <AgentManifestEditor
          agentRef={manifest.id}
          chrome="inline"
          bind:editing={agentEditorEditing}
          cancelSignal={agentEditorCancelSignal}
        />
      </div>
    {:else}
      <div class="insp-section">
        <h4>Session</h4>
        <dl class="kv">
          <dt>Endpoint</dt><dd class="mono">{app.endpoint.replace("ws://", "")}</dd>
          <dt>State</dt><dd>{app.connected ? "connected" : app.status}</dd>
          {#if app.connected}
            <dt>Models</dt><dd>{app.modelInventoryLabel}</dd>
            {#if app.runtimeCwd}<dt>Workspace</dt><dd class="mono">{app.runtimeCwd}</dd>{/if}
            {#if app.healthRttLabel}<dt>Health RTT</dt><dd>{app.healthRttLabel}</dd>{/if}
          {/if}
        </dl>
      </div>
    {/if}
  </div>
</div>
