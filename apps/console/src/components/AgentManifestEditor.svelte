<script lang="ts">
  import { untrack } from "svelte";
  import {
    booleanValue,
    cloneRecord,
    ensureArray,
    ensureArrayObject,
    ensureObject,
    errorMessage,
    isOperationBacked,
    manifestFromRecord,
    manifestHash,
    numberValue,
    operationRef,
    recordArray,
    recordAt,
    recordObject,
    sanitizeRecordName,
    setOptional,
    splitList,
    stringArray,
    stringValue,
    toolKindLabel,
    type ManifestRecord,
  } from "../lib/agentManifestDraft";
  import { app } from "../lib/app.svelte";
  import type { AgentPlanResponse, PublishedAgentRecord } from "../lib/schema";
  import Icon from "./Icon.svelte";

  type EditorTab = "fields" | "tools" | "runtime" | "source";
  type PlanMode = "manifest" | "source";

  let {
    agentRef,
    threadId = "",
    allowContinue = false,
    threadBusy = false,
    chrome = "card",
    editing = $bindable(false),
    cancelSignal = 0,
  }: {
    agentRef: string;
    threadId?: string;
    allowContinue?: boolean;
    threadBusy?: boolean;
    chrome?: "card" | "inline";
    editing?: boolean;
    cancelSignal?: number;
  } = $props();

  let loading = $state(false);
  let planning = $state(false);
  let publishing = $state(false);
  let record = $state<PublishedAgentRecord | undefined>();
  let draftManifest = $state<ManifestRecord | undefined>();
  let source = $state("");
  let plan = $state<AgentPlanResponse | undefined>();
  let error = $state("");
  let note = $state("");
  let activeTab = $state<EditorTab>("fields");
  let lastPlanMode = $state<PlanMode>("manifest");
  let addToolType = $state<"bash_tool" | "direct_tool">("bash_tool");
  let addToolOperation = $state("");
  let addToolId = $state("");
  let addToolSurface = $state("");
  let addToolGrants = $state("");

  let loadSeq = 0;
  let planSeq = 0;
  let handledCancelSignal = $state(0);
  let planTimer: ReturnType<typeof setTimeout> | undefined;

  const identity = $derived(recordObject(draftManifest, "identity"));
  const modelProfiles = $derived(recordArray(draftManifest, "model_profiles"));
  const defaultProfile = $derived(recordAt(modelProfiles, 0));
  const tools = $derived(recordArray(draftManifest, "tools"));
  const policies = $derived(recordObject(draftManifest, "policies"));
  const budgets = $derived(recordObject(policies, "budgets"));
  const runtime = $derived(recordObject(draftManifest, "runtime"));
  const compaction = $derived(recordObject(runtime, "compaction"));
  const overrides = $derived(recordObject(runtime, "overrides"));
  const overrideAllow = $derived(stringArray(overrides.allow));
  const baseManifestHash = $derived(record ? manifestHash(record) : "");
  const expectedLatestVersion = $derived(record?.version ?? "");
  const canPublish = $derived(Boolean(plan && source.trim() && !error && !loading && !planning && !publishing));
  const canContinue = $derived(canPublish && allowContinue && Boolean(threadId) && !threadBusy);

  $effect(() => {
    const ref = agentRef;
    untrack(() => void loadAgent(ref));
  });

  $effect(() => {
    const signal = cancelSignal;
    if (signal === handledCancelSignal) return;
    handledCancelSignal = signal;
    if (editing) cancelEdit();
  });

  function resetDraft() {
    record = undefined;
    draftManifest = undefined;
    source = "";
    plan = undefined;
    error = "";
    note = "";
    activeTab = "fields";
    editing = false;
  }

  async function loadAgent(ref: string) {
    const seq = ++loadSeq;
    clearPlanTimer();
    resetDraft();
    if (!ref) return;
    loading = true;
    try {
      const loaded = await app.readAgent(ref);
      if (seq !== loadSeq) return;
      if (!loaded) {
        error = "Agent record could not be loaded.";
        return;
      }
      record = loaded;
      const manifest = cloneRecord(manifestFromRecord(loaded));
      if (!manifest) {
        error = "Agent record did not include a resolved manifest.";
        return;
      }
      draftManifest = manifest;
      const firstPlan = await app.planAgentDraft({
        manifest,
        baseRef: ref,
        baseManifestHash: manifestHash(loaded),
        expectedLatestVersion: loaded.version,
      });
      if (seq !== loadSeq) return;
      if (firstPlan?.suggestedNextVersion) {
        updateManifestSilently((draft) => {
          const id = ensureObject(draft, "identity");
          id.version = firstPlan.suggestedNextVersion;
        });
      }
      await runPlan("manifest");
    } catch (err) {
      if (seq === loadSeq) error = errorMessage(err);
    } finally {
      if (seq === loadSeq) loading = false;
    }
  }

  function startEdit() {
    editing = true;
    note = "";
  }

  function cancelEdit() {
    const loaded = record;
    if (!loaded) return;
    void loadAgent(agentRef);
  }

  function queuePlan(mode: PlanMode) {
    lastPlanMode = mode;
    clearPlanTimer();
    planning = true;
    planTimer = setTimeout(() => void runPlan(mode), 280);
  }

  async function runPlan(mode: PlanMode) {
    clearPlanTimer();
    const seq = ++planSeq;
    planning = true;
    error = "";
    try {
      const nextPlan = await app.planAgentDraft({
        ...(mode === "source" ? { source } : { manifest: draftManifest }),
        baseRef: agentRef,
        baseManifestHash,
        expectedLatestVersion,
      });
      if (seq !== planSeq || !nextPlan) return;
      plan = nextPlan;
      source = nextPlan.source;
      const nextManifest = cloneRecord(nextPlan.manifest);
      if (nextManifest) draftManifest = nextManifest;
    } catch (err) {
      if (seq === planSeq) {
        plan = undefined;
        error = errorMessage(err);
      }
    } finally {
      if (seq === planSeq) planning = false;
    }
  }

  async function publish(continueAfter: boolean) {
    if (!draftManifest || !record) return;
    publishing = true;
    error = "";
    note = "";
    try {
      await runPlan(lastPlanMode);
      if (!source.trim() || error) return;
      if (continueAfter) {
        await app.publishAgentDraftAndContinue(
          { source, baseRef: agentRef, baseManifestHash, expectedLatestVersion },
          threadId,
        );
        note = "Published and opened the rebound thread.";
      } else {
        await app.publishAgentDraft({ source, baseRef: agentRef, baseManifestHash, expectedLatestVersion });
        note = "Published a new manifest version.";
      }
      editing = false;
    } catch (err) {
      error = errorMessage(err);
    } finally {
      publishing = false;
    }
  }

  function updateManifest(mutator: (draft: ManifestRecord) => void) {
    updateManifestSilently(mutator);
    queuePlan("manifest");
  }

  function updateManifestSilently(mutator: (draft: ManifestRecord) => void) {
    const current = cloneRecord(draftManifest) ?? {};
    mutator(current);
    draftManifest = current;
  }

  function updateIdentity(key: string, value: string | boolean | number | undefined) {
    updateManifest((draft) => setOptional(ensureObject(draft, "identity"), key, value));
  }

  function updateProfile(key: string, value: string | number | undefined) {
    updateManifest((draft) => {
      const profiles = ensureArray(draft, "model_profiles");
      const profile = ensureArrayObject(profiles, 0);
      setOptional(profile, key, value);
    });
  }

  function updateProfileParam(key: string, value: string | number | undefined) {
    updateManifest((draft) => {
      const profiles = ensureArray(draft, "model_profiles");
      const profile = ensureArrayObject(profiles, 0);
      const params = ensureObject(profile, "params");
      setOptional(params, key, value);
    });
  }

  function updatePolicy(key: string, value: string | boolean | number | undefined) {
    updateManifest((draft) => setOptional(ensureObject(draft, "policies"), key, value));
  }

  function updateBudget(key: string, value: number | undefined) {
    updateManifest((draft) => {
      const policy = ensureObject(draft, "policies");
      const policyBudgets = ensureObject(policy, "budgets");
      setOptional(policyBudgets, key, value);
    });
  }

  function updateRuntime(key: string, value: string | boolean | number | undefined) {
    updateManifest((draft) => setOptional(ensureObject(draft, "runtime"), key, value));
  }

  function updateCompaction(key: string, value: number | undefined) {
    updateManifest((draft) => {
      const rt = ensureObject(draft, "runtime");
      const comp = ensureObject(rt, "compaction");
      setOptional(comp, key, value);
    });
  }

  function toggleOverride(key: string, enabled: boolean) {
    updateManifest((draft) => {
      const rt = ensureObject(draft, "runtime");
      const policy = ensureObject(rt, "overrides");
      const current = new Set(stringArray(policy.allow));
      if (enabled) current.add(key);
      else current.delete(key);
      policy.allow = Array.from(current).sort();
    });
  }

  function updateTool(index: number, key: string, value: string | string[] | undefined) {
    updateManifest((draft) => {
      const rows = ensureArray(draft, "tools");
      const row = ensureArrayObject(rows, index);
      setOptional(row, key, value);
    });
  }

  function removeTool(index: number) {
    updateManifest((draft) => {
      const rows = ensureArray(draft, "tools");
      rows.splice(index, 1);
    });
  }

  function addOperationTool() {
    const operation = app.tools.find((tool) => tool.id === addToolOperation) ?? app.tools[0];
    if (!operation) return;
    const id = sanitizeRecordName(addToolId || operation.id);
    const surface = sanitizeRecordName(addToolSurface || (addToolType === "bash_tool" ? operation.id : operation.inputs[0] || operation.id));
    const grants = splitList(addToolGrants);
    updateManifest((draft) => {
      const rows = ensureArray(draft, "tools");
      rows.push(
        addToolType === "bash_tool"
          ? {
              type: "bash_tool",
              id,
              command: surface,
              operation_ref: operationRef(operation),
              grants,
            }
          : {
              type: "direct_tool",
              id,
              tool_name: surface,
              operation_ref: operationRef(operation),
              grants,
            },
      );
    });
    addToolId = "";
    addToolSurface = "";
    addToolGrants = "";
  }

  function onSourceInput(event: Event) {
    source = inputValue(event);
    queuePlan("source");
  }

  function inputValue(event: Event) {
    return (event.currentTarget as HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement).value;
  }

  function inputNumber(event: Event) {
    const value = inputValue(event).trim();
    return value ? Number(value) : undefined;
  }

  function inputBoolean(event: Event) {
    return (event.currentTarget as HTMLInputElement).checked;
  }

  function clearPlanTimer() {
    if (planTimer) clearTimeout(planTimer);
    planTimer = undefined;
  }

</script>

<div class="agent-editor" class:agent-editor-inline={chrome === "inline"} class:is-editing={editing}>
  {#if chrome === "card"}
    <div class="agent-editor-head">
      <div>
        <h4>Agent Definition</h4>
        <p class="agent-editor-ref mono">{agentRef}</p>
      </div>
      {#if !editing}
        <button class="btn" disabled={loading || !record} onclick={startEdit}>
          <Icon name="FileText" size={13} /> Edit
        </button>
      {:else}
        <button class="icon-btn" title="Cancel editing" onclick={cancelEdit} disabled={publishing}>
          <Icon name="X" size={13} />
        </button>
      {/if}
    </div>
  {/if}

  {#if loading}
    <p class="env-note">Loading manifest...</p>
  {:else if error && !editing}
    <p class="env-note danger">{error}</p>
  {:else if record && !editing}
    <dl class="kv">
      {#if chrome === "inline"}<dt>Ref</dt><dd class="mono" title={agentRef}>{agentRef}</dd>{/if}
      <dt>Version</dt><dd>v{record.version}</dd>
      <dt>Hash</dt><dd class="mono">{baseManifestHash.replace(/^sha256:/, "").slice(0, 12)}</dd>
      <dt>Next</dt><dd class="mono">{plan?.suggestedNextVersion ?? "-"}</dd>
    </dl>
  {:else if editing && draftManifest}
    <div class="agent-tabs" role="tablist" aria-label="Manifest editor tabs">
      {#each ["fields", "tools", "runtime", "source"] as tab}
        <button class:active={activeTab === tab} role="tab" aria-selected={activeTab === tab} onclick={() => (activeTab = tab as EditorTab)}>
          {tab}
        </button>
      {/each}
    </div>

    {#if activeTab === "fields"}
      <div class="agent-form">
        <label>
          <span>Name</span>
          <input class="input mono" value={stringValue(identity.name)} disabled />
        </label>
        <label>
          <span>Version</span>
          <input class="input mono" value={stringValue(identity.version)} oninput={(event) => updateIdentity("version", inputValue(event))} />
        </label>
        <label>
          <span>Display name</span>
          <input class="input" value={stringValue(identity.display_name)} oninput={(event) => updateIdentity("display_name", inputValue(event))} />
        </label>
        <label>
          <span>Description</span>
          <textarea class="input" rows="3" value={stringValue(identity.description)} oninput={(event) => updateIdentity("description", inputValue(event))}></textarea>
        </label>
        <label>
          <span>Profile id</span>
          <input class="input mono" value={stringValue(defaultProfile.id)} oninput={(event) => updateProfile("id", inputValue(event))} />
        </label>
        <label>
          <span>Provider ref</span>
          <input class="input mono" value={stringValue(defaultProfile.provider_ref)} oninput={(event) => updateProfile("provider_ref", inputValue(event))} />
        </label>
        <label>
          <span>Model ref</span>
          <input class="input mono" value={stringValue(defaultProfile.model_ref)} oninput={(event) => updateProfile("model_ref", inputValue(event))} />
        </label>
        <div class="agent-grid-two">
          <label>
            <span>Max tokens</span>
            <input class="input mono" type="number" min="1" value={numberValue(recordObject(defaultProfile, "params").max_tokens) ?? ""} oninput={(event) => updateProfileParam("max_tokens", inputNumber(event))} />
          </label>
          <label>
            <span>Temperature</span>
            <input class="input mono" type="number" min="0" step="0.1" value={numberValue(recordObject(defaultProfile, "params").temperature) ?? ""} oninput={(event) => updateProfileParam("temperature", inputNumber(event))} />
          </label>
        </div>
        <label>
          <span>Reasoning effort</span>
          <input class="input mono" value={stringValue(recordObject(defaultProfile, "params").reasoning_effort)} oninput={(event) => updateProfileParam("reasoning_effort", inputValue(event))} />
        </label>
        <div class="agent-grid-two">
          <label>
            <span>Network</span>
            <select class="input" value={stringValue(policies.network) || "deny"} onchange={(event) => updatePolicy("network", inputValue(event))}>
              <option value="deny">deny</option>
              <option value="declared-origins">declared-origins</option>
            </select>
          </label>
          <label>
            <span>Filesystem</span>
            <select class="input" value={stringValue(policies.filesystem) || "vfs"} onchange={(event) => updatePolicy("filesystem", inputValue(event))}>
              <option value="vfs">vfs</option>
              <option value="none">none</option>
            </select>
          </label>
        </div>
        <label class="agent-check">
          <input type="checkbox" checked={booleanValue(policies.allow_child_agents)} onchange={(event) => updatePolicy("allow_child_agents", inputBoolean(event))} />
          <span>Allow child agents</span>
        </label>
        <div class="agent-grid-two">
          <label>
            <span>Max turns</span>
            <input class="input mono" type="number" min="1" value={numberValue(budgets.max_turns) ?? ""} oninput={(event) => updateBudget("max_turns", inputNumber(event))} />
          </label>
          <label>
            <span>Tool calls/turn</span>
            <input class="input mono" type="number" min="1" value={numberValue(budgets.max_tool_calls_per_turn) ?? ""} oninput={(event) => updateBudget("max_tool_calls_per_turn", inputNumber(event))} />
          </label>
        </div>
      </div>
    {:else if activeTab === "tools"}
      <div class="agent-tool-list">
        {#each tools as tool, index (index)}
          <div class="agent-tool-row">
            <div class="agent-tool-row-head">
              <span class="pill mono">{toolKindLabel(tool)}</span>
              <input class="input mono" value={stringValue(tool.id)} oninput={(event) => updateTool(index, "id", inputValue(event))} />
              {#if isOperationBacked(tool)}
                <button class="icon-btn" title="Remove tool" onclick={() => removeTool(index)}>
                  <Icon name="Trash2" size={13} />
                </button>
              {/if}
            </div>
            {#if stringValue(tool.type) === "bash_tool"}
              <label><span>Command</span><input class="input mono" value={stringValue(tool.command)} oninput={(event) => updateTool(index, "command", inputValue(event))} /></label>
              <label><span>Operation ref</span><input class="input mono" value={stringValue(tool.operation_ref)} oninput={(event) => updateTool(index, "operation_ref", inputValue(event))} /></label>
            {:else if stringValue(tool.type) === "direct_tool"}
              <label><span>Tool name</span><input class="input mono" value={stringValue(tool.tool_name)} oninput={(event) => updateTool(index, "tool_name", inputValue(event))} /></label>
              <label><span>Operation ref</span><input class="input mono" value={stringValue(tool.operation_ref)} oninput={(event) => updateTool(index, "operation_ref", inputValue(event))} /></label>
            {:else}
              <p class="env-note">Protocol import is preserved here. Use Source for advanced edits.</p>
              <label><span>Server ref</span><input class="input mono" value={stringValue(tool.server_ref)} disabled /></label>
            {/if}
            <label><span>Grants</span><input class="input mono" value={stringArray(tool.grants).join(", ")} oninput={(event) => updateTool(index, "grants", splitList(inputValue(event)))} /></label>
          </div>
        {:else}
          <p class="env-note">No tools declared.</p>
        {/each}
      </div>
      <div class="agent-add-tool">
        <h4>Add Operation Tool</h4>
        <select class="input" bind:value={addToolOperation}>
          <option value="">Select operation</option>
          {#each app.tools as tool (tool.id)}
            <option value={tool.id}>{tool.name} · {tool.version}</option>
          {/each}
        </select>
        <div class="agent-grid-two">
          <select class="input" bind:value={addToolType}>
            <option value="bash_tool">bash_tool</option>
            <option value="direct_tool">direct_tool</option>
          </select>
          <input class="input mono" placeholder="id" bind:value={addToolId} />
        </div>
        <input class="input mono" placeholder={addToolType === "bash_tool" ? "command" : "tool_name"} bind:value={addToolSurface} />
        <input class="input mono" placeholder="grants, comma separated" bind:value={addToolGrants} />
        <button class="btn" disabled={!app.tools.length} onclick={addOperationTool}>
          <Icon name="Plus" size={13} /> Add tool
        </button>
      </div>
    {:else if activeTab === "runtime"}
      <div class="agent-form">
        <label>
          <span>Default cwd</span>
          <input class="input mono" value={stringValue(runtime.default_cwd)} oninput={(event) => updateRuntime("default_cwd", inputValue(event))} />
        </label>
        <label class="agent-check">
          <input type="checkbox" checked={booleanValue(runtime.streaming)} onchange={(event) => updateRuntime("streaming", inputBoolean(event))} />
          <span>Streaming</span>
        </label>
        <div class="agent-grid-two">
          <label>
            <span>Turn timeout</span>
            <input class="input mono" type="number" min="1" value={numberValue(runtime.turn_timeout_ms) ?? ""} oninput={(event) => updateRuntime("turn_timeout_ms", inputNumber(event))} />
          </label>
          <label>
            <span>Cancel grace</span>
            <input class="input mono" type="number" min="1" value={numberValue(runtime.cancellation_grace_ms) ?? ""} oninput={(event) => updateRuntime("cancellation_grace_ms", inputNumber(event))} />
          </label>
        </div>
        <label>
          <span>Compaction bytes</span>
          <input class="input mono" type="number" min="1" value={numberValue(compaction.auto_at_text_bytes) ?? ""} oninput={(event) => updateCompaction("auto_at_text_bytes", inputNumber(event))} />
        </label>
        <h4>Allowed Overrides</h4>
        {#each ["default_cwd", "streaming", "turn_timeout_ms", "cancellation_grace_ms", "compaction_auto_at_text_bytes"] as key}
          <label class="agent-check">
            <input type="checkbox" checked={overrideAllow.includes(key)} onchange={(event) => toggleOverride(key, inputBoolean(event))} />
            <span class="mono">{key}</span>
          </label>
        {/each}
      </div>
    {:else}
      <textarea class="agent-source mono" spellcheck="false" value={source} oninput={onSourceInput}></textarea>
    {/if}

    {#if plan?.diagnostics.length}
      <div class="agent-diagnostics">
        {#each plan.diagnostics as diagnostic, index (index)}
          <p><Icon name="TriangleAlert" size={12} /> {diagnostic.message}</p>
        {/each}
      </div>
    {/if}
    {#if error}<p class="env-note danger">{error}</p>{/if}
    {#if note}<p class="env-note success">{note}</p>{/if}
    {#if threadBusy && allowContinue}<p class="env-note">Continuation is available when the source thread is idle.</p>{/if}

    <div class="agent-editor-actions">
      {#if chrome === "inline"}
        <button class="btn" disabled={publishing} onclick={cancelEdit}>
          <Icon name="X" size={13} />
          Cancel
        </button>
      {/if}
      <button class="btn primary" disabled={!canPublish} aria-busy={publishing} onclick={() => void publish(false)}>
        <Icon name={publishing ? "RefreshCw" : "Rocket"} size={13} class={publishing ? "spin" : ""} />
        Publish
      </button>
      {#if allowContinue}
        <button class="btn" disabled={!canContinue} aria-busy={publishing} onclick={() => void publish(true)}>
          <Icon name="GitBranch" size={13} />
          Publish and continue
        </button>
      {/if}
      {#if planning}<span class="agent-plan-state">Validating...</span>{/if}
    </div>
  {/if}
</div>
