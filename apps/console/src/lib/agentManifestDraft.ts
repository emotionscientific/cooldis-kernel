import type { PublishedAgentRecord, ToolDef } from "./schema";

export type ManifestRecord = Record<string, unknown>;

export function manifestFromRecord(value: PublishedAgentRecord | undefined) {
  if (!value) return undefined;
  return (value.resolved_manifest ?? value.resolvedManifest) as unknown;
}

export function manifestHash(value: PublishedAgentRecord) {
  return stringValue(value.manifest_hash) || stringValue(value.manifestHash);
}

export function agentRecordRef(record: PublishedAgentRecord) {
  return (
    stringValue(record.ref_uri) ||
    stringValue(record.refUri) ||
    `agent://${record.name}@${record.version}`
  );
}

export function recordObject(value: unknown, key: string): ManifestRecord {
  const child = isRecord(value) ? value[key] : undefined;
  return isRecord(child) ? child : {};
}

export function recordArray(value: unknown, key: string): ManifestRecord[] {
  const child = isRecord(value) ? value[key] : undefined;
  return Array.isArray(child) ? child.filter(isRecord) : [];
}

export function recordAt(values: ManifestRecord[], index: number): ManifestRecord {
  return values[index] ?? {};
}

export function ensureObject(parent: ManifestRecord, key: string) {
  const child = parent[key];
  if (!isRecord(child)) parent[key] = {};
  return parent[key] as ManifestRecord;
}

export function ensureArray(parent: ManifestRecord, key: string) {
  if (!Array.isArray(parent[key])) parent[key] = [];
  return parent[key] as unknown[];
}

export function ensureArrayObject(values: unknown[], index: number) {
  if (!isRecord(values[index])) values[index] = {};
  return values[index] as ManifestRecord;
}

export function setOptional(
  parent: ManifestRecord,
  key: string,
  value: string | boolean | number | string[] | undefined,
) {
  if (value === undefined || value === "" || (Array.isArray(value) && value.length === 0)) {
    delete parent[key];
  } else {
    parent[key] = value;
  }
}

export function cloneRecord(value: unknown): ManifestRecord | undefined {
  if (!isRecord(value)) return undefined;
  return JSON.parse(JSON.stringify(value)) as ManifestRecord;
}

export function stringValue(value: unknown) {
  return typeof value === "string" ? value : "";
}

export function numberValue(value: unknown) {
  return typeof value === "number" ? value : undefined;
}

export function booleanValue(value: unknown) {
  return typeof value === "boolean" ? value : false;
}

export function stringArray(value: unknown) {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

export function splitList(value: string) {
  return value
    .split(/[,\n]/)
    .map((item) => item.trim())
    .filter(Boolean);
}

export function sanitizeRecordName(value: string) {
  return (value.trim() || "tool").replace(/[^A-Za-z0-9_.-]/g, "-").replace(/^-+|-+$/g, "") || "tool";
}

export function toolKindLabel(tool: ManifestRecord) {
  const type = stringValue(tool.type);
  if (type === "bash_tool") return "bash";
  if (type === "direct_tool") return "direct";
  if (type === "protocol_tool_import") return "protocol";
  return type || "tool";
}

export function isOperationBacked(tool: ManifestRecord) {
  return stringValue(tool.type) === "bash_tool" || stringValue(tool.type) === "direct_tool";
}

export function operationRef(tool: ToolDef) {
  const hash = tool.artifactHash.replace(/^sha256:/, "");
  return `op://${tool.id}@sha256:${hash}`;
}

export function errorMessage(err: unknown) {
  return err instanceof Error ? err.message : String(err);
}

function isRecord(value: unknown): value is ManifestRecord {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
