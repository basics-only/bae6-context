export type RegistryErrorCode =
  | "invalid_request"
  | "not_found"
  | "conflict"
  | "forbidden";

export class RegistryError extends Error {
  constructor(
    readonly code: RegistryErrorCode,
    message: string,
  ) {
    super(message);
  }
}

export type State = "pending" | "approved" | "revoked";
export type ScanStatus = "unscanned" | "clean";

export interface Capability {
  name: string;
  description?: string;
}

export interface Authentication {
  method: "oauth2" | "none";
  resource?: string;
  requiredScopes?: string[];
}

export interface VersionInput {
  version: string;
  endpoint: string;
  protocolVersion: string;
  transport: "streamable-http";
  authentication: Authentication;
  tools: Capability[];
  resources: Capability[];
  prompts: Capability[];
  allowedNetworkZones: string[];
}

export interface Version extends VersionInput {
  state: State;
  scanStatus: ScanStatus;
  publishedBy: string;
  publishedVia: string;
  publishedAt: string;
  decidedBy?: string;
  decidedAt?: string;
}

export interface Server {
  id: string;
  organization: string;
  team: string;
  name: string;
  description?: string;
  createdAt: string;
  versions: Version[];
}

export interface NewServer {
  id: string;
  team: string;
  name: string;
  description?: string;
  version: VersionInput;
}

export interface Health {
  version: string;
  status: "healthy" | "unhealthy";
  checkedAt: string;
  reason?: HealthReason;
}

export type HealthReason =
  | "endpoint_blocked"
  | "unreachable"
  | "timeout"
  | "bad_status"
  | "redirected"
  | "resource_mismatch"
  | "invalid_metadata";

export interface AuditEvent {
  id: string;
  time: string;
  organization: string;
  actor: string;
  action: string;
  outcome: "success";
  requestId: string;
  detail: Record<string, unknown>;
}

export interface Caller {
  /** Who is accountable: the user when one is delegated, else the workload. */
  subject: string;
  /** The calling workload, whether or not a user is delegated. */
  workload: string;
  /** For the audit trail: `<workload> for <user>` when delegated. */
  actor: string;
  requestId: string;
}

export interface Change {
  next: Server;
  audit: AuditEvent;
}

export interface RegistryStore {
  /**
   * Runs `change` on the server's current state while holding it exclusively, then stores
   * the result and its audit event together. `change` returning null stores nothing.
   */
  mutate(
    org: string,
    id: string,
    change: (current: Server | undefined) => Change | null,
  ): Promise<Server | undefined>;
  get(
    org: string,
    id: string,
    approvedOnly: boolean,
  ): Promise<Server | undefined>;
  /** Servers with `id > after`, ordered by id. */
  list(
    org: string,
    page: { after?: string; limit: number; approvedOnly: boolean },
  ): Promise<Server[]>;
  recordHealth(org: string, id: string, health: Health): Promise<void>;
  getHealth(
    org: string,
    id: string,
    version: string,
  ): Promise<Health | undefined>;
}

export const MAX_VERSIONS = 50;
export const SERVER_ID = /^[a-z0-9][a-z0-9._-]{0,63}$/;
export const SLUG = /^[a-z0-9][a-z0-9-]{0,62}$/;
export const VERSION_NAME = /^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$/;
