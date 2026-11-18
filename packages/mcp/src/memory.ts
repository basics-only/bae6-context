import type {
  AuditEvent,
  Change,
  Health,
  RegistryStore,
  Server,
} from "./types.js";

/** For tests and local development: one process, nothing persisted. */
export class MemoryStore implements RegistryStore {
  readonly audit: AuditEvent[] = [];
  private readonly servers = new Map<string, Server>();
  private readonly healths = new Map<string, Health>();

  private visible(server: Server, approvedOnly: boolean): Server | undefined {
    if (!approvedOnly) return structuredClone(server);
    const versions = server.versions.filter((v) => v.state === "approved");
    return versions.length === 0
      ? undefined
      : structuredClone({ ...server, versions });
  }

  async mutate(
    org: string,
    id: string,
    change: (current: Server | undefined) => Change | null,
  ) {
    const key = `${org}/${id}`;
    const current = this.servers.get(key);
    const result = change(current && structuredClone(current));
    if (!result) return current && structuredClone(current);
    this.servers.set(key, structuredClone(result.next));
    this.audit.push(structuredClone(result.audit));
    return structuredClone(result.next);
  }

  async get(org: string, id: string, approvedOnly: boolean) {
    const server = this.servers.get(`${org}/${id}`);
    return server && this.visible(server, approvedOnly);
  }

  async list(
    org: string,
    page: { after?: string; limit: number; approvedOnly: boolean },
  ) {
    return [...this.servers.values()]
      .filter(
        (s) =>
          s.organization === org &&
          (page.after === undefined || s.id > page.after),
      )
      .sort((a, b) => (a.id < b.id ? -1 : 1))
      .flatMap((s) => this.visible(s, page.approvedOnly) ?? [])
      .slice(0, page.limit);
  }

  async recordHealth(org: string, id: string, health: Health) {
    this.healths.set(`${org}/${id}/${health.version}`, { ...health });
  }

  async getHealth(org: string, id: string, version: string) {
    const h = this.healths.get(`${org}/${id}/${version}`);
    return h && { ...h };
  }
}
