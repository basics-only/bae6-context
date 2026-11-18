import { randomUUID } from "node:crypto";
import type { EgressPolicy } from "./endpoint.js";
import {
  type AuditEvent,
  type Caller,
  type Change,
  type Health,
  MAX_VERSIONS,
  RegistryError,
  type RegistryStore,
  type ScanStatus,
  type Server,
  type Version,
  type VersionInput,
} from "./types.js";
import {
  parseApprove,
  parseNewServer,
  parseRevoke,
  parseVersion,
} from "./validate.js";

const HEALTH_FRESH_MS = 30_000;

const notFound = (what: string) =>
  new RegistryError("not_found", `${what} not found`);

export type Probe = (version: Version) => Promise<Health>;

export class Registry {
  constructor(
    private readonly store: RegistryStore,
    private readonly policy: EgressPolicy = {},
    private readonly now: () => Date = () => new Date(),
  ) {}

  private event(
    org: string,
    who: Caller,
    action: string,
    detail: Record<string, unknown>,
  ): AuditEvent {
    return {
      id: `aud_${randomUUID()}`,
      time: this.now().toISOString(),
      organization: org,
      actor: who.actor,
      action,
      outcome: "success",
      requestId: who.requestId,
      detail,
    };
  }

  private pending(input: VersionInput, who: Caller): Version {
    return {
      ...input,
      state: "pending",
      scanStatus: "unscanned",
      publishedBy: who.subject,
      publishedAt: this.now().toISOString(),
    };
  }

  private async change(
    org: string,
    id: string,
    apply: (current: Server | undefined) => Change | null,
  ): Promise<Server> {
    const server = await this.store.mutate(org, id, apply);
    if (!server) throw notFound(`server ${id}`);
    return server;
  }

  async createServer(org: string, raw: unknown, who: Caller): Promise<Server> {
    const input = parseNewServer(raw, this.policy);
    return this.change(org, input.id, (current) => {
      if (current) {
        throw new RegistryError(
          "conflict",
          `server ${input.id} already exists`,
        );
      }
      const version = this.pending(input.version, who);
      return {
        next: {
          id: input.id,
          organization: org,
          team: input.team,
          name: input.name,
          ...(input.description === undefined
            ? {}
            : { description: input.description }),
          createdAt: version.publishedAt,
          versions: [version],
        },
        audit: this.event(org, who, "servers.create", {
          server: input.id,
          team: input.team,
          version: version.version,
          endpoint: version.endpoint,
        }),
      };
    });
  }

  async addVersion(
    org: string,
    id: string,
    raw: unknown,
    who: Caller,
  ): Promise<Server> {
    const input = parseVersion(raw, this.policy);
    return this.change(org, id, (current) => {
      if (!current) throw notFound(`server ${id}`);
      if (current.versions.some((v) => v.version === input.version)) {
        throw new RegistryError(
          "conflict",
          `version ${input.version} is already published`,
        );
      }
      if (current.versions.length >= MAX_VERSIONS) {
        throw new RegistryError(
          "conflict",
          `a server has at most ${MAX_VERSIONS} versions`,
        );
      }
      return {
        next: {
          ...current,
          versions: [...current.versions, this.pending(input, who)],
        },
        audit: this.event(org, who, "servers.publish_version", {
          server: id,
          version: input.version,
          endpoint: input.endpoint,
        }),
      };
    });
  }

  async approve(
    org: string,
    id: string,
    raw: unknown,
    who: Caller,
  ): Promise<Server> {
    const { version: name, scanStatus } = parseApprove(raw);
    return this.change(org, id, (current) => {
      const target = current?.versions.find((v) => v.version === name);
      if (!current || !target) throw notFound(`version ${name}`);
      if (target.state === "approved") return null;
      if (target.state === "revoked") {
        throw new RegistryError(
          "conflict",
          "a revoked version cannot be approved",
        );
      }
      if (target.publishedBy === who.subject) {
        throw new RegistryError(
          "forbidden",
          "a version cannot be approved by its publisher",
        );
      }
      return {
        next: this.decided(current, [name], "approved", who, scanStatus),
        audit: this.event(org, who, "servers.approve", {
          server: id,
          version: name,
          scanStatus,
        }),
      };
    });
  }

  async revoke(
    org: string,
    id: string,
    raw: unknown,
    who: Caller,
  ): Promise<Server> {
    const { version: name } = parseRevoke(raw);
    return this.change(org, id, (current) => {
      if (!current) throw notFound(`server ${id}`);
      if (
        name !== undefined &&
        !current.versions.some((v) => v.version === name)
      ) {
        throw notFound(`version ${name}`);
      }
      const names = current.versions
        .filter(
          (v) =>
            v.state !== "revoked" && (name === undefined || v.version === name),
        )
        .map((v) => v.version);
      if (names.length === 0) return null;
      return {
        next: this.decided(current, names, "revoked", who),
        audit: this.event(org, who, "servers.revoke", {
          server: id,
          versions: names,
        }),
      };
    });
  }

  private decided(
    server: Server,
    names: string[],
    to: "approved" | "revoked",
    who: Caller,
    scanStatus?: ScanStatus,
  ): Server {
    const decidedAt = this.now().toISOString();
    return {
      ...server,
      versions: server.versions.map((v) =>
        names.includes(v.version)
          ? {
              ...v,
              state: to,
              ...(scanStatus === undefined ? {} : { scanStatus }),
              decidedBy: who.subject,
              decidedAt,
            }
          : v,
      ),
    };
  }

  /** `manage` also sees pending and revoked versions; otherwise only approved ones. */
  async get(org: string, id: string, manage: boolean): Promise<Server> {
    const server = await this.store.get(org, id, !manage);
    if (!server) throw notFound(`server ${id}`);
    return server;
  }

  async list(
    org: string,
    page: { after?: string; limit: number },
    manage: boolean,
  ): Promise<{ items: Server[]; next?: string }> {
    const rows = await this.store.list(org, {
      ...page,
      limit: page.limit + 1,
      approvedOnly: !manage,
    });
    const items = rows.slice(0, page.limit);
    const last = items.at(-1);
    return rows.length > page.limit && last
      ? { items, next: last.id }
      : { items };
  }

  /** Probes at most once per 30 seconds per version; approved versions only. */
  async health(
    org: string,
    id: string,
    requested: string | undefined,
    probe: Probe,
  ): Promise<Health> {
    const server = await this.store.get(org, id, true);
    const version =
      requested === undefined
        ? server?.versions.at(-1)
        : server?.versions.find((v) => v.version === requested);
    if (!version) throw notFound(`version ${requested ?? "(approved)"}`);
    const cached = await this.store.getHealth(org, id, version.version);
    if (
      cached &&
      this.now().getTime() - Date.parse(cached.checkedAt) < HEALTH_FRESH_MS
    ) {
      return cached;
    }
    const result = await probe(version);
    await this.store.recordHealth(org, id, result);
    return result;
  }
}
