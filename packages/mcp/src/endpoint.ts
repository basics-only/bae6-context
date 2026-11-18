import { lookup as dnsLookup, type LookupAddress } from "node:dns";
import { BlockList, isIP, type LookupFunction } from "node:net";
import { RegistryError } from "./types.js";

export interface EgressPolicy {
  /** When non-empty, the endpoint host must equal a suffix or end with `.suffix`. */
  allowedHostSuffixes?: readonly string[];
  /** Development only: allows http, IP literals, other ports and non-public addresses. */
  allowInsecure?: boolean;
}

const nonPublicV4 = new BlockList();
for (const [net, bits] of [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16],
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.0.2.0", 24],
  ["192.88.99.0", 24],
  ["192.168.0.0", 16],
  ["198.18.0.0", 15],
  ["198.51.100.0", 24],
  ["203.0.113.0", 24],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
] as const) {
  nonPublicV4.addSubnet(net, bits, "ipv4");
}

// Only global unicast is public; mapped, NAT64, 6to4 and Teredo forms can carry a private IPv4.
const globalV6 = new BlockList();
globalV6.addSubnet("2000::", 3, "ipv6");
const nonGlobalV6 = new BlockList();
for (const [net, bits] of [
  ["2001::", 32],
  ["2001:db8::", 32],
  ["2002::", 16],
  ["3fff::", 20],
] as const) {
  nonGlobalV6.addSubnet(net, bits, "ipv6");
}

export function isPublicAddress(address: string): boolean {
  const family = isIP(address);
  if (family === 4) return !nonPublicV4.check(address, "ipv4");
  if (family === 6) {
    return (
      globalV6.check(address, "ipv6") && !nonGlobalV6.check(address, "ipv6")
    );
  }
  return false;
}

const bad = (message: string): never => {
  throw new RegistryError("invalid_request", message);
};

/** The normalized endpoint, or throws. Names are not resolved here; see `guardedLookup`. */
export function checkEndpoint(raw: unknown, policy: EgressPolicy = {}): string {
  if (typeof raw !== "string" || raw === "" || raw.length > 2048) {
    return bad("endpoint must be a string of 1 to 2048 characters");
  }
  let u: URL;
  try {
    u = new URL(raw);
  } catch {
    return bad("endpoint must be an absolute URL");
  }
  const insecure = policy.allowInsecure === true;
  if (u.protocol !== "https:" && !(insecure && u.protocol === "http:")) {
    bad("endpoint must use https");
  }
  if (u.username !== "" || u.password !== "") {
    bad("endpoint must not carry credentials");
  }
  if (u.search !== "" || u.hash !== "") {
    bad("endpoint must not have a query or fragment");
  }
  const host = u.hostname.replace(/\.$/, "");
  if (!insecure) {
    if (isIP(host.replace(/^\[|\]$/g, "")) !== 0) {
      bad("endpoint must name a host, not an address");
    }
    if (
      host === "localhost" ||
      host.endsWith(".localhost") ||
      !host.includes(".")
    ) {
      bad("endpoint must name a public host");
    }
    if (u.port !== "") bad("endpoint must use the default port");
  }
  const suffixes = policy.allowedHostSuffixes ?? [];
  if (
    suffixes.length > 0 &&
    !suffixes.some((s) => host === s || host.endsWith(`.${s}`))
  ) {
    bad("endpoint host is not allowed by the egress policy");
  }
  return u.href;
}

export type Resolver = (
  host: string,
  done: (err: NodeJS.ErrnoException | null, found: LookupAddress[]) => void,
) => void;

const systemResolver: Resolver = (host, done) =>
  dnsLookup(host, { all: true, verbatim: true }, done);

/**
 * A `lookup` for http(s).request that refuses names resolving to any non-public address.
 * The connection uses the checked answer, so a second DNS answer cannot redirect it.
 */
export function guardedLookup(
  policy: EgressPolicy = {},
  resolve: Resolver = systemResolver,
): LookupFunction {
  return (host, options, callback) => {
    resolve(host, (err, found) => {
      if (err) return callback(err, "", 0);
      if (
        found.length === 0 ||
        (policy.allowInsecure !== true &&
          !found.every((a) => isPublicAddress(a.address)))
      ) {
        return callback(
          Object.assign(
            new Error("endpoint resolves to a non-public address"),
            {
              code: "EBLOCKED",
            },
          ),
          "",
          0,
        );
      }
      if (options.all) return callback(null, found);
      const [first] = found as [LookupAddress];
      callback(null, first.address, first.family);
    });
  };
}
