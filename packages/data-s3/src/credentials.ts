import { readFile } from "node:fs/promises";
import type { AwsCredentials } from "./sigv4.js";

const REFRESH_EARLY_MS = 5 * 60_000;

/**
 * EKS Pod Identity (container credentials endpoint) when configured, else static
 * environment keys. shortcut: no IRSA web-identity exchange; add it if a cluster needs it.
 */
export function awsCredentials(
  env: NodeJS.ProcessEnv,
): () => Promise<AwsCredentials> {
  const uri = env.AWS_CONTAINER_CREDENTIALS_FULL_URI;
  if (!uri) {
    return async () => {
      const { AWS_ACCESS_KEY_ID: id, AWS_SECRET_ACCESS_KEY: secret } = env;
      if (!id || !secret) throw new Error("no AWS credentials configured");
      return {
        accessKeyId: id,
        secretAccessKey: secret,
        ...(env.AWS_SESSION_TOKEN
          ? { sessionToken: env.AWS_SESSION_TOKEN }
          : {}),
      };
    };
  }
  let current: { value: AwsCredentials; expires: number } | undefined;
  let inflight: Promise<{ value: AwsCredentials; expires: number }> | undefined;
  const fetchOne = async () => {
    const tokenFile = env.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE;
    const token = tokenFile
      ? (await readFile(tokenFile, "utf8")).trim()
      : env.AWS_CONTAINER_AUTHORIZATION_TOKEN;
    const res = await fetch(uri, {
      headers: token ? { authorization: token } : {},
      signal: AbortSignal.timeout(5000),
    });
    if (!res.ok) throw new Error(`credentials endpoint returned ${res.status}`);
    const b = (await res.json()) as Record<string, string>;
    if (!b.AccessKeyId || !b.SecretAccessKey) {
      throw new Error("credentials endpoint returned no keys");
    }
    return {
      value: {
        accessKeyId: b.AccessKeyId,
        secretAccessKey: b.SecretAccessKey,
        ...(b.Token ? { sessionToken: b.Token } : {}),
      },
      expires: b.Expiration
        ? Date.parse(b.Expiration)
        : Date.now() + REFRESH_EARLY_MS * 2,
    };
  };
  return async () => {
    if (current && current.expires - REFRESH_EARLY_MS > Date.now()) {
      return current.value;
    }
    inflight ??= fetchOne().finally(() => {
      inflight = undefined;
    });
    current = await inflight;
    return current.value;
  };
}
