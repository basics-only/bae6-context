import { createHash, createHmac } from "node:crypto";

export interface AwsCredentials {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
}

export const sha256 = (s: string | Uint8Array) =>
  createHash("sha256").update(s).digest("hex");
const hmac = (key: string | Buffer, s: string) =>
  createHmac("sha256", key).update(s).digest();
export const encode = (s: string) =>
  encodeURIComponent(s).replace(
    /[!'()*]/g,
    (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`,
  );

/**
 * AWS Signature Version 4 for S3: the path is signed as sent (encoded once) and the payload
 * hash travels in x-amz-content-sha256. `url.pathname` must already be encoded with `encode`.
 */
export function signS3(input: {
  method: string;
  url: URL;
  payloadHash: string;
  region: string;
  credentials: AwsCredentials;
  now?: Date;
}): Record<string, string> {
  const { url, credentials } = input;
  const amzDate = (input.now ?? new Date())
    .toISOString()
    .replace(/[:-]|\.\d{3}/g, "");
  const date = amzDate.slice(0, 8);
  const headers: Record<string, string> = {
    host: url.host,
    "x-amz-content-sha256": input.payloadHash,
    "x-amz-date": amzDate,
  };
  if (credentials.sessionToken) {
    headers["x-amz-security-token"] = credentials.sessionToken;
  }
  const names = Object.keys(headers).sort();
  const query = [...url.searchParams]
    .map(([k, v]) => `${encode(k)}=${encode(v)}`)
    .sort()
    .join("&");
  const canonical = [
    input.method,
    url.pathname,
    query,
    names.map((n) => `${n}:${headers[n]}\n`).join(""),
    names.join(";"),
    input.payloadHash,
  ].join("\n");
  const scope = `${date}/${input.region}/s3/aws4_request`;
  const toSign = ["AWS4-HMAC-SHA256", amzDate, scope, sha256(canonical)].join(
    "\n",
  );
  const key = hmac(
    hmac(
      hmac(hmac(`AWS4${credentials.secretAccessKey}`, date), input.region),
      "s3",
    ),
    "aws4_request",
  );
  const signature = createHmac("sha256", key).update(toSign).digest("hex");
  const { host: _, ...send } = headers;
  return {
    ...send,
    authorization: `AWS4-HMAC-SHA256 Credential=${credentials.accessKeyId}/${scope}, SignedHeaders=${names.join(";")}, Signature=${signature}`,
  };
}
