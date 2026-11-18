const QUERY_START = new Set([
  "SELECT",
  "WITH",
  "VALUES",
  "SHOW",
  "DESCRIBE",
  "DESC",
  "EXPLAIN",
  "FROM",
]);

const WRITES = new Set([
  "INSERT",
  "UPDATE",
  "DELETE",
  "MERGE",
  "CREATE",
  "DROP",
  "ALTER",
  "TRUNCATE",
  "COPY",
  "OPTIMIZE",
  "VACUUM",
  "GRANT",
  "REVOKE",
  "RESTORE",
  "CACHE",
  "UNCACHE",
  "REFRESH",
  "SET",
  "RESET",
  "USE",
  "CALL",
  "EXECUTE",
  "MSCK",
  "ANALYZE",
  "LOAD",
  "ADD",
]);

/**
 * Words outside comments, string literals and quoted identifiers, plus whether a `;` is
 * followed by more text. Unterminated quotes or comments yield null.
 */
function scan(sql: string): { words: string[]; multiple: boolean } | null {
  const words: string[] = [];
  let multiple = false;
  let sawSemicolon = false;
  let i = 0;
  while (i < sql.length) {
    const c = sql[i] as string;
    const next = sql[i + 1];
    if (c === "-" && next === "-") {
      const end = sql.indexOf("\n", i);
      i = end === -1 ? sql.length : end + 1;
    } else if (c === "/" && next === "*") {
      const end = sql.indexOf("*/", i + 2);
      if (end === -1) return null;
      i = end + 2;
    } else if (c === "'" || c === '"' || c === "`") {
      let j = i + 1;
      for (; j < sql.length; j++) {
        if (sql[j] === "\\" && c !== "`") j++;
        else if (sql[j] === c) break;
      }
      if (j >= sql.length) return null;
      if (sawSemicolon) multiple = true;
      i = j + 1;
    } else if (c === ";") {
      sawSemicolon = true;
      i++;
    } else if (/[A-Za-z_]/.test(c)) {
      let j = i + 1;
      while (j < sql.length && /[A-Za-z0-9_]/.test(sql[j] as string)) j++;
      if (sawSemicolon) multiple = true;
      words.push(sql.slice(i, j).toUpperCase());
      i = j;
    } else {
      if (sawSemicolon && !/\s/.test(c)) multiple = true;
      i++;
    }
  }
  return { words, multiple };
}

/**
 * A conservative check that a statement only reads. Databricks has no read-only session, so
 * this backs up, and never replaces, the principal's Unity Catalog grants. It refuses some
 * valid reads (an unquoted column named like a write keyword must be backquoted).
 */
export function isReadOnlyStatement(sql: string): boolean {
  const s = scan(sql);
  if (!s || s.multiple || s.words.length === 0) return false;
  return (
    QUERY_START.has(s.words[0] as string) && !s.words.some((w) => WRITES.has(w))
  );
}
