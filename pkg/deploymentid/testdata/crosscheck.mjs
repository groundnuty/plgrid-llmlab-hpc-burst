// Second independent check of the golden vectors: canonicalise each
// expected-object.json with the `canonicalize` npm package (a JCS implementation
// in JavaScript by Samuel Erdtman, Apache-2.0) and compare the bytes with
// canonical.json, which gen_vectors.py produced with a different library.
//
//   npm install canonicalize@<version in README.md>
//   node crosscheck.mjs [vectors-dir]
import canonicalize from "canonicalize";
import { createHash } from "node:crypto";
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = process.argv[2] ?? join(dirname(fileURLToPath(import.meta.url)), "vectors");
let failed = 0;
for (const name of readdirSync(root).sort()) {
  const obj = JSON.parse(readFileSync(join(root, name, "expected-object.json"), "utf8"));
  const got = Buffer.from(canonicalize(obj), "utf8");
  const want = readFileSync(join(root, name, "canonical.json"));
  const sum = createHash("sha256").update(got).digest("hex");
  const ok = got.equals(want);
  if (!ok) failed++;
  console.log(`${ok ? "match   " : "MISMATCH"} ${name} ${got.length} bytes ${sum}`);
}
process.exit(failed ? 1 : 0);
