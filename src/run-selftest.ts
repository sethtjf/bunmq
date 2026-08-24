import { runSelfTest } from "./selftest";

async function main() {
  const results = await runSelfTest();
  for (const x of results) {
    const mark = x.pass ? "ok  " : "FAIL";
    console.log(`${mark} ${x.name}  ${x.ms}ms${x.error ? "  " + x.error : ""}`);
  }
  const failed = results.filter((r) => !r.pass);
  if (failed.length) {
    process.exitCode = 1;
  }
}

void main();
