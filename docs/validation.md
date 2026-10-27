# Storage safety fixes: validation

Baseline: commit `a956b21`, built before changes. Both baseline and fixed binaries
used GCC 14.3.0, C++20, CMake Release (`-O3`), and the same environment and disk.
No learning workflow was used for the fixes; the existing learning journal is ignored.

## Changes audited

- Failed WAL rollback or rollback sync prevents further writes and compaction;
  reads reject the failed store until close/reopen.
- Recovery distinguishes I/O failures from incomplete tails, checks durable repair,
  rejects malformed headers and detected non-tail CRC corruption without truncation,
  and checks remaining file length before allocating record payloads.
- Directory sync errors propagate; startup also persists directory ancestor entries.
- Owning descriptors release process locks on constructor failure and temporary
  files/descriptors on compaction exceptions. Compaction retains the replacement
  descriptor across rename, avoiding a fallible reopen.
- Post-WAL memory-update exceptions mark the store failed before releasing its shard
  lock. Read checks occur under shard locks so waiting readers cannot serve a
  divergent view after that failure.
- TTL overflow is rejected; CLI parsing rejects trailing TTL characters and reports
  operation exceptions. Release assertions remain active and CTest runs both suites.
- The existing benchmark rejects failed storage operations rather than counting them.

## Tests

Final Release, ThreadSanitizer, and AddressSanitizer/UndefinedBehaviorSanitizer
builds each passed both CTest suites (2/2). LeakSanitizer also passed in the latter
run. LeakSanitizer initially failed because of tracing restrictions inside the
sandbox; the successful final ASan/UBSan/leak run used the approved outside-sandbox
execution. Final builds were warning-free.

The regression suite covers partial/zero/interrupted writes, interrupted reads,
failed append sync, failed rollback/truncation/sync, recovery open/read/repair errors,
creation/directory sync failures, temp-write/temp-sync/rename errors, descriptor
and temp-file cleanup, post-sync allocation failure, malformed and non-tail-corrupt
records, every truncation boundary in a TTL record, empty/binary keys and values,
TTL bounds, locking between instances, concurrent writers/readers/compaction, and
abrupt subprocess exits during WAL writes and around compaction sync/rename.

CLI checks passed for malformed TTL, TTL overflow, writes, compaction, and restart.
`git diff --check` passed, and `git check-ignore` confirms the journal is ignored.

To run the checked-in suites:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
./build/kvstore_bench
```

Sanitizer builds used `-O1 -g -fno-omit-frame-pointer` with either
`-fsanitize=thread` or `-fsanitize=address,undefined`.

## Existing full benchmark

The default workload remains 500 operations per thread, 1,000 keys, 64-byte
values, and 1,000,000 reads per thread for read-only runs. The final run did not
overlap tests or other benchmark runs. An earlier overlapping run was discarded.

| Workload | Threads | Baseline ops/sec | Fixed ops/sec | Change |
|---|---:|---:|---:|---:|
| read | 1 | 8,339,472 | 8,367,347 | +0.3% |
| read | 2 | 13,609,389 | 13,320,239 | -2.1% |
| read | 4 | 22,404,447 | 21,893,946 | -2.3% |
| read | 8 | 30,546,973 | 31,469,135 | +3.0% |
| read | 16 | 39,412,275 | 40,893,172 | +3.8% |
| write | 1 | 210 | 208 | -1.0% |
| write | 2 | 233 | 220 | -5.6% |
| write | 4 | 237 | 225 | -5.1% |
| write | 8 | 236 | 196 | -16.9% |
| write | 16 | 236 | 179 | -24.2% |
| mixed | 1 | 1,254 | 1,050 | -16.3% |
| mixed | 2 | 1,198 | 1,213 | +1.3% |
| mixed | 4 | 1,137 | 1,011 | -11.1% |
| mixed | 8 | 1,147 | 1,212 | +5.7% |
| mixed | 16 | 1,169 | 1,175 | +0.5% |

Compaction of 1,000 live keys: **11.06 ms baseline, 11.66 ms fixed**.

## Follow-up on write variability

The full run's 16-thread write result was lower, so a separate matched write-only
comparison alternated the binaries in this order: baseline, fixed, fixed, baseline,
baseline, fixed. Each run used 16 threads, 128 writes per thread, the same key/value
generator and write benchmark function, and a fresh database. The driver was
identical for both libraries and compiled against each library's matching header.
Runs were sequential with no test workload running.

```text
baseline write   threads=16  225        ops/sec
fixed write   threads=16  234        ops/sec
fixed write   threads=16  236        ops/sec
baseline write   threads=16  234        ops/sec
baseline write   threads=16  234        ops/sec
fixed write   threads=16  234        ops/sec
```

Median: **234 ops/sec baseline and 234 ops/sec fixed**. The full-run write slowdown
was not reproduced in this paired check. Read throughput remained within about
4% of baseline across thread counts; the single compaction sample was 0.60 ms
slower. These are local measurements, not a statistical guarantee of identical
performance or a tail-latency study.

## Remaining design limits

- Compaction still blocks writes; aggregate scans still provide rolling views.
  Group commit and concurrent/segmented compaction remain deferred enhancements.
- A failed operation with uncertain durability may appear after recovery. Reads
  from a failed store throw; callers must close/reopen before continuing.
- The existing WAL format cannot independently validate record boundaries: damaged
  lengths can make some middle corruption look like a damaged final record or
  an incomplete tail. Last-record CRC repair remains compatible with
  the existing recovery policy. A versioned format with independent header framing
  would be a separate enhancement.
- Process-crash tests do not emulate power loss, storage hardware failures, or
  prove starvation freedom. Durability assumes successful filesystem syncs are honored.

## Follow-up full audit

The follow-up audit reproduced another data-loss case: opening a relative database,
changing the process working directory, then compacting redirected the WAL and
subsequent acknowledged writes away from the original database. Construction now
binds paths to an absolute location. Empty and NUL-containing paths are rejected
before any file operation, preventing POSIX filename truncation from aliasing the
lock, WAL, and temporary file names.

The audit also fixed the fault harness to follow the replacement descriptor after
compaction and now runs rollback cases on both original and compacted WALs.
A 300-operation ordered-map reference model checks updates/deletes/TTL/scans across
compaction and restarts. Concurrent tests include shared hot keys and compare the
entire in-memory result with recovered data. `BUILD_TESTING=OFF` now omits tests;
the separate disabled-tests build succeeded and CTest listed zero tests.

The benchmark tables above precede this follow-up. The production change in this
follow-up affects path validation/resolution during construction; write, read,
and compaction hot paths are unchanged. The full benchmark was not repeated.

Final follow-up results: Release **2/2 passed (28.25 s)**, ThreadSanitizer
**2/2 passed (28.59 s)**, ASan/UBSan with LeakSanitizer **2/2 passed (28.63 s)**.
All final builds were warning-free and `git diff --check` passed. No additional
reproducible implementation defects were found after the follow-up fixes; the
remaining format, process-lifetime, filesystem, and scheduling limits are explicit.
