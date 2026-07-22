# Vendored demoparser — provenance & regeneration

`vendor/parser` and `vendor/csgoproto` are vendored, in-tree copies of the CS2
demo parser from **[LaihoE/demoparser](https://github.com/LaihoE/demoparser)**,
extracted at:

| | |
|---|---|
| Commit | `edc8b46` |
| Tag | `v0.41.1` |
| Crates | `parser` (0.1.1) + its path dep `csgoproto` (0.1.5) |
| License | MIT (see `vendor/NOTICE`) |

`parser` is a **dev-dependency of `crates/cs2-demo-rank`** only — it is used by
the cross-validation examples (`examples/test_demo.rs`, `examples/validate_demo.rs`)
to compare our minimal rank extractor against the reference parser. It is not
part of any shipped binary. We vendor rather than depend on a sibling checkout
so the workspace is a single self-contained checkout with no build-time network
(CI does a plain `actions/checkout` and `cargo build --locked`).

## What we changed vs. pristine upstream

1. **Deleted `vendor/csgoproto/build.rs`.** Upstream's build script ran
   `git clone --depth=1 https://github.com/SteamDatabase/GameTracking-CS2` at
   build time (network required, unpinned → non-reproducible) and prost-generated
   `src/protobuf.rs` into the source tree. We instead **commit the generated
   `src/protobuf.rs`, `src/maps.rs`, `src/message_type.rs`** (referenced as
   modules in `csgoproto/src/lib.rs`), so builds are offline and deterministic.
   Removed the now-unused `prost-build` build-dependency.

2. **Deleted `vendor/parser/build.rs`.** It ran `cargo run` in `../csgoproto`
   to regenerate `maps.rs`/`message_type.rs` — same story; those files are
   committed.

3. **Dropped the `[profile.*]` tables** from `vendor/parser/Cargo.toml` (Cargo
   ignores profiles in path-dependency crates; the root workspace owns them).

4. **Omitted `vendor/parser/test_demo.dem`** (a ~60 MB fixture used only by the
   upstream `#[cfg(test)] mod e2e_test`, which is not compiled when `parser` is
   consumed as a path dependency).

## Bumping to a newer upstream

```bash
# From a checkout of LaihoE/demoparser at the desired rev <REV>:
D=/path/to/demoparser
cd /path/to/steam_bot
rm -rf vendor/parser vendor/csgoproto
mkdir -p vendor/parser vendor/csgoproto
rsync -a --exclude target/ --exclude GameTracking-CS2/ --exclude .git/ \
      --exclude Cargo.lock --exclude build.rs --exclude test_demo.dem \
      "$D/src/parser/"    vendor/parser/
rsync -a --exclude target/ --exclude GameTracking-CS2/ --exclude .git/ \
      --exclude Cargo.lock --exclude build.rs \
      "$D/src/csgoproto/" vendor/csgoproto/

# Re-apply the hermetic surgery (items 1–3 above):
#   - drop [build-dependencies] prost-build from vendor/csgoproto/Cargo.toml
#   - drop [profile.*] from vendor/parser/Cargo.toml
# Ensure the generated csgoproto src/{protobuf,maps,message_type}.rs are present
# (build the sibling once so its build.rs writes them, then re-copy).

# Verify it still builds + the examples compile:
cargo build --locked --workspace
cargo clippy --all-targets --workspace -- -D warnings
```

`csgoproto`'s generated files are the *pinned schema snapshot* for that upstream
rev. Regenerating them from raw `.proto` requires upstream's build.rs (which
clones GameTracking-CS2); do that in an upstream checkout and copy the resulting
`.rs` files back.
