# Head-to-head comparison

Compiles Luxbyte's routing analysis and
[shahrah](https://github.com/rs-alireza/shahrah)'s into one process and runs
them against the same statements on the same machine, so the comparison
contains no estimation and no cross-machine arithmetic.

It is kept out of the main workspace because shahrah parses with `pg_query`,
which binds to PostgreSQL's C parser and needs `libclang` at build time.
Nothing in Luxbyte itself needs it.

## Running it

```bash
# Windows, if LLVM is not installed: pip install libclang
export LIBCLANG_PATH=/path/to/directory/containing/libclang.dll

cargo run --release --manifest-path compare/Cargo.toml   # correctness table
cargo bench --manifest-path compare/Cargo.toml           # speed
```

The shahrah revision is pinned so the numbers stay reproducible.
