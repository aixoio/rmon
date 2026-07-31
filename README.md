# rmon

A terminal-based network throughput monitor written in Rust. It displays live upload and download rates alongside separate rolling charts for each direction.

## Requirements

- [Rust](https://www.rust-lang.org/tools/install) with Cargo
- A terminal that supports ANSI escape sequences

## Install

Clone the repository and build an optimized binary:

```sh
cargo build --release
```

The binary is written to `target/release/rmon`.

## Usage

Run directly with Cargo during development:

```sh
cargo run --release
```

Or run the built binary:

```sh
./target/release/rmon
```

`rmon` refreshes every 500 ms and reports aggregate traffic across the system's network interfaces. The charts retain the most recent two minutes of samples.

## Controls

| Key | Action |
| --- | --- |
| `q` or `Esc` | Quit |
| `Ctrl-C` | Quit |

## License

Distributed under the [BSD 3-Clause License](LICENSE).
