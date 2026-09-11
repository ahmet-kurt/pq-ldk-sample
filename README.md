# pq-ldk-sample: A Post-Quantum Lightning Node

This repository is the node implementation that drives [pq-rust-lightning](https://github.com/ahmet-kurt/pq-rust-lightning), our post-quantum research fork of rust-lightning, with real Lightning nodes. It is a modified copy of [ldk-sample](https://github.com/lightningdevkit/ldk-sample), the reference node of the Lightning Development Kit (LDK), and it ran the interoperability, payment latency and gossip overhead experiments of the paper below. The fork's [README](https://github.com/ahmet-kurt/pq-rust-lightning/blob/main/README.md) describes the design behind this node.

If you use this code in your research, please cite the paper:

```bibtex
@misc{kurt2026pqln,
  author = {Ahmet Kurt and Abdul-Salem Beibitkhan and Yacoub Hanna and Abdullah Aydeger},
  title  = {{PQLN}: Post-Quantum Security for the {Bitcoin} {Lightning} {Network's} Off-Chain Surfaces},
  year   = {2026},
  url    = {https://arxiv.org/abs/2609.13781}
}
```

Like the fork, this node is a research artifact rather than a production node, and it supports the same networks as ldk-sample, namely regtest, testnet and signet.

* [What This Node Adds to ldk-sample](#what-this-node-adds-to-ldk-sample)
* [Requirements](#requirements)
* [Building](#building)
* [Running](#running)
* [Commands](#commands)
* [Example: Two Post-Quantum Nodes on regtest](#example-two-post-quantum-nodes-on-regtest)
* [Logs and Evaluation Hooks](#logs-and-evaluation-hooks)
* [Limitations](#limitations)
* [License](#license)

## What This Node Adds to ldk-sample

The changes against upstream ldk-sample fall into three groups.

**The port to current rust-lightning.** The node targets rust-lightning at commit 384e0d6 of its `main` branch (August 2026), the base commit of the fork, so it follows the API changes since the last ldk-sample release. Upstream dropped native DNS resolution, so this node no longer pays human-readable names. The crates of rust-lightning are path dependencies that resolve to `../pq-rust-lightning`, so the same manifest builds against the fork or against a vanilla checkout, and for that reason the repository ships no `Cargo.lock` and no CI workflow.

**The post-quantum surface.** Behind the `post-quantum` cargo feature, the node opens a dedicated listen port for the hybrid ML-KEM BOLT 8 handshake and connects to such a port with the `connectpeerpq` command. It exposes the fork's three enforcement flags as startup flags and creates the signature-only BOLT 11 invoice variant. It also prints the node's ML-DSA and ML-KEM identities in `nodeinfo` and the pins it holds for other nodes in `listnodes`. The remaining protections need no command of their own, because the fork attaches its ML-DSA signatures to gossip, invoices and offers on its own and verifies them inside the pay path.

**Evaluation support independent of the feature.** The node supports BOLT 12 refunds. It acts as a static invoice server for asynchronous payments, so it persists and serves invoices for often-offline recipients and replays intercepted onion messages when such a recipient reconnects. It also registers itself as an asynchronous recipient and forwards over unannounced channels so that it can serve a private recipient. It adds the `announce`, `listnodes` and `graphinfo` commands and the `--gossip-stats` flag for the gossip experiments. It sets TCP_NODELAY on every peer connection, because Nagle's algorithm otherwise holds the small `commitment_signed` that follows an `update_add_htlc` until the peer's delayed acknowledgment arrives, which adds about 40 ms per hop on a 1500-byte MTU link and only for the vanilla message size. It also locks the coins selected for a funding transaction, so concurrent channel opens cannot spend the same UTXO.

## Requirements

- Linux. The experiments of the paper ran on Ubuntu 26.04, and on Windows the node builds inside WSL2.
- A stable Rust toolchain and a C toolchain such as `gcc` or `clang` for the `secp256k1-sys` dependency of rust-lightning.
- Bitcoin Core with RPC access. The paper used Bitcoin Core v31.1 in regtest mode.
- A checkout of the fork beside this repository, so that the path dependencies resolve:

```
some-folder/
    pq-rust-lightning/    # the fork, or a vanilla rust-lightning checkout at commit 384e0d6
    pq-ldk-sample/        # this repository
```

## Building

A post-quantum node enables the feature of this crate together with the features of the underlying crates:

```bash
cd pq-ldk-sample
cargo build --release --features "post-quantum,lightning/post-quantum,lightning-invoice/post-quantum,lightning-net-tokio/post-quantum"
```

A build without the feature produces a vanilla node:

```bash
cargo build --release
```

Without the feature, the build compiles none of the fork's post-quantum code, so the node behaves as an unmodified rust-lightning node. We built the vanilla binary of the paper against a pristine rust-lightning checkout at commit 384e0d6 placed at `../pq-rust-lightning`, so every difference between the two binaries comes from the fork alone.

To build a node at another parameter set, check out the [`configurable`](https://github.com/ahmet-kurt/pq-rust-lightning/tree/configurable) branch of the fork and add its features to the list, for example `lightning/pq-fn-dsa-512` for FN-DSA-512 or `lightning/pq-ml-kem-1024` for ML-KEM-1024.

## Running

```bash
./target/release/pq-ldk-sample <bitcoind-rpc-username>:<bitcoind-rpc-password>@<bitcoind-rpc-host>:<bitcoind-rpc-port> <ldk_storage_directory_path> [<ldk-peer-listening-port>] [<bitcoin-network>] [<announced-node-name>] [<announced-listen-addr>...] [flags]
```

The positional arguments are those of ldk-sample.

- The RPC credentials of `bitcoind` are optional, since the node also reads them from the `RPC_USER` and `RPC_PASSWORD` environment variables, from a `.env` file in the current directory, or from the `.cookie` file of the Bitcoin data directory.
- `ldk_storage_directory_path` is the directory that holds the node's data. The node keeps everything under `<ldk_storage_directory_path>/.ldk` and writes its log to `<ldk_storage_directory_path>/.ldk/logs/logs.txt`.
- `ldk-peer-listening-port` defaults to 9735 and accepts classical BOLT 8 connections.
- `bitcoin-network` defaults to `testnet`, and the options are `testnet`, `regtest` and `signet`.
- `announced-node-name` and `announced-listen-addr` default to nothing, which disables the public announcement of the node. The name is an alias of up to 32 bytes, and each address is a reachable IPv4 or IPv6 `host:port` of the node.

The flags can appear anywhere on the command line.

| Flag | Build | Effect |
|---|---|---|
| `--pq-listen-port=<port>` | post-quantum | Accepts hybrid post-quantum BOLT 8 connections on a dedicated port, beside the classical port. |
| `--pq-require-payments` | post-quantum | Sets `require_post_quantum_payments`, so the node refuses to send a payment that cannot be routed entirely over hops with pinned ML-KEM keys. |
| `--pq-require-inbound` | post-quantum | Sets `require_post_quantum_inbound`, so the node fails back any inbound HTLC that carries no ML-KEM ciphertext list. |
| `--pq-blinded-paths` | post-quantum | Sets `build_post_quantum_blinded_paths`, so the node builds post-quantum blinded paths for its offers, invoices and refunds. |
| `--gossip-stats` | both | Writes one `GOSSIP-STATS:` line with the wire size of every received gossip message to the log. |

A vanilla build rejects the `--pq-*` flags with an error.

## Commands

The node reads commands from its console, and `help` lists all of them. The following commands are new or changed relative to ldk-sample.

| Command | Build | Description |
|---|---|---|
| `connectpeerpq pubkey@host:port [kem_key_hex]` | post-quantum | Connects to a peer's post-quantum port with the hybrid handshake. The responder's ML-KEM key comes from the gossip pin, or from the hex argument when the peer is not pinned yet. |
| `getinvoice <amt_msats> <expiry_secs> [--pq-omit-pubkey]` | post-quantum | Creates a BOLT 11 invoice with an ML-DSA signature and, by default, the embedded ML-DSA public key. The flag omits the key, so the invoice stays small enough for a QR code and the payer verifies it against the gossip pin. |
| `nodeinfo` | post-quantum | Additionally prints the node's `pq_node_id` (ML-DSA public key) and `pq_kem_node_id` (ML-KEM encapsulation key). |
| `listnodes` | both | Lists the nodes of the network graph. The post-quantum build also prints whether each node's ML-DSA and ML-KEM keys are pinned. |
| `graphinfo` | both | Prints one `GRAPHINFO` line with the node, announcement, channel and update counts of the graph, and the pin count in the post-quantum build. |
| `announce` | both | Broadcasts the node's `node_announcement` at once instead of waiting for the periodic timer. It requires an announced channel. |
| `getrefund <amt_msats> <expiry_secs>` and `claimrefund <refund>` | both | Creates a BOLT 12 refund and pays one. |
| `asyncpaths <recipient_id>` | both | On a static invoice server, creates the blinded paths for an asynchronous recipient to pass to `setasyncserver`. |
| `setasyncserver <path_hex>...` and `getasyncoffer` | both | On an asynchronous recipient, registers the server's paths and prints the resulting asynchronous receive offer. |

The commands of ldk-sample keep working as before. `sendpayment` pays a BOLT 11 invoice or a BOLT 12 offer and runs the fork's post-quantum verification inside the pay path, and `getoffer` produces an offer that commits a post-quantum key in the post-quantum build.

## Example: Two Post-Quantum Nodes on regtest

The following session connects two post-quantum nodes over the hybrid transport, opens a public channel, lets the nodes pin each other's keys from gossip, and pays a post-quantum invoice over a hybrid onion. It uses the RPC credentials `ldk:ldk` and the default regtest RPC port 18443.

Start Bitcoin Core on regtest and fund its wallet, since the nodes fund their channels from it:

```bash
bitcoind -regtest -server=1 -listen=0 -rpcuser=ldk -rpcpassword=ldk -fallbackfee=0.0001 -daemon
bitcoin-cli -regtest -rpcuser=ldk -rpcpassword=ldk createwallet default
bitcoin-cli -regtest -rpcuser=ldk -rpcpassword=ldk generatetoaddress 150 "$(bitcoin-cli -regtest -rpcuser=ldk -rpcpassword=ldk getnewaddress)"
```

Start Alice and Bob in two terminals. Each node listens for classical connections on its peer port and for post-quantum connections on the port of `--pq-listen-port`:

```bash
./target/release/pq-ldk-sample ldk:ldk@127.0.0.1:18443 /tmp/alice 9735 regtest alice 127.0.0.1:9735 --pq-listen-port=9736
```

```bash
./target/release/pq-ldk-sample ldk:ldk@127.0.0.1:18443 /tmp/bob 9737 regtest bob 127.0.0.1:9737 --pq-listen-port=9738
```

In Bob's console, run `nodeinfo` and note his `node_pubkey` and `pq_kem_node_id`. Alice has not seen Bob's gossip yet, so she passes his ML-KEM key explicitly when she connects to his post-quantum port:

```
connectpeerpq <bob_node_pubkey>@127.0.0.1:9738 <bob_pq_kem_node_id>
```

The console prints `SUCCESS: connected to peer <bob_node_pubkey> over post-quantum transport`, Alice's log records `PQ: initiating hybrid ML-KEM-768 BOLT 8 handshake (2322 byte act one)` and `PQ: completed hybrid ML-KEM-768 BOLT 8 handshake (initiator)`, and Bob's log records the responder side. Alice now opens a public channel to Bob and confirms it with six blocks:

```
openchannel <bob_node_pubkey>@127.0.0.1:9737 1000000 --public
```

```bash
bitcoin-cli -regtest -rpcuser=ldk -rpcpassword=ldk generatetoaddress 6 "$(bitcoin-cli -regtest -rpcuser=ldk -rpcpassword=ldk getnewaddress)"
```

Both consoles print `EVENT: Channel <channel_id> with peer <node_pubkey> is ready to be used!` once the channel is usable. The nodes announce the channel and then broadcast their `node_announcement` messages, which carry their post-quantum keys, and the `announce` command triggers the broadcast at once. Each log records `PQ: signed node_announcement (ML-DSA-44 pubkey 1312 B, ML-KEM-768 key 1184 B, sig 2420 B)` on the sending side and `PQ: pinned ML-DSA pubkey for node <id>` and `PQ: pinned ML-KEM key for node <id>` on the receiving side, and `listnodes` shows the other node with `pq_pinned: true` and `pq_kem_pinned: true`.

Bob now creates a signature-only invoice for 10,000 sat, and Alice pays it:

```
getinvoice 10000000 3600 --pq-omit-pubkey
```

```
sendpayment <invoice>
```

Bob's log records `PQ: attached ML-DSA signature to BOLT 11 invoice (signature-only, 2420-byte signature)`. Alice's log records `PQ: verified BOLT 11 invoice ML-DSA signature against trusted key`, since she verifies the invoice against Bob's pinned key, and then `PQ: built hybrid ML-KEM payment onion with a 21760-byte ciphertext trail over 1 unblinded hop(s)`. Alice's console prints `EVENT: successfully sent payment ...` and Bob's console prints `EVENT: claimed payment ...` when the payment settles.

A vanilla node built as described above joins the same network over the classical port with `connectpeer`, stores the post-quantum gossip without relaying it, and completes every payment classically. With `--pq-require-payments`, a post-quantum sender refuses such a classical route and logs `PQ: refusing to send a non-post-quantum payment (require_post_quantum_payments is set)`, and with `--pq-require-inbound`, a post-quantum receiver fails back a classical HTLC and logs `PQ: failing back a non-post-quantum inbound HTLC (require_post_quantum_inbound is set)`.

## Logs and Evaluation Hooks

The node writes its LDK log to `<ldk_storage_directory_path>/.ldk/logs/logs.txt` at every level except gossip. Every post-quantum operation of the fork logs a line with the prefix `PQ:`, so the log shows whether a handshake, a payment or a verification ran protected or classical. The evaluation harness of the paper relies on these lines, on the `GOSSIP-STATS:` lines from `--gossip-stats` for every received `channel_announcement`, `channel_update` and `node_announcement`, and on the `GRAPHINFO` line of the `graphinfo` command, polled until a node's graph is complete.

## Limitations

- The node does not pay human-readable names, since upstream rust-lightning dropped native DNS resolution and this node does not wire up the external resolver.
- Like ldk-sample, the node supports regtest, testnet and signet only.
- The post-quantum flags exist only in the post-quantum build, and a vanilla build refuses them.

## License

Like ldk-sample, this repository is licensed under either the Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE)) or the MIT License ([LICENSE-MIT](LICENSE-MIT)), at your option.
