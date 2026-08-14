# pq-ldk-sample
Sample node implementation for pq-rust-lightning, a post-quantum fork of rust-lightning. It is based on [ldk-sample](https://github.com/lightningdevkit/ldk-sample) and is used to exercise the fork's post-quantum functionality with real nodes, including interoperability tests against vanilla rust-lightning.

## Installation
```
git clone https://github.com/ahmet-kurt/pq-ldk-sample
```
The crate depends on rust-lightning through path dependencies, so a checkout of pq-rust-lightning must sit next to this directory as `../pq-rust-lightning`. Building without the post-quantum feature also works against a vanilla rust-lightning checkout at commit 384e0d6 placed at the same path, which produces a vanilla node for interoperability testing.

## Usage
```
cd pq-ldk-sample
cargo run <bitcoind-rpc-username>:<bitcoind-rpc-password>@<bitcoind-rpc-host>:<bitcoind-rpc-port> <ldk_storage_directory_path> [<ldk-peer-listening-port>] [<bitcoin-network>] [<announced-node-name>] [<announced-listen-addr>]
```

For a post-quantum node, enable the feature together with the dependency features:
```
cargo run --features "post-quantum,lightning/post-quantum,lightning-invoice/post-quantum,lightning-net-tokio/post-quantum" -- <arguments as above>
```

`bitcoind`'s RPC username and password likely can be found through `cat ~/.bitcoin/.cookie`.

`bitcoin-network`: defaults to `testnet`. Options: `testnet`, `regtest`, and `signet`.

`ldk-peer-listening-port`: defaults to 9735.

`announced-listen-addr` and `announced-node-name`: default to nothing, disabling any public announcements of this node.
`announced-listen-addr` can be set to an IPv4 or IPv6 address to announce that as a publicly-connectable address for this node.
`announced-node-name` can be any string up to 32 bytes in length, representing this node's alias.

The post-quantum build additionally accepts the following flags after the arguments above.

`--pq-listen-port=<port>`: accepts hybrid post-quantum BOLT 8 connections on a dedicated port.

`--pq-require-payments`: refuses to send a payment that cannot be routed entirely over post-quantum capable hops.

`--pq-require-inbound`: fails back any inbound HTLC that is not post-quantum protected.

`--pq-blinded-paths`: builds post-quantum blinded paths for this node's BOLT 12 offers and invoices.

## License

Licensed under either:

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
