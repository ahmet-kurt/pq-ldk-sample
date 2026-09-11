use crate::disk::{INBOUND_PAYMENTS_FNAME, OUTBOUND_PAYMENTS_FNAME};
use crate::hex_utils;
use crate::{
	ChainMonitor, ChannelManager, HTLCStatus, InboundPaymentInfoStorage, MillisatAmount,
	NetworkGraph, OutboundPaymentInfoStorage, PaymentInfo, PeerManager,
};
use bitcoin::hashes::sha256::Hash as Sha256;
use bitcoin::hashes::Hash;
use bitcoin::network::Network;
use bitcoin::secp256k1::PublicKey;
use lightning::blinded_path::message::BlindedMessagePath;
use lightning::chain::channelmonitor::Balance;
use lightning::ln::channelmanager::{
	Bolt11InvoiceParameters, OptionalBolt11PaymentParams, OptionalOfferPaymentParams, PaymentId,
};
use lightning::ln::msgs::SocketAddress;
use lightning::ln::outbound_payment::{RecipientOnionFields, Retry};
use lightning::ln::types::ChannelId;
use lightning::offers::offer::{self, Offer};
use lightning::offers::refund::Refund;
use lightning::onion_message::dns_resolution::HumanReadableName;
use lightning::routing::gossip::NodeId;
use lightning::routing::router::{PaymentParameters, RouteParameters, RouteParametersConfig};
#[cfg(feature = "post-quantum")]
use lightning::sign::NodeSigner;
use lightning::sign::{EntropySource, KeysManager};
use lightning::types::payment::PaymentPreimage;
use lightning::util::config::{ChannelHandshakeConfig, ChannelHandshakeLimits, UserConfig};
use lightning::util::persist::KVStore;
use lightning::util::ser::{Readable, Writeable};
use lightning_invoice::Bolt11Invoice;
use lightning_persister::fs_store::v1::FilesystemStore;
#[cfg(feature = "post-quantum")]
use std::convert::TryInto;
use std::env;
use std::io::Write;
use std::net::{SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use tokio::io::{AsyncBufReadExt, BufReader};

pub(crate) struct LdkUserInfo {
	pub(crate) bitcoind_rpc_username: String,
	pub(crate) bitcoind_rpc_password: String,
	pub(crate) bitcoind_rpc_port: u16,
	pub(crate) bitcoind_rpc_host: String,
	pub(crate) ldk_storage_dir_path: String,
	pub(crate) ldk_peer_listening_port: u16,
	pub(crate) ldk_announced_listen_addr: Vec<SocketAddress>,
	pub(crate) ldk_announced_node_name: [u8; 32],
	pub(crate) network: Network,
	pub(crate) gossip_stats: bool,
	#[cfg(feature = "post-quantum")]
	pub(crate) pq_listen_port: Option<u16>,
	#[cfg(feature = "post-quantum")]
	pub(crate) pq_require_payments: bool,
	#[cfg(feature = "post-quantum")]
	pub(crate) pq_require_inbound: bool,
	#[cfg(feature = "post-quantum")]
	pub(crate) pq_blinded_paths: bool,
}

pub(crate) async fn poll_for_user_input(
	peer_manager: Arc<PeerManager>, channel_manager: Arc<ChannelManager>,
	chain_monitor: Arc<ChainMonitor>, keys_manager: Arc<KeysManager>,
	network_graph: Arc<NetworkGraph>, inbound_payments: Arc<Mutex<InboundPaymentInfoStorage>>,
	outbound_payments: Arc<Mutex<OutboundPaymentInfoStorage>>, fs_store: Arc<FilesystemStore>,
	ldk_announced_node_name: [u8; 32], ldk_announced_listen_addr: Vec<SocketAddress>,
) {
	println!(
		"LDK startup successful. Enter \"help\" to view available commands. Press Ctrl-D to quit."
	);
	println!("LDK logs are available at <your-supplied-ldk-data-dir-path>/.ldk/logs");
	println!("Local Node ID is {}.", channel_manager.get_our_node_id());

	let mut input = BufReader::new(tokio::io::stdin()).lines();
	'read_command: loop {
		print!("> ");
		std::io::stdout().flush().unwrap(); // Without flushing, the `>` doesn't print
		let line = match input.next_line().await {
			Ok(Some(l)) => l,
			Err(e) => {
				break println!("ERROR: {}", e);
			},
			Ok(None) => {
				break println!("ERROR: End of stdin");
			},
		};

		let mut words = line.split_whitespace();
		if let Some(word) = words.next() {
			match word {
				"help" => help(),
				"openchannel" => {
					let peer_pubkey_and_ip_addr = words.next();
					let channel_value_sat = words.next();
					if peer_pubkey_and_ip_addr.is_none() || channel_value_sat.is_none() {
						println!("ERROR: openchannel has 2 required arguments: `openchannel pubkey@host:port channel_amt_satoshis` [--public] [--with-anchors]");
						continue;
					}
					let peer_pubkey_and_ip_addr = peer_pubkey_and_ip_addr.unwrap();

					let mut pubkey_and_addr = peer_pubkey_and_ip_addr.split("@");
					let pubkey = pubkey_and_addr.next();
					let peer_addr_str = pubkey_and_addr.next();
					let pubkey = hex_utils::to_compressed_pubkey(pubkey.unwrap());
					if pubkey.is_none() {
						println!("ERROR: unable to parse given pubkey for node");
						continue;
					}
					let pubkey = pubkey.unwrap();

					if peer_addr_str.is_none() {
						if peer_manager.peer_by_node_id(&pubkey).is_none() {
							println!("ERROR: Peer address not provided and peer is not connected");
							continue;
						}
					} else {
						let (pubkey, peer_addr) =
							match parse_peer_info(peer_pubkey_and_ip_addr.to_string()) {
								Ok(info) => info,
								Err(e) => {
									println!("{:?}", e.into_inner().unwrap());
									continue;
								},
							};

						if connect_peer_if_necessary(pubkey, peer_addr, peer_manager.clone())
							.await
							.is_err()
						{
							continue;
						};
					}

					let chan_amt_sat: Result<u64, _> = channel_value_sat.unwrap().parse();
					if chan_amt_sat.is_err() {
						println!("ERROR: channel amount must be a number");
						continue;
					}
					let (mut announce_channel, mut with_anchors) = (false, false);
					while let Some(word) = words.next() {
						match word {
							"--public" | "--public=true" => announce_channel = true,
							"--public=false" => announce_channel = false,
							"--with-anchors" | "--with-anchors=true" => with_anchors = true,
							"--with-anchors=false" => with_anchors = false,
							_ => {
								println!("ERROR: invalid boolean flag format. Valid formats: `--option`, `--option=true` `--option=false`");
								continue;
							},
						}
					}

					let _ = open_channel(
						pubkey,
						chan_amt_sat.unwrap(),
						announce_channel,
						with_anchors,
						channel_manager.clone(),
					);
				},
				"sendpayment" => {
					let invoice_str = words.next();
					if invoice_str.is_none() {
						println!("ERROR: sendpayment requires an invoice: `sendpayment <invoice> [amount_msat]`");
						continue;
					}
					let invoice_str = invoice_str.unwrap();

					let mut user_provided_amt: Option<u64> = None;
					if let Some(amt_msat_str) = words.next() {
						match amt_msat_str.parse() {
							Ok(amt) => user_provided_amt = Some(amt),
							Err(e) => {
								println!("ERROR: couldn't parse amount_msat: {}", e);
								continue;
							},
						};
					}

					if let Ok(offer) = Offer::from_str(invoice_str) {
						let random_bytes = keys_manager.get_secure_random_bytes();
						let payment_id = PaymentId(random_bytes);

						let amt_msat = match (offer.amount(), user_provided_amt) {
							(Some(offer::Amount::Bitcoin { amount_msats }), _) => amount_msats,
							(_, Some(amt)) => amt,
							(amt, _) => {
								println!("ERROR: Cannot process non-Bitcoin-denominated offer value {:?}", amt);
								continue;
							},
						};
						if user_provided_amt.is_some() && user_provided_amt != Some(amt_msat) {
							println!("Amount didn't match offer of {}msat", amt_msat);
							continue;
						}

						while user_provided_amt.is_none() {
							print!("Paying offer for {} msat. Continue (Y/N)? >", amt_msat);
							std::io::stdout().flush().unwrap();

							let line = match input.next_line().await {
								Ok(Some(l)) => l,
								Err(e) => {
									println!("ERROR: {}", e);
									break 'read_command;
								},
								Ok(None) => {
									println!("ERROR: End of stdin");
									break 'read_command;
								},
							};

							if line.starts_with("Y") {
								break;
							}
							if line.starts_with("N") {
								continue 'read_command;
							}
						}

						outbound_payments.lock().unwrap().payments.insert(
							payment_id,
							PaymentInfo {
								preimage: None,
								secret: None,
								status: HTLCStatus::Pending,
								amt_msat: MillisatAmount(Some(amt_msat)),
							},
						);
						fs_store
							.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound_payments.encode())
							.await
							.unwrap();

						let params = OptionalOfferPaymentParams {
							retry_strategy: Retry::Timeout(Duration::from_secs(10)),
							..Default::default()
						};
						let amt = Some(amt_msat);
						let pay = channel_manager.pay_for_offer(&offer, amt, payment_id, params);
						if pay.is_ok() {
							println!("Payment in flight");
						} else {
							println!("ERROR: Failed to pay: {:?}", pay);
						}
					} else if HumanReadableName::from_encoded(invoice_str).is_ok() {
						// Paying to a human readable name now requires resolving it into an
						// `OfferFromHrn` externally (e.g. via the bitcoin-payment-instructions
						// crate) and calling `pay_for_offer_from_hrn`, which this sample no
						// longer wires up.
						println!("ERROR: paying human readable names is not supported");
						continue;
					} else {
						match Bolt11Invoice::from_str(invoice_str) {
							Ok(invoice) => {
								send_payment(
									&channel_manager,
									&invoice,
									user_provided_amt,
									&outbound_payments,
									&*fs_store,
								)
								.await
							},
							Err(e) => {
								println!("ERROR: invalid invoice: {:?}", e);
							},
						}
					}
				},
				"keysend" => {
					let dest_pubkey = match words.next() {
						Some(dest) => match hex_utils::to_compressed_pubkey(dest) {
							Some(pk) => pk,
							None => {
								println!("ERROR: couldn't parse destination pubkey");
								continue;
							},
						},
						None => {
							println!("ERROR: keysend requires a destination pubkey: `keysend <dest_pubkey> <amt_msat>`");
							continue;
						},
					};
					let amt_msat_str = match words.next() {
						Some(amt) => amt,
						None => {
							println!("ERROR: keysend requires an amount in millisatoshis: `keysend <dest_pubkey> <amt_msat>`");
							continue;
						},
					};
					let amt_msat: u64 = match amt_msat_str.parse() {
						Ok(amt) => amt,
						Err(e) => {
							println!("ERROR: couldn't parse amount_msat: {}", e);
							continue;
						},
					};
					keysend(
						&channel_manager,
						dest_pubkey,
						amt_msat,
						&*keys_manager,
						&outbound_payments,
						&*fs_store,
					)
					.await;
				},
				"getoffer" => {
					let offer_builder = channel_manager.create_offer_builder();
					if let Err(e) = offer_builder {
						println!("ERROR: Failed to initiate offer building: {:?}", e);
						continue;
					}

					let amt_str = words.next();
					let offer = if amt_str.is_some() {
						let amt_msat: Result<u64, _> = amt_str.unwrap().parse();
						if amt_msat.is_err() {
							println!("ERROR: getoffer provided payment amount was not a number");
							continue;
						}
						offer_builder.unwrap().amount_msats(amt_msat.unwrap()).build()
					} else {
						offer_builder.unwrap().build()
					};

					if offer.is_err() {
						println!("ERROR: Failed to build offer: {:?}", offer.unwrap_err());
					} else {
						// Note that unlike BOLT11 invoice creation we don't bother to add a
						// pending inbound payment here, as offers can be reused and don't
						// correspond with individual payments.
						println!("{}", offer.unwrap());
					}
				},
				"getinvoice" => {
					let amt_str = words.next();
					if amt_str.is_none() {
						println!("ERROR: getinvoice requires an amount in millisatoshis");
						continue;
					}

					let amt_msat: Result<u64, _> = amt_str.unwrap().parse();
					if amt_msat.is_err() {
						println!("ERROR: getinvoice provided payment amount was not a number");
						continue;
					}

					let expiry_secs_str = words.next();
					if expiry_secs_str.is_none() {
						println!("ERROR: getinvoice requires an expiry in seconds");
						continue;
					}

					let expiry_secs: Result<u32, _> = expiry_secs_str.unwrap().parse();
					if expiry_secs.is_err() {
						println!("ERROR: getinvoice provided expiry was not a number");
						continue;
					}

					let pq_omit_pubkey = match words.next() {
						Some("--pq-omit-pubkey") => true,
						Some(flag) => {
							println!("ERROR: getinvoice got unknown flag {}", flag);
							continue;
						},
						None => false,
					};

					let write_future = {
						let mut inbound_payments = inbound_payments.lock().unwrap();
						get_invoice(
							amt_msat.unwrap(),
							&mut inbound_payments,
							&channel_manager,
							expiry_secs.unwrap(),
							pq_omit_pubkey,
						);
						fs_store.write("", "", INBOUND_PAYMENTS_FNAME, inbound_payments.encode())
					};
					write_future.await.unwrap();
				},
				"getrefund" => {
					let amt_msat: u64 = match words.next().map(|a| a.parse()) {
						Some(Ok(amt)) => amt,
						_ => {
							println!("ERROR: getrefund requires an amount in millisatoshis and an expiry in seconds: `getrefund <amt_msats> <expiry_secs>`");
							continue;
						},
					};
					let expiry_secs: u64 = match words.next().map(|e| e.parse()) {
						Some(Ok(expiry)) => expiry,
						_ => {
							println!("ERROR: getrefund requires an expiry in seconds: `getrefund <amt_msats> <expiry_secs>`");
							continue;
						},
					};
					let payment_id = PaymentId(keys_manager.get_secure_random_bytes());
					let absolute_expiry = SystemTime::now()
						.duration_since(SystemTime::UNIX_EPOCH)
						.unwrap() + Duration::from_secs(expiry_secs);
					let refund = match channel_manager.create_refund_builder(
						amt_msat,
						absolute_expiry,
						payment_id,
						Retry::Timeout(Duration::from_secs(10)),
						RouteParametersConfig::default(),
					) {
						Ok(builder) => match builder.build() {
							Ok(refund) => refund,
							Err(e) => {
								println!("ERROR: Failed to build refund: {:?}", e);
								continue;
							},
						},
						Err(e) => {
							println!("ERROR: Failed to initiate refund building: {:?}", e);
							continue;
						},
					};
					outbound_payments.lock().unwrap().payments.insert(
						payment_id,
						PaymentInfo {
							preimage: None,
							secret: None,
							status: HTLCStatus::Pending,
							amt_msat: MillisatAmount(Some(amt_msat)),
						},
					);
					fs_store
						.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound_payments.encode())
						.await
						.unwrap();
					println!("SUCCESS: generated refund, we will pay the invoice it draws");
					println!("{}", refund);
				},
				"claimrefund" => {
					let refund_str = match words.next() {
						Some(refund) => refund,
						None => {
							println!("ERROR: claimrefund requires a refund string: `claimrefund <refund>`");
							continue;
						},
					};
					let refund = match Refund::from_str(refund_str) {
						Ok(refund) => refund,
						Err(e) => {
							println!("ERROR: invalid refund: {:?}", e);
							continue;
						},
					};
					match channel_manager.request_refund_payment(&refund) {
						Ok(invoice) => {
							let payment_hash = invoice.payment_hash();
							inbound_payments.lock().unwrap().payments.insert(
								payment_hash,
								PaymentInfo {
									preimage: None,
									secret: None,
									status: HTLCStatus::Pending,
									amt_msat: MillisatAmount(Some(invoice.amount_msats())),
								},
							);
							fs_store
								.write("", "", INBOUND_PAYMENTS_FNAME, inbound_payments.encode())
								.await
								.unwrap();
							println!(
								"SUCCESS: sent invoice for refund of {} msat with payment hash {}, awaiting payment",
								invoice.amount_msats(),
								payment_hash
							);
						},
						Err(e) => {
							println!("ERROR: failed to request refund payment: {:?}", e);
						},
					}
				},
				"asyncpaths" => {
					let recipient_id = match words.next() {
						Some(id) => id.as_bytes().to_vec(),
						None => {
							println!("ERROR: asyncpaths requires a recipient id: `asyncpaths <recipient_id>`");
							continue;
						},
					};
					match channel_manager.blinded_paths_for_async_recipient(recipient_id, None) {
						Ok(paths) => {
							let path_hexes = paths
								.iter()
								.map(|path| hex_utils::hex_str(&path.encode()))
								.collect::<Vec<_>>();
							println!("SUCCESS: paths for the async recipient to hand to `setasyncserver`:");
							println!("{}", path_hexes.join(" "));
						},
						Err(()) => {
							println!("ERROR: failed to create async recipient paths, do we have connected peers?");
						},
					}
				},
				"setasyncserver" => {
					let mut paths = Vec::new();
					for path_hex in words.by_ref() {
						let path = hex_utils::to_vec(path_hex)
							.and_then(|bytes| BlindedMessagePath::read(&mut &bytes[..]).ok());
						match path {
							Some(path) => paths.push(path),
							None => {
								println!("ERROR: couldn't parse blinded message path hex");
								paths.clear();
								break;
							},
						}
					}
					if paths.is_empty() {
						println!("ERROR: setasyncserver requires paths from the server's `asyncpaths`: `setasyncserver <path_hex>...`");
						continue;
					}
					match channel_manager.set_paths_to_static_invoice_server(paths) {
						Ok(()) => {
							println!("SUCCESS: static invoice server paths set, building an async receive offer with the server");
						},
						Err(()) => {
							println!("ERROR: failed to set static invoice server paths");
						},
					}
				},
				"getasyncoffer" => match channel_manager.get_async_receive_offer() {
					Ok(offer) => println!("{}", offer),
					Err(()) => {
						println!("ERROR: no async receive offer ready yet, run `setasyncserver` and wait for the server exchange");
					},
				},
				#[cfg(feature = "post-quantum")]
				"connectpeerpq" => {
					let peer_pubkey_and_ip_addr = words.next();
					if peer_pubkey_and_ip_addr.is_none() {
						println!("ERROR: connectpeerpq requires peer connection info: `connectpeerpq pubkey@host:port [kem_key_hex]`");
						continue;
					}
					let (pubkey, peer_addr) =
						match parse_peer_info(peer_pubkey_and_ip_addr.unwrap().to_string()) {
							Ok(info) => info,
							Err(e) => {
								println!("{:?}", e.into_inner().unwrap());
								continue;
							},
						};
					// The responder's static ML-KEM key either comes in hex out of band or from
					// the key we pinned from the peer's gossip.
					let kem_key = match words.next() {
						Some(kem_hex) => {
							let kem_vec = hex_utils::to_vec(kem_hex);
							match kem_vec.and_then(|v| v.try_into().ok()) {
								Some(key) => key,
								None => {
									println!("ERROR: couldn't parse kem_key_hex as an ML-KEM encapsulation key");
									continue;
								},
							}
						},
						None => {
							let pinned = network_graph
								.read_only()
								.node(&NodeId::from_pubkey(&pubkey))
								.and_then(|node| node.pq_kem_node_id());
							match pinned {
								Some(key) => key,
								None => {
									println!("ERROR: no ML-KEM key pinned for peer from gossip, pass kem_key_hex explicitly");
									continue;
								},
							}
						},
					};
					if peer_manager.peer_by_node_id(&pubkey).is_some() {
						println!("ERROR: already connected to peer {}, disconnect first", pubkey);
						continue;
					}
					if do_connect_peer_pq(pubkey, kem_key, peer_addr, peer_manager.clone())
						.await
						.is_ok()
					{
						println!(
							"SUCCESS: connected to peer {} over post-quantum transport",
							pubkey
						);
					} else {
						println!("ERROR: failed to connect to peer over post-quantum transport");
					}
				},
				"connectpeer" => {
					let peer_pubkey_and_ip_addr = words.next();
					if peer_pubkey_and_ip_addr.is_none() {
						println!("ERROR: connectpeer requires peer connection info: `connectpeer pubkey@host:port`");
						continue;
					}
					let (pubkey, peer_addr) =
						match parse_peer_info(peer_pubkey_and_ip_addr.unwrap().to_string()) {
							Ok(info) => info,
							Err(e) => {
								println!("{:?}", e.into_inner().unwrap());
								continue;
							},
						};
					if connect_peer_if_necessary(pubkey, peer_addr, peer_manager.clone())
						.await
						.is_ok()
					{
						println!("SUCCESS: connected to peer {}", pubkey);
					}
				},
				"disconnectpeer" => {
					let peer_pubkey = words.next();
					if peer_pubkey.is_none() {
						println!("ERROR: disconnectpeer requires peer public key: `disconnectpeer <peer_pubkey>`");
						continue;
					}

					let peer_pubkey =
						match bitcoin::secp256k1::PublicKey::from_str(peer_pubkey.unwrap()) {
							Ok(pubkey) => pubkey,
							Err(e) => {
								println!("ERROR: {}", e.to_string());
								continue;
							},
						};

					if do_disconnect_peer(
						peer_pubkey,
						peer_manager.clone(),
						channel_manager.clone(),
					)
					.is_ok()
					{
						println!("SUCCESS: disconnected from peer {}", peer_pubkey);
					}
				},
				"listchannels" => list_channels(&channel_manager, &network_graph),
				"listpayments" => list_payments(
					&inbound_payments.lock().unwrap(),
					&outbound_payments.lock().unwrap(),
				),
				"closechannel" => {
					let channel_id_str = words.next();
					if channel_id_str.is_none() {
						println!("ERROR: closechannel requires a channel ID: `closechannel <channel_id> <peer_pubkey>`");
						continue;
					}
					let channel_id_vec = hex_utils::to_vec(channel_id_str.unwrap());
					if channel_id_vec.is_none() || channel_id_vec.as_ref().unwrap().len() != 32 {
						println!("ERROR: couldn't parse channel_id");
						continue;
					}
					let mut channel_id = [0; 32];
					channel_id.copy_from_slice(&channel_id_vec.unwrap());

					let peer_pubkey_str = words.next();
					if peer_pubkey_str.is_none() {
						println!("ERROR: closechannel requires a peer pubkey: `closechannel <channel_id> <peer_pubkey>`");
						continue;
					}
					let peer_pubkey_vec = match hex_utils::to_vec(peer_pubkey_str.unwrap()) {
						Some(peer_pubkey_vec) => peer_pubkey_vec,
						None => {
							println!("ERROR: couldn't parse peer_pubkey");
							continue;
						},
					};
					let peer_pubkey = match PublicKey::from_slice(&peer_pubkey_vec) {
						Ok(peer_pubkey) => peer_pubkey,
						Err(_) => {
							println!("ERROR: couldn't parse peer_pubkey");
							continue;
						},
					};

					close_channel(channel_id, peer_pubkey, channel_manager.clone());
				},
				"forceclosechannel" => {
					let channel_id_str = words.next();
					if channel_id_str.is_none() {
						println!("ERROR: forceclosechannel requires a channel ID: `forceclosechannel <channel_id> <peer_pubkey>`");
						continue;
					}
					let channel_id_vec = hex_utils::to_vec(channel_id_str.unwrap());
					if channel_id_vec.is_none() || channel_id_vec.as_ref().unwrap().len() != 32 {
						println!("ERROR: couldn't parse channel_id");
						continue;
					}
					let mut channel_id = [0; 32];
					channel_id.copy_from_slice(&channel_id_vec.unwrap());

					let peer_pubkey_str = words.next();
					if peer_pubkey_str.is_none() {
						println!("ERROR: forceclosechannel requires a peer pubkey: `forceclosechannel <channel_id> <peer_pubkey>`");
						continue;
					}
					let peer_pubkey_vec = match hex_utils::to_vec(peer_pubkey_str.unwrap()) {
						Some(peer_pubkey_vec) => peer_pubkey_vec,
						None => {
							println!("ERROR: couldn't parse peer_pubkey");
							continue;
						},
					};
					let peer_pubkey = match PublicKey::from_slice(&peer_pubkey_vec) {
						Ok(peer_pubkey) => peer_pubkey,
						Err(_) => {
							println!("ERROR: couldn't parse peer_pubkey");
							continue;
						},
					};

					force_close_channel(channel_id, peer_pubkey, channel_manager.clone());
				},
				"nodeinfo" => node_info(
					&channel_manager,
					&chain_monitor,
					&peer_manager,
					&network_graph,
					&keys_manager,
				),
				"listpeers" => list_peers(peer_manager.clone()),
				"listnodes" => list_nodes(&network_graph),
				"graphinfo" => graph_info(&network_graph),
				"announce" => {
					// Broadcast our node_announcement now instead of waiting for the periodic
					// timer, useful when testing gossip propagation.
					if channel_manager.list_channels().iter().any(|chan| chan.is_announced) {
						peer_manager.broadcast_node_announcement(
							[0; 3],
							ldk_announced_node_name,
							ldk_announced_listen_addr.clone(),
						);
						println!("SUCCESS: broadcast node announcement");
					} else {
						println!("ERROR: no announced channels, the announcement would not relay");
					}
				},
				"signmessage" => {
					const MSG_STARTPOS: usize = "signmessage".len() + 1;
					if line.trim().as_bytes().len() <= MSG_STARTPOS {
						println!("ERROR: signmsg requires a message");
						continue;
					}
					println!(
						"{:?}",
						lightning::util::message_signing::sign(
							&line.trim().as_bytes()[MSG_STARTPOS..],
							&keys_manager.get_node_secret_key()
						)
					);
				},
				"quit" | "exit" => break,
				_ => println!("Unknown command. See `\"help\" for available commands."),
			}
		}
	}
}

fn help() {
	let package_version = env!("CARGO_PKG_VERSION");
	let package_name = env!("CARGO_PKG_NAME");
	println!("\nVERSION:");
	println!("  {} v{}", package_name, package_version);
	println!("\nUSAGE:");
	println!("  Command [arguments]");
	println!("\nCOMMANDS:");
	println!("  help\tShows a list of commands.");
	println!("  quit\tClose the application.");
	println!("\n  Channels:");
	println!("      openchannel pubkey@[host:port] <amt_satoshis> [--public] [--with-anchors]");
	println!("      closechannel <channel_id> <peer_pubkey>");
	println!("      forceclosechannel <channel_id> <peer_pubkey>");
	println!("      listchannels");
	println!("\n  Peers:");
	println!("      connectpeer pubkey@host:port");
	#[cfg(feature = "post-quantum")]
	println!("      connectpeerpq pubkey@host:port [kem_key_hex]");
	println!("      disconnectpeer <peer_pubkey>");
	println!("      listpeers");
	println!("\n  Payments:");
	println!("      sendpayment <invoice|offer> [<amount_msat>]");
	println!("      keysend <dest_pubkey> <amt_msats>");
	println!("      listpayments");
	println!("\n  Invoices:");
	#[cfg(not(feature = "post-quantum"))]
	println!("      getinvoice <amt_msats> <expiry_secs>");
	#[cfg(feature = "post-quantum")]
	println!("      getinvoice <amt_msats> <expiry_secs> [--pq-omit-pubkey]");
	println!("      getoffer [<amt_msats>]");
	println!("\n  Refunds:");
	println!("      getrefund <amt_msats> <expiry_secs>");
	println!("      claimrefund <refund>");
	println!("\n  Async payments:");
	println!("      asyncpaths <recipient_id>\t(static invoice server side)");
	println!("      setasyncserver <path_hex>...\t(async recipient side)");
	println!("      getasyncoffer\t(async recipient side)");
	println!("\n  Other:");
	println!("      signmessage <message>");
	println!("      nodeinfo");
	println!("      listnodes");
	println!("      graphinfo");
	println!("      announce");
}

fn node_info(
	channel_manager: &Arc<ChannelManager>, chain_monitor: &Arc<ChainMonitor>,
	peer_manager: &Arc<PeerManager>, network_graph: &Arc<NetworkGraph>,
	keys_manager: &Arc<KeysManager>,
) {
	println!("\t{{");
	println!("\t\t node_pubkey: {}", channel_manager.get_our_node_id());
	#[cfg(feature = "post-quantum")]
	{
		if let Some(pq_node_id) = keys_manager.get_pq_node_id() {
			println!("\t\t pq_node_id: {}", hex_utils::hex_str(&pq_node_id));
		}
		if let Some(pq_kem_node_id) = keys_manager.get_pq_kem_node_id() {
			println!("\t\t pq_kem_node_id: {}", hex_utils::hex_str(&pq_kem_node_id));
		}
	}
	#[cfg(not(feature = "post-quantum"))]
	let _ = keys_manager;
	let chans = channel_manager.list_channels();
	println!("\t\t num_channels: {}", chans.len());
	println!("\t\t num_usable_channels: {}", chans.iter().filter(|c| c.is_usable).count());
	let balances = chain_monitor.get_claimable_balances(&[]);
	let local_balance_sat = balances.iter().map(|b| b.claimable_amount_satoshis()).sum::<u64>();
	println!("\t\t local_balance_sats: {}", local_balance_sat);
	let close_fees_map = |b| match b {
		&Balance::ClaimableOnChannelClose {
			ref balance_candidates,
			confirmed_balance_candidate_index,
			..
		} => balance_candidates[confirmed_balance_candidate_index].transaction_fee_satoshis,
		_ => 0,
	};
	let close_fees_sats = balances.iter().map(close_fees_map).sum::<u64>();
	println!("\t\t eventual_close_fees_sats: {}", close_fees_sats);
	let pending_payments_map = |b| match b {
		&Balance::MaybeTimeoutClaimableHTLC { amount_satoshis, outbound_payment, .. } => {
			if outbound_payment {
				amount_satoshis
			} else {
				0
			}
		},
		_ => 0,
	};
	let pending_payments = balances.iter().map(pending_payments_map).sum::<u64>();
	println!("\t\t pending_outbound_payments_sats: {}", pending_payments);
	println!("\t\t num_peers: {}", peer_manager.list_peers().len());
	let graph_lock = network_graph.read_only();
	println!("\t\t network_nodes: {}", graph_lock.nodes().len());
	println!("\t\t network_channels: {}", graph_lock.channels().len());
	println!("\t}},");
}

fn list_peers(peer_manager: Arc<PeerManager>) {
	println!("\t{{");
	for peer_details in peer_manager.list_peers() {
		println!("\t\t pubkey: {}", peer_details.counterparty_node_id);
	}
	println!("\t}},");
}

fn list_nodes(network_graph: &Arc<NetworkGraph>) {
	print!("[");
	let graph_lock = network_graph.read_only();
	for (node_id, node_info) in graph_lock.nodes().unordered_iter() {
		println!("");
		println!("\t{{");
		println!("\t\tnode_id: {},", node_id);
		println!("\t\thas_announcement: {},", node_info.announcement_info.is_some());
		#[cfg(feature = "post-quantum")]
		{
			println!("\t\tpq_pinned: {},", node_info.pq_node_id().is_some());
			println!("\t\tpq_kem_pinned: {},", node_info.pq_kem_node_id().is_some());
		}
		println!("\t}},");
	}
	println!("]");
}

fn graph_info(network_graph: &Arc<NetworkGraph>) {
	let graph_lock = network_graph.read_only();
	let announced =
		graph_lock.nodes().unordered_iter().filter(|(_, n)| n.announcement_info.is_some()).count();
	let updates: usize = graph_lock
		.channels()
		.unordered_iter()
		.map(|(_, c)| c.one_to_two.is_some() as usize + c.two_to_one.is_some() as usize)
		.sum();
	#[cfg(feature = "post-quantum")]
	{
		let pinned =
			graph_lock.nodes().unordered_iter().filter(|(_, n)| n.pq_node_id().is_some()).count();
		println!(
			"GRAPHINFO nodes={} announced={} channels={} updates={} pinned={}",
			graph_lock.nodes().len(),
			announced,
			graph_lock.channels().len(),
			updates,
			pinned
		);
	}
	#[cfg(not(feature = "post-quantum"))]
	println!(
		"GRAPHINFO nodes={} announced={} channels={} updates={}",
		graph_lock.nodes().len(),
		announced,
		graph_lock.channels().len(),
		updates
	);
}

fn list_channels(channel_manager: &Arc<ChannelManager>, network_graph: &Arc<NetworkGraph>) {
	print!("[");
	for chan_info in channel_manager.list_channels() {
		println!("");
		println!("\t{{");
		println!("\t\tchannel_id: {},", chan_info.channel_id);
		if let Some(funding_txo) = chan_info.funding_txo {
			println!("\t\tfunding_txid: {},", funding_txo.txid);
		}

		println!(
			"\t\tpeer_pubkey: {},",
			hex_utils::hex_str(&chan_info.counterparty.node_id.serialize())
		);
		if let Some(node_info) = network_graph
			.read_only()
			.nodes()
			.get(&NodeId::from_pubkey(&chan_info.counterparty.node_id))
		{
			if let Some(announcement) = &node_info.announcement_info {
				println!("\t\tpeer_alias: {}", announcement.alias());
			}
		}

		if let Some(id) = chan_info.short_channel_id {
			println!("\t\tshort_channel_id: {},", id);
		}
		println!("\t\tis_channel_ready: {},", chan_info.is_channel_ready);
		println!("\t\tchannel_value_satoshis: {},", chan_info.channel_value_satoshis);
		println!("\t\toutbound_capacity_msat: {},", chan_info.outbound_capacity_msat);
		if chan_info.is_usable {
			println!("\t\tavailable_balance_for_send_msat: {},", chan_info.outbound_capacity_msat);
			println!("\t\tavailable_balance_for_recv_msat: {},", chan_info.inbound_capacity_msat);
		}
		println!("\t\tchannel_can_send_payments: {},", chan_info.is_usable);
		println!("\t\tpublic: {},", chan_info.is_announced);
		println!("\t}},");
	}
	println!("]");
}

fn list_payments(
	inbound_payments: &InboundPaymentInfoStorage, outbound_payments: &OutboundPaymentInfoStorage,
) {
	print!("[");
	for (payment_hash, payment_info) in &inbound_payments.payments {
		println!("");
		println!("\t{{");
		println!("\t\tamount_millisatoshis: {},", payment_info.amt_msat);
		println!("\t\tpayment_hash: {},", payment_hash);
		println!("\t\thtlc_direction: inbound,");
		println!(
			"\t\thtlc_status: {},",
			match payment_info.status {
				HTLCStatus::Pending => "pending",
				HTLCStatus::Succeeded => "succeeded",
				HTLCStatus::Failed => "failed",
			}
		);

		println!("\t}},");
	}

	for (payment_hash, payment_info) in &outbound_payments.payments {
		println!("");
		println!("\t{{");
		println!("\t\tamount_millisatoshis: {},", payment_info.amt_msat);
		println!("\t\tpayment_hash: {},", payment_hash);
		println!("\t\thtlc_direction: outbound,");
		println!(
			"\t\thtlc_status: {},",
			match payment_info.status {
				HTLCStatus::Pending => "pending",
				HTLCStatus::Succeeded => "succeeded",
				HTLCStatus::Failed => "failed",
			}
		);

		println!("\t}},");
	}
	println!("]");
}

pub(crate) async fn connect_peer_if_necessary(
	pubkey: PublicKey, peer_addr: SocketAddr, peer_manager: Arc<PeerManager>,
) -> Result<(), ()> {
	if peer_manager.peer_by_node_id(&pubkey).is_some() {
		return Ok(());
	}
	let res = do_connect_peer(pubkey, peer_addr, peer_manager).await;
	if res.is_err() {
		println!("ERROR: failed to connect to peer");
	}
	res
}

/// Opens a TCP connection to a peer with TCP_NODELAY set, like the listeners in main.rs, so that
/// every message leaves at once. With Nagle's algorithm, the small commitment_signed that follows
/// an update_add_htlc waits for the peer's delayed acknowledgment, which adds about 40 ms per hop
/// on links with a 1500-byte MTU.
async fn connect_nodelay(peer_addr: SocketAddr) -> Option<std::net::TcpStream> {
	let connect = async {
		let stream = tokio::net::TcpStream::connect(peer_addr).await?;
		stream.set_nodelay(true)?;
		stream.into_std()
	};
	match tokio::time::timeout(Duration::from_secs(10), connect).await {
		Ok(Ok(stream)) => Some(stream),
		_ => None,
	}
}

pub(crate) async fn do_connect_peer(
	pubkey: PublicKey, peer_addr: SocketAddr, peer_manager: Arc<PeerManager>,
) -> Result<(), ()> {
	let stream = match connect_nodelay(peer_addr).await {
		Some(stream) => stream,
		None => return Err(()),
	};
	let connection_closed_future =
		lightning_net_tokio::setup_outbound(Arc::clone(&peer_manager), pubkey, stream);
	let mut connection_closed_future = Box::pin(connection_closed_future);
	loop {
		tokio::select! {
			_ = &mut connection_closed_future => return Err(()),
			_ = tokio::time::sleep(Duration::from_millis(10)) => {},
		};
		if peer_manager.peer_by_node_id(&pubkey).is_some() {
			return Ok(());
		}
	}
}

#[cfg(feature = "post-quantum")]
async fn do_connect_peer_pq(
	pubkey: PublicKey, kem_key: [u8; lightning::ln::peer_handler::PQ_KEM_EK_LEN],
	peer_addr: SocketAddr, peer_manager: Arc<PeerManager>,
) -> Result<(), ()> {
	let stream = match connect_nodelay(peer_addr).await {
		Some(stream) => stream,
		None => return Err(()),
	};
	let connection_closed_future = lightning_net_tokio::setup_outbound_pq(
		Arc::clone(&peer_manager),
		pubkey,
		kem_key,
		stream,
	);
	let mut connection_closed_future = Box::pin(connection_closed_future);
	loop {
		tokio::select! {
			_ = &mut connection_closed_future => return Err(()),
			_ = tokio::time::sleep(Duration::from_millis(10)) => {},
		};
		if peer_manager.peer_by_node_id(&pubkey).is_some() {
			return Ok(());
		}
	}
}

fn do_disconnect_peer(
	pubkey: bitcoin::secp256k1::PublicKey, peer_manager: Arc<PeerManager>,
	channel_manager: Arc<ChannelManager>,
) -> Result<(), ()> {
	//check for open channels with peer
	for channel in channel_manager.list_channels() {
		if channel.counterparty.node_id == pubkey {
			println!("Error: Node has an active channel with this peer, close any channels first");
			return Err(());
		}
	}

	//check the pubkey matches a valid connected peer
	if peer_manager.peer_by_node_id(&pubkey).is_none() {
		println!("Error: Could not find peer {}", pubkey);
		return Err(());
	}

	peer_manager.disconnect_by_node_id(pubkey);
	Ok(())
}

fn open_channel(
	peer_pubkey: PublicKey, channel_amt_sat: u64, announce_for_forwarding: bool,
	with_anchors: bool, channel_manager: Arc<ChannelManager>,
) -> Result<(), ()> {
	let config = UserConfig {
		channel_handshake_limits: ChannelHandshakeLimits {
			// lnd's max to_self_delay is 2016, so we want to be compatible.
			their_to_self_delay: 2016,
			..Default::default()
		},
		channel_handshake_config: ChannelHandshakeConfig {
			announce_for_forwarding,
			negotiate_anchors_zero_fee_htlc_tx: with_anchors,
			..Default::default()
		},
		..Default::default()
	};

	match channel_manager.create_channel(peer_pubkey, channel_amt_sat, 0, 0, None, Some(config)) {
		Ok(_) => {
			println!("EVENT: initiated channel with peer {}. ", peer_pubkey);
			return Ok(());
		},
		Err(e) => {
			println!("ERROR: failed to open channel: {:?}", e);
			return Err(());
		},
	}
}

async fn send_payment(
	channel_manager: &ChannelManager, invoice: &Bolt11Invoice, required_amount_msat: Option<u64>,
	outbound_payments: &Mutex<OutboundPaymentInfoStorage>, fs_store: &FilesystemStore,
) {
	let payment_id = PaymentId(invoice.payment_hash().0);
	let payment_secret = Some(*invoice.payment_secret());
	let amt_msat = match (invoice.amount_milli_satoshis(), required_amount_msat) {
		// pay_for_bolt11_invoice only validates that the amount we pay is >= the invoice's
		// required amount, not that its equal (to allow for overpayment). As that is somewhat
		// surprising, here we check and reject all disagreements in amount.
		(Some(inv_amt), Some(req_amt)) if inv_amt != req_amt => {
			println!(
				"Amount didn't match invoice value of {}msat",
				invoice.amount_milli_satoshis().unwrap_or(0)
			);
			print!("> ");
			return;
		},
		(Some(inv_amt), _) => inv_amt,
		(_, Some(req_amt)) => req_amt,
		(None, None) => {
			println!("Need an amount to pay an amountless invoice");
			print!("> ");
			return;
		},
	};
	let write_future = {
		let mut outbound_payments = outbound_payments.lock().unwrap();
		outbound_payments.payments.insert(
			payment_id,
			PaymentInfo {
				preimage: None,
				secret: payment_secret,
				status: HTLCStatus::Pending,
				amt_msat: MillisatAmount(Some(amt_msat)),
			},
		);
		fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound_payments.encode())
	};
	write_future.await.unwrap();

	let optional_params = OptionalBolt11PaymentParams {
		retry_strategy: Retry::Timeout(Duration::from_secs(10)),
		..Default::default()
	};
	match channel_manager.pay_for_bolt11_invoice(
		invoice,
		payment_id,
		required_amount_msat,
		optional_params,
	) {
		Ok(_) => {
			let payee_pubkey = invoice.get_payee_pub_key();
			println!("EVENT: initiated sending {} msats to {}", amt_msat, payee_pubkey);
			print!("> ");
		},
		Err(e) => {
			println!("ERROR: failed to send payment: {:?}", e);
			print!("> ");
			let write_future = {
				let mut outbound_payments = outbound_payments.lock().unwrap();
				outbound_payments.payments.get_mut(&payment_id).unwrap().status =
					HTLCStatus::Failed;
				fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound_payments.encode())
			};
			write_future.await.unwrap();
		},
	};
}

async fn keysend<E: EntropySource>(
	channel_manager: &ChannelManager, payee_pubkey: PublicKey, amt_msat: u64, entropy_source: &E,
	outbound_payments: &Mutex<OutboundPaymentInfoStorage>, fs_store: &FilesystemStore,
) {
	let payment_preimage = PaymentPreimage(entropy_source.get_secure_random_bytes());
	let payment_id = PaymentId(Sha256::hash(&payment_preimage.0[..]).to_byte_array());

	let route_params = RouteParameters::from_payment_params_and_value(
		PaymentParameters::for_keysend(payee_pubkey, 40, false),
		amt_msat,
	);
	let write_future = {
		let mut outbound_payments = outbound_payments.lock().unwrap();
		outbound_payments.payments.insert(
			payment_id,
			PaymentInfo {
				preimage: None,
				secret: None,
				status: HTLCStatus::Pending,
				amt_msat: MillisatAmount(Some(amt_msat)),
			},
		);
		fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound_payments.encode())
	};
	write_future.await.unwrap();
	match channel_manager.send_spontaneous_payment(
		Some(payment_preimage),
		RecipientOnionFields::spontaneous_empty(amt_msat),
		payment_id,
		route_params,
		Retry::Timeout(Duration::from_secs(10)),
	) {
		Ok(_payment_hash) => {
			println!("EVENT: initiated sending {} msats to {}", amt_msat, payee_pubkey);
			print!("> ");
		},
		Err(e) => {
			println!("ERROR: failed to send payment: {:?}", e);
			print!("> ");
			let write_future = {
				let mut outbound_payments = outbound_payments.lock().unwrap();
				outbound_payments.payments.get_mut(&payment_id).unwrap().status =
					HTLCStatus::Failed;
				fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound_payments.encode())
			};
			write_future.await.unwrap();
		},
	};
}

fn get_invoice(
	amt_msat: u64, inbound_payments: &mut InboundPaymentInfoStorage,
	channel_manager: &ChannelManager, expiry_secs: u32, pq_omit_pubkey: bool,
) {
	let mut invoice_params: Bolt11InvoiceParameters = Default::default();
	invoice_params.amount_msats = Some(amt_msat);
	invoice_params.invoice_expiry_delta_secs = Some(expiry_secs);
	#[cfg(feature = "post-quantum")]
	{
		invoice_params.pq_omit_pubkey = pq_omit_pubkey;
	}
	#[cfg(not(feature = "post-quantum"))]
	let _ = pq_omit_pubkey;
	let invoice = match channel_manager.create_bolt11_invoice(invoice_params) {
		Ok(inv) => {
			println!("SUCCESS: generated invoice: {}", inv);
			inv
		},
		Err(e) => {
			println!("ERROR: failed to create invoice: {:?}", e);
			return;
		},
	};

	let payment_hash = invoice.payment_hash();
	inbound_payments.payments.insert(
		payment_hash,
		PaymentInfo {
			preimage: None,
			secret: Some(invoice.payment_secret().clone()),
			status: HTLCStatus::Pending,
			amt_msat: MillisatAmount(Some(amt_msat)),
		},
	);
}

fn close_channel(
	channel_id: [u8; 32], counterparty_node_id: PublicKey, channel_manager: Arc<ChannelManager>,
) {
	match channel_manager.close_channel(&ChannelId(channel_id), &counterparty_node_id) {
		Ok(()) => println!("EVENT: initiating channel close"),
		Err(e) => println!("ERROR: failed to close channel: {:?}", e),
	}
}

fn force_close_channel(
	channel_id: [u8; 32], counterparty_node_id: PublicKey, channel_manager: Arc<ChannelManager>,
) {
	match channel_manager.force_close_broadcasting_latest_txn(
		&ChannelId(channel_id),
		&counterparty_node_id,
		"Manually force-closed".to_string(),
	) {
		Ok(()) => println!("EVENT: initiating channel force-close"),
		Err(e) => println!("ERROR: failed to force-close channel: {:?}", e),
	}
}

pub(crate) fn parse_peer_info(
	peer_pubkey_and_ip_addr: String,
) -> Result<(PublicKey, SocketAddr), std::io::Error> {
	let mut pubkey_and_addr = peer_pubkey_and_ip_addr.split("@");
	let pubkey = pubkey_and_addr.next();
	let peer_addr_str = pubkey_and_addr.next();
	if peer_addr_str.is_none() {
		return Err(std::io::Error::new(
			std::io::ErrorKind::Other,
			"ERROR: incorrectly formatted peer info. Should be formatted as: `pubkey@host:port`",
		));
	}

	let peer_addr = peer_addr_str.unwrap().to_socket_addrs().map(|mut r| r.next());
	if peer_addr.is_err() || peer_addr.as_ref().unwrap().is_none() {
		return Err(std::io::Error::new(
			std::io::ErrorKind::Other,
			"ERROR: couldn't parse pubkey@host:port into a socket address",
		));
	}

	let pubkey = hex_utils::to_compressed_pubkey(pubkey.unwrap());
	if pubkey.is_none() {
		return Err(std::io::Error::new(
			std::io::ErrorKind::Other,
			"ERROR: unable to parse given pubkey for node",
		));
	}

	Ok((pubkey.unwrap(), peer_addr.unwrap().unwrap()))
}
