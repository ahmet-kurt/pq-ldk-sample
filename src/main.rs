mod args;
pub mod bitcoind_client;
mod cli;
mod convert;
mod disk;
mod hex_utils;
mod sweep;

use crate::bitcoind_client::BitcoindClient;
use crate::disk::FilesystemLogger;
use bitcoin::blockdata::transaction::Transaction;
use bitcoin::consensus::encode;
use bitcoin::io;
use bitcoin::network::Network;
use bitcoin::secp256k1::PublicKey;
use bitcoin_bech32::WitnessProgram;
use disk::{INBOUND_PAYMENTS_FNAME, OUTBOUND_PAYMENTS_FNAME};
use lightning::blinded_path::message::{BlindedMessagePath, NextMessageHop};
use lightning::chain::{chainmonitor, ChannelMonitorUpdateStatus};
use lightning::chain::{BlockLocator, Filter};
use lightning::events::bump_transaction::BumpTransactionEventHandler;
use lightning::events::{Event, PaymentFailureReason, PaymentPurpose};
use lightning::ln::channelmanager::{self, RecentPaymentDetails};
use lightning::ln::channelmanager::{
	ChainParameters, ChannelManagerReadArgs, PaymentId, SimpleArcChannelManager,
};
use lightning::ln::msgs::DecodeError;
use lightning::ln::msgs::OnionMessage;
use lightning::ln::peer_handler::{
	IgnoringMessageHandler, MessageHandler, PeerManager as LdkPeerManager,
};
use lightning::ln::types::ChannelId;
use lightning::offers::static_invoice::StaticInvoice;
use lightning::onion_message::messenger::{
	DefaultMessageRouter, OnionMessenger as LdkOnionMessenger,
};
use lightning::routing::gossip;
use lightning::routing::gossip::{NodeId, P2PGossipSync};
use lightning::routing::router::DefaultRouter;
use lightning::routing::scoring::ProbabilisticScoringFeeParameters;
use lightning::sign::{EntropySource, InMemorySigner, KeysManager, NodeSigner};
use lightning::types::payment::{PaymentHash, PaymentPreimage, PaymentSecret};
use lightning::util::config::UserConfig;
use lightning::util::hash_tables::hash_map::Entry;
use lightning::util::hash_tables::HashMap;
use lightning::util::persist::{
	self, KVStore, MonitorUpdatingPersisterAsync, OUTPUT_SWEEPER_PERSISTENCE_KEY,
	OUTPUT_SWEEPER_PERSISTENCE_PRIMARY_NAMESPACE, OUTPUT_SWEEPER_PERSISTENCE_SECONDARY_NAMESPACE,
};
use lightning::util::ser::{Readable, ReadableArgs, Writeable, Writer};
use lightning::util::sweep as ldk_sweep;
use lightning::util::wallet_utils::Wallet;
use lightning::{chain, impl_ser_tlv_based, impl_ser_tlv_based_enum};
use lightning_background_processor::{process_events_async, GossipSync, NO_LIQUIDITY_MANAGER};
use lightning_block_sync::gossip::TokioSpawner;
use lightning_block_sync::{init, poll, HeaderCache, SpvClient};
use lightning_dns_resolver::OMDomainResolver;
use lightning_net_tokio::SocketDescriptor;
use lightning_persister::fs_store::v1::FilesystemStore;
use rand::{thread_rng, Rng};
use std::collections::HashMap as StdHashMap;
use std::convert::TryInto;
use std::fmt;
use std::fs;
use std::fs::File;
use std::io::{BufReader, Write};
use std::net::ToSocketAddrs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime};

#[derive(Copy, Clone)]
pub(crate) enum HTLCStatus {
	Pending,
	Succeeded,
	Failed,
}

impl_ser_tlv_based_enum!(HTLCStatus,
	(0, Pending) => {},
	(1, Succeeded) => {},
	(2, Failed) => {},
);

pub(crate) struct MillisatAmount(Option<u64>);

impl fmt::Display for MillisatAmount {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self.0 {
			Some(amt) => write!(f, "{}", amt),
			None => write!(f, "unknown"),
		}
	}
}

impl Readable for MillisatAmount {
	fn read<R: io::Read>(r: &mut R) -> Result<Self, DecodeError> {
		let amt: Option<u64> = Readable::read(r)?;
		Ok(MillisatAmount(amt))
	}
}

impl Writeable for MillisatAmount {
	fn write<W: Writer>(&self, w: &mut W) -> Result<(), io::Error> {
		self.0.write(w)
	}
}

pub(crate) struct PaymentInfo {
	preimage: Option<PaymentPreimage>,
	secret: Option<PaymentSecret>,
	status: HTLCStatus,
	amt_msat: MillisatAmount,
}

impl_ser_tlv_based!(PaymentInfo, {
	(0, preimage, required),
	(2, secret, required),
	(4, status, required),
	(6, amt_msat, required),
});

pub(crate) struct InboundPaymentInfoStorage {
	payments: HashMap<PaymentHash, PaymentInfo>,
}

impl_ser_tlv_based!(InboundPaymentInfoStorage, {
	(0, payments, required),
});

pub(crate) struct OutboundPaymentInfoStorage {
	payments: HashMap<PaymentId, PaymentInfo>,
}

impl_ser_tlv_based!(OutboundPaymentInfoStorage, {
	(0, payments, required),
});

type ChainMonitor = chainmonitor::ChainMonitor<
	InMemorySigner,
	Arc<dyn Filter + Send + Sync>,
	Arc<BitcoindClient>,
	Arc<BitcoindClient>,
	Arc<FilesystemLogger>,
	chainmonitor::AsyncPersister<
		Arc<FilesystemStore>,
		TokioSpawner,
		Arc<FilesystemLogger>,
		Arc<KeysManager>,
		Arc<KeysManager>,
		Arc<BitcoindClient>,
		Arc<BitcoindClient>,
	>,
	Arc<KeysManager>,
>;

pub(crate) type GossipVerifier = lightning_block_sync::gossip::GossipVerifier<
	TokioSpawner,
	Arc<lightning_block_sync::rpc::RpcClient>,
>;

// Note that if you do not use an `OMDomainResolver` here you should use SimpleArcPeerManager
// instead.
pub(crate) type PeerManager = LdkPeerManager<
	SocketDescriptor,
	Arc<ChannelManager>,
	Arc<P2PGossipSync<Arc<NetworkGraph>, Arc<GossipVerifier>, Arc<FilesystemLogger>>>,
	Arc<OnionMessenger>,
	Arc<FilesystemLogger>,
	IgnoringMessageHandler,
	Arc<KeysManager>,
	Arc<ChainMonitor>,
>;

pub(crate) type ChannelManager =
	SimpleArcChannelManager<ChainMonitor, BitcoindClient, BitcoindClient, FilesystemLogger>;

pub(crate) type NetworkGraph = gossip::NetworkGraph<Arc<FilesystemLogger>>;

// Note that if you do not use an `OMDomainResolver` here you should use SimpleArcOnionMessenger
// instead.
type OnionMessenger = LdkOnionMessenger<
	Arc<KeysManager>,
	Arc<KeysManager>,
	Arc<FilesystemLogger>,
	Arc<ChannelManager>,
	Arc<DefaultMessageRouter<Arc<NetworkGraph>, Arc<FilesystemLogger>, Arc<KeysManager>>>,
	Arc<ChannelManager>,
	Arc<ChannelManager>,
	Arc<OMDomainResolver<IgnoringMessageHandler>>,
	IgnoringMessageHandler,
>;

pub(crate) type BumpTxEventHandler = BumpTransactionEventHandler<
	Arc<BitcoindClient>,
	Arc<Wallet<Arc<BitcoindClient>, Arc<FilesystemLogger>>>,
	Arc<KeysManager>,
	Arc<FilesystemLogger>,
>;

pub(crate) type OutputSweeper = ldk_sweep::OutputSweeper<
	Arc<BitcoindClient>,
	Arc<BitcoindClient>,
	Arc<BitcoindClient>,
	Arc<dyn Filter + Send + Sync>,
	Arc<FilesystemStore>,
	Arc<FilesystemLogger>,
	Arc<KeysManager>,
>;

// Needed due to rust-lang/rust#63033.
struct OutputSweeperWrapper(Arc<OutputSweeper>);

/// Onion messages we intercepted for offline peers, keyed by the peer to replay them to once it
/// reconnects. This is what lets us serve an often-offline async payment recipient.
type InterceptedOmStore = Mutex<StdHashMap<PublicKey, Vec<OnionMessage>>>;

/// The maximum number of onion messages we hold per offline peer before dropping the oldest.
const MAX_INTERCEPTED_OMS_PER_PEER: usize = 50;

/// Static invoices we persist as a static invoice server on behalf of async recipients, keyed by
/// the recipient id and invoice slot, together with the path invoice requests are forwarded over.
type StaticInvoiceStore = Mutex<StdHashMap<(Vec<u8>, u16), (StaticInvoice, BlindedMessagePath)>>;

fn handle_ldk_events<'a>(
	channel_manager: Arc<ChannelManager>, bitcoind_client: &'a BitcoindClient,
	network_graph: &'a NetworkGraph, keys_manager: &'a KeysManager,
	bump_tx_event_handler: &'a BumpTxEventHandler, peer_manager: Arc<PeerManager>,
	onion_messenger: Arc<OnionMessenger>, intercepted_oms: Arc<InterceptedOmStore>,
	static_invoices: Arc<StaticInvoiceStore>,
	inbound_payments: Arc<Mutex<InboundPaymentInfoStorage>>,
	outbound_payments: Arc<Mutex<OutboundPaymentInfoStorage>>, fs_store: Arc<FilesystemStore>,
	output_sweeper: OutputSweeperWrapper, network: Network, event: Event,
) -> impl core::future::Future<Output = ()> + 'a {
	async move {
		match event {
			Event::FundingGenerationReady {
				temporary_channel_id,
				counterparty_node_id,
				channel_value_satoshis,
				output_script,
				..
			} => {
				// Construct the raw transaction with one output, that is paid the amount of the
				// channel.
				let addr = WitnessProgram::from_scriptpubkey(
					&output_script.as_bytes(),
					match network {
						Network::Bitcoin => bitcoin_bech32::constants::Network::Bitcoin,
						Network::Regtest => bitcoin_bech32::constants::Network::Regtest,
						Network::Signet => bitcoin_bech32::constants::Network::Signet,
						Network::Testnet | _ => bitcoin_bech32::constants::Network::Testnet,
					},
				)
				.expect("Lightning funding tx should always be to a SegWit output")
				.to_address();
				let mut outputs = vec![StdHashMap::new()];
				outputs[0].insert(addr, channel_value_satoshis as f64 / 100_000_000.0);
				let raw_tx = bitcoind_client.create_raw_transaction(outputs).await;

				// Have your wallet put the inputs into the transaction such that the output is
				// satisfied.
				let funded_tx = bitcoind_client.fund_raw_transaction(raw_tx).await;

				// Sign the final funding transaction and give it to LDK, who will eventually broadcast it.
				let signed_tx =
					bitcoind_client.sign_raw_transaction_with_wallet(funded_tx.hex).await;
				assert_eq!(signed_tx.complete, true);
				let final_tx: Transaction =
					encode::deserialize(&hex_utils::to_vec(&signed_tx.hex).unwrap()).unwrap();
				// Give the funding transaction back to LDK for opening the channel.
				if channel_manager
					.funding_transaction_generated(
						temporary_channel_id,
						counterparty_node_id,
						final_tx,
					)
					.is_err()
				{
					println!(
						"\nERROR: Channel went away before we could fund it. The peer disconnected or refused the channel.");
					print!("> ");
					std::io::stdout().flush().unwrap();
				}
			},
			Event::FundingTxBroadcastSafe { .. } => {
				// We don't use the manual broadcasting feature, so this event should never be seen.
			},
			Event::PaymentClaimable { payment_hash, purpose, amount_msat, .. } => {
				println!(
					"\nEVENT: received payment from payment hash {} of {} millisatoshis",
					payment_hash, amount_msat,
				);
				print!("> ");
				std::io::stdout().flush().unwrap();
				let payment_preimage = match purpose {
					PaymentPurpose::Bolt11InvoicePayment { payment_preimage, .. } => {
						payment_preimage
					},
					PaymentPurpose::Bolt12OfferPayment { payment_preimage, .. } => payment_preimage,
					PaymentPurpose::Bolt12RefundPayment { payment_preimage, .. } => {
						payment_preimage
					},
					PaymentPurpose::SpontaneousPayment(preimage) => Some(preimage),
				};
				channel_manager.claim_funds(payment_preimage.unwrap());
			},
			Event::PaymentClaimed { payment_hash, purpose, amount_msat, .. } => {
				println!(
					"\nEVENT: claimed payment from payment hash {} of {} millisatoshis",
					payment_hash, amount_msat,
				);
				print!("> ");
				std::io::stdout().flush().unwrap();
				let (payment_preimage, payment_secret) = match purpose {
					PaymentPurpose::Bolt11InvoicePayment {
						payment_preimage,
						payment_secret,
						..
					} => (payment_preimage, Some(payment_secret)),
					PaymentPurpose::Bolt12OfferPayment {
						payment_preimage, payment_secret, ..
					} => (payment_preimage, Some(payment_secret)),
					PaymentPurpose::Bolt12RefundPayment {
						payment_preimage,
						payment_secret,
						..
					} => (payment_preimage, Some(payment_secret)),
					PaymentPurpose::SpontaneousPayment(preimage) => (Some(preimage), None),
				};
				let write_future = {
					let mut inbound = inbound_payments.lock().unwrap();
					match inbound.payments.entry(payment_hash) {
						Entry::Occupied(mut e) => {
							let payment = e.get_mut();
							payment.status = HTLCStatus::Succeeded;
							payment.preimage = payment_preimage;
							payment.secret = payment_secret;
						},
						Entry::Vacant(e) => {
							e.insert(PaymentInfo {
								preimage: payment_preimage,
								secret: payment_secret,
								status: HTLCStatus::Succeeded,
								amt_msat: MillisatAmount(Some(amount_msat)),
							});
						},
					}
					fs_store.write("", "", INBOUND_PAYMENTS_FNAME, inbound.encode())
				};
				write_future.await.unwrap();
			},
			Event::PaymentSent {
				payment_preimage,
				payment_hash,
				fee_paid_msat,
				payment_id,
				..
			} => {
				let write_future = {
					let mut outbound = outbound_payments.lock().unwrap();
					for (id, payment) in outbound.payments.iter_mut() {
						if *id == payment_id.unwrap() {
							payment.preimage = Some(payment_preimage);
							payment.status = HTLCStatus::Succeeded;
							println!(
								"\nEVENT: successfully sent payment of {} millisatoshis{} from \
										 payment hash {} with preimage {}",
								payment.amt_msat,
								if let Some(fee) = fee_paid_msat {
									format!(" (fee {} msat)", fee)
								} else {
									"".to_string()
								},
								payment_hash,
								payment_preimage
							);
							print!("> ");
							std::io::stdout().flush().unwrap();
						}
					}
					fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound.encode())
				};
				write_future.await.unwrap();
			},
			Event::OpenChannelRequest {
				ref temporary_channel_id,
				ref counterparty_node_id,
				..
			} => {
				let mut random_bytes = [0u8; 16];
				random_bytes.copy_from_slice(&keys_manager.get_secure_random_bytes()[..16]);
				let user_channel_id = u128::from_be_bytes(random_bytes);
				let res = channel_manager.accept_inbound_channel(
					temporary_channel_id,
					counterparty_node_id,
					user_channel_id,
					None,
				);

				if let Err(e) = res {
					print!(
						"\nEVENT: Failed to accept inbound channel ({}) from {}: {:?}",
						temporary_channel_id,
						hex_utils::hex_str(&counterparty_node_id.serialize()),
						e,
					);
				} else {
					print!(
						"\nEVENT: Accepted inbound channel ({}) from {}",
						temporary_channel_id,
						hex_utils::hex_str(&counterparty_node_id.serialize()),
					);
				}
				print!("> ");
				std::io::stdout().flush().unwrap();
			},
			Event::PaymentPathSuccessful { .. } => {},
			Event::PaymentPathFailed { .. } => {},
			Event::ProbeSuccessful { .. } => {},
			Event::ProbeFailed { .. } => {},
			Event::PaymentFailed { payment_hash, reason, payment_id, .. } => {
				if let Some(hash) = payment_hash {
					print!(
						"\nEVENT: Failed to send payment to payment ID {}, payment hash {}: {:?}",
						payment_id,
						hash,
						if let Some(r) = reason {
							r
						} else {
							PaymentFailureReason::RetriesExhausted
						}
					);
				} else {
					print!(
						"\nEVENT: Failed fetch invoice for payment ID {}: {:?}",
						payment_id,
						if let Some(r) = reason {
							r
						} else {
							PaymentFailureReason::RetriesExhausted
						}
					);
				}
				print!("> ");
				std::io::stdout().flush().unwrap();

				let write_future = {
					let mut outbound = outbound_payments.lock().unwrap();
					if outbound.payments.contains_key(&payment_id) {
						let payment = outbound.payments.get_mut(&payment_id).unwrap();
						payment.status = HTLCStatus::Failed;
					}
					fs_store.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound.encode())
				};
				write_future.await.unwrap();
			},
			Event::InvoiceReceived { .. } => {
				// We don't use the manual invoice payment logic, so this event should never be seen.
			},
			Event::PaymentForwarded {
				prev_htlcs,
				next_htlcs,
				total_fee_earned_msat,
				claim_from_onchain_tx,
				outbound_amount_forwarded_msat,
				..
			} => {
				let read_only_network_graph = network_graph.read_only();
				let nodes = read_only_network_graph.nodes();
				let channels = channel_manager.list_channels();

				let node_str = |channel_id: &Option<ChannelId>| match channel_id {
					None => String::new(),
					Some(channel_id) => match channels.iter().find(|c| c.channel_id == *channel_id)
					{
						None => String::new(),
						Some(channel) => {
							match nodes.get(&NodeId::from_pubkey(&channel.counterparty.node_id)) {
								None => "private node".to_string(),
								Some(node) => match &node.announcement_info {
									None => "unnamed node".to_string(),
									Some(announcement) => {
										format!("node {}", announcement.alias())
									},
								},
							}
						},
					},
				};
				let channel_str = |channel_id: &Option<ChannelId>| {
					channel_id
						.map(|channel_id| format!(" with channel {}", channel_id))
						.unwrap_or_default()
				};
				let prev_channel_id = prev_htlcs.first().map(|htlc| htlc.channel_id);
				let next_channel_id = next_htlcs.first().map(|htlc| htlc.channel_id);
				let from_prev_str = format!(
					" from {}{}",
					node_str(&prev_channel_id),
					channel_str(&prev_channel_id),
				);
				let to_next_str =
					format!(" to {}{}", node_str(&next_channel_id), channel_str(&next_channel_id));

				let from_onchain_str = if claim_from_onchain_tx {
					"from onchain downstream claim"
				} else {
					"from HTLC fulfill message"
				};
				if let Some(fee_earned) = total_fee_earned_msat {
					println!(
						"\nEVENT: Forwarded payment for {} msat{}{}, earning {} msat {}",
						outbound_amount_forwarded_msat,
						from_prev_str,
						to_next_str,
						fee_earned,
						from_onchain_str
					);
				} else {
					println!(
						"\nEVENT: Forwarded payment for {} msat{}{}, claiming onchain {}",
						outbound_amount_forwarded_msat,
						from_prev_str,
						to_next_str,
						from_onchain_str
					);
				}
				print!("> ");
				std::io::stdout().flush().unwrap();
			},
			Event::HTLCHandlingFailed { .. } => {},
			Event::SpendableOutputs { outputs, channel_id, counterparty_node_id } => {
				output_sweeper
					.0
					.track_spendable_outputs(outputs, channel_id, counterparty_node_id, false, None)
					.await
					.unwrap();
			},
			Event::ChannelPending { channel_id, counterparty_node_id, .. } => {
				println!(
					"\nEVENT: Channel {} with peer {} is pending awaiting funding lock-in!",
					channel_id,
					hex_utils::hex_str(&counterparty_node_id.serialize()),
				);
				print!("> ");
				std::io::stdout().flush().unwrap();
			},
			Event::ChannelReady { ref channel_id, ref counterparty_node_id, .. } => {
				println!(
					"\nEVENT: Channel {} with peer {} is ready to be used!",
					channel_id,
					hex_utils::hex_str(&counterparty_node_id.serialize()),
				);
				print!("> ");
				std::io::stdout().flush().unwrap();
			},
			Event::ChannelClosed { channel_id, reason, counterparty_node_id, .. } => {
				println!(
					"\nEVENT: Channel {} with counterparty {} closed due to: {:?}",
					channel_id,
					counterparty_node_id.map(|id| format!("{}", id)).unwrap_or("".to_owned()),
					reason
				);
				print!("> ");
				std::io::stdout().flush().unwrap();
			},
			Event::DiscardFunding { .. } => {
				// A "real" node should probably "lock" the UTXOs spent in funding transactions until
				// the funding transaction either confirms, or this event is generated.
			},
			Event::HTLCIntercepted { .. } => {},
			Event::OnionMessageIntercepted { next_hop, message, .. } => {
				// Hold onion messages destined for an offline peer and replay them when the peer
				// reconnects, so e.g. a `held_htlc_available` message for an often-offline async
				// payment recipient is not lost.
				let next_node_id = match next_hop {
					NextMessageHop::NodeId(node_id) => Some(node_id),
					NextMessageHop::ShortChannelId(scid) => channel_manager
						.list_channels()
						.iter()
						.find(|chan| {
							chan.short_channel_id == Some(scid)
								|| chan.outbound_scid_alias == Some(scid)
								|| chan.inbound_scid_alias == Some(scid)
						})
						.map(|chan| chan.counterparty.node_id),
				};
				match next_node_id {
					Some(node_id) => {
						let mut intercepted_oms = intercepted_oms.lock().unwrap();
						let oms = intercepted_oms.entry(node_id).or_insert_with(Vec::new);
						if oms.len() >= MAX_INTERCEPTED_OMS_PER_PEER {
							oms.remove(0);
						}
						oms.push(message);
						println!(
							"\nEVENT: intercepted an onion message for offline peer {}, holding it for replay",
							node_id
						);
						print!("> ");
						std::io::stdout().flush().unwrap();
					},
					None => {
						println!("\nEVENT: dropping an intercepted onion message with an unknown next hop");
						print!("> ");
						std::io::stdout().flush().unwrap();
					},
				}
			},
			Event::OnionMessagePeerConnected { peer_node_id } => {
				let oms = intercepted_oms.lock().unwrap().remove(&peer_node_id);
				if let Some(oms) = oms {
					let num_oms = oms.len();
					for om in oms {
						if let Err(e) = onion_messenger.forward_onion_message(om, &peer_node_id) {
							println!(
								"\nERROR: failed to replay an intercepted onion message to {}: {:?}",
								peer_node_id, e
							);
						}
					}
					println!(
						"\nEVENT: replayed {} held onion message(s) to reconnected peer {}",
						num_oms, peer_node_id
					);
					print!("> ");
					std::io::stdout().flush().unwrap();
				}
			},
			Event::PersistStaticInvoice {
				invoice,
				invoice_request_path,
				invoice_slot,
				recipient_id,
				invoice_persisted_path,
			} => {
				static_invoices
					.lock()
					.unwrap()
					.insert((recipient_id.clone(), invoice_slot), (invoice, invoice_request_path));
				channel_manager.static_invoice_persisted(invoice_persisted_path);
				println!(
					"\nEVENT: persisted a static invoice in slot {} for async recipient {}",
					invoice_slot,
					String::from_utf8_lossy(&recipient_id)
				);
				print!("> ");
				std::io::stdout().flush().unwrap();
			},
			Event::StaticInvoiceRequested {
				recipient_id,
				invoice_slot,
				reply_path,
				invoice_request,
			} => {
				let entry = static_invoices
					.lock()
					.unwrap()
					.get(&(recipient_id.clone(), invoice_slot))
					.cloned();
				match entry {
					Some((invoice, invoice_request_path)) => {
						let res = channel_manager.respond_to_static_invoice_request(
							invoice,
							reply_path,
							invoice_request,
							invoice_request_path,
						);
						match res {
							Ok(()) => {
								println!(
									"\nEVENT: served the static invoice in slot {} for async recipient {}",
									invoice_slot,
									String::from_utf8_lossy(&recipient_id)
								);
							},
							Err(e) => {
								println!("\nERROR: failed to serve a static invoice: {:?}", e);
							},
						}
					},
					None => {
						println!(
							"\nEVENT: received a static invoice request for unknown recipient {} slot {}",
							String::from_utf8_lossy(&recipient_id),
							invoice_slot
						);
					},
				}
				print!("> ");
				std::io::stdout().flush().unwrap();
			},
			Event::BumpTransaction(event) => bump_tx_event_handler.handle_event(&event).await,
			Event::ConnectionNeeded { node_id, addresses } => {
				tokio::spawn(async move {
					for address in addresses {
						if let Ok(sockaddrs) = address.to_socket_addrs() {
							for addr in sockaddrs {
								let pm = Arc::clone(&peer_manager);
								if cli::connect_peer_if_necessary(node_id, addr, pm).await.is_ok() {
									return;
								}
							}
						}
					}
				});
			},
			_ => {},
		}
	}
}

async fn start_ldk() {
	let args = match args::parse_startup_args() {
		Ok(user_args) => user_args,
		Err(()) => return,
	};

	// Initialize the LDK data directory if necessary.
	let ldk_data_dir = format!("{}/.ldk", args.ldk_storage_dir_path);
	fs::create_dir_all(ldk_data_dir.clone()).unwrap();

	// ## Setup
	// Step 1: Initialize the Logger
	let logger = Arc::new(FilesystemLogger::new(ldk_data_dir.clone()));

	// Initialize our bitcoind client.
	let bitcoind_client = match BitcoindClient::new(
		args.bitcoind_rpc_host.clone(),
		args.bitcoind_rpc_port,
		args.bitcoind_rpc_username.clone(),
		args.bitcoind_rpc_password.clone(),
		args.network,
		tokio::runtime::Handle::current(),
		Arc::clone(&logger),
	)
	.await
	{
		Ok(client) => Arc::new(client),
		Err(e) => {
			println!("Failed to connect to bitcoind client: {}", e);
			return;
		},
	};

	// Check that the bitcoind we've connected to is running the network we expect
	let bitcoind_chain = bitcoind_client.get_blockchain_info().await.chain;
	if bitcoind_chain
		!= match args.network {
			bitcoin::Network::Bitcoin => "main",
			bitcoin::Network::Regtest => "regtest",
			bitcoin::Network::Signet => "signet",
			bitcoin::Network::Testnet | _ => "test",
		} {
		println!(
			"Chain argument ({}) didn't match bitcoind chain ({})",
			args.network, bitcoind_chain
		);
		return;
	}

	// Step 2: Initialize the FeeEstimator

	// BitcoindClient implements the FeeEstimator trait, so it'll act as our fee estimator.
	let fee_estimator = bitcoind_client.clone();

	// Step 3: Initialize the BroadcasterInterface

	// BitcoindClient implements the BroadcasterInterface trait, so it'll act as our transaction
	// broadcaster.
	let broadcaster = bitcoind_client.clone();

	// Step 4: Initialize the KeysManager

	// The key seed that we use to derive the node privkey (that corresponds to the node pubkey) and
	// other secret key material.
	let keys_seed_path = format!("{}/keys_seed", ldk_data_dir.clone());
	let keys_seed = if let Ok(seed) = fs::read(keys_seed_path.clone()) {
		assert_eq!(seed.len(), 32);
		let mut key = [0; 32];
		key.copy_from_slice(&seed);
		key
	} else {
		let mut key = [0; 32];
		thread_rng().fill_bytes(&mut key);
		match File::create(keys_seed_path.clone()) {
			Ok(mut f) => {
				std::io::Write::write_all(&mut f, &key)
					.expect("Failed to write node keys seed to disk");
				f.sync_all().expect("Failed to sync node keys seed to disk");
			},
			Err(e) => {
				println!("ERROR: Unable to create keys seed file {}: {}", keys_seed_path, e);
				return;
			},
		}
		key
	};
	let cur = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
	let keys_manager =
		Arc::new(KeysManager::new(&keys_seed, cur.as_secs(), cur.subsec_nanos(), true));

	let bump_tx_event_handler = Arc::new(BumpTransactionEventHandler::new(
		Arc::clone(&broadcaster),
		Arc::new(Wallet::new(Arc::clone(&bitcoind_client), Arc::clone(&logger))),
		Arc::clone(&keys_manager),
		Arc::clone(&logger),
	));

	// Step 5: Initialize Persistence
	let fs_store = Arc::new(FilesystemStore::new(ldk_data_dir.clone().into()));
	let persister = MonitorUpdatingPersisterAsync::new(
		Arc::clone(&fs_store),
		TokioSpawner,
		Arc::clone(&logger),
		1000,
		Arc::clone(&keys_manager),
		Arc::clone(&keys_manager),
		Arc::clone(&bitcoind_client),
		Arc::clone(&bitcoind_client),
	);
	// Alternatively, you can use the `FilesystemStore` as a `Persist` directly, at the cost of
	// larger `ChannelMonitor` update writes (but no deletion or cleanup):
	//let persister = Arc::clone(&fs_store);

	// Step 6: Read ChannelMonitor state from disk
	let mut channelmonitors = persister.read_all_channel_monitors_with_updates().await.unwrap();
	// If you are using the `FilesystemStore` as a `Persist` directly, use
	// `lightning::util::persist::read_channel_monitors` like this:
	// read_channel_monitors(Arc::clone(&persister), Arc::clone(&keys_manager), Arc::clone(&keys_manager)).unwrap();

	// Step 7: Initialize the ChainMonitor
	let chain_monitor: Arc<ChainMonitor> = Arc::new(chainmonitor::ChainMonitor::new_async_beta(
		None,
		Arc::clone(&broadcaster),
		Arc::clone(&logger),
		Arc::clone(&fee_estimator),
		persister,
		Arc::clone(&keys_manager),
		keys_manager.get_peer_storage_key(),
		false,
	));

	// Step 8: Poll for the best chain tip, which may be used by the channel manager & spv client
	let polled_chain_tip = init::validate_best_block_header(bitcoind_client.as_ref())
		.await
		.expect("Failed to fetch best block header and best block");

	// Step 9: Initialize routing ProbabilisticScorer
	let network_graph_path = format!("{}/network_graph", ldk_data_dir.clone());
	let network_graph =
		Arc::new(disk::read_network(Path::new(&network_graph_path), args.network, logger.clone()));

	let scorer_path = format!("{}/scorer", ldk_data_dir.clone());
	let scorer = Arc::new(RwLock::new(disk::read_scorer(
		Path::new(&scorer_path),
		Arc::clone(&network_graph),
		Arc::clone(&logger),
	)));

	// Step 10: Create Routers
	let scoring_fee_params = ProbabilisticScoringFeeParameters::default();
	let router = Arc::new(DefaultRouter::new(
		network_graph.clone(),
		logger.clone(),
		keys_manager.clone(),
		scorer.clone(),
		scoring_fee_params,
	));

	let message_router =
		Arc::new(DefaultMessageRouter::new(Arc::clone(&network_graph), Arc::clone(&keys_manager)));

	// Step 11: Initialize the ChannelManager
	let mut user_config = UserConfig::default();
	user_config.channel_handshake_limits.force_announced_channel_preference = false;
	user_config.channel_handshake_config.negotiate_anchors_zero_fee_htlc_tx = true;
	// Forward over unannounced channels too, so this node can be the always-online
	// channel counterparty of a private async payment recipient.
	user_config.accept_forwards_to_priv_channels = true;
	#[cfg(feature = "post-quantum")]
	{
		user_config.require_post_quantum_payments = args.pq_require_payments;
		user_config.build_post_quantum_blinded_paths = args.pq_blinded_paths;
		user_config.require_post_quantum_inbound = args.pq_require_inbound;
	}
	let mut restarting_node = true;
	let (channel_manager_blockhash, channel_manager) = {
		if let Ok(f) = fs::File::open(format!("{}/manager", ldk_data_dir.clone())) {
			let mut channel_monitor_references = Vec::new();
			for (_, channel_monitor) in channelmonitors.iter() {
				channel_monitor_references.push(channel_monitor);
			}
			let read_args = ChannelManagerReadArgs::new(
				keys_manager.clone(),
				keys_manager.clone(),
				keys_manager.clone(),
				fee_estimator.clone(),
				chain_monitor.clone(),
				broadcaster.clone(),
				router,
				Arc::clone(&message_router),
				logger.clone(),
				user_config,
				channel_monitor_references,
			);
			<(BlockLocator, ChannelManager)>::read(&mut BufReader::new(f), read_args).unwrap()
		} else {
			// We're starting a fresh node.
			restarting_node = false;

			let polled_best_block = polled_chain_tip.to_block_locator();
			let chain_params =
				ChainParameters { network: args.network, best_block: polled_best_block };
			let fresh_channel_manager = channelmanager::ChannelManager::new(
				fee_estimator.clone(),
				chain_monitor.clone(),
				broadcaster.clone(),
				router,
				Arc::clone(&message_router),
				logger.clone(),
				keys_manager.clone(),
				keys_manager.clone(),
				keys_manager.clone(),
				user_config,
				chain_params,
				cur.as_secs() as u32,
			);
			(polled_best_block, fresh_channel_manager)
		}
	};

	// Step 12: Initialize the OutputSweeper.
	let (sweeper_best_block, output_sweeper) = match fs_store
		.read(
			OUTPUT_SWEEPER_PERSISTENCE_PRIMARY_NAMESPACE,
			OUTPUT_SWEEPER_PERSISTENCE_SECONDARY_NAMESPACE,
			OUTPUT_SWEEPER_PERSISTENCE_KEY,
		)
		.await
	{
		Err(e) if e.kind() == io::ErrorKind::NotFound => {
			let sweeper = OutputSweeper::new(
				channel_manager.current_best_block(),
				broadcaster.clone(),
				fee_estimator.clone(),
				None,
				keys_manager.clone(),
				bitcoind_client.clone(),
				fs_store.clone(),
				logger.clone(),
			);
			(channel_manager.current_best_block(), sweeper)
		},
		Ok(mut bytes) => {
			let read_args = (
				broadcaster.clone(),
				fee_estimator.clone(),
				None,
				keys_manager.clone(),
				bitcoind_client.clone(),
				fs_store.clone(),
				logger.clone(),
			);
			let mut reader = io::Cursor::new(&mut bytes);
			<(BlockLocator, OutputSweeper)>::read(&mut reader, read_args)
				.expect("Failed to deserialize OutputSweeper")
		},
		Err(e) => panic!("Failed to read OutputSweeper with {}", e),
	};

	// Step 13: Sync ChannelMonitors, ChannelManager and OutputSweeper to chain tip
	let mut chain_listener_channel_monitors = Vec::new();
	let (cache, chain_tip) = if restarting_node {
		let mut chain_listeners = vec![
			(channel_manager_blockhash, &channel_manager as &(dyn chain::Listen + Send + Sync)),
			(sweeper_best_block, &output_sweeper as &(dyn chain::Listen + Send + Sync)),
		];

		for (blockhash, channel_monitor) in channelmonitors.drain(..) {
			let funding_txo = channel_monitor.get_funding_txo();
			chain_listener_channel_monitors.push((
				blockhash,
				(channel_monitor, broadcaster.clone(), fee_estimator.clone(), logger.clone()),
				funding_txo,
			));
		}

		for monitor_listener_info in chain_listener_channel_monitors.iter_mut() {
			chain_listeners.push((
				monitor_listener_info.0,
				&monitor_listener_info.1 as &(dyn chain::Listen + Send + Sync),
			));
		}

		init::synchronize_listeners(bitcoind_client.as_ref(), args.network, chain_listeners)
			.await
			.unwrap()
	} else {
		(HeaderCache::new(), polled_chain_tip)
	};

	// Step 14: Give ChannelMonitors to ChainMonitor
	for (_, (channel_monitor, _, _, _), _) in chain_listener_channel_monitors {
		let channel_id = channel_monitor.channel_id();
		// Note that this may not return `Completed` for ChannelMonitors which were last written by
		// a version of LDK prior to 0.1.
		assert_eq!(
			chain_monitor.load_existing_monitor(channel_id, channel_monitor),
			Ok(ChannelMonitorUpdateStatus::Completed)
		);
	}

	// Step 15: Optional: Initialize the P2PGossipSync, verifying announced channels against the
	// chain via the bitcoind RPC client.
	let utxo_lookup: Arc<GossipVerifier> = Arc::new(GossipVerifier::new(
		Arc::clone(&bitcoind_client.bitcoind_rpc_client),
		TokioSpawner,
	));
	let gossip_sync = Arc::new(P2PGossipSync::new(
		Arc::clone(&network_graph),
		Some(utxo_lookup),
		Arc::clone(&logger),
	));

	// Step 16 an OMDomainResolver as a service to other nodes
	// As a service to other LDK users, using an `OMDomainResolver` allows others to resolve BIP
	// 353 Human Readable Names for others, providing them DNSSEC proofs over lightning onion
	// messages. Doing this only makes sense for a always-online public routing node, and doesn't
	// provide you any direct value, but its nice to offer the service for others.
	let channel_manager: Arc<ChannelManager> = Arc::new(channel_manager);
	let resolver = "8.8.8.8:53".to_socket_addrs().unwrap().next().unwrap();
	let domain_resolver = Arc::new(OMDomainResolver::ignoring_incoming_proofs(resolver));

	// Step 17: Initialize the PeerManager
	//
	// We intercept onion messages for offline peers and replay them when the peer reconnects.
	// This lets this node act as the always-online counterparty of an often-offline async
	// payment recipient, holding e.g. `held_htlc_available` onion messages until the recipient
	// comes back online.
	let onion_messenger: Arc<OnionMessenger> =
		Arc::new(OnionMessenger::new_with_offline_peer_interception(
			Arc::clone(&keys_manager),
			Arc::clone(&keys_manager),
			Arc::clone(&logger),
			Arc::clone(&channel_manager),
			Arc::clone(&message_router),
			Arc::clone(&channel_manager),
			Arc::clone(&channel_manager),
			domain_resolver,
			IgnoringMessageHandler {},
			true,
		));
	let mut ephemeral_bytes = [0; 32];
	let current_time = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
	rand::thread_rng().fill_bytes(&mut ephemeral_bytes);
	let lightning_msg_handler = MessageHandler {
		chan_handler: Arc::clone(&channel_manager),
		route_handler: Arc::clone(&gossip_sync),
		onion_message_handler: Arc::clone(&onion_messenger),
		custom_message_handler: IgnoringMessageHandler {},
		send_only_message_handler: Arc::clone(&chain_monitor),
	};
	let peer_manager: Arc<PeerManager> = Arc::new(PeerManager::new(
		lightning_msg_handler,
		current_time.try_into().unwrap(),
		&ephemeral_bytes,
		logger.clone(),
		Arc::clone(&keys_manager),
	));

	// ## Running LDK
	// Step 18: Initialize networking

	let peer_manager_connection_handler = peer_manager.clone();
	let listening_port = args.ldk_peer_listening_port;
	let stop_listen_connect = Arc::new(AtomicBool::new(false));
	let stop_listen = Arc::clone(&stop_listen_connect);
	tokio::spawn(async move {
		let listener = tokio::net::TcpListener::bind(format!("[::]:{}", listening_port))
			.await
			.expect("Failed to bind to listen port - is something else already listening on it?");
		loop {
			let peer_mgr = peer_manager_connection_handler.clone();
			let tcp_stream = listener.accept().await.unwrap().0;
			if stop_listen.load(Ordering::Acquire) {
				return;
			}
			tokio::spawn(async move {
				lightning_net_tokio::setup_inbound(
					peer_mgr.clone(),
					tcp_stream.into_std().unwrap(),
				)
				.await;
			});
		}
	});

	// The hybrid post-quantum BOLT 8 handshake cannot fall back to the classical one, so it runs
	// on its own dedicated port. Post-quantum peers connect here, classical peers keep using the
	// port above.
	#[cfg(feature = "post-quantum")]
	if let Some(pq_listening_port) = args.pq_listen_port {
		let peer_manager_connection_handler = peer_manager.clone();
		let stop_listen = Arc::clone(&stop_listen_connect);
		tokio::spawn(async move {
			let listener =
				tokio::net::TcpListener::bind(format!("[::]:{}", pq_listening_port)).await.expect(
					"Failed to bind to PQ listen port - is something else already listening on it?",
				);
			loop {
				let peer_mgr = peer_manager_connection_handler.clone();
				let tcp_stream = listener.accept().await.unwrap().0;
				if stop_listen.load(Ordering::Acquire) {
					return;
				}
				tokio::spawn(async move {
					lightning_net_tokio::setup_inbound_pq(
						peer_mgr.clone(),
						tcp_stream.into_std().unwrap(),
					)
					.await;
				});
			}
		});
	}

	// Step 19: Connect and Disconnect Blocks
	let output_sweeper: Arc<OutputSweeper> = Arc::new(output_sweeper);
	let channel_manager_listener = channel_manager.clone();
	let chain_monitor_listener = chain_monitor.clone();
	let output_sweeper_listener = output_sweeper.clone();
	let bitcoind_block_source = bitcoind_client.clone();
	let network = args.network;
	tokio::spawn(async move {
		let chain_poller = poll::ChainPoller::new(bitcoind_block_source.as_ref(), network);
		let chain_listener =
			(chain_monitor_listener, &(channel_manager_listener, output_sweeper_listener));
		let mut spv_client = SpvClient::new(chain_tip, chain_poller, cache, &chain_listener);
		loop {
			spv_client.poll_best_tip().await.unwrap();
			tokio::time::sleep(Duration::from_secs(1)).await;
		}
	});

	let inbound_payments = Arc::new(Mutex::new(disk::read_inbound_payment_info(Path::new(
		&format!("{}/{}", ldk_data_dir, INBOUND_PAYMENTS_FNAME),
	))));
	let outbound_payments = Arc::new(Mutex::new(disk::read_outbound_payment_info(Path::new(
		&format!("{}/{}", ldk_data_dir, OUTBOUND_PAYMENTS_FNAME),
	))));
	let recent_payments_payment_ids = channel_manager
		.list_recent_payments()
		.into_iter()
		.filter_map(|p| match p {
			RecentPaymentDetails::Pending { payment_id, .. } => Some(payment_id),
			RecentPaymentDetails::Fulfilled { payment_id, .. } => Some(payment_id),
			RecentPaymentDetails::Abandoned { payment_id, .. } => Some(payment_id),
			RecentPaymentDetails::AwaitingInvoice { payment_id } => Some(payment_id),
		})
		.collect::<Vec<PaymentId>>();
	for (payment_id, payment_info) in outbound_payments
		.lock()
		.unwrap()
		.payments
		.iter_mut()
		.filter(|(_, i)| matches!(i.status, HTLCStatus::Pending))
	{
		if !recent_payments_payment_ids.contains(payment_id) {
			payment_info.status = HTLCStatus::Failed;
		}
	}
	fs_store
		.write("", "", OUTBOUND_PAYMENTS_FNAME, outbound_payments.lock().unwrap().encode())
		.await
		.unwrap();

	// Step 20: Handle LDK Events
	let intercepted_oms: Arc<InterceptedOmStore> = Arc::new(Mutex::new(StdHashMap::new()));
	let static_invoices: Arc<StaticInvoiceStore> = Arc::new(Mutex::new(StdHashMap::new()));
	let channel_manager_event_listener = Arc::clone(&channel_manager);
	let bitcoind_client_event_listener = Arc::clone(&bitcoind_client);
	let network_graph_event_listener = Arc::clone(&network_graph);
	let keys_manager_event_listener = Arc::clone(&keys_manager);
	let inbound_payments_event_listener = Arc::clone(&inbound_payments);
	let outbound_payments_event_listener = Arc::clone(&outbound_payments);
	let fs_store_event_listener = Arc::clone(&fs_store);
	let peer_manager_event_listener = Arc::clone(&peer_manager);
	let onion_messenger_event_listener = Arc::clone(&onion_messenger);
	let intercepted_oms_event_listener = Arc::clone(&intercepted_oms);
	let static_invoices_event_listener = Arc::clone(&static_invoices);
	let output_sweeper_event_listener = Arc::clone(&output_sweeper);
	let network = args.network;
	let event_handler = move |event: Event| {
		let channel_manager_event_listener = Arc::clone(&channel_manager_event_listener);
		let bitcoind_client_event_listener = Arc::clone(&bitcoind_client_event_listener);
		let network_graph_event_listener = Arc::clone(&network_graph_event_listener);
		let keys_manager_event_listener = Arc::clone(&keys_manager_event_listener);
		let bump_tx_event_handler = Arc::clone(&bump_tx_event_handler);
		let inbound_payments_event_listener = Arc::clone(&inbound_payments_event_listener);
		let outbound_payments_event_listener = Arc::clone(&outbound_payments_event_listener);
		let fs_store_event_listener = Arc::clone(&fs_store_event_listener);
		let peer_manager_event_listener = Arc::clone(&peer_manager_event_listener);
		let onion_messenger_event_listener = Arc::clone(&onion_messenger_event_listener);
		let intercepted_oms_event_listener = Arc::clone(&intercepted_oms_event_listener);
		let static_invoices_event_listener = Arc::clone(&static_invoices_event_listener);
		let output_sweeper_event_listener = Arc::clone(&output_sweeper_event_listener);
		async move {
			handle_ldk_events(
				channel_manager_event_listener,
				&bitcoind_client_event_listener,
				&network_graph_event_listener,
				&keys_manager_event_listener,
				&bump_tx_event_handler,
				peer_manager_event_listener,
				onion_messenger_event_listener,
				intercepted_oms_event_listener,
				static_invoices_event_listener,
				inbound_payments_event_listener,
				outbound_payments_event_listener,
				fs_store_event_listener,
				OutputSweeperWrapper(output_sweeper_event_listener),
				network,
				event,
			)
			.await;
			Ok(())
		}
	};

	// Step 21: Background Processing
	let (bp_exit, bp_exit_check) = tokio::sync::watch::channel(());
	let mut background_processor = tokio::spawn(process_events_async(
		Arc::clone(&fs_store),
		event_handler,
		Arc::clone(&chain_monitor),
		Arc::clone(&channel_manager),
		Some(onion_messenger),
		GossipSync::p2p(Arc::clone(&gossip_sync)),
		Arc::clone(&peer_manager),
		NO_LIQUIDITY_MANAGER,
		Some(Arc::clone(&output_sweeper)),
		Arc::clone(&logger),
		Some(Arc::clone(&scorer)),
		move |t| {
			let mut bp_exit_fut_check = bp_exit_check.clone();
			Box::pin(async move {
				tokio::select! {
					_ = tokio::time::sleep(t) => false,
					_ = bp_exit_fut_check.changed() => true,
				}
			})
		},
		false,
		|| Some(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap()),
	));

	// Regularly reconnect to channel peers.
	let connect_cm = Arc::clone(&channel_manager);
	let connect_pm = Arc::clone(&peer_manager);
	let stop_connect = Arc::clone(&stop_listen_connect);
	let graph_connect = Arc::clone(&network_graph);
	tokio::spawn(async move {
		let mut interval = tokio::time::interval(Duration::from_secs(1));
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
		loop {
			interval.tick().await;
			for node_id in connect_cm
				.list_channels()
				.iter()
				.map(|chan| chan.counterparty.node_id)
				.filter(|id| connect_pm.peer_by_node_id(id).is_none())
			{
				if stop_connect.load(Ordering::Acquire) {
					return;
				}
				let id = NodeId::from_pubkey(&node_id);
				let addrs = if let Some(node) = graph_connect.read_only().node(&id) {
					if let Some(ann) = &node.announcement_info {
						let non_onion = |addr| match addr {
							&lightning::ln::msgs::SocketAddress::OnionV2(_) => None,
							&lightning::ln::msgs::SocketAddress::OnionV3 { .. } => None,
							_ => Some(addr.clone()),
						};
						ann.addresses().iter().filter_map(non_onion).collect::<Vec<_>>()
					} else {
						Vec::new()
					}
				} else {
					Vec::new()
				};
				for addr in addrs {
					let sockaddrs = addr.to_socket_addrs();
					if sockaddrs.is_err() {
						continue;
					}
					for sockaddr in sockaddrs.unwrap() {
						let _ =
							cli::do_connect_peer(node_id, sockaddr, Arc::clone(&connect_pm)).await;
					}
				}
			}
		}
	});

	// Regularly broadcast our node_announcement. This is only required (or possible) if we have
	// some public channels.
	let peer_man = Arc::clone(&peer_manager);
	let chan_man = Arc::clone(&channel_manager);
	let announced_node_name = args.ldk_announced_node_name;
	let announced_listen_addr = args.ldk_announced_listen_addr.clone();
	tokio::spawn(async move {
		// First wait a minute until we have some peers and maybe have opened a channel.
		tokio::time::sleep(Duration::from_secs(60)).await;
		// Then, update our announcement once an hour to keep it fresh but avoid unnecessary churn
		// in the global gossip network.
		let mut interval = tokio::time::interval(Duration::from_secs(3600));
		loop {
			interval.tick().await;
			// Don't bother trying to announce if we don't have any public channls, though our
			// peers should drop such an announcement anyway. Note that announcement may not
			// propagate until we have a channel with 6+ confirmations.
			if chan_man.list_channels().iter().any(|chan| chan.is_announced) {
				peer_man.broadcast_node_announcement(
					[0; 3],
					announced_node_name,
					announced_listen_addr.clone(),
				);
			}
		}
	});

	tokio::spawn(sweep::migrate_deprecated_spendable_outputs(
		ldk_data_dir.clone(),
		Arc::clone(&keys_manager),
		Arc::clone(&logger),
		Arc::clone(&fs_store),
		Arc::clone(&output_sweeper),
	));

	// Start the CLI.
	let cli_channel_manager = Arc::clone(&channel_manager);
	let cli_chain_monitor = Arc::clone(&chain_monitor);
	let cli_fs_store = Arc::clone(&fs_store);
	let cli_peer_manager = Arc::clone(&peer_manager);
	let cli_poll = tokio::task::spawn(cli::poll_for_user_input(
		cli_peer_manager,
		cli_channel_manager,
		cli_chain_monitor,
		keys_manager,
		network_graph,
		inbound_payments,
		outbound_payments,
		cli_fs_store,
		args.ldk_announced_node_name,
		args.ldk_announced_listen_addr.clone(),
	));

	// Exit if either CLI polling exits or the background processor exits (which shouldn't happen
	// unless we fail to write to the filesystem).
	let mut bg_res = Ok(Ok(()));
	tokio::select! {
		_ = cli_poll => {},
		bg_exit = &mut background_processor => {
			bg_res = bg_exit;
		},
	}

	// Disconnect our peers and stop accepting new connections. This ensures we don't continue
	// updating our channel data after we've stopped the background processor.
	stop_listen_connect.store(true, Ordering::Release);
	peer_manager.disconnect_all_peers();

	if let Err(e) = bg_res {
		let persist_res = fs_store
			.write(
				persist::CHANNEL_MANAGER_PERSISTENCE_PRIMARY_NAMESPACE,
				persist::CHANNEL_MANAGER_PERSISTENCE_SECONDARY_NAMESPACE,
				persist::CHANNEL_MANAGER_PERSISTENCE_KEY,
				channel_manager.encode(),
			)
			.await
			.unwrap();
		use lightning::util::logger::Logger;
		lightning::log_error!(
			&*logger,
			"Last-ditch ChannelManager persistence result: {:?}",
			persist_res
		);
		panic!(
			"ERR: background processing stopped with result {:?}, exiting.\n\
			Last-ditch ChannelManager persistence result {:?}",
			e, persist_res
		);
	}

	// Stop the background processor.
	if !bp_exit.is_closed() {
		bp_exit.send(()).unwrap();
		background_processor.await.unwrap().unwrap();
	}
}

#[tokio::main]
pub async fn main() {
	#[cfg(not(target_os = "windows"))]
	{
		// Catch Ctrl-C with a dummy signal handler.
		unsafe {
			let mut new_action: libc::sigaction = core::mem::zeroed();
			let mut old_action: libc::sigaction = core::mem::zeroed();

			extern "C" fn dummy_handler(
				_: libc::c_int, _: *const libc::siginfo_t, _: *const libc::c_void,
			) {
			}

			new_action.sa_sigaction = dummy_handler as *const () as libc::sighandler_t;
			new_action.sa_flags = libc::SA_SIGINFO;

			libc::sigaction(
				libc::SIGINT,
				&new_action as *const libc::sigaction,
				&mut old_action as *mut libc::sigaction,
			);
		}
	}

	start_ldk().await;
}
