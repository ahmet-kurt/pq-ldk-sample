use crate::disk::FilesystemLogger;
use crate::{GossipVerifier, NetworkGraph};
use bitcoin::secp256k1::PublicKey;
use lightning::ln::msgs::{
	BaseMessageHandler, ChannelAnnouncement, ChannelUpdate, Init, LightningError,
	MessageSendEvent, NodeAnnouncement, QueryChannelRange, QueryShortChannelIds,
	ReplyChannelRange, ReplyShortChannelIdsEnd, RoutingMessageHandler,
};
use lightning::log_info;
use lightning::routing::gossip::{NodeId, P2PGossipSync};
use lightning::types::features::{InitFeatures, NodeFeatures};
use lightning::util::logger::Logger;
use lightning::util::ser::Writeable;
use std::sync::Arc;

type NodeGossipSync =
	P2PGossipSync<Arc<NetworkGraph>, Arc<GossipVerifier>, Arc<FilesystemLogger>>;

/// Wraps the node's `P2PGossipSync` to record the wire size of every gossip
/// message received from a peer when the node runs with `--gossip-stats`.
/// Sizes cover the serialized message plus its two type bytes, i.e. the BOLT 8
/// payload the message occupies on the wire. The evaluation harness sums the
/// GOSSIP-STATS lines from the log to measure gossip traffic.
pub(crate) struct GossipStats {
	inner: Arc<NodeGossipSync>,
	enabled: bool,
	logger: Arc<FilesystemLogger>,
}

impl GossipStats {
	pub(crate) fn new(
		inner: Arc<NodeGossipSync>, enabled: bool, logger: Arc<FilesystemLogger>,
	) -> Self {
		Self { inner, enabled, logger }
	}
}

impl BaseMessageHandler for GossipStats {
	fn get_and_clear_pending_msg_events(&self) -> Vec<MessageSendEvent> {
		self.inner.get_and_clear_pending_msg_events()
	}
	fn peer_disconnected(&self, their_node_id: PublicKey) {
		self.inner.peer_disconnected(their_node_id)
	}
	fn provided_node_features(&self) -> NodeFeatures {
		self.inner.provided_node_features()
	}
	fn provided_init_features(&self, their_node_id: PublicKey) -> InitFeatures {
		self.inner.provided_init_features(their_node_id)
	}
	fn peer_connected(
		&self, their_node_id: PublicKey, msg: &Init, inbound: bool,
	) -> Result<(), ()> {
		self.inner.peer_connected(their_node_id, msg, inbound)
	}
}

impl RoutingMessageHandler for GossipStats {
	fn handle_node_announcement(
		&self, their_node_id: Option<PublicKey>, msg: &NodeAnnouncement,
	) -> Result<bool, LightningError> {
		if self.enabled && their_node_id.is_some() {
			log_info!(
				self.logger,
				"GOSSIP-STATS: recv node_announcement node={} bytes={}",
				msg.contents.node_id,
				msg.serialized_length() + 2
			);
		}
		self.inner.handle_node_announcement(their_node_id, msg)
	}
	fn handle_channel_announcement(
		&self, their_node_id: Option<PublicKey>, msg: &ChannelAnnouncement,
	) -> Result<bool, LightningError> {
		if self.enabled && their_node_id.is_some() {
			log_info!(
				self.logger,
				"GOSSIP-STATS: recv channel_announcement scid={} bytes={}",
				msg.contents.short_channel_id,
				msg.serialized_length() + 2
			);
		}
		self.inner.handle_channel_announcement(their_node_id, msg)
	}
	fn handle_channel_update(
		&self, their_node_id: Option<PublicKey>, msg: &ChannelUpdate,
	) -> Result<Option<(NodeId, NodeId)>, LightningError> {
		if self.enabled && their_node_id.is_some() {
			log_info!(
				self.logger,
				"GOSSIP-STATS: recv channel_update scid={} dir={} bytes={}",
				msg.contents.short_channel_id,
				msg.contents.channel_flags & 1,
				msg.serialized_length() + 2
			);
		}
		self.inner.handle_channel_update(their_node_id, msg)
	}
	fn get_next_channel_announcement(
		&self, starting_point: u64,
	) -> Option<(ChannelAnnouncement, Option<ChannelUpdate>, Option<ChannelUpdate>)> {
		self.inner.get_next_channel_announcement(starting_point)
	}
	fn get_next_node_announcement(
		&self, starting_point: Option<&NodeId>,
	) -> Option<NodeAnnouncement> {
		self.inner.get_next_node_announcement(starting_point)
	}
	fn handle_reply_channel_range(
		&self, their_node_id: PublicKey, msg: ReplyChannelRange,
	) -> Result<(), LightningError> {
		self.inner.handle_reply_channel_range(their_node_id, msg)
	}
	fn handle_reply_short_channel_ids_end(
		&self, their_node_id: PublicKey, msg: ReplyShortChannelIdsEnd,
	) -> Result<(), LightningError> {
		self.inner.handle_reply_short_channel_ids_end(their_node_id, msg)
	}
	fn handle_query_channel_range(
		&self, their_node_id: PublicKey, msg: QueryChannelRange,
	) -> Result<(), LightningError> {
		self.inner.handle_query_channel_range(their_node_id, msg)
	}
	fn handle_query_short_channel_ids(
		&self, their_node_id: PublicKey, msg: QueryShortChannelIds,
	) -> Result<(), LightningError> {
		self.inner.handle_query_short_channel_ids(their_node_id, msg)
	}
	fn processing_queue_high(&self) -> bool {
		self.inner.processing_queue_high()
	}
}
