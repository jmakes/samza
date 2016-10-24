package org.apache.samza.pipeline.api;

import org.apache.samza.system.IncomingMessageEnvelope;


/**
 * Represents empirically and semantically how streams are keyed.
 *
 * When you talk about streams, you say things like "they're both keyed on member ID"
 * That is a semantic concept like a foreign key that can be independent of message/key structure. But unfortunately it is mostly taken on faith.
 *
 * However, you still need to know procedurally how to extract the key, since the messages/keys could have different
 * structures.
 *
 * Implementations of this interface should address both of these items.
 */
public interface KeyExtractor {
  // An identifier that indicates how the partitions are keyed. Taken on faith?
  String getPartitionKeyName();

  Object extractKey(IncomingMessageEnvelope message); // TODO IncomingMessageEnvelope or other type?
}
