# frozen_string_literal: true
require 'set'

module Kafka

  # A consumer group partition assignment strategy that assigns partitions to
  # consumers in a round-robin fashion.
  class RoundRobinAssignmentStrategy
    def protocol_name
      "roundrobin"
    end

    # Assign the topic partitions to the group members.
    #
    # @param cluster [Kafka::Cluster]
    # @param members [Hash<String, Kafka::Protocol::JoinGroupResponse::Metadata>] a hash
    #   mapping member ids to metadata
    # @param partitions [Array<Kafka::ConsumerGroup::Assignor::Partition>] a list of
    #   partitions the consumer group processes
    # @return [Hash<String, Array<Kafka::ConsumerGroup::Assignor::Partition>] a hash
    #   mapping member ids to partitions.
    def call(cluster:, members:, partitions:)
      topics = Set.new
      partitions_by_topic = Hash.new {|h, k| h[k] = [] }
      partitions.each do |partition|
        topic = partition.topic
        topics.add(topic)
        partitions_by_topic[topic] << partition
      end

      members_by_topic = Hash.new {|h, k| h[k] = [] }
      topics.each do |topic|
        members.each do |id, metadata|
          members_by_topic[topic] << id if metadata.topics.include?(topic)
        end
      end

      partitions_per_member = Hash.new {|h, k| h[k] = [] }
      topics.each do |topic|
        topic_partitions = partitions_by_topic[topic]
        topic_members = members_by_topic[topic]

        topic_partitions.each_with_index do |partition, index|
          partitions_per_member[topic_members[index % topic_members.count]] << partition
        end
      end

      partitions_per_member
    end
  end
end






