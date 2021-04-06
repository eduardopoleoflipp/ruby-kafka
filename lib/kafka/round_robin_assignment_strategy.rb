# frozen_string_literal: true

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
      partitions_per_member = {}

      # This is just a dummy (prove of concept) assigment strategy. It assigns all topic
      # partitions to the one member that belongs to that topic.
      members.each do |member, metadata|
        member_partitions = partitions.select { |p| metadata.topics.include?(p.topic) }
        partitions_per_member[member] = member_partitions
      end

      partitions_per_member
    end
  end
end
