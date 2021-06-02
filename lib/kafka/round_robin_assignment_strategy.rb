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
      partitions_per_member = Hash.new {|h, k| h[k] = [] }

      members_ids = members.sort_by do |id, data| 
        data.topics.size 
      end.map { |id, data| id }.uniq
      idx = 0

      partitions.each do |partition|
        topic = partition.topic
        while !members[members_ids[idx]].topics.include?(topic)
          idx = next_index(members_ids, idx)
        end

        partitions_per_member[members_ids[idx]] << partition
        idx = next_index(members_ids, idx)
      end
      partitions_per_member
    end

    def next_index(members_ids, idx)
      idx += 1
      idx = 0 if idx == members_ids.size

      idx
    end
  end
end






