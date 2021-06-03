# frozen_string_literal: true

describe Kafka::RoundRobinAssignmentStrategy do
  let(:strategy) { described_class.new }

  it "assigns all partitions" do
    members = Hash[(0...10).map {|i| ["member#{i}", double(topics: ['greetings'])] }]
    partitions = (0...30).map {|i| double(:"partition#{i}", topic: "greetings", partition_id: i) }

    assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

    partitions.each do |partition|
      member = assignments.values.find {|assigned_partitions|
        assigned_partitions.find {|assigned_partition|
          assigned_partition == partition
        }
      }

      expect(member).to_not be_nil
    end
  end

  it "spreads all partitions between members" do
    topics = ["topic1", "topic2"]
    members = Hash[(0...10).map {|i| ["member#{i}", double(topics: topics)] }]
    partitions = topics.product((0...5).to_a).map {|topic, i|
      double(:"partition#{i}", topic: topic, partition_id: i)
    }

    assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

    partitions.each do |partition|
      member = assignments.values.find {|assigned_partitions|
        assigned_partitions.find {|assigned_partition|
          assigned_partition == partition
        }
      }

      expect(member).to_not be_nil
    end

    num_partitions_assigned = assignments.values.map do |assigned_partitions|
      assigned_partitions.count
    end

    expect(num_partitions_assigned).to all eq(1)
  end

  Metadata = Struct.new(:topics)
  [
    {
      name: "uneven topics",
      topics: { "topic1" => [0], "topic2" => (0..50).to_a },
      members: {
        "member1" => Metadata.new(["topic1", "topic2"]),
        "member2" => Metadata.new(["topic1", "topic2"])
      },
    },
    {
      name: "only one partition",
      topics: { "topic1" => [0] },
      members: {
        "member1" => Metadata.new(["topic1"]),
        "member2" => Metadata.new(["topic1"])
      },
    },
    {
      name: "lots of partitions",
      topics: { "topic1" => (0..100).to_a },
      members: { "member1" => Metadata.new(["topic1"]) },
    },
    {
      name: "lots of members",
      topics: { "topic1" => (0..10).to_a, "topic2" => (0..10).to_a },
      members: Hash[(0..50).map { |i| ["member#{i}", Metadata.new(["topic1", "topic2"])] }]
    },
    {
      name: "odd number of partitions",
      topics: { "topic1" => (0..14).to_a },
      members: {
        "member1" => Metadata.new(["topic1"]),
        "member2" => Metadata.new(["topic1"])
      },
    },
    {
      name: "five topics, 10 partitions, 3 consumers",
      topics: { "topic1" => [0, 1], "topic2" => [0, 1], "topic3" => [0, 1], "topic4" => [0, 1], "topic5" => [0, 1] },
      members: {
        "member1" => Metadata.new(["topic1", "topic2", "topic3", "topic4", "topic5"]),
        "member2" => Metadata.new(["topic1", "topic2", "topic3", "topic4", "topic5"]),
        "member3" => Metadata.new(["topic1", "topic2", "topic3", "topic4", "topic5"])
      },
    }
  ].each do |options|
    name, topics, members = options[:name], options[:topics], options[:members]
    it name do
      partitions = topics.flat_map {|topic, partition_ids|
        partition_ids.map {|i|
          double(:"partition#{i}", topic: topic, partition_id: i)
        }
      }

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect_all_partitions_assigned(topics, assignments)
      expect_even_assignments(topics, assignments)
    end
  end

  def expect_all_partitions_assigned(topics, assignments)
    topics.each do |topic, partition_ids|
      partition_ids.each do |partition_id|
        assigned = assignments.values.find do |assigned_partitions|
          assigned_partitions.find {|assigned_partition|
            assigned_partition.topic == topic && assigned_partition.partition_id == partition_id
          }
        end
        expect(assigned).to_not be_nil
      end
    end
  end

  def expect_even_assignments(topics, assignments)
    num_partitions = topics.values.flatten.count
    assignments.values.each do |assigned_partition|
      num_assigned = assigned_partition.count
      expect(num_assigned).to be_within(1).of(num_partitions.to_f / assignments.count)
    end
  end

  context "when subscriptions are not identical but might share some commone topics" do
    it "fairly assigns partitions amongst all members regardles of the order provided" do
      members = {
        "member2" => double(topics: ["topic1", "topic2", "topic3"]),
        "member0" => double(topics: ["topic1"]),
        "member1" => double(topics: ["topic1","topic2"])
      }

      # do I need to sort this too?
      partitions = [
        partition1 = double(:"partition1", topic: "topic2", partition_id: 0),
        partition0 = double(:"partition0", topic: "topic1", partition_id: 0),
        partition4 = double(:"partition4", topic: "topic3", partition_id: 1),
        partition2 = double(:"partition2", topic: "topic2", partition_id: 1),
        partition3 = double(:"partition3", topic: "topic3", partition_id: 0),
        partition5 = double(:"partition5", topic: "topic3", partition_id: 2)
      ]

      assignments = strategy.call(cluster: nil, members: members, partitions: partitions)

      expect(assignments["member0"]).to match_array([partition0])
      expect(assignments["member1"]).to match_array([partition1])
      expect(assignments["member2"]).to match_array([
        partition2, partition3, partition4, partition5
      ])
    end
  end

  context "when there are not common topics subscriptions" do
    it "assigns "
  end
end


# C0, C1, C2,
# t0, t1, t2, with 1, 2, and 3 partitions, respectively.
# t0p0, t1p0, t1p1, t2p0, t2p1, t2p2.

# C0 is subscribed to t0;
# C1 is subscribed to t0, t1;
# and C2 is subscribed to t0, t1, t2.

# That assignment will be:
# C0: [t0p0]
# C1: [t1p0]
# C2: [t1p1, t2p0, t2p1, t2p2]


# result = { 
#   C0: [t0p0],
#   C1: [t1p0],
#   C2: [t1p1, t2p0, t2p1, t2p2]
# }
