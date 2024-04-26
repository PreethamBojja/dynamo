defmodule GossipProtocolTest do
    require Logger
    use ExUnit.Case
    require Emulation

    test "Gossip Protocol Test" do
        Emulation.init()
        sender_nodes_status = %{"node1" => {"healthy", 5}, "node2" => {"failed", 8}, "node3" => {"failed", 2}}
        receiver_nodes_status = %{"node1" => {"failed", 3}, "node2" => {"healthy", 9}}

        expected = %{"node1" => {"healthy", 5}, "node2" => {"healthy", 9}, "node3" => {"failed", 2}}
        updated_receiver_nodes_status = Dynamo.gossipExchange("sender", "receiver", sender_nodes_status, receiver_nodes_status)
        assert updated_receiver_nodes_status == expected
        
    after
        Emulation.terminate()
    end
end