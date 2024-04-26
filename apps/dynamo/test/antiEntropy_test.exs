defmodule AntiEntropyTest do
    require Logger
    use ExUnit.Case
    require Emulation
    require Dynamo.VectorClock

    test "Anti Entropy Test" do
        Emulation.init()
        # Let sender has 3 (1, 2, 3) vnodes
        # Let receiver has 2 (1, 2) vnodes
        # Test: data from sender vnode 3 is replicated in receiver vnode 1
        sender_kv_store = %{
            "s1" => {"v1", %{"a" => 1}, {"sender", 1}, 2, 0},
            "item1" => {"data1", %{"a" => 2}, {"sender", 3}, 3, 1},
            "item2" => {"data1", %{"a" => 3}, {"sender", 3}, 3, 2},
            "item3" => {"data3", %{"a" => 4}, {"sender", 3}, 3, 4} # This data is missing in receiver
        }
        receiver_kv_store = %{
            "r1" => {"x1", %{"b" => 1}, {"receiver", 1}, 1, 1}, # Receivers own data
            "item1" => {"old_data1", %{"a" => 0}, {"sender", 3}, 1, 0}, # Casually related case
            "item2" => {"?_data2", %{"b" => 1}, {"sender", 3}, 1, 3} # Casually unrelated, check clocks
        }

        updated_receiver_kv_store = Dynamo.syncronisation("sender", "receiver", sender_kv_store, receiver_kv_store)
        IO.inspect(updated_receiver_kv_store)

    after
        Emulation.terminate()
    end
end