defmodule DynamoTest do
  require Logger
  use ExUnit.Case
  require Emulation
  require Logger

  test "Dynamo test" do
    Emulation.init()
    # Emulation.append_fuzzers([Fuzzers.delay(50.0)])

    nodes = [:s1, :s2, :s3, :s4, :s5, :s6, :s7]
    clients = [:c1, :c2, :c3, :c4, :c5, :c6]
    config = Dynamo.new_configuration(nodes, 5, clients, 3, 2, 1)
    nodes
    |> Enum.map(fn x ->
      Emulation.spawn(
        x,
        fn -> Dynamo.make_server(config) end
      )
    end)

    config = Dynamo.Client.new_client_configuration(clients)
    clients
    |> Enum.map(fn x ->
      Emulation.spawn(
        x,
        fn -> Dynamo.Client.make_client(config) end
      )
    end)

    client =
      Emulation.spawn(:client, fn ->
        Emulation.send(:c1, %Dynamo.ClientPutRequest{
          key: Bojja,
          value: "First",
          server_list: nodes
        })
      end)

    Process.sleep(100)
    # view = [:s4, :s5, :s6]
    # view |> Enum.map(fn x -> Emulation.send(x, :state) end)
    # states =
    #   view
    #   |> Enum.map(fn x ->
    #     receive do
    #       {^x, state} -> state
    #     end
    #   end)

    # IO.inspect(states)

    client2 =
      Emulation.spawn(:client2, fn ->
        Emulation.send(:c2, %Dynamo.ClientPutRequest{
          key: Sai,
          value: "Second",
          server_list: nodes
        })
      end)
       Process.sleep(100)

     client3 =
      Emulation.spawn(:client3, fn ->
        Emulation.send(:c3, %Dynamo.ClientPutRequest{
          key: Preetham,
          value: "Third",
          server_list: nodes
        })
      end)
       Process.sleep(100)

     client4 =
      Emulation.spawn(:client4, fn ->
        Emulation.send(:c4, %Dynamo.ClientPutRequest{
          key: Reddy,
          value: "Fourth",
          server_list: nodes
        })
      end)

    Process.sleep(1000)
    # view = [:s4, :s5, :s6]
    # view |> Enum.map(fn x -> Emulation.send(x, :state) end)
    # states =
    #   view
    #   |> Enum.map(fn x ->
    #     receive do
    #       {^x, state} -> state
    #     end
    #   end)

    # IO.inspect(states)

    client6 =
      Emulation.spawn(:client6, fn ->
        Emulation.send(:c3, %Dynamo.ClientGetRequest{
          key: Bojja,
          server_list: nodes
        })

        receive do
          m ->
            true
            IO.inspect(m)
        end
      end)

    handle = Process.monitor(client6)

    receive do
      {:DOWN, ^handle, _, _, _} -> 
        nodes |> Enum.map(fn x -> Emulation.send(x, :state) end)

        states =
          nodes
          |> Enum.map(fn x ->
            receive do
              {^x, state} -> state
            end
          end)

        IO.inspect(states)

        true
    after
      1_0000 -> assert false
    end
    

    # IO.inspect(config)
    # Dynamo.test_hashing()
  after
    Emulation.terminate()
  end
end
