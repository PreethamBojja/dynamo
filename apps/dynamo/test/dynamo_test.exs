defmodule DynamoTest do
  require Logger
  use ExUnit.Case
  require Emulation
  require Logger

  test "Dynamo test" do
    Emulation.init()

    nodes = [:s1, :s2, :s3, :s4, :s5, :s6, :s7]
    clients = [:c1, :c2, :c3]
    config = Dynamo.new_configuration(nodes, 5, clients, 3, 2, 2)
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
          value: "sb9509",
          server_list: nodes
        })

        receive do
          {c , {:put, :ok, return}} ->
            IO.inspect("Put done 1")
        end
      end)

    handle = Process.monitor(client)

    receive do
      {:DOWN, ^handle, _, _, _} -> true
    after
      1_000 -> assert false
    end

    client2 =
      Emulation.spawn(:client2, fn ->
        Emulation.send(:c1, %Dynamo.ClientPutRequest{
          key: Maithili,
          value: "sc10648",
          server_list: nodes
        })

        receive do
          {c , {:put, :ok, return}} ->
            IO.inspect("Put done 2")
        end
      end)

    handle = Process.monitor(client2)

    receive do
      {:DOWN, ^handle, _, _, _} -> 
        # nodes |> Enum.map(fn x -> Emulation.send(x, :state) end)

        # states =
        #   nodes
        #   |> Enum.map(fn x ->
        #     receive do
        #       {^x, state} -> state
        #     end
        #   end)

        # IO.inspect(states)

        true
    after
      1_000 -> assert false
    end

    client3 =
      Emulation.spawn(:client3, fn ->
        Emulation.send(:c1, %Dynamo.ClientGetRequest{
          key: Maithili,
          server_list: nodes
        })

        receive do
          m ->
            true
            IO.inspect(m)
        end
      end)

    handle = Process.monitor(client3)

    receive do
      {:DOWN, ^handle, _, _, _} -> 
        # nodes |> Enum.map(fn x -> Emulation.send(x, :state) end)

        # states =
        #   nodes
        #   |> Enum.map(fn x ->
        #     receive do
        #       {^x, state} -> state
        #     end
        #   end)

        # IO.inspect(states)

        true
    after
      1_000 -> assert false
    end
    

    # IO.inspect(config)
    # Dynamo.test_hashing()
  after
    Emulation.terminate()
  end
end
