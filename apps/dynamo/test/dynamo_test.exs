defmodule DynamoTest do
  require Logger
  use ExUnit.Case
  require Emulation
  require Logger

  test "Dynamo test" do
    Emulation.init()
    # Emulation.append_fuzzers([Fuzzers.delay(10.0)])
    Emulation.append_fuzzers([Fuzzers.drop(0.0), Fuzzers.delay(0.0)])
    # Emulation.append_fuzzers([Fuzzers.drop(0.05)])

    nodes = [:s1, :s2, :s3, :s4, :s5, :s6, :s7, :s8, :s9]
    clients = [:c1, :c2, :c3, :c4, :c5, :c6]
    config = Dynamo.new_configuration(nodes, 7, clients, 4, 1, 4)
    nodes
    |> Enum.map(fn x ->
      Emulation.spawn(
        x,
        fn -> Dynamo.make_server(config) end
      )
    end)
    
    Process.sleep(1200)

    # Process.sleep(100)

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

        receive do
          {_, {:put, :ok, key}} ->
            true
        end
      end)
      # Process.sleep(400)

      handle = Process.monitor(client)

      receive do
        {:DOWN, ^handle, _, _, _} -> 
          true
      after
        1_000 -> assert false
      end  

      # failure2 =
      # Emulation.spawn(:failure2, fn ->
      #   Emulation.send(:s5, {:failNode, 2000})
      # end)
      # failure3 =
      # Emulation.spawn(:failure3, fn ->
      #   Emulation.send(:s6, {:failNode, 2000})
      # end)

    # Process.sleep(100)
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

    random_nodes = Enum.take_random(nodes, 3)
    random_number_1 = 300 + :rand.uniform(250)
    random_number_2 = 300 + :rand.uniform(250)
    random_number_3 = 300 + :rand.uniform(250)

    IO.inspect("randomly failed nodes are #{inspect(Enum.at(random_nodes,0))}, #{inspect(Enum.at(random_nodes,1))} for #{inspect(random_number_1)}, #{inspect(random_number_2)}
    --------------------------------------------------------------------------------------")
    failure1 =
      Emulation.spawn(:failure1, fn ->
        Emulation.send(Enum.at(random_nodes,0), {:failNode, random_number_1})
      end)
    failure2 =
      Emulation.spawn(:failure2, fn ->
        Emulation.send(Enum.at(random_nodes,1), {:failNode, random_number_2})
      end)
    failure3 =
      Emulation.spawn(:failure3, fn ->
        Emulation.send(Enum.at(random_nodes,2), {:failNode, random_number_3})
      end)

    client2 =
      Emulation.spawn(:client2, fn ->
        Emulation.send(:c2, %Dynamo.ClientPutRequest{
          key: Bojja,
          value: "Second",
          server_list: nodes
        })
        receive do
          {_, {:put, :ok, key}} ->
            true
        end
      end)
      handle = Process.monitor(client2)

      receive do
        {:DOWN, ^handle, _, _, _} -> 
          true
      after
        3_000 -> assert false
      end  
      # Process.sleep(400)

     client3 =
      Emulation.spawn(:client3, fn ->
        Emulation.send(:c3, %Dynamo.ClientPutRequest{
          key: Bojja,
          value: "Third",
          server_list: nodes
        })
        receive do
          {_, {:put, :ok, key}} ->
            true
        end
      end)
      # Process.sleep(400)
      handle = Process.monitor(client3)

      receive do
        {:DOWN, ^handle, _, _, _} -> 
          true
      after
        3_000 -> assert false
      end  
      Process.sleep(100)

     client4 =
      Emulation.spawn(:client4, fn ->
        Emulation.send(:c4, %Dynamo.ClientPutRequest{
          key: Bojja,
          value: "Fourth",
          server_list: nodes
        })
        receive do
          {_, {:put, :ok, key}} ->
            true
        end
      end)
      # Process.sleep(400)
      handle = Process.monitor(client4)

      receive do
        {:DOWN, ^handle, _, _, _} -> 
          true
      after
        5_000 -> assert false
      end  

    # Process.sleep(1000)
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
        IO.inspect("---------------------------------------------------")

        receive do
          {:c3, {:get, _key , responses}} ->
            [{value, _vclock}] = responses
            IO.puts("GET : #{inspect(value)}")
            true
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
              {^x, state} -> {state.node , Map.get(state.kv_store,Bojja,nil)}
            end
          end)

        IO.inspect(states)

        true
    after
      3_000 -> assert false
    end
    

    # IO.inspect(config)
    # Dynamo.test_hashing()
  after
    Emulation.terminate()
  end
end
