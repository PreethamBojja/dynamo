defmodule Dynamo do
  # Importing modules
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
  # import Logger
  
  alias ExHashRing.Ring
  alias ExHashRing.Node
  alias ExHashRing.Hash
  alias __MODULE__

  # Requiring modules
  require Fuzzers

  @enforce_keys [:nodes, :vnodes, :clients, :read_quorum, :write_quorum, :replication_factor, :ring]
  defstruct(
    node: nil,
    # List of server names
    nodes: nil,
    # No of virtual nodes per server
    vnodes: nil,
    # List of client names
    clients: nil,
    # N: no of nodes to replicate
    replication_factor: nil,
    # R
    read_quorum: nil,
    # W
    write_quorum: nil,
    # Consistent Hash Ring
    ring: nil,
    # Key value store 
    kv_store: nil,
    # Seq number from co-ordinator
    seq: nil,
    # Map from seq number to responses count
    response_count: nil,
    # Map from seq number to responses list
    responses: nil,
    # Map from each vnode to keys stored 
    vnodeToKeys: nil,
    # Map from each server to all nodes status
    status_of_nodes: nil,
    # Map of server request to their timer
    requestTimerMap: nil,
    # Map of seq to client request to their timer
    clientRequestTimerMap: nil,
    # Map of hinted handOff with vnode index
    hintedHandedOffMap: nil,
    # Simulated failure boolean
    inFailedState: nil
  )

  # 
  @doc """
  Create a new configuration
  """
  @spec new_configuration(list(), non_neg_integer(), list(), non_neg_integer(), non_neg_integer(), non_neg_integer()) :: %Dynamo{
    nodes: list(),
    clients: list(),
    read_quorum: non_neg_integer(),
    write_quorum: non_neg_integer(),
    replication_factor: non_neg_integer()
  }
  def new_configuration(nodes, vnodes, clients, replication_factor, read_quorum, write_quorum) do
    {:ok, ring} = Ring.start_link()
    %Dynamo{
      nodes: nodes,
      vnodes: vnodes,
      clients: clients,
      replication_factor: replication_factor,
      read_quorum: read_quorum,
      write_quorum: write_quorum,
      ring: ring,
      kv_store: %{},
      seq: -1,
      response_count: %{},
      responses: %{},
      vnodeToKeys: %{},
      status_of_nodes: Enum.into(nodes, %{}, fn node -> {node, {"Healthy", 0}} end),
      requestTimerMap: %{},
      clientRequestTimerMap: %{},
      inFailedState: false
    }
  end

  # 
  @doc """
  Make a node as server
  """
  @spec make_server(%Dynamo{}) :: no_return()
  def make_server(state) do
    Ring.add_node(state.ring, whoami(), state.vnodes)
    # timer = Emulation.timer(50, :antientropy)
    timer = Emulation.timer(50, :gossip)
    state = %{state | status_of_nodes: Map.put(state.status_of_nodes, whoami(), {"Healthy", Emulation.now()})}
    server(%{state | node: whoami()})
  end

  def find_start_index(list, candidate) when is_list(list) do
    find_start_index(list, candidate, 0)
  end
  
  defp find_start_index([head | tail], candidate, index) do
    if head > candidate do
      index
    else
      find_start_index(tail, candidate, index + 1)
    end
  end
  
  def circular_traversal(list, start_index, count, unhealthy_nodes) do
    length = length(list)
    start_index = rem(start_index, length)
    traverse_circular(list, start_index, count, unhealthy_nodes, MapSet.new(), [], MapSet.new(), [])
  end
  
  defp traverse_circular(_list, _index, 0, unhealthy_nodes, distinct_items, preference_list, distinct_skipped_items, skipped_items_list) do
    {preference_list, skipped_items_list}
  end
  
  defp traverse_circular(list, index, count, unhealthy_nodes, distinct_items, preference_list, distinct_skipped_items, skipped_items_list) do
    element = Enum.at(list, index)
  
    case MapSet.member?(distinct_items, element) do
      true ->
        next_index = rem(index + 1, length(list))
        traverse_circular(list, next_index, count, unhealthy_nodes, distinct_items, preference_list, distinct_skipped_items, skipped_items_list)
      false ->
        case MapSet.member?(unhealthy_nodes, element) do
          true ->
            case MapSet.member?(distinct_skipped_items, element) do
              true ->
                next_index = rem(index + 1, length(list))
                traverse_circular(list, next_index, count, unhealthy_nodes, distinct_items, preference_list, distinct_skipped_items, skipped_items_list)

              false ->
                skipped_items_list = skipped_items_list ++ [index]
                next_index = rem(index + 1, length(list))
                distinct_items = MapSet.put(distinct_items, element)
                traverse_circular(list, next_index, count, unhealthy_nodes, distinct_items, preference_list, distinct_skipped_items, skipped_items_list)
            end

          false ->
            preference_list = preference_list ++ [index]
            next_index = rem(index + 1, length(list))
            distinct_items = MapSet.put(distinct_items, element)
            traverse_circular(list, next_index, count - 1, unhealthy_nodes, distinct_items, preference_list, distinct_skipped_items, skipped_items_list)
        end
    end
  end
  

  def get_preference_list(state, key) do
    ring = state.ring
    {:ok, nodes} = Ring.get_nodes_with_replicas(ring)
    {hashList, nodeList} = Enum.unzip(Node.expand(nodes))
    start_index = find_start_index(hashList, Hash.of(key))
    unhealthy_nodes = state.status_of_nodes
                      |> Enum.filter(fn {_node, {status, _timestamp}} -> status == "Failed" end)
                      |> Enum.map(fn {node, _} -> node end)
    unhealthy_nodes = MapSet.new(unhealthy_nodes)
    {pref_list_indices, skip_list_indices} = circular_traversal(nodeList, start_index, state.replication_factor, unhealthy_nodes)
    pref_list =  Enum.map(pref_list_indices, fn index -> Enum.at(nodeList, index) end)
    skip_list =  Enum.map(skip_list_indices, fn index -> Enum.at(nodeList, index) end)
    {pref_list, pref_list_indices, skip_list, skip_list_indices}
  end

  # Broadcast a message to all nodes in the configuration (excluding the sender.)
  @spec bcast(list(), any()) :: list()
  defp bcast(node_list, msg) do
    node_list
    |> Enum.map(fn node -> if node != whoami() do send(node, msg) end end)
  end

  # AntiEntropy syncronisation, same intution as Merkle tress, can be optimised through Merkle Tree comparision
  def syncronisation(sender, receiver, sender_kv_store, receiver_kv_store) do
    # Collecting all {sender_vnode, receiver_vnode} where server_node matches sender_node
    sender_receiver_vnode_pairs = receiver_kv_store
    |> Enum.map(fn {_, {_, _, {server, sender_vnode}, receiver_vnode, _}} ->
      if server == sender, do: {sender_vnode, receiver_vnode}, else: nil
    end)
    |> Enum.reject(&is_nil/1)

    Enum.reduce(sender_receiver_vnode_pairs, receiver_kv_store, fn {sender_vnode, receiver_vnode}, acc_kv_store ->
      sender_vnode_kv_store = sender_kv_store
      |> Enum.filter(fn {_, {_, _, {_, vnode}, _, _}} -> vnode == sender_vnode end)
      
      Enum.reduce(sender_vnode_kv_store, acc_kv_store, fn {key, sender_key_data}, acc_inner_kv_store ->
        case Map.get(acc_inner_kv_store, key) do
          nil ->
            {v, sender_vector_clock, {s, sv}, _, sender_clock} = sender_key_data
            updated_receiver_data = {v, sender_vector_clock, {s, sv}, receiver_vnode, sender_clock}
            acc_inner_kv_store = Map.put(acc_inner_kv_store, key, updated_receiver_data)

          receiver_key_data ->
            {v, sender_vector_clock, {s, sv}, _, sender_clock} = sender_key_data
            {_, receiver_vector_clock, {_, _}, _, receiver_clock} = receiver_key_data
            if Dynamo.VectorClock.lessThanEqualTo(sender_vector_clock, receiver_vector_clock) do
              # Receiver is more or equally updated so do nothing
              acc_inner_kv_store
            else
              if Dynamo.VectorClock.greaterThan(sender_vector_clock, receiver_vector_clock) do
                # Receiver can be less updated.
                updated_receiver_data = {v, sender_vector_clock, {s, sv}, receiver_vnode, sender_clock}
                acc_inner_kv_store = Map.put(acc_inner_kv_store, key, updated_receiver_data)
              else 
                # Reciver and Sender are unrelated. Compare clocks
                if sender_clock > receiver_clock do 
                  updated_receiver_data = {v, sender_vector_clock, {s, sv}, receiver_vnode, sender_clock}
                  acc_inner_kv_store = Map.put(acc_inner_kv_store, key, updated_receiver_data)
                else 
                  # Do nothing
                  acc_inner_kv_store
                end
              end
            end
        end
      end)
    end)
  end

  # Implementing Gossip Protocol
  def gossipExchange(sender, receiver, sender_nodes_status, receiver_nodes_status) do
    Enum.reduce(sender_nodes_status, receiver_nodes_status, fn {node, {sender_node_status, sender_node_timestamp}}, acc_nodes_status ->
      case Map.get(acc_nodes_status, node) do
        nil ->
          acc_nodes_status = Map.put(acc_nodes_status, node, {sender_node_status, sender_node_timestamp})
        {receiver_node_status, receiver_node_timestamp} ->
          if sender_node_timestamp > receiver_node_timestamp do
            acc_nodes_status = Map.put(acc_nodes_status, node, {sender_node_status, sender_node_timestamp})
          else
            acc_nodes_status
          end
      end
    end)
  end

  def makeHealthy(state, sender) do
    %{state | status_of_nodes: Map.put(state.status_of_nodes, sender, {"Healthy", Emulation.now()})}
  end

  def server(state) do
    receive do 
      {sender,
       %Dynamo.ServerPutRequest{
        key: key,
        value: value,
        client: client,
        replication: replication,
        seq: seq,
        context: context,
        vnodeIndex: vindex
       }} ->

        IO.puts("Put request received at #{whoami()}")
        if state.inFailedState == true do
          if not replication do 
            new_server = Enum.random(state.nodes)
            send(new_server, %Dynamo.ServerPutRequest{
              key: key,
              value: value,
              client: client,
              replication: replication,
              seq: seq,
              context: context,
              vnodeIndex: vindex
             })
          end
          server(state)
        else
          if not replication do 
            # IO.inspect("Getting preference list")
            {preference_list, preference_list_indices, skip_list, skip_list_indices} = get_preference_list(state,key)
            IO.inspect("Done preference list")
            start_index = Enum.at(preference_list_indices,0)
            if(Enum.at(preference_list,0) == whoami()) do
              IO.inspect(preference_list)
              IO.inspect(skip_list)
              if context != nil do 
                state = %{state | kv_store: Map.put(state.kv_store,key,{value, context,{whoami(), start_index}, start_index, Emulation.now()})}
              end
              state = %{state | kv_store: Dynamo.VectorClock.updateVectorClock(state.kv_store, key, value, whoami(), state.seq + 1, start_index)}
              state = %{state | vnodeToKeys: Map.put(state.vnodeToKeys, start_index, Enum.uniq(Enum.concat(Map.get(state.vnodeToKeys, start_index,[]), [key])))}
              t = Emulation.timer(500, {:clientPutRequestTimeOut, whoami(), client, key, value, state.seq+1})
              state = %{state | clientRequestTimerMap: Map.put(state.clientRequestTimerMap, {whoami(), client, key, value, state.seq+1}, t)}
              IO.puts("Put done at for key:#{key} and value:#{value} at seq:#{state.seq + 1} at #{whoami()}")
              state = 
                if state.write_quorum == 1 do
                  requestTimer = Map.get(state.clientRequestTimerMap, {whoami(), client, key, value, state.seq + 1}, nil)
                  Emulation.cancel_timer(requestTimer)
                  state = %{state | clientRequestTimerMap: Map.delete(state.clientRequestTimerMap, {whoami(), key, value, state.seq + 1})}
                  send(client, {:ok, key})
                  state
                else
                  state
                end 
              preference_list_tuple = Enum.zip(preference_list, preference_list_indices)  
              new_state = Enum.reduce(preference_list_tuple, state, fn {node, vnode_index}, acc_state ->
                                              if node != whoami() do
                                                t = Emulation.timer(25, {:requestTimeout, whoami(), node, key, state.seq + 1})
                                                new_state = %{acc_state | 
                                                  requestTimerMap: Map.put(acc_state.requestTimerMap, {whoami(), node, key, state.seq + 1}, t)
                                                }
                                                
                                                send(node, %Dynamo.ServerPutRequest{
                                                  key: key,
                                                  value: value,
                                                  client: client,
                                                  replication: true,
                                                  seq: state.seq + 1,
                                                  context: Map.get(new_state.kv_store, key),
                                                  vnodeIndex: vnode_index
                                                })
                                                
                                                new_state
                                              else
                                                acc_state
                                              end
                                            end)
  
  
              # IO.puts("Got into co-ordinator and started broadcast")
              server(%{ new_state | seq: state.seq + 1})
            else
              send(Enum.at(preference_list,0), %Dynamo.ServerPutRequest{
                key: key,
                value: value,
                client: client,
                replication: false,
                seq: nil,
                context: context
               })  
              server(state)
            end
          else
            # IO.puts("Got into replication at #{whoami()}")
            # LAST WRITE WINS
            IO.puts("Put replicated at for key:#{key} and value:#{value} at seq:#{seq} at #{whoami()}")
            new_context = case context do 
                              {a, b, c, _, e} -> {a, b, c, vindex, e} 
                              _ -> context end
            state = %{state | kv_store: Map.put(state.kv_store, key, new_context)}
            state = %{state | vnodeToKeys: Map.put(state.vnodeToKeys, vindex, Enum.uniq(Enum.concat(Map.get(state.vnodeToKeys, vindex,[]), [key])))}
            send(sender, %Dynamo.ServerPutResponse{
              key: key,
              value: value,
              status: :ok,
              client: client,
              seq: seq
              }) 
            server(%{state | seq: seq})
          end
        end
      
      {sender,
       %Dynamo.ServerGetRequest{
         key: key,
         client: client,
         replication: replication,
         seq: seq,
         context: context
       }} ->
        
        if state.inFailedState == true do
          if not replication do 
            new_server = Enum.random(state.nodes)
            send(new_server, %Dynamo.ServerGetRequest{
              key: key,
              client: client,
              replication: replication,
              seq: seq,
              context: context
            })
          end
          server(state)
        else
          IO.puts("Get request received at #{whoami()}")
          if not replication do 
            # IO.inspect("Getting preference list")
            {preference_list, preference_list_indices, skip_list, skip_list_indices} = get_preference_list(state,key)
            # IO.inspect("Done preference list")
            # if(Enum.at(preference_list,0) == whoami()) do
            if Enum.member?(preference_list, whoami()) do
              IO.inspect(preference_list)
              IO.inspect(skip_list)
              value = 
              if Map.get(state.kv_store,key,nil) == nil do
                []
              else
                Map.get(state.kv_store,key,nil) 
              end
              state = %{state | responses: Map.put(state.responses, state.seq + 1, [value])}
              t = Emulation.timer(500, {:clientGetRequestTimeOut, whoami(), client, key, state.seq+1})
              state = %{state | clientRequestTimerMap: Map.put(state.clientRequestTimerMap, {whoami(), client, key, state.seq+1}, t)}
              #  IO.puts("Got into co-ordinator and started broadcast")
              state = 
                if state.read_quorum == 1 do
                  requestTimer = Map.get(state.clientRequestTimerMap, {whoami(), client, key, state.seq + 1}, nil)
                  Emulation.cancel_timer(requestTimer)
                  state = %{state | clientRequestTimerMap: Map.delete(state.clientRequestTimerMap, {whoami(), client, key, state.seq + 1})}
                  send(client, {key, Map.get(state.responses, state.seq + 1)})
                  state
                else
                  state
                end
              new_state = Enum.reduce(preference_list, state, fn node, acc_state ->
                                              if node != whoami() do
                                                t = Emulation.timer(25, {:requestTimeout, whoami(), node, key, state.seq + 1})
                                                new_state = %{acc_state | 
                                                  requestTimerMap: Map.put(acc_state.requestTimerMap, {whoami(), node, key, state.seq + 1}, t)
                                                }
                                                
                                                send(node, %Dynamo.ServerGetRequest{
                                                            key: key,
                                                            client: client,
                                                            replication: true,
                                                            seq: state.seq + 1
                                                          })
                                                
                                                new_state
                                              else
                                                acc_state
                                              end
                                            end)
              server(%{ new_state | seq: state.seq + 1})
            else
              send(Enum.at(preference_list,0), %Dynamo.ServerGetRequest{
                key: key,
                client: client,
                replication: false,
                seq: nil
              })  
              server(state)
            end
          else
            IO.puts("Get replicated at for key:#{key} at seq:#{seq} at #{whoami()}")
            value = if Map.get(state.kv_store,key,nil) == nil do
                []
              else
                Map.get(state.kv_store,key,nil)
              end
            send(sender, %Dynamo.ServerGetResponse{
              key: key,
              value:  value,
              status: :ok,
              client: client,
              seq: seq
              }) 
            server(%{ state | seq: seq})
          end
        end

      {sender,
        %Dynamo.ServerPutResponse{
          key: key,
          status: status,
          client: client,
          value: value,
          seq: seq,
          context: context
        }} ->

        if state.inFailedState == true do
          server(state)
        else  
          IO.puts("Got into server put response at #{whoami()} with seq: #{seq}")
          makeHealthy(state, sender)
          if status == :ok and Map.get(state.requestTimerMap, {whoami(), sender, key, seq}, nil) != nil do
              requestTimer = Map.get(state.requestTimerMap, {whoami(), sender, key, seq}, nil)
              Emulation.cancel_timer(requestTimer)
              state = %{state | requestTimerMap: Map.delete(state.requestTimerMap, {whoami(), sender, key, seq})}
              state = if Map.get(state.response_count, seq, nil) == nil do
                        %{state | response_count: Map.put(state.response_count, seq, 1)}
                      else
                        count = Map.get(state.response_count, seq)
                        %{state | response_count: Map.put(state.response_count, seq, count + 1)} 
                      end 
              if Map.get(state.response_count, seq, nil) == state.write_quorum - 1 do
                requestTimer = Map.get(state.clientRequestTimerMap, {whoami(), client, key, value, seq}, nil)
                Emulation.cancel_timer(Map.get(state.clientRequestTimerMap, {whoami(), client, key, value, seq}, nil))    
                state = %{state | clientRequestTimerMap: Map.delete(state.clientRequestTimerMap, {whoami(), client, key, value, seq})}
                send(client, {:ok, key})
              end
              server(state)  
            end
            server(state)
        end

      {sender,
        %Dynamo.ServerGetResponse{
          key: key,
          value: value,
          status: status,
          client: client,
          seq: seq,
          context: context
        }} ->
          
          if state.inFailedState == true do
            server(state)
          else
            IO.puts("Got into server get response at #{whoami()} with seq: #{seq}")
            makeHealthy(state, sender)
            if status == :ok and Map.get(state.requestTimerMap, {whoami(), sender, key, seq}, nil) != nil do
              requestTimer = Map.get(state.requestTimerMap, {whoami(), sender, key, seq}, nil)
              Emulation.cancel_timer(requestTimer)
              state = %{state | requestTimerMap: Map.delete(state.requestTimerMap, {whoami(), key, seq})}
              state = if Map.get(state.response_count, seq, nil) == nil do
                        %{state | response_count: Map.put(state.response_count, seq, 1)}
                      else
                        count = Map.get(state.response_count, seq)
                        %{state | response_count: Map.put(state.response_count, seq, count + 1)} 
                      end 

              responses = 
              if value == [] do 
                Map.get(state.responses, seq, [])
              else
                Map.get(state.responses, seq, []) ++ [value]
              end
              state = %{state | responses: Map.put(state.responses, seq, responses)}
              
              state = 
                if Map.get(state.response_count, seq, 0) == state.read_quorum - 1 do
                  requestTimer = Map.get(state.clientRequestTimerMap, {whoami(), client, key, seq}, nil)
                  if requestTimer!= nil do
                    Emulation.cancel_timer(requestTimer)
                  end
                  state = %{state | clientRequestTimerMap: Map.delete(state.clientRequestTimerMap, {whoami(), client, key, seq})}
                  reconciledResponses = Dynamo.VectorClock.syntaticReconcilationWithValues(Map.get(state.responses, seq))
                  state = %{state | responses: Map.put(state.responses, seq, reconciledResponses)}
                  send(client, {key, Map.get(state.responses, seq)})
                  state
                else
                  state
                end
              server(state)  
            end
            server(state)
          end

      {:requestTimeout, node, failed_node, key, seq} ->

        IO.inspect("Request timed out for #{node} with #{failed_node} with key : #{key} for seq : #{seq} ")
        state = %{state | requestTimerMap: Map.delete(state.requestTimerMap, {node, failed_node, key, seq})}
        state = %{state | status_of_nodes: Map.put(state.status_of_nodes, failed_node, {"Failed", Emulation.now()})}
        server(state)

      {:clientPutRequestTimeOut, node, client, key, value, seq} ->

        IO.inspect("Client Request timed out for Put at #{node} with #{client} with key : #{key} and value : #{value}")
        # IO.inspect(Map.get(state.clientRequestTimerMap, {node, client, key, value, seq}))
        state = %{state | clientRequestTimerMap: Map.delete(state.clientRequestTimerMap, {node, client, key, value, seq})}
        server = Enum.random(state.nodes)
        if state.inFailedState == true do
          server(state)
        else
          send(server, %Dynamo.ServerPutRequest{
            key: key,
            value: value,
            client: client,
            replication: false,
            seq: nil,
            context: nil
          })
          server(state)
        end
      
      {:clientGetRequestTimeOut, node, client, key, seq} ->

        IO.inspect("Client Request timed out for Get at #{node} with #{client} with key : #{key}")
        state = %{state | clientRequestTimerMap: Map.delete(state.clientRequestTimerMap, {node, client, key, seq})}
        server = Enum.random(state.nodes)
        if state.inFailedState == true do
          server(state)
        else
          send(server, %Dynamo.ServerGetRequest {
            key: key,
            client: client,
            replication: false,
            seq: nil
            })
          server(state)
        end


      {sender, {:failNode, failTime}} ->
        state = %{state | status_of_nodes: Map.put(state.status_of_nodes, whoami(), {"Failed", Emulation.now()})}
        state = %{state | inFailedState: true}
        t = Emulation.timer(failTime, :recover)
        server(state)

      :recover ->
        IO.inspect("[][][][][][][][[][][][][][][][][][][][][][]]")
        state = %{state | status_of_nodes: Map.put(state.status_of_nodes, whoami(), {"Healthy", Emulation.now()})}
        state = %{state | inFailedState: false}
        server(state)  


      :antientropy ->
        if state.inFailedState == true do
          server(state)
        else
          other_nodes = List.delete(state.nodes, whoami())
          select_node = Enum.random(other_nodes)
          send(select_node, {:merkle_request, state.kv_store})
          timer = Emulation.timer(50, :antientropy)
          server(state)
        end

      {sender, {:merkle_request, sender_kv_store}} ->
        if state.inFailedState == true do
          server(state)
        else
          makeHealthy(state, sender)
          receiver_kv_store = state.kv_store
          updated_kv_store = syncronisation(sender, whoami(), sender_kv_store, receiver_kv_store)
          server(%{state | kv_store: updated_kv_store})
        end

      :gossip ->
        if state.inFailedState == true do
          server(state)
        else
          other_nodes = List.delete(state.nodes, whoami())
          select_node = Enum.random(other_nodes)
          send(select_node, {:gossip_request, state.status_of_nodes})
          timer = Emulation.timer(50, :gossip)
          server(state)
        end

      {sender, {:gossip_request, sender_nodes_status}} ->
        if state.inFailedState == true do
          server(state)
        else
          makeHealthy(state, sender)
          receiver_nodes_status = state.status_of_nodes
          updated_status_of_nodes = gossipExchange(sender, whoami(), sender_nodes_status, receiver_nodes_status)
          send(sender, {:gossip_response, updated_status_of_nodes})
          server(%{state | status_of_nodes: updated_status_of_nodes})
        end
      
      {sender, {:gossip_response, sender_nodes_status}} ->
        if state.inFailedState == true do
          server(state)
        else
          makeHealthy(state, sender)
          receiver_nodes_status = state.status_of_nodes
          updated_status_of_nodes = gossipExchange(sender, whoami(), sender_nodes_status, receiver_nodes_status)
          server(%{state | status_of_nodes: updated_status_of_nodes})
        end

      {sender, :state} ->
          send(sender,{whoami(), state})
          server(state) 

    end     
  end
end

#----------------------------------------------------------------------------------------------------------

defmodule Dynamo.Client do
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]

  @moduledoc """
  A client that can be used to connect and send
  requests to the Dynamo servers.
  """
  alias __MODULE__
  @enforce_keys [:client_list]
  defstruct(
    client_list: nil,
    client: nil,
    # Used to store last get operations vector clock to send back as context
    global_vector_clock: nil
  )

  @doc """
  Construct a new Dynamo Client Configuration with list of clients.
  """
  @spec new_client_configuration(list()) :: %Client{client_list: list()}
  def new_client_configuration(client_list) do
    %Client{client_list: client_list,
            global_vector_clock: %{}}
  end

  # 
  @doc """
  Make a node as client
  """
  @spec make_client(%Client{}) :: no_return()
  def make_client(state) do
    client(state)
  end

  @spec client(%Dynamo.Client{}) :: no_return()
  def client(state) do
    receive do

      {sender,
       %Dynamo.ClientPutRequest{
         key: key,
         value: value,
         server_list: server_list
       }} ->

        server = Enum.random(server_list)
        context = 
          if Map.get(state.global_vector_clock, key, nil) != nil do
            Dynamo.VectorClock.clientReconcilation(Map.get(state.global_vector_clock,key))
          else
            nil
          end
        
        send(server, %Dynamo.ServerPutRequest{
          key: key,
          value: value,
          client: whoami(),
          replication: false,
          seq: nil,
          context: context
        })
        client(%{state | client: sender})

      {sender, 
       %Dynamo.ClientGetRequest{
         key: key,
         server_list: server_list
       }} ->
        
        server = Enum.random(server_list)
        send(server, %Dynamo.ServerGetRequest {
          key: key,
          client: whoami(),
          replication: false,
          seq: nil
        })
        client(%{state | client: sender})

      {_sender, {:ok, key}} ->
        send(state.client, {:put, :ok, key})
        IO.inspect("----------------------------------------")
        IO.inspect("Put done for client:#{whoami()} with key:#{key} ")
        IO.inspect("----------------------------------------")
        client(state)

      {_sender, {key, responses}} ->
        IO.inspect("----------------------------------------")
        IO.inspect("Get done for client:#{whoami()} with key:#{key} and value:#{inspect(Enum.at(responses,0))} ")
        IO.inspect("----------------------------------------")
        clockList = Enum.map(responses, fn {_first, second, _third, _fourth, _fifth} -> second end)
        clientResponse = Enum.map(responses, fn {first, second, _third, _fourth, _fifth} -> {first, second} end)
        send(state.client, {:get, key, clientResponse})
        state = %{state | global_vector_clock: Map.put(state.global_vector_clock, key, clockList)}
        client(state)
    end
  end
end

#----------------------------------------------------------------------------------------------------------

defmodule Dynamo.VectorClock do
  
  def updateVectorClock(store, key, value, node, counter, vnode) do
    case Map.get(store, key) do
      {_, vclock, _, _ , _} ->
        if Map.has_key?(vclock, node) do
          oldCounter = Map.get(vclock, node)
          counter =
            if counter > oldCounter do
              counter
            else
              oldCounter
            end
          vclock = Map.put(vclock, node, counter)
          Map.put(store, key, {value, vclock, {node, vnode}, vnode, Emulation.now()})
        else
          vclock = Map.put(vclock, node, counter)
          Map.put(store, key, {value, vclock, {node, vnode}, vnode, Emulation.now()})
        end
      nil ->
        vclock = %{node => counter}
        Map.put(store, key, {value, vclock, {node, vnode}, vnode,  Emulation.now()})
    end
  end

  def equalTo(clock1, clock2) do
      Map.equal?(clock1, clock2)
  end

  def notEqualTo(clock1, clock2) do
      not equalTo(clock1, clock2)
  end

  def lessThan(clock1, clock2) do
      if Enum.sort(Map.keys(clock1)) == Enum.sort(Map.keys(clock2)) do
          merged_clock = Map.merge(clock1, clock2, fn _k, c1, c2 -> c1 < c2 end)
          compare_results = Map.values(merged_clock)
          Enum.all?(compare_results, fn x -> x == true end)
      else
          false
      end
  end

  def lessThanEqualTo(clock1, clock2) do
    lessThan(clock1, clock2) or equalTo(clock1, clock2)
  end

  def greaterThan(clock1, clock2) do
    lessThan(clock2, clock1)
  end

  def greaterThanEqualTo(clock1, clock2) do
    greaterThan(clock1, clock2) or equalTo(clock1, clock2)
  end

  #-----------------------------------------------------------------------

  def syntaticReconcilationMerger(currIndex, clock, result) do
    if currIndex == length(result) do
      {result, false}
    else
      cond do
        lessThanEqualTo(clock, Enum.at(result, currIndex)) ->
          {result, true}

          lessThan(Enum.at(result, currIndex), clock) ->
          result = List.update_at(result, currIndex, fn _ -> clock end)
          {result, true}

        true ->
          syntaticReconcilationMerger(currIndex + 1, clock, result)
      end
    end
  end

  defp syntaticReconcilationHelper(currIndex, clockList, result) do
    if currIndex == length(clockList) do
      result
    else
      {result, succ} = syntaticReconcilationMerger(0, Enum.at(clockList, currIndex), result)

      result =
        if succ do
          result
        else
          result ++ [Enum.at(clockList, currIndex)]
        end

      syntaticReconcilationHelper(currIndex + 1, clockList, result)
    end
  end

  def syntaticReconcilation(clockList) do
    syntaticReconcilationHelper(0, clockList, [])
  end

  #----------------------------------------------------------------------------------------------

  def syntaticReconcilationMergerWithValues(currIndex, clock, result) do
    if currIndex == length(result) do
      {result, false}
    else
      cond do
        lessThanEqualTo(elem(clock, 1), elem(Enum.at(result, currIndex), 1)) ->
          {result, true}

        lessThan(elem(Enum.at(result, currIndex), 1), elem(clock, 1)) ->
          result = List.update_at(result, currIndex, fn _ -> clock end)
          {result, true}

        true ->
          syntaticReconcilationMergerWithValues(currIndex + 1, clock, result)
      end
    end
  end

  defp syntaticReconcilationWithValuesHelper(currIndex, divergedValues, result) do
    if currIndex == length(divergedValues) do
      result
    else
      {result, succ} =
        syntaticReconcilationMergerWithValues(0, Enum.at(divergedValues, currIndex), result)

      result =
        if succ do
          result
        else
          result ++ [Enum.at(divergedValues, currIndex)]
        end

        syntaticReconcilationWithValuesHelper(currIndex + 1, divergedValues, result)
    end
  end

  def syntaticReconcilationWithValues(divergedValues) do
    # IO.inspect(divergedValues)
    syntaticReconcilationWithValuesHelper(0, divergedValues, [])
  end

  #------------------------------------------------------------------------------------------------------

  #Client reconcilation
  defp clientReconcilationHelper([], result) do
    result
  end 

  defp clientReconcilationHelper([clock | rest], result) do
    reconciledClock = Map.merge(clock, result, fn _k, c1, c2 -> max(c1, c2) end)
    clientReconcilationHelper(rest, reconciledClock)
  end
  
  def clientReconcilation(divergedClocks) do
    clientReconcilationHelper(divergedClocks, %{})
  end
  
end