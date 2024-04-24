defmodule Dynamo do
  # Importing modules
  import Emulation, only: [send: 2, whoami: 0]

  import Kernel,
    except: [spawn: 3, spawn: 1, spawn_link: 1, spawn_link: 3, send: 2]
  import Logger
  
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
    responses: nil
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
    IO.inspect(Ring.add_node(ring, :a, 3))
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
      responses: %{}
    }
  end

  # 
  @doc """
  Make a node as server
  """
  @spec make_server(%Dynamo{}) :: no_return()
  def make_server(state) do
    Ring.add_node(state.ring, whoami(), state.vnodes)
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
  
  def circular_traversal(list, start_index, count) do
    length = length(list)
    start_index = rem(start_index, length)
    traverse_circular(list, start_index, count, MapSet.new(), [])
  end
  
  defp traverse_circular(_list, _index, 0, distinct_items, _preference_list) do
    MapSet.to_list(distinct_items)
  end
  
  defp traverse_circular(list, index, count, distinct_items, preference_list) do
    element = Enum.at(list, index)
  
    case MapSet.member?(distinct_items, element) do
      true ->
        next_index = rem(index + 1, length(list))
        traverse_circular(list, next_index, count, distinct_items, preference_list)
      false ->
        preference_list = preference_list ++ [index]
        next_index = rem(index + 1, length(list))
        distinct_items = MapSet.put(distinct_items, element)
        traverse_circular(list, next_index, count - 1, distinct_items, preference_list)
    end
  end
  

  def get_preference_list(state, key) do
    ring = state.ring
    {:ok, nodes} = Ring.get_nodes_with_replicas(ring)
    {hashList, nodeList} = Enum.unzip(Node.expand(nodes))
    start_index = find_start_index(hashList, Hash.of(key))
    {circular_traversal(nodeList, start_index, state.replication_factor), start_index}
  end

  # Broadcast a message to all nodes in the configuration (excluding the sender.)
  @spec bcast(list(), any()) :: list()
  defp bcast(node_list, msg) do
    node_list
    |> Enum.map(fn node -> if node != whoami() do send(node, msg) end end)
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
        context: context
       }} ->

        # IO.puts("Put request received at #{whoami()}")
        
        {preference_list, start_index} = get_preference_list(state,key)
        if not replication do 
          if(Enum.at(preference_list,0) == whoami()) do
            IO.inspect(preference_list)
            if context != nil do 
              state = %{state | kv_store: Map.put(state.kv_store,key,{value, context},{whoami(), start_index})}
            end
            state = %{state | kv_store: Dynamo.VectorClock.updateVectorClock(state.kv_store, key, value, whoami(), state.seq + 1, start_index)}   
            bcast(preference_list, %Dynamo.ServerPutRequest{
              key: key,
              value: value,
              client: client,
              replication: true,
              seq: state.seq + 1,
              context: Map.get(state.kv_store,key)
             })
            # IO.puts("Got into co-ordinator and started broadcast")
            server(%{ state | seq: state.seq + 1})
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
          state = %{state | kv_store: Map.put(state.kv_store, key, context)}
          send(sender, %Dynamo.ServerPutResponse{
            key: key,
            status: :ok,
            client: client,
            seq: seq
            }) 
          server(%{state | seq: seq})
        end
      
      {sender,
       %Dynamo.ServerGetRequest{
         key: key,
         client: client,
         replication: replication,
         seq: seq,
         context: context
       }} ->
        
        # IO.puts("Get request received at #{whoami()}")
        
        {preference_list, start_index} = get_preference_list(state,key)
        if not replication do 
          if(Enum.at(preference_list,0) == whoami()) do
            IO.inspect(preference_list)
            state = %{state | responses: Map.put(state.responses, state.seq + 1, [Map.get(state.kv_store,key)])}
            bcast(preference_list, %Dynamo.ServerGetRequest{
              key: key,
              client: client,
              replication: true,
              seq: state.seq + 1
             })
            #  IO.puts("Got into co-ordinator and started broadcast")
            server(%{ state | seq: state.seq + 1})
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
          # {value , vclock} = Map.get(state.kv_store, key)
          send(sender, %Dynamo.ServerGetResponse{
            key: key,
            value:  Map.get(state.kv_store, key),
            status: :ok,
            client: client,
            seq: seq
            }) 
          server(%{ state | seq: seq})
        end

      {sender,
        %Dynamo.ServerPutResponse{
          key: key,
          status: status,
          client: client,
          seq: seq,
          context: context
        }} ->
          # IO.puts("Got into server put response at #{whoami()}")  
          if status == :ok do
            state = if Map.get(state.response_count, seq, nil) == nil do
                      %{state | response_count: Map.put(state.response_count, seq, 1)}
                    else
                      count = Map.get(state.response_count, seq)
                      %{state | response_count: Map.put(state.response_count, seq, count + 1)} 
                    end 
            if Map.get(state.response_count, seq, nil) == state.write_quorum - 1 do
              send(client, {:ok, key})
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
        # IO.puts("Got into server get response at #{whoami()}")  
        if status == :ok do
          responses = Map.get(state.responses, seq, []) ++ [value]
          state = %{state | responses: Map.put(state.responses, seq, responses)}
          state = if Map.get(state.response_count, seq, nil) == nil do
                    %{state | response_count: Map.put(state.response_count, seq, 1)}
                  else
                    count = Map.get(state.response_count, seq)
                    %{state | response_count: Map.put(state.response_count, seq, count + 1)} 
                  end 
          
          state = 
            if Map.get(state.response_count, seq, nil) == state.read_quorum - 1 do
              reconciledResponses = Dynamo.VectorClock.syntaticReconcilationWithValues(Map.get(state.responses, seq))
              state = %{state | responses: Map.put(state.responses, seq, reconciledResponses)}
              send(client, {key, Map.get(state.responses, seq)})
              state
            end
          server(state)  
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
        IO.puts("Sent to server #{server} from #{whoami()}")
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
          context: nil
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

      {sender, {:ok, key}} ->
        IO.inspect(state.client)
        send(state.client, {:put, :ok, key})
        client(state)

      {sender, {key, responses}} ->
        clockList = Enum.map(responses, fn {_first, second, _third} -> second end)
        clientResponse = Enum.map(responses, fn {first, second, _third} -> {first, second} end)
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
      {_, vclock, _} = {value, vclock, vnodeSig} ->
        if Map.has_key?(vclock, node) do
          oldCounter = Map.get(vclock, node)
          counter =
            if counter > oldCounter do
              counter
            else
              oldCounter
            end
          vclock = Map.put(vclock, node, counter)
          Map.put(store, key, {value, vclock, {node, vnode}})
        else
          vclock = Map.put(vclock, node, counter)
          Map.put(store, key, {value, vclock, {node, vnode}})
        end
      nil ->
        vclock = %{node => counter}
        Map.put(store, key, {value, vclock, {node, vnode}})
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