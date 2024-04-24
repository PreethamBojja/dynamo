defmodule Dynamo.ClientPutRequest do
    @moduledoc """
    Put Request for Client
    """
    alias __MODULE__
    @enforce_keys [:key, :value, :server_list]
    defstruct(
        key: nil,
        value: nil,
        server_list: nil,
        seq: nil,
        context: nil
    )

    @doc """
    Create a new Client Put Request.
    """
    @spec new(non_neg_integer(), non_neg_integer(), list(), non_neg_integer()) ::
            %ClientPutRequest{
                key: non_neg_integer(),
                value: non_neg_integer(),
                server_list: list()
            }
    def new(key, value, server_list, seq) do
        %ClientPutRequest{
        key: key,
        value: value,
        server_list: server_list,
        seq: seq,
        context: nil
        }
    end
end
  
defmodule Dynamo.ClientGetRequest do
    @moduledoc """
    Get Request for Client
    """
    alias __MODULE__
    @enforce_keys [:key, :server_list]
    defstruct(
        key: nil,
        server_list: nil,
        seq: nil,
        context: nil
    )

    @doc """
    Create a new Client Get Request.
    """
    @spec new(non_neg_integer(), list(), non_neg_integer()) ::
            %ClientGetRequest{
                key: non_neg_integer(),
                server_list: list()
            }
    def new(key, server_list, seq) do
        %ClientGetRequest{
        key: key,
        server_list: server_list,
        seq: seq,
        context: nil
        }
    end
end

defmodule Dynamo.ServerPutRequest do
    @moduledoc """
    Put Request for Client
    """
    alias __MODULE__
    @enforce_keys [:key, :value, :client, :seq]
    defstruct(
        key: nil,
        value: nil,
        client: nil,
        replication: nil,
        seq: nil,
        context: nil
    )

    @doc """
    Create a new Client Put Request.
    """
    @spec new(non_neg_integer(), non_neg_integer(), atom(), non_neg_integer()) ::
            %ServerPutRequest{
                key: non_neg_integer(),
                value: non_neg_integer(),
                client: atom()
            }
    def new(key, value, client, seq) do
        %ServerPutRequest{
        key: key,
        value: value,
        client: client,
        seq: seq,
        replication: false,
        context: nil
        }
    end
end
  
defmodule Dynamo.ServerGetRequest do
    @moduledoc """
    Get Request for Client
    """
    alias __MODULE__
    @enforce_keys [:key, :client, :seq]
    defstruct(
        key: nil,
        client: nil,
        replication: nil,
        seq: nil,
        context: nil
    )

    @doc """
    Create a new Client Get Request.
    """
    @spec new(non_neg_integer(), atom(), non_neg_integer()) ::
            %ServerGetRequest{
                key: non_neg_integer(),
                client: atom()
            }
    def new(key, client, seq) do
        %ServerGetRequest{
        key: key,
        client: client,
        replication: false,
        seq: seq,
        context: nil
        }
    end
end

defmodule Dynamo.ServerPutResponse do
    @moduledoc """
    Put Request for Client
    """
    alias __MODULE__
    @enforce_keys [:key, :status, :client, :seq]
    defstruct(
        key: nil,
        client: nil,
        status: nil,
        seq: nil,
        context: nil
    )

    @doc """
    Create a new Client Put Request.
    """
    @spec new(non_neg_integer(), atom(), atom(), non_neg_integer()) ::
            %ServerPutResponse{
                key: non_neg_integer(),
                status: atom(),
                client: atom(),
                seq: non_neg_integer()
            }
    def new(key, client, status, seq) do
        %ServerPutResponse{
        key: key,
        client: client,
        status: status,
        seq: seq,
        context: nil
        }
    end
end

defmodule Dynamo.ServerGetResponse do
    @moduledoc """
    Put Request for Client
    """
    alias __MODULE__
    @enforce_keys [:value, :status, :client, :seq]
    defstruct(
        key: nil,
        value: nil,
        client: nil,
        status: nil,
        seq: nil,
        context: nil
    )

    @doc """
    Create a new Client Put Request.
    """
    @spec new(any(), non_neg_integer(), atom(), atom(), non_neg_integer()) ::
            %ServerGetResponse{
                key: any(),
                value: non_neg_integer(),
                status: atom(),
                client: atom(),
                seq: non_neg_integer()
            }
    def new(key, value, client, status, seq) do
        %ServerGetResponse{
        key: key,
        value: value,
        client: client,
        status: status,
        seq: seq,
        context: nil
        }
    end
end