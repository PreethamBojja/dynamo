defmodule VectorClockTest do
    require Logger
    use ExUnit.Case
    require Emulation
    require Logger
    require Dynamo.VectorClock
  
    test "Client Reconcilation test" do
        Emulation.init()
        clockA = {1, %{"a" => 8, "b" => 9}, {"a", 3}}
        clockB = {2, %{"a" => 5, "b" => 6}, {"a", 3}}
        clockC = {3, %{"c" => 5, "d" => 6}, {"a", 3}}
        clientReconciled1 = [{1, %{"a" => 8, "b" => 9}, {"a", 3}}, {3, %{"c" => 5, "d" => 6}, {"a", 3}}]
        clientReconciled2 = [%{"a" => 8, "b" => 9}, %{"c" => 5, "d" => 6}]
        assert clientReconciled1 == Dynamo.VectorClock.syntaticReconcilationWithValues([clockA] ++ [clockB] ++ [clockC])
        assert clientReconciled2 == Dynamo.VectorClock.syntaticReconcilation([elem(clockA,1)] ++ [elem(clockB,1)] ++ [elem(clockC,1)])

    after
        Emulation.terminate()
    end

    test "Syntatic Reconcilation test" do
        Emulation.init()
        clockA = %{"a" => 8, "b" => 9}
        clockB = %{"a" => 5, "b" => 6}
        clockC = %{"c" => 5, "d" => 6}
        reconciledClock = %{"a" => 8, "b" => 9, "c" => 5, "d" => 6}
        assert reconciledClock == (Dynamo.VectorClock.clientReconcilation([clockA] ++ [clockB] ++ [clockC]))
    after
        Emulation.terminate()
    end

end
  