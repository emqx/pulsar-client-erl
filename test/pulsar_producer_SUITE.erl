%%%%--------------------------------------------------------------------
%%%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%%%
%%%% Licensed under the Apache License, Version 2.0 (the "License");
%%%% you may not use this file except in compliance with the License.
%%%% You may obtain a copy of the License at
%%%%
%%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%%
%%%% Unless required by applicable law or agreed to in writing, software
%%%% distributed under the License is distributed on an "AS IS" BASIS,
%%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%% See the License for the specific language governing permissions and
%%%% limitations under the License.
%%%%--------------------------------------------------------------------
-module(pulsar_producer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(TEST_SUIT_CLIENT, ?MODULE).
-define(DEFAULT_PULSAR_HOST, "pulsar://toxiproxy:6650").

%%--------------------------------------------------------------------
%% CT Boilerplate
%%--------------------------------------------------------------------

all() ->
    [ t_queue_item_marshaller
    , t_port_exit
    ].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(pulsar),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(pulsar),
    ok.

init_per_testcase(TestCase, Config) when
    TestCase =:= t_port_exit
->
    PulsarHost = os:getenv("PULSAR_HOST", ?DEFAULT_PULSAR_HOST),
    {ok, _ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    TestPID = self(),
    Counter = counters:new(1, [atomics]),
    Callback =
        fun(Response) ->
          counters:add(Counter, 1, 1),
          erlang:send(TestPID, Response),
          ok
        end,
    ProducerOpts = #{ batch_size => 100
                    , strategy => random
                    , callback => Callback
                    , replayq_dir => "/tmp/replayq1"
                    , replayq_seg_bytes => 20 * 1024 * 1024
                    , replayq_offload_mode => false
                    , replayq_max_total_bytes => 1_000_000_000
                    , retention_period => 1_000
                    },
    {ok, Producers} = pulsar:ensure_supervised_producers( ?TEST_SUIT_CLIENT
                                                         , <<"my-topic">>
                                                         , ProducerOpts
                                                         ),
    Batch = [#{key => <<"k">>, value => <<"v">>}],
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers, Batch),
    [ {pulsar_host, PulsarHost}
    , {producer_pid, ProducerPid}
    , {producers, Producers}
    , {async_counter, Counter}
    | Config];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, Config) when
    TestCase =:= t_port_exit
->
    Producers = ?config(producers, Config),
    pulsar:stop_and_delete_supervised_producers(Producers),
    pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Helper fns
%%--------------------------------------------------------------------

drain_messages(ExpectedN, Acc) when ExpectedN =< 0 ->
    lists:reverse(Acc);
drain_messages(ExpectedN, Acc) ->
    receive
        Msg ->
            drain_messages(ExpectedN - 1, [Msg | Acc])
    after
        60_000 ->
            ct:fail("expected messages have not arrived;~n  so far: ~100p", [Acc])
    end.

%%--------------------------------------------------------------------
%% Testcases
%%--------------------------------------------------------------------

t_queue_item_marshaller(_Config) ->
    Pid = spawn_link(
             fun() ->
               receive
                 die -> ok
               end
             end),
    Messages = [#{key => <<"k">>, value => <<"v">>}],
    Ref = monitor(process, Pid, [{alias, reply_demonitor}]),
    From = {Pid, Ref},
    QueueItem0 = pulsar_producer:make_queue_item(From, Messages),
    QueueItemBin = pulsar_producer:queue_item_marshaller(QueueItem0),
    ?assert(is_binary(QueueItemBin)),
    QueueItem1 = pulsar_producer:queue_item_marshaller(QueueItemBin),
    ?assertNot(is_binary(QueueItem1)),
    ?assertEqual(QueueItem0, QueueItem1),
    %% if the pid in `From' is dead, especially if it's from a
    %% previous incarnation of the Erlang VM, we should convert it to
    %% an `undefined' atom.
    Pid ! die,
    receive
        {'DOWN', Ref, process, Pid, _} ->
            ok
    after
        100 ->
            ct:fail("pid should have died")
    end,
    QueueItem2 = pulsar_producer:queue_item_marshaller(QueueItemBin),
    ?assertNot(is_binary(QueueItem2)),
    ?assertNotEqual(QueueItem0, QueueItem2),
    ?assertMatch({undefined, _, _}, QueueItem2),
    ok.

t_port_exit(Config) ->
    ProducerPid = ?config(producer_pid, Config),
    pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000),
    {_, #{sock := Sock}} = sys:get_state(ProducerPid),
    true = is_port(Sock),
    ?check_trace(
       #{timetrap => 2_000},
       begin
           {_, {ok, _}} =
               ?wait_async_action(
                  exit(Sock, die),
                  #{?snk_kind := "pulsar_socket_close"}
                 ),
           ok
       end,
       fun(Trace) ->
           ?assertMatch([#{reason := die}], ?of_kind("pulsar_socket_close", Trace)),
           ok
       end
      ),
    ok.
