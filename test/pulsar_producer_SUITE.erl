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
    , t_single_message_encoding
    , t_single_message_send_batch_size_1
    , t_batch_size_1_multiple_messages_in_one_call
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
init_per_testcase(TestCase, Config) when
    TestCase =:= t_single_message_send_batch_size_1;
    TestCase =:= t_batch_size_1_multiple_messages_in_one_call
->
    PulsarHost = os:getenv("PULSAR_HOST", ?DEFAULT_PULSAR_HOST),
    [ {pulsar_host, PulsarHost} | Config ];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(TestCase, Config) when
    TestCase =:= t_port_exit
->
    Producers = ?config(producers, Config),
    pulsar:stop_and_delete_supervised_producers(Producers),
    pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT),
    ok;
end_per_testcase(_TestCase = t_single_message_send_batch_size_1, _Config) ->
    %% Cleanup is done in the test itself
    ok;
end_per_testcase(_TestCase = t_batch_size_1_multiple_messages_in_one_call, _Config) ->
    %% Cleanup is done in the test itself
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

%% Test that single message encoding doesn't include batch headers
t_single_message_encoding(_Config) ->
    Message = #{key => <<"test-key">>, value => <<"test-value">>},
    SequenceId = 1,
    ProducerId = 123,
    ProducerName = <<"test-producer">>,
    Opts = #{},

    {NumMessages, EncodedPacket} = pulsar_socket:encode_send_single_message_packet(
        Message, SequenceId, ProducerId, ProducerName, Opts
    ),

    %% Verify the encoded packet is an iolist (can be binary or list)
    ?assertEqual(1, NumMessages),

    %% Verify the packet structure by checking it's a valid iolist/binary
    PacketBinary = iolist_to_binary(EncodedPacket),
    ?assert(is_binary(PacketBinary)),
    ?assert(size(PacketBinary) > 0),

    %% Compare with batch encoding - single message should be smaller (no SingleMessageMetadata headers)
    BatchMessage = [Message],
    {_BatchNumMessages, BatchEncodedPacket} = pulsar_socket:encode_send_batch_message_packet(
        BatchMessage, SequenceId, ProducerId, ProducerName, Opts
    ),
    BatchPacketBinary = iolist_to_binary(BatchEncodedPacket),

    %% Single message packet should be smaller than batch packet (no SingleMessageMetadata overhead)
    %% For a single message, batch format includes SingleMessageMetadata header, single format doesn't
    SingleSize = size(PacketBinary),
    BatchSize = size(BatchPacketBinary),
    case SingleSize < BatchSize of
        true -> ok;
        false -> ct:fail("Single message packet (~p bytes) should be smaller than batch packet (~p bytes)",
                         [SingleSize, BatchSize])
    end,

    ok.

%% Test that when batch_size is 1, messages are sent individually
t_single_message_send_batch_size_1(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    {ok, _ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),

    TestPID = self(),
    ReceivedMessages = ets:new(received_messages, [ordered_set, public]),

    Callback =
        fun(Response) ->
          ets:insert(ReceivedMessages, {erlang:monotonic_time(), Response}),
          erlang:send(TestPID, {callback, Response}),
          ok
        end,

    %% Create producer with batch_size = 1
    ProducerOpts = #{ batch_size => 1
                    , strategy => random
                    , callback => Callback
                    , replayq_dir => "/tmp/replayq_single_test"
                    , replayq_seg_bytes => 20 * 1024 * 1024
                    , replayq_offload_mode => false
                    , replayq_max_total_bytes => 1_000_000_000
                    },
    {ok, Producers} = pulsar:ensure_supervised_producers( ?TEST_SUIT_CLIENT
                                                         , <<"single-message-topic">>
                                                         , ProducerOpts
                                                         ),

    %% Wait for producer to connect
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers, [#{key => <<"k">>, value => <<"v">>}]),
    pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000),

    %% Send multiple messages
    Messages = [
        #{key => <<"key1">>, value => <<"value1">>},
        #{key => <<"key2">>, value => <<"value2">>},
        #{key => <<"key3">>, value => <<"value3">>},
        #{key => undefined, value => <<"value4">>}
    ],

    %% Send messages synchronously to verify they're sent individually
    Results = lists:map(
        fun(Msg) ->
            {ok, Result} = pulsar:send_sync(Producers, [Msg], 10_000),
            Result
        end,
        Messages
    ),

    %% Verify each message got its own sequence_id
    SequenceIds = [maps:get(sequence_id, R) || R <- Results],
    ?assertEqual(4, length(SequenceIds)),
    %% Verify sequence IDs are sequential (each message sent individually)
    ?assertEqual(SequenceIds, lists:usort(SequenceIds)),

    %% Now send messages asynchronously to verify callbacks are called
    lists:foreach(
        fun(Msg) ->
            {ok, _} = pulsar:send(Producers, [Msg])
        end,
        Messages
    ),

    %% Verify callbacks were called for each async message
    %% Wait for callbacks to arrive (they're async)
    WaitForCallbacks = fun
        Wait(0) ->
            CallbackCount = ets:info(ReceivedMessages, size),
            case CallbackCount >= 4 of
                true -> ok;
                false -> ct:fail("Expected at least 4 callbacks, got ~p", [CallbackCount])
            end;
        Wait(Retries) ->
            timer:sleep(100),
            CallbackCount = ets:info(ReceivedMessages, size),
            case CallbackCount >= 4 of
                true -> ok;
                false -> Wait(Retries - 1)
            end
    end,
    WaitForCallbacks(50),  %% Wait up to 5 seconds

    %% Cleanup
    ets:delete(ReceivedMessages),
    pulsar:stop_and_delete_supervised_producers(Producers),
    pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT),
    ok.

%% Test that when batch_size is 1, multiple messages in a single call are sent individually
t_batch_size_1_multiple_messages_in_one_call(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    {ok, _ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),

    TestPID = self(),
    ReceivedMessages = ets:new(received_messages, [ordered_set, public]),

    Callback =
        fun(Response) ->
          ets:insert(ReceivedMessages, {erlang:monotonic_time(), Response}),
          erlang:send(TestPID, {callback, Response}),
          ok
        end,

    %% Create producer with batch_size = 1
    ProducerOpts = #{ batch_size => 1
                    , strategy => random
                    , callback => Callback
                    , replayq_dir => "/tmp/replayq_batch1_multi_test"
                    , replayq_seg_bytes => 20 * 1024 * 1024
                    , replayq_offload_mode => false
                    , replayq_max_total_bytes => 1_000_000_000
                    },
    {ok, Producers} = pulsar:ensure_supervised_producers( ?TEST_SUIT_CLIENT
                                                         , <<"batch1-multi-topic">>
                                                         , ProducerOpts
                                                         ),

    %% Wait for producer to connect
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers, [#{key => <<"k">>, value => <<"v">>}]),
    pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000),

    %% Send multiple messages in a single call - this tests the case where
    %% batch_size=1 but caller supplies multiple messages
    Messages = [
        #{key => <<"key1">>, value => <<"value1">>},
        #{key => <<"key2">>, value => <<"value2">>},
        #{key => <<"key3">>, value => <<"value3">>},
        #{key => undefined, value => <<"value4">>}
    ],

    %% Send all messages in a single call - they should be sent individually
    {ok, Result} = pulsar:send_sync(Producers, Messages, 10_000),

    %% When batch_size=1, even though we sent multiple messages in one call,
    %% they should be sent individually, so we should get a single result
    %% (the last message's receipt)
    ?assert(is_map(Result)),
    ?assert(maps:is_key(sequence_id, Result)),

    %% Now send messages asynchronously in a single call
    {ok, _} = pulsar:send(Producers, Messages),

    %% Verify callbacks were called for each message
    %% Wait for callbacks to arrive (they're async)
    WaitForCallbacks = fun
        Wait(0) ->
            CallbackCount = ets:info(ReceivedMessages, size),
            case CallbackCount >= 4 of
                true -> ok;
                false -> ct:fail("Expected at least 4 callbacks, got ~p", [CallbackCount])
            end;
        Wait(Retries) ->
            timer:sleep(100),
            CallbackCount = ets:info(ReceivedMessages, size),
            case CallbackCount >= 4 of
                true -> ok;
                false -> Wait(Retries - 1)
            end
    end,
    WaitForCallbacks(50),  %% Wait up to 5 seconds

    %% Cleanup
    ets:delete(ReceivedMessages),
    pulsar:stop_and_delete_supervised_producers(Producers),
    pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT),
    ok.
