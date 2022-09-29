%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(pulsar_relup_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("src/pulsar_producer_internal.hrl").

-define(TEST_SUIT_CLIENT, ?MODULE).
-define(DEFAULT_PULSAR_HOST, "pulsar://pulsar:6650").

%%--------------------------------------------------------------------
%% CT Boilerplate
%%--------------------------------------------------------------------

all() ->
    [ t_producer_pids
    , t_suspend_resume_producers
    , t_collect_and_downgrade_send_requests
    , t_change_producers_up_detail
    , t_change_producers_down_detail
    ].

init_per_suite(Config) ->
    PulsarHost = os:getenv("PULSAR_HOST", ?DEFAULT_PULSAR_HOST),
    {ok, _} = application:ensure_all_started(pulsar),
    {ok, _ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ProducerOpts = #{ batch_size => 100
                    , strategy => random
                    , replayq_dir => "/tmp/replayq_" ++ ?MODULE_STRING
                    , replayq_seg_bytes => 20 * 1024 * 1024
                    , replayq_offload_mode => false
                    , replayq_max_total_bytes => 1_000_000_000
                    , retention_period => 1_000
                    },
    {ok, Producers} = pulsar:ensure_supervised_producers( ?TEST_SUIT_CLIENT
                                                         , <<"my-topic">>
                                                         , ProducerOpts
                                                         ),
    [ {producers, Producers}
    | Config].

end_per_suite(Config) ->
    Producers = ?config(producers, Config),
    ok = pulsar:stop_and_delete_supervised_producers(Producers),
    ok = application:stop(pulsar),
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    lists:foreach(fun sys:resume/1, pulsar_relup:producer_pids()),
    ok.

%%--------------------------------------------------------------------
%% Helper fns
%%--------------------------------------------------------------------

get_current_function(PID) ->
    {current_function, CurrentFn} = process_info(PID, current_function),
    CurrentFn.

%%--------------------------------------------------------------------
%% Testcases
%%--------------------------------------------------------------------

is_before_replayq_test_() ->
    [ ?_assert(pulsar_relup:is_before_replayq({0,5,0}))
    , ?_assert(pulsar_relup:is_before_replayq({0,6,4}))
    , ?_assert(not pulsar_relup:is_before_replayq({0,7,0}))
    , ?_assert(not pulsar_relup:is_before_replayq({0,7,1}))
    , ?_assert(not pulsar_relup:is_before_replayq({0,7,0,1}))
    ].

t_producer_pids(_Config) ->
    ProducerPIDs = sets:from_list(pulsar_relup:producer_pids(), [{version, 2}]),
    ExpectedProducerPIDs =
        sets:from_list(
          [P || P <- processes(),
                case proc_lib:initial_call(P) of
                    {pulsar_producer, init, _} -> true;
                    _ -> false
                end],
         [{version, 2}]),
    ?assertEqual(1, sets:size(ExpectedProducerPIDs)),
    ?assertEqual(ExpectedProducerPIDs, ProducerPIDs),
    ok.

t_suspend_resume_producers(_Config) ->
    [ProducerPID] = pulsar_relup:producer_pids(),
    ?assertMatch({gen_statem, loop_receive, _}, get_current_function(ProducerPID)),
    ?assertEqual(ok, pulsar_relup:suspend_producers()),
    ?assertMatch({sys, suspend_loop, _}, get_current_function(ProducerPID)),
    ?assertEqual(ok, pulsar_relup:resume_producers()),
    ?assertMatch({gen_statem, loop_receive, _}, get_current_function(ProducerPID)),
    ok.

t_change_producers_up(_Config) ->
    [ProducerPID] = pulsar_relup:producer_pids(),
    pulsar_relup:suspend_producers(),
    %% precondition: we have a record state (pre-replayq).  assuming
    %% `down' is correct...
    FromVsn0 = {0, 7, 0},
    ToVsn0 = {0, 6, 0},
    pulsar_relup:change_producers_down(FromVsn0, ToVsn0),
    ?assert(is_tuple(sys:get_state(ProducerPID))),

    %% if `ToVsn' (somehow) still uses a record state, don't touch the
    %% state.
    FromVsn1 = {0, 6, 0},
    ToVsn1 = {0, 6, 4},
    ok = pulsar_relup:change_producers_up(ToVsn1, FromVsn1),
    ?assert(is_tuple(sys:get_state(ProducerPID))),

    %% change the record to a map state.
    FromVsn2 = {0, 6, 4},
    ToVsn2 = {0, 7, 0},
    ok = pulsar_relup:change_producers_up(ToVsn2, FromVsn2),
    ?assert(is_map(sys:get_state(ProducerPID))),

    ok.

t_collect_and_downgrade_send_requests(_Config) ->
    {ok, FakeProducer} = pulsar_fake_producer:start_link(),
    ok = sys:suspend(FakeProducer),
    ProducerPIDs = [FakeProducer],

    %% put some sync and async requests in the mailbox
    Messages = [#{key => <<"k">>, value => <<"v">>}],
    ok = pulsar_producer:send(FakeProducer, Messages),
    try
        pulsar_producer:send_sync(FakeProducer, Messages, 1)
    catch
        error:timeout -> ok
    end,
    State0 = sys:get_state(FakeProducer),

    ?assertMatch(
       {messages, [ ?SEND_REQ(undefined, Messages)
                  , ?SEND_REQ({_, _}, Messages)
                  ]},
       process_info(FakeProducer, messages)),

    %% if (somehow) is not before replayq, does nothing
    FromVsn0 = {0, 7, 1},
    ToVsn0 = {0, 7, 0},
    ok = pulsar_relup:post_producer_code_load(ProducerPIDs, FromVsn0, ToVsn0),
    ?assertMatch(
       {messages, [ ?SEND_REQ(undefined, Messages)
                  , ?SEND_REQ({_, _}, Messages)
                  ]},
       process_info(FakeProducer, messages)),

    %% otherwise, we should downgrade to the old call/cast format
    FromVsn1 = {0, 7, 0},
    ToVsn1 = {0, 6, 4},

    ok = pulsar_relup:post_producer_code_load(ProducerPIDs, FromVsn1, ToVsn1),

    ?assertMatch(
       {messages, [ {'$gen_cast', {send, Messages}}
                  , {'$gen_call', {_, _}, {send, Messages}}
                  ]},
       process_info(FakeProducer, messages)),

    State1 = sys:get_state(FakeProducer),
    ?assertEqual(State0, State1),

    ok.

t_change_producers_up_detail(_Config) ->
    {ok, FakeProducer} = pulsar_fake_producer:start_link(),
    ok = sys:suspend(FakeProducer),
    ProducerPIDs = [FakeProducer],
    {State0, Data0} = sys:get_state(FakeProducer),

    ToVsn = {0, 7, 0},
    FromVsn = {0, 6, 4},
    ok = pulsar_relup:change_producers_up(ProducerPIDs, ToVsn, FromVsn),

    receive
        {code_change, Direction, State, Data, Extra} ->
            ?assertEqual(ToVsn, Direction),
            ?assertEqual(State0, State),
            ?assertEqual(Data0, Data),
            ?assertEqual(#{from_version => FromVsn, to_version => ToVsn}, Extra),
            ok
    after
        100 ->
            ct:fail("did not call code_change")
    end,

    ok.

t_change_producers_down_detail(_Config) ->
    {ok, FakeProducer} = pulsar_fake_producer:start_link(),
    ok = sys:suspend(FakeProducer),
    ProducerPIDs = [FakeProducer],
    {State0, Data0} = sys:get_state(FakeProducer),

    ToVsn = {0, 6, 4},
    FromVsn = {0, 7, 0},
    ok = pulsar_relup:change_producers_down(ProducerPIDs, FromVsn, ToVsn),

    receive
        {code_change, Direction, State, Data, Extra} ->
            ?assertEqual({down, ToVsn}, Direction),
            ?assertEqual(State0, State),
            ?assertEqual(Data0, Data),
            ?assertEqual(#{from_version => FromVsn, to_version => ToVsn}, Extra),
            ok
    after
        100 ->
            ct:fail("did not call code_change")
    end,

    ok.
