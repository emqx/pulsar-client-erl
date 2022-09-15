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

-define(TEST_SUIT_CLIENT, ?MODULE).
-define(DEFAULT_PULSAR_HOST, "pulsar://pulsar:6650").

%%--------------------------------------------------------------------
%% CT Boilerplate
%%--------------------------------------------------------------------

all() ->
    [ t_code_change_replayq
    , t_code_change_requests
    , t_state_rec_roundtrip
    ].

init_per_suite(Cfg) ->
    {ok, _} = application:ensure_all_started(pulsar),
    Cfg.

end_per_suite(_Args) ->
    ok = application:stop(pulsar),
    ok.

init_per_testcase(t_code_change_replayq, Config) ->
    PulsarHost = os:getenv("PULSAR_HOST", ?DEFAULT_PULSAR_HOST),
    {ok, _ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ProducerOpts = #{ batch_size => 100
                    , strategy => random
                    , callback => {pulsar_SUITE, producer_callback, []}
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
    | Config];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_code_change_replayq, Config) ->
    Producers = ?config(producers, Config),
    pulsar:stop_and_delete_supervised_producers(Producers),
    pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Helper fns
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Testcases
%%--------------------------------------------------------------------

t_code_change_replayq(Config) ->
    ProducerPid = ?config(producer_pid, Config),

    {_StatemState0, State0} = sys:get_state(ProducerPid),

    ?assert(is_map(State0)),
    ?assertMatch(
       #{ replayq := #{ config := _
                      , sizer := _
                      , stats := _
                      }
        },
       State0),
    #{replayq := Q, opts := Opts0} = State0,
    ?assertNot(replayq:is_mem_only(Q)),
    ?assertMatch(#{retention_period := 1_000}, Opts0),
    OriginalSize = map_size(State0),
    %% FIXME: another way to check if open?
    #{w_cur := #{fd := {_, _, #{pid := ReplayQPID}}}} = Q,

    %% check downgrade has no replayq, and replayq is closed.
    ok = sys:suspend(ProducerPid),
    ok = sys:change_code(ProducerPid, pulsar_producer, {down, vsn}, extra),
    %% ok = sys:resume(ProducerPid),
    {_StatemState1, State1} = sys:get_state(ProducerPid),
    ?assertEqual(state, element(1, State1)),
    %% state record has 1 element more (the record name), but also has
    %% one field less (`replayq').
    ?assertEqual(OriginalSize, tuple_size(State1)),
    Opts1 = element(9, State1),
    ?assertNot(maps:is_key(replayq, Opts1)),
    ?assertNot(maps:is_key(retention_period, Opts1)),
    %% replayq should be already closed
    ?assertNot(is_process_alive(ReplayQPID)),

    %% check upgrade has replayq and retention_period.
    %% ok = sys:suspend(ProducerPid),
    ok = sys:change_code(ProducerPid, pulsar_producer, vsn, extra),
    ok = sys:resume(ProducerPid),
    {_StatemState2, State2} = sys:get_state(ProducerPid),
    ?assert(is_map(State2)),
    ?assertEqual(OriginalSize, map_size(State2)),

    ?assertMatch(
       #{ replayq := #{ config := _
                      , sizer := _
                      , stats := _
                      }
        },
       State2),
    #{replayq := Q2, opts := Opts2} = State2,
    ?assertMatch(#{retention_period := infinity}, Opts2),
    %% new replayq is mem-only, since we can't configure it.
    ?assert(replayq:is_mem_only(Q2)),

    ok.

t_code_change_requests(_Config) ->
    %% new format:
    %% {replayq:ack_ref(), [gen_statem:from()], [{timestamp(), [pulsar:message()]}]}
    SequenceId = 1,
    AckRef = {1,1},
    Froms = [{self(), erlang:make_ref()}],
    Timestamp0 = erlang:system_time(millisecond),
    Messages0 = [#{key => <<"k1">>, value => <<"v1">>},
                 #{key => <<"k2">>, value => <<"v2">>}],
    Timestamp1 = erlang:system_time(millisecond),
    Messages1 = [#{key => <<"k3">>, value => <<"v3">>}],
    Request = {AckRef, Froms, [{Timestamp0, Messages0}, {Timestamp1, Messages1}]},
    Requests0 = #{SequenceId => Request},

    Requests1 = pulsar_producer:code_change_requests({down, vsn}, Requests0),
    %% old format
    ExpectedBatchLen = length(Messages0 ++ Messages1),
    ?assertEqual(#{SequenceId => {SequenceId, ExpectedBatchLen}}, Requests1),

    Requests2 = pulsar_producer:code_change_requests(vsn, Requests1),
    %% new format again, but we don't have timestamp nor "from"
    %% information, so we keep that information as-is.
    ?assertEqual(#{SequenceId => {SequenceId, ExpectedBatchLen}}, Requests2),

    ok.

t_state_rec_roundtrip(_Config) ->
    StateMap =
        maps:from_list([{K, erlang:make_ref()}
                        || K <- [ batch_size
                                , broker_server
                                , callback
                                , last_bin
                                , opts
                                , partitiontopic
                                , producer_id
                                , producer_name
                                , request_id
                                , requests
                                , sequence_id
                                , sock
                                ]]),
    ?assertEqual(StateMap,
                 pulsar_producer:from_old_state_record(
                   pulsar_producer:to_old_state_record(StateMap))).
