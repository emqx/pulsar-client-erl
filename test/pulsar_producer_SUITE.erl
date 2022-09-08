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

    ?assertEqual(state, element(1, State0)),
    % replayq in opts
    Opts0 = element(9, State0),
    ?assertMatch(
       #{ replayq := #{ config := _
                      , sizer := _
                      , stats := _
                      }
        , retention_period := 1_000
        },
       Opts0),
    #{replayq := Q} = Opts0,
    ?assertNot(replayq:is_mem_only(Q)),
    OriginalSize = tuple_size(State0),
    %% FIXME: another way to check if open?
    #{w_cur := #{fd := {_, _, #{pid := ReplayQPID}}}} = Q,

    %% check downgrade has no replayq, and replayq is closed.
    ok = sys:suspend(ProducerPid),
    ok = sys:change_code(ProducerPid, pulsar_producer, {down, vsn}, extra),
    ok = sys:resume(ProducerPid),
    {_StatemState1, State1} = sys:get_state(ProducerPid),
    ?assertEqual(state, element(1, State1)),
    ?assertEqual(OriginalSize, tuple_size(State1)),
    Opts1 = element(9, State1),
    ?assertNot(maps:is_key(replayq, Opts1)),
    ?assertNot(maps:is_key(retention_period, Opts1)),
    %% replayq should be already closed
    ?assertNot(is_process_alive(ReplayQPID)),

    %% check upgrade has replayq and retention_period.
    ok = sys:suspend(ProducerPid),
    ok = sys:change_code(ProducerPid, pulsar_producer, vsn, extra),
    ok = sys:resume(ProducerPid),
    {_StatemState2, State2} = sys:get_state(ProducerPid),
    ?assertEqual(state, element(1, State2)),
    ?assertEqual(OriginalSize, tuple_size(State2)),
    Opts2 = element(9, State2),

    ?assertMatch(
       #{ replayq := #{ config := _
                      , sizer := _
                      , stats := _
                      }
        , retention_period := infinity
        },
       Opts2),
    #{replayq := Q2} = Opts2,
    %% new replayq is mem-only, since we can't configure it.
    ?assert(replayq:is_mem_only(Q2)),

    ok.
