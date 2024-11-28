%%%%--------------------------------------------------------------------
%%%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(pulsar_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(TEST_SUIT_CLIENT, client_erl_suit).
-define(BATCH_SIZE , 100).
-define(PULSAR_HOST, "pulsar://toxiproxy:6650").
-define(PULSAR_CLONE_HOST, "pulsar://toxiproxy:6651").
-define(PULSAR_BASIC_AUTH_HOST, "pulsar://pulsar-basic-auth:6650").
-define(PULSAR_TOKEN_AUTH_HOST, "pulsar://pulsar-token-auth:6650").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [ t_pulsar_client
    , t_pulsar_basic_auth
    , t_pulsar_token_auth
    , t_pulsar
    , t_pulsar_client_tune_error
    , t_per_request_callbacks
    , t_producers_all_connected
    , t_consumers_all_connected
    , t_topic_lookup_redirect
    , {group, resilience}
    ].

resilience_tests() ->
    resilience_multi_failure_tests() ++ resilience_single_failure_tests().

resilience_multi_failure_tests() ->
    [ t_pulsar_replayq
    , t_pulsar_replayq_producer_restart
    , t_pulsar_drop_expired_batch
    ].

resilience_single_failure_tests() ->
    [ t_pulsar_drop_expired_batch_resend_inflight
    , t_overflow
    , t_client_down_producers_restart
    ].

groups() ->
    [ {resilience, [ {group, timeout}
                   , {group, down}
                   | resilience_single_failure_tests()
                   ]}
    , {timeout, resilience_multi_failure_tests()}
    , {down, resilience_multi_failure_tests()}
    ].

init_per_suite(Config) ->
    pulsar_test_utils:clear_screen(),
    ct:timetrap({minutes, 3}),
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    pulsar_test_utils:reset_proxy(ProxyHost, ProxyPort),
    Config.

end_per_suite(_Args) ->
    ok.

init_per_group(resilience, Config) ->
    PulsarHost = os:getenv("PULSAR_HOST", ?PULSAR_HOST),
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    pulsar_test_utils:reset_proxy(ProxyHost, ProxyPort),
    {ok, _} = application:ensure_all_started(pulsar),
    [ {pulsar_host, PulsarHost}
    , {proxy_host, ProxyHost}
    , {proxy_port, ProxyPort}
    , {fake_pulsar_host, PulsarHost}
    , {resilience_test, true}
    | Config];
init_per_group(timeout, Config) ->
    [ {failure_type, timeout}
    | Config];
init_per_group(down, Config) ->
    [ {failure_type, down}
    | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    pulsar_test_utils:reset_proxy(ProxyHost, ProxyPort),
    ok.

init_per_testcase(t_pulsar_basic_auth, Config) ->
    PulsarHost = os:getenv("PULSAR_BASIC_AUTH_HOST", ?PULSAR_BASIC_AUTH_HOST),
    {ok, _} = application:ensure_all_started(pulsar),
    [ {pulsar_host, PulsarHost}
    | Config];
init_per_testcase(t_pulsar_token_auth, Config) ->
    PulsarHost = os:getenv("PULSAR_TOKEN_AUTH_HOST", ?PULSAR_TOKEN_AUTH_HOST),
    {ok, _} = application:ensure_all_started(pulsar),
    [ {pulsar_host, PulsarHost}
    | Config];
init_per_testcase(_TestCase, Config) ->
    PulsarHost = os:getenv("PULSAR_HOST", ?PULSAR_HOST),
    {ok, _} = application:ensure_all_started(pulsar),
    ReplayqDir = "/tmp/replayq" ++ integer_to_list(erlang:unique_integer()),
    [ {pulsar_host, PulsarHost}
    , {replayq_dir, ReplayqDir}
    | Config].

end_per_testcase(TestCase, Config)
  when TestCase =:= t_pulsar_replayq;
       TestCase =:= t_pulsar_replayq_producer_restart;
       TestCase =:= t_pulsar_drop_expired_batch ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    ReplayqDir = ?config(replayq_dir, Config),
    pulsar_test_utils:reset_proxy(ProxyHost, ProxyPort),
    application:stop(pulsar),
    snabbkaffe:stop(),
    file:del_dir_r(ReplayqDir),
    pulsar_test_utils:uninstall_event_logging(),
    ok;
end_per_testcase(t_pulsar_drop_expired_batch_resend_inflight, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    ReplayqDir = ?config(replayq_dir, Config),
    pulsar_test_utils:reset_proxy(ProxyHost, ProxyPort),
    application:stop(pulsar),
    snabbkaffe:stop(),
    catch meck:unload([pulsar_producer]),
    file:del_dir_r(ReplayqDir),
    pulsar_test_utils:uninstall_event_logging(),
    ok;
end_per_testcase(t_overflow, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    ReplayqDir = ?config(replayq_dir, Config),
    pulsar_test_utils:reset_proxy(ProxyHost, ProxyPort),
    application:stop(pulsar),
    snabbkaffe:stop(),
    file:del_dir_r(ReplayqDir),
    pulsar_test_utils:uninstall_event_logging(),
    ok;
end_per_testcase(_TestCase, _Config) ->
    application:stop(pulsar),
    snabbkaffe:stop(),
    pulsar_test_utils:uninstall_event_logging(),
    meck:unload(),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_pulsar_client(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),

    timer:sleep(1000),
    %% for coverage
    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),

    ?assertMatch({ok,{<<"test">>, PNum}} when is_integer(PNum),
        pulsar_client_manager:get_topic_metadata(?TEST_SUIT_CLIENT, <<"test">>)),

    ?assertMatch({ok, #{
            brokerServiceUrl := BrokerServiceUrl,
            proxy_through_service_url := IsProxy
        }} when (is_list(BrokerServiceUrl) orelse is_binary(BrokerServiceUrl))
                andalso is_boolean(IsProxy),
        pulsar_client_manager:lookup_topic(?TEST_SUIT_CLIENT, <<"test-partition-0">>)),

    ?assertEqual(true, pulsar_client_manager:get_status(?TEST_SUIT_CLIENT)),

    ?assertEqual(ok, pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT)),

    timer:sleep(50),

    ?assertEqual([], supervisor:which_children(pulsar_client_sup)),

    ok.

t_pulsar_basic_auth(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    %% avoid inter-testcase flakiness
    ok = pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT),
    {Pid0, Ref} = spawn_monitor(fun() ->
      Res = pulsar:ensure_supervised_client(
              ?TEST_SUIT_CLIENT,
              [PulsarHost],
              #{ connect_timeout => 30_000
               , conn_opts => #{ auth_data => <<"wrong:credentials">>
                               , auth_method_name => <<"basic">>
                               }}),
      exit(Res)
    end),
    %% Should fail fast if there is an authn error.
    receive
        {'DOWN', Ref, process, Pid0, Reason} ->
            ?assertMatch({error, #{}}, Reason),
            {error, BrokerErrorMap} = Reason,
            ?assertMatch([#{error := 'AuthenticationError'}], maps:values(BrokerErrorMap)),
            ok = pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT),
            ok
    after
        5_000 ->
            ct:fail("authentication error took too long")
    end,

    ConnOpts = #{ auth_data => <<"super:secretpass">>
                , auth_method_name => <<"basic">>
                },
    ok = pulsar:ensure_supervised_client(
                         ?TEST_SUIT_CLIENT,
                         [PulsarHost],
                         #{conn_opts => ConnOpts}),

    ConsumerOpts = #{ cb_init_args => #{send_to => self()}
                    , cb_module => pulsar_echo_consumer
                    , sub_type => 'Shared'
                    , subscription => "my-subscription"
                    , max_consumer_num => 1
                    , name => my_test
                    , consumer_id => 1
                    , conn_opts => ConnOpts
                    },
    {ok, _Consumer} = pulsar:ensure_supervised_consumers( ?TEST_SUIT_CLIENT
                                                        , <<"my-topic">>
                                                        , ConsumerOpts
                                                        ),
    timer:sleep(2000),
    ProducerOpts = #{ batch_size => ?BATCH_SIZE
                    , strategy => random
                    , callback => {?MODULE, producer_callback, []}
                    , conn_opts => ConnOpts
                    },
    {ok, Producers} = pulsar:ensure_supervised_producers( ?TEST_SUIT_CLIENT
                                                        , <<"my-topic">>
                                                        , ProducerOpts
                                                        ),
    timer:sleep(1000),
    Data = #{key => <<"pulsar">>, value => <<"hello world">>},
    {ok, PubRes} = pulsar:send_sync(Producers, [Data]),
    ?assertNotMatch({error, _}, PubRes),
    receive
        {pulsar_message, _Topic, _Message, Payloads} ->
            ?assertEqual([<<"hello world">>], Payloads),
            ok
    after
        2000 ->
            error("message not received!")
    end,
    ok.

t_pulsar_token_auth(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    %% avoid inter-testcase flakiness
    ok = pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT),
    {Pid0, Ref} = spawn_monitor(fun() ->
      Res = pulsar:ensure_supervised_client(
              ?TEST_SUIT_CLIENT,
              [PulsarHost],
              #{ connect_timeout => 30_000
               , conn_opts => #{ auth_data => <<"wrong_token">>
                               , auth_method_name => <<"token">>
                               }}),
      error(Res)
    end),
    %% Should fail fast if there is an authn error.
    receive
        {'DOWN', Ref, process, Pid0, Reason} ->
            ?assertMatch({{error, #{}}, _}, Reason),
            {{error, BrokerErrorMap}, _} = Reason,
            ?assertMatch([#{error := 'AuthenticationError'}], maps:values(BrokerErrorMap)),
            ok = pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT),
            ok
    after
        5_000 ->
            ct:fail("authentication error took too long")
    end,

    ConnOpts = #{ auth_data => <<"eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJhZG1pbiJ9.UlVtumfA7z2dSwrtk8Vvt8T_GUiqnfPoHgZWaGcPv051oiR13v-2_oTdYGVwMYbQ56-pM4DocSbc-mSwhh8mhw">>
                , auth_method_name => <<"token">>
                },
    ok = pulsar:ensure_supervised_client(
                         ?TEST_SUIT_CLIENT,
                         [PulsarHost],
                         #{conn_opts => ConnOpts}),

    ConsumerOpts = #{ cb_init_args => #{send_to => self()}
                    , cb_module => pulsar_echo_consumer
                    , sub_type => 'Shared'
                    , subscription => "my-subscription"
                    , max_consumer_num => 1
                    , name => my_test
                    , consumer_id => 1
                    , conn_opts => ConnOpts
                    },
    {ok, _Consumer} = pulsar:ensure_supervised_consumers( ?TEST_SUIT_CLIENT
                                                        , <<"my-topic">>
                                                        , ConsumerOpts
                                                        ),
    timer:sleep(2000),
    ProducerOpts = #{ batch_size => ?BATCH_SIZE
                    , strategy => random
                    , callback => {?MODULE, producer_callback, []}
                    , conn_opts => ConnOpts
                    },
    {ok, Producers} = pulsar:ensure_supervised_producers( ?TEST_SUIT_CLIENT
                                                        , <<"my-topic">>
                                                        , ProducerOpts
                                                        ),
    timer:sleep(1000),
    Data = #{key => <<"pulsar">>, value => <<"hello world">>},
    {ok, PubRes} = pulsar:send_sync(Producers, [Data]),
    ?assertNotMatch({error, _}, PubRes),
    receive
        {pulsar_message, _Topic, _Message, Payloads} ->
            ?assertEqual([<<"hello world">>], Payloads),
            ok
    after
        2000 ->
            error("message not received!")
    end,
    ok.

t_pulsar(Config) ->
    t_pulsar_(random, Config),
    t_pulsar_(roundrobin, Config).
t_pulsar_(Strategy, Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    {ok, _} = application:ensure_all_started(pulsar),
    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ConsumerOpts = #{
        cb_init_args => [],
        cb_module => ?MODULE,
        sub_type => 'Shared',
        subscription => "pulsar_test_suite_subscription",
        max_consumer_num => 1,
        name => pulsar_test_suite_subscription
    },
    %% start producers
    {ok, Consumers} = pulsar:ensure_supervised_consumers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ConsumerOpts),
    timer:sleep(200),
    {ok, Consumers} = pulsar:ensure_supervised_consumers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ConsumerOpts),

    ProducerOpts = #{
        batch_size => ?BATCH_SIZE,
        strategy => Strategy,
        callback => {?MODULE, producer_callback, []}
    },
    Data = #{key => <<"pulsar">>, value => <<"hello world">>},
    {ok, Producers} = pulsar:ensure_supervised_producers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ProducerOpts),
    %% wait for consumer and producer to connect
    timer:sleep(1_000),
    {ok, Producers} = pulsar:ensure_supervised_producers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ProducerOpts),


    ?assertMatch({ok, #{sequence_id := _}}, pulsar:send_sync(Producers, [Data], 300)),
    %% keepalive test
    timer:sleep(30*1000 + 5*1000),
    ?assertMatch({ok, #{sequence_id := _}}, pulsar:send_sync(Producers, [Data])),
    timer:sleep(500),

    %% restart test
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers, [Data]),
    [{_, ConsumersPid, _, _} | _] = supervisor:which_children(pulsar_consumers_sup),
    {_, _, _, _, _, _, ConsumerMap} = sys:get_state(ConsumersPid),
    [ConsumerPid | _] = maps:keys(ConsumerMap),
    ok = snabbkaffe:start_trace(),
    snabbkaffe:block_until(
      ?match_n_events(2, #{?snk_kind := test_consumer_handle_message}),
      5_000),
    ?assertEqual(connected, pulsar_consumer:get_state(ConsumerPid)),
    ?tp(test_consumer_handle_message, #{}),
    erlang:exit(ProducerPid, kill),
    erlang:exit(ConsumerPid, kill),
    timer:sleep(2000),

    %% match the send_sync message
    ?assertEqual(2, pulsar_metrics:consumer()),
    ?assertEqual(2, pulsar_metrics:producer()),
    %% loop send data
    lists:foreach(fun(_) ->
            pulsar:send(Producers, [Data]), timer:sleep(30)
        end, lists:seq(1, ?BATCH_SIZE)),
    timer:sleep(2000),
    %% should be equal BatchSize
    %% send == consumer
    ?assertEqual(pulsar_metrics:producer(), pulsar_metrics:consumer()),
    %% stop consumers
    ?assertEqual(ok, pulsar:stop_and_delete_supervised_consumers(Consumers)),
    %% stop producers
    ?assertEqual(ok, pulsar:stop_and_delete_supervised_producers(Producers)),
    %% stop clients
    [ClientPid] = get_worker_pids(?TEST_SUIT_CLIENT),
    ?assertEqual(ok, pulsar:stop_and_delete_supervised_client(?TEST_SUIT_CLIENT)),
    %% check alive
    ?assertEqual(false, is_process_alive(ClientPid)),
    ?assertEqual([], supervisor:which_children(pulsar_client_sup)),
    %% metrics test
    [{producer, PCount}, {consumer, CCounter}] = pulsar_metrics:all(),
    ?assertEqual(PCount, CCounter),
    %% 4 producer , 4 consumer , 2 all
    AllMetrics = pulsar_metrics:all_detail(),
    ?assertMatch(N when N > 0, length(AllMetrics)),
    ?assertEqual({PCount, CCounter}, count_test(AllMetrics)),
    ?assertEqual(0, pulsar_metrics:producer("no exist topic name")),
    ?assertEqual(0, pulsar_metrics:consumer("no exist topic name")),
    application:stop(pulsar).

count_test(AllMetrics)-> count_test(AllMetrics, 0, 0).
count_test([], PCountNow, CCountNow) ->
    {PCountNow, CCountNow};
count_test([{{_, all}, _}| Tail], PCountNow, CCountNow) ->
    count_test(Tail, PCountNow, CCountNow);
count_test([{{producer, _}, Data}| Tail], PCountNow, CCountNow) ->
    count_test(Tail, PCountNow + Data, CCountNow);
count_test([{{consumer, _}, Data}| Tail], PCountNow, CCountNow) ->
    count_test(Tail, PCountNow, CCountNow + Data).

t_pulsar_drop_expired_batch(Config) ->
    PulsarHost = ?config(fake_pulsar_host, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    FailureType = ?config(failure_type, Config),
    StabilizationPeriod = timer:seconds(15),
    {ok, _} = application:ensure_all_started(pulsar),
    ok = snabbkaffe:start_trace(),

    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),
    ConsumerOpts = #{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => "pulsar_test_expired" ++ integer_to_list(erlang:unique_integer()),
        max_consumer_num => 1,
        name => pulsar_test_expired
    },
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME) ++ integer_to_list(erlang:unique_integer()),
    {ok, _Consumers} = pulsar:ensure_supervised_consumers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ConsumerOpts),
    ct:pal("started consumer"),
    RetentionPeriodMS = 1_000,
    ProducerOpts = #{
        %% to speed up the test a bit
        tcp_opts => [ {send_timeout, 5_000}
                    , {send_timeout_close, true}
                    ],
        connnect_timeout => 5_000,
        batch_size => ?BATCH_SIZE,
        strategy => random,
        callback => {?MODULE, echo_callback, [self()]},
        retention_period => RetentionPeriodMS
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    ct:pal("started producer"),
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers,
                                                      [#{key => <<"k">>, value => <<>>}]),
    pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 15, _Sleep = 5_000),
    ?assertEqual(connected, pulsar_producer:get_state(ProducerPid)),

    ct:pal("cutting connection with pulsar..."),
    pulsar_test_utils:enable_failure(FailureType, ProxyHost, ProxyPort),
    ct:pal("connection cut"),
    ct:sleep(StabilizationPeriod),
    %% we wait for the producer to be connected and then to be idle
    %% here before producing the messages to avoid it hanging during
    %% the send, at which point the check for expiration was already
    %% done...
    pulsar_test_utils:wait_for_state(ProducerPid, idle, _Retries = 15, _Sleep = 5_000),

    %% Produce messages that'll expire
    ok = snabbkaffe:start_trace(),
    ct:pal("sending messages..."),
    TestPid = self(),
    CallbackFn =
        fun(Resp) ->
          TestPid ! {per_request_response, Resp},
          ok
        end,
    {{ok, _WorkerPid}, {ok, _}} =
        ?wait_async_action(
           pulsar:send(Producers,
                       [#{key => <<"k">>, value => integer_to_binary(SeqNo)}
                        || SeqNo <- lists:seq(1, 150)],
                       %% test per-request callback
                       #{callback_fn => {CallbackFn, []}}),
          #{?snk_kind := pulsar_producer_send_requests_enqueued},
          _Timeout1 = 35_000),
    {{ok, _WorkerPid}, {ok, _}} =
        ?wait_async_action(
           pulsar:send(Producers,
                       [#{key => <<"k">>, value => integer_to_binary(SeqNo)}
                        || SeqNo <- lists:seq(1, 150)]),
          #{?snk_kind := pulsar_producer_send_requests_enqueued},
          _Timeout1 = 35_000),
    ct:pal("waiting for retention period to expire..."),
    ct:sleep(RetentionPeriodMS * 10),

    ct:pal("reestablishing connection with pulsar..."),
    pulsar_test_utils:heal_failure(FailureType, ProxyHost, ProxyPort),
    ct:pal("connection reestablished"),

    ct:pal("waiting for producer to be connected again"),
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers,
                                                      [#{key => <<"k">>, value => <<>>}]),
    pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 15, _Sleep = 5_000),
    ct:pal("producer connected"),

    receive
        {pulsar_message, _Topic, _Receipt, Payloads} ->
            error({should_have_expired, Payloads})
    after
        1_000 ->
            ok
    end,
    receive
        {produce_response, {error, expired}} ->
            ok
    after
        1_000 ->
            error(should_have_invoked_callback)
    end,
    receive
        {per_request_response, {error, expired}} ->
            ok
    after
        1_000 ->
            error(should_have_invoked_callback)
    end,

    ct:pal("producing message that should be received now"),
    pulsar:send(Producers, [#{key => <<"k">>, value => <<"should receive">>}]),

    receive
        {pulsar_message, _Topic1, _Receipt1, [<<"should receive">>]} ->
            ok
    after
        30_000 ->
            error(timeout)
    end,

    ok.

t_pulsar_drop_expired_batch_resend_inflight(Config) ->
    PulsarHost = ?config(fake_pulsar_host, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    ReplayqDir = ?config(replayq_dir, Config),
    FailureType = down,
    StabilizationPeriod = timer:seconds(15),
    {ok, _} = application:ensure_all_started(pulsar),
    Tab = pulsar_test_utils:install_event_logging(),

    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),
    ConsumerOpts = #{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => "pulsar_test_expired",
        max_consumer_num => 1,
        name => pulsar_test_expired
    },
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    {ok, _Consumers} = pulsar:ensure_supervised_consumers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ConsumerOpts),
    ct:pal("started consumer"),

    meck:new(pulsar_producer, [non_strict, no_history, no_link, passthrough]),
    meck:expect(pulsar_producer, handle_response,
      fun({send_receipt, _}, State) ->
              %% ignore receipts so we can test resend
              State;
         (Arg, State) ->
              meck:passthrough([Arg, State])
      end),
    RetentionPeriodMS = 1_000,
    ProducerOpts = #{
        %% to speed up the test a bit
        tcp_opts => [{send_timeout, 5_000}],
        connnect_timeout => 5_000,
        batch_size => ?BATCH_SIZE,
        strategy => random,
        callback => {?MODULE, echo_callback, [self()]},
        replayq_dir => ReplayqDir,
        replayq_seg_bytes => 20 * 1024 * 1024,
        replayq_offload_mode => false,
        replayq_max_total_bytes => 1_000_000_000,
        retention_period => RetentionPeriodMS
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    ct:pal("started producer"),
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers,
                                                      [#{key => <<"k">>, value => <<>>}]),
    pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing])),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing_bytes])),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, inflight])),

    %% 1. produce some messages and, while we are processing them,
    %% simulate a disconnect message to change state to `idle'.
    %% 2. also cut the connection with pulsar, so it'll stay idle
    %% while the messages expire.
    %% 3. restore the connection so that inflight messages will be
    %% resent.
    ct:pal("forcing disconnect"),
    ?check_trace(
       begin
         ?force_ordering(#{?snk_kind := pulsar_producer_send_req_enter},
                         #{?snk_kind := will_force_disconnect}),
         ?force_ordering(#{?snk_kind := force_disconnect},
                         #{?snk_kind := pulsar_producer_send_req_exit}),
         spawn_link(fun() ->
           ?tp(will_force_disconnect, #{}),
           ProducerPid ! {tcp_closed, port},
           pulsar_test_utils:enable_failure(FailureType, ProxyHost, ProxyPort),
           ?tp(force_disconnect, #{}),
           ok
         end),

         %% Produce messages that'll expire when resent
         pulsar:send(Producers,
                     [#{key => <<"k">>, value => integer_to_binary(SeqNo)}
                      || SeqNo <- lists:seq(1, 150)]),
         ?block_until(#{?snk_kind := pulsar_producer_send_req_exit}),
         ct:sleep(RetentionPeriodMS * 2),

         ct:pal("healing failure..."),
         pulsar_test_utils:heal_failure(FailureType, ProxyHost, ProxyPort),
         ct:pal("failure healed"),
         ct:sleep(StabilizationPeriod * 2),
         ct:pal("waiting for producer to be connected again"),
         {_, ProducerPid} = pulsar_producers:pick_producer(Producers,
                                                           [#{key => <<"k">>, value => <<>>}]),
         pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000),
         ct:pal("producer connected"),
         ?assertMatch(
            #{metrics_data := #{gauge_set := 0}},
            pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing])),
         ?assertMatch(
            #{metrics_data := #{gauge_set := 0}},
            pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing_bytes])),
         ?retry(100, 10, ?assertMatch(
            #{metrics_data := #{gauge_set := N}} when N > 0,
            pulsar_test_utils:get_latest_event(Tab, [pulsar, inflight]))),
         ?assertMatch(
            #{metrics_data := #{reason := expired}},
            pulsar_test_utils:get_latest_event(Tab, [pulsar, dropped])),

         %% we can't assert that the messages have been expired, as
         %% the produce request might have succeeded in Pulsar's side,
         %% but our producer doens't know that because of the network
         %% partition.  we can assert that the messages were deemed
         %% expired and dropped by checking the trace, and by checking
         %% that the callback was called with the expiration error.
         receive
             {produce_response, {error, expired}} ->
                 ok
         after
             1_000 ->
                 error(should_have_invoked_callback)
         end,

         ct:pal("producing message that should be received now"),
         pulsar:send(Producers, [#{key => <<"k">>, value => <<"should receive">>}]),

         receive
             {pulsar_message, _Topic1, _Receipt1, [<<"should receive">>]} ->
                 ok
         after
             15_000 ->
                 error(timeout)
         end,
         ok
       end,
       fun(Trace) ->
         ?assertMatch([_], ?of_kind(pulsar_producer_resend_all_expired,
                                    Trace)),
         ok
       end),

    ok.

t_pulsar_replayq(Config) ->
    PulsarHost = ?config(fake_pulsar_host, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    FailureType = ?config(failure_type, Config),
    StabilizationPeriod = timer:seconds(15),
    {ok, _} = application:ensure_all_started(pulsar),

    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    ConsumerOpts = #{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => "pulsar_test_replayq",
        max_consumer_num => 1,
        name => pulsar_test_replayq
    },
    {ok, _Consumers} = pulsar:ensure_supervised_consumers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ConsumerOpts),
    ct:pal("started consumer"),
    ProducerOpts = #{
        batch_size => ?BATCH_SIZE,
        strategy => random,
        callback => {?MODULE, producer_callback, []}
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    ct:pal("started producer"),
    ct:sleep(2_000),

    TestPid = self(),
    ProduceInterval = 100,
    StartSequentialProducer =
        fun Go (SeqNo0) ->
          receive
            stop -> TestPid ! {done, SeqNo0}
          after
            0 ->
              SeqNo = SeqNo0 + 1,
              pulsar:send(Producers, [#{key => <<"k">>, value => integer_to_binary(SeqNo)}]),
              SeqNo rem 10 =:= 0 andalso (TestPid ! {sent, SeqNo}),
              timer:sleep(ProduceInterval),
              Go(SeqNo)
          end
        end,

    SequentialProducer = spawn(fun() -> StartSequentialProducer(0) end),
    ct:pal("started sequential producer"),

    %% produce some messages in the connected state.
    wait_until_produced(100, 100 * ProduceInterval + 100),
    ct:pal("produced 100 messages"),

    %% cut the connection and produce more messages
    ct:pal("cutting connection with pulsar..."),
    pulsar_test_utils:enable_failure(FailureType, ProxyHost, ProxyPort),
    ct:pal("connection cut"),

    timer:sleep(StabilizationPeriod),
    wait_until_produced(500, 400 * ProduceInterval + 100),
    ct:pal("produced 500 messages"),

    %% reestablish connection and produce some more
    ct:pal("reestablishing connection with pulsar..."),
    pulsar_test_utils:heal_failure(FailureType, ProxyHost, ProxyPort),
    ct:pal("connection reestablished"),
    timer:sleep(StabilizationPeriod),
    wait_until_produced(600, 100 * ProduceInterval + 100),
    ct:pal("produced 600 messages"),

    %% stop producer and check we received all msgs
    SequentialProducer ! stop,
    TotalProduced =
        receive
            {done, Total} -> Total
        after
            10_000 ->
                error(producer_didnt_stop)
        end,

    ct:pal("produced ~b messages in total", [TotalProduced]),

    ExpectedPayloads = sets:from_list([integer_to_binary(N) || N <- lists:seq(1, TotalProduced)],
                                      [{version, 2}]),
    wait_until_consumed(ExpectedPayloads, timer:seconds(30)),

    ct:pal("all ~b expected messages were received", [TotalProduced]),

    ok.

t_pulsar_replayq_producer_restart(Config) ->
    PulsarHost = ?config(fake_pulsar_host, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    FailureType = ?config(failure_type, Config),
    ReplayqDir = ?config(replayq_dir, Config),
    StabilizationPeriod = timer:seconds(15),
    {ok, _} = application:ensure_all_started(pulsar),
    Tab = pulsar_test_utils:install_event_logging(),

    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    ConsumerOpts = #{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => "pulsar_test_replayq",
        max_consumer_num => 1,
        name => pulsar_test_replayq
    },
    {ok, _Consumers} = pulsar:ensure_supervised_consumers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ConsumerOpts),
    ct:pal("started consumer"),
    ProducerOpts = #{
        batch_size => ?BATCH_SIZE,
        strategy => random,
        callback => {?MODULE, producer_callback, []},
        replayq_dir => ReplayqDir,
        replayq_seg_bytes => 20 * 1024 * 1024,
        replayq_offload_mode => false,
        replayq_max_total_bytes => 1_000_000_000,
        retention_period => infinity
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    ct:pal("started producer"),
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers,
                                                      [#{key => <<"k">>, value => <<"v">>}]),
    pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000),
    ct:pal("producer connected"),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing])),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing_bytes])),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, inflight])),

    TestPid = self(),
    ProduceInterval = 100,
    StartSequentialProducer =
        fun Go (SeqNo0) ->
          receive
            stop -> TestPid ! {done, SeqNo0}
          after
            0 ->
              SeqNo = SeqNo0 + 1,
              pulsar:send(Producers, [#{key => <<"k">>, value => integer_to_binary(SeqNo)}]),
              SeqNo rem 10 =:= 0 andalso (TestPid ! {sent, SeqNo}),
              timer:sleep(ProduceInterval),
              Go(SeqNo)
          end
        end,

    SequentialProducer = spawn(fun() -> StartSequentialProducer(0) end),
    ct:pal("started sequential producer"),

    %% produce some messages in the connected state.
    wait_until_produced(100, 100 * ProduceInterval + 100),
    ct:pal("produced 100 messages"),

    %% cut the connection and produce more messages
    ct:pal("cutting connection with pulsar..."),
    pulsar_test_utils:enable_failure(FailureType, ProxyHost, ProxyPort),
    ct:pal("connection cut"),

    timer:sleep(StabilizationPeriod),
    wait_until_produced(250, 150 * ProduceInterval + 100),
    ct:pal("produced 250 messages"),

    %% stop producing and kill the producer
    SequentialProducer ! stop,
    TotalProduced =
        receive
            {done, Total} -> Total
        after
            10_000 ->
                error(producer_didnt_stop)
        end,
    ct:pal("produced ~b messages in total", [TotalProduced]),
    %% give it some time for the producer to enqueue them before
    %% killing it.
    ct:sleep(1_000),
    %% When using the `timeout' toxic, the requests will more likely be inflight than
    %% queued.
    #{metrics_data := #{gauge_set := Queuing1}} =
        pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing]),
    #{metrics_data := #{gauge_set := QueuingBytes1}} =
        pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing_bytes]),
    #{metrics_data := #{gauge_set := Inflight1}} =
        pulsar_test_utils:get_latest_event(Tab, [pulsar, inflight]),
    Metrics1 = #{queuing => Queuing1, queuing_bytes => QueuingBytes1, inflight => Inflight1},
    case FailureType of
        down ->
            ?assert(Queuing1 > 0, Metrics1),
            ?assert(QueuingBytes1 > 0, Metrics1),
            ?assert(Inflight1 > 0, Metrics1);
        timeout ->
            ?assertEqual(0, Queuing1, Metrics1),
            ?assertEqual(0, QueuingBytes1, Metrics1),
            ?assert(Inflight1 > 0, Metrics1)
    end,

    Ref = monitor(process, ProducerPid),
    exit(ProducerPid, kill),
    receive
        {'DOWN', Ref, process, ProducerPid, killed} ->
            ok
    after
        500 ->
            error(producer_still_alive)
    end,
    %% After producer is restarted, should re-set gauges.
    ct:sleep(300),
    #{metrics_data := #{gauge_set := Queuing2}} =
        pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing]),
    #{metrics_data := #{gauge_set := QueuingBytes2}} =
        pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing_bytes]),
    #{metrics_data := #{gauge_set := Inflight2}} =
        pulsar_test_utils:get_latest_event(Tab, [pulsar, inflight]),
    Metrics2 = #{queuing => Queuing2, queuing_bytes => QueuingBytes2, inflight => Inflight2},
    ?assertEqual(Metrics1, Metrics2),

    %% reestablish connection and wait until producer catches up
    ct:pal("reestablishing connection with pulsar..."),
    pulsar_test_utils:heal_failure(FailureType, ProxyHost, ProxyPort),
    ct:pal("connection reestablished"),

    ExpectedPayloads = sets:from_list([integer_to_binary(N) || N <- lists:seq(1, TotalProduced)],
                                      [{version, 2}]),
    wait_until_consumed(ExpectedPayloads, timer:seconds(30)),

    ct:pal("all ~b expected messages were received", [TotalProduced]),

    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing])),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing_bytes])),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, inflight])),

    ok.

t_overflow(Config) ->
    PulsarHost = ?config(fake_pulsar_host, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    ReplayqDir = ?config(replayq_dir, Config),
    {ok, _} = application:ensure_all_started(pulsar),
    Tab = pulsar_test_utils:install_event_logging(),

    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    ConsumerOpts = #{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => "pulsar_test_" ++ atom_to_list(?FUNCTION_NAME),
        max_consumer_num => 1,
        name => pulsar_test_replayq
    },
    {ok, _Consumers} = pulsar:ensure_supervised_consumers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ConsumerOpts),
    ct:pal("started consumer"),
    MaxTotalBytes = 100,
    ProducerOpts = #{
        batch_size => ?BATCH_SIZE,
        strategy => random,
        callback => {?MODULE, echo_callback, [self()]},
        replayq_dir => ReplayqDir,
        replayq_seg_bytes => 20 * 1024 * 1024,
        replayq_offload_mode => false,
        replayq_max_total_bytes => MaxTotalBytes,
        retention_period => infinity
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    ct:pal("started producer"),
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers,
                                                      [#{key => <<"k">>, value => <<"v">>}]),
    pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries0 = 5, _Sleep0 = 5_000),
    ct:pal("producer connected"),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing])),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing_bytes])),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, inflight])),

    ct:pal("cutting connection with pulsar"),
    pulsar_test_utils:enable_failure(down, ProxyHost, ProxyPort),
    ct:pal("connection cut"),
    %% ensure that the producer noticed pulsar is down
    pulsar_test_utils:wait_for_state(ProducerPid, idle, _Retries1 = 5, _Sleep1 = 5_000),
    ct:pal("producer disconnected"),
    %% `From' is `undefined' for casts
    From = undefined,
    Data = [#{key => <<"k">>, value => <<"a">>}],
    NumItems = 5,
    SampleItem = pulsar_producer:make_queue_item(From, Data),
    ItemSize = pulsar_producer:queue_item_sizer(SampleItem),
    ExpectedOverflow = NumItems * ItemSize - MaxTotalBytes,
    %% pre-condition
    true = ExpectedOverflow > 0,
    MessagesToDrop = round(math:ceil(ExpectedOverflow / ItemSize)),
    MessagesToKeep = NumItems - MessagesToDrop,
    BytesToKeep = MessagesToKeep * ItemSize,
    ct:pal("expected overflow: ~b bytes; average size per item: ~b; messages to drop: ~b",
           [ExpectedOverflow, ItemSize, MessagesToDrop]),

    ok = snabbkaffe:start_trace(),
    ct:pal("sending messages"),
    lists:foreach(
      fun(N) ->
        {{ok, _WorkerPid}, {ok, _}} =
            ?wait_async_action(
               pulsar:send(Producers, [#{key => <<"k">>, value => integer_to_binary(N)}]),
               #{?snk_kind := pulsar_producer_send_requests_enqueued},
               10_000)
      end,
      lists:seq(1, NumItems)),
    ct:pal("messages enqueued"),
    ?assertMatch(
       #{metrics_data := #{gauge_set := MessagesToKeep}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing])),
    ?assertMatch(
       #{metrics_data := #{gauge_set := BytesToKeep}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing_bytes]),
       #{max_total_bytes => MaxTotalBytes}),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, inflight])),
    ?assertMatch(
       #{metrics_data := #{reason := queue_full}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, dropped])),

    ct:pal("waiting for overflow callbacks"),
    Resps = wait_callbacks(MessagesToDrop, _Timeout2 = 10_000),
    ?assert(lists:all(fun(R) -> R =:= {error, overflow} end, Resps),
            #{responses => Resps}),

    %% now restore the connection and check that the last enqueued
    %% messages survived.
    ct:pal("healing network failure"),
    pulsar_test_utils:heal_failure(down, ProxyHost, ProxyPort),

    ExpectedPayloads =
        sets:from_list(
          [integer_to_binary(N)
           || N <- lists:seq(MessagesToDrop + 1, NumItems)],
          [{version, 2}]),
    %% pre-condition: must have at least 1
    true = sets:size(ExpectedPayloads) > 0,
    ct:pal("waiting for payloads to be published and consumed: ~p", [ExpectedPayloads]),
    wait_until_consumed(ExpectedPayloads, timer:seconds(30)),

    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing])),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, queuing_bytes])),
    ?assertMatch(
       #{metrics_data := #{gauge_set := 0}},
       pulsar_test_utils:get_latest_event(Tab, [pulsar, inflight])),

    ok.

%% check that an error when tuning the socket does not make
%% pulsar_client bring the application down...
t_pulsar_client_tune_error(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    {ok, _} = application:ensure_all_started(pulsar),

    %% let it start once
    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    [ClientPid] = get_worker_pids(?TEST_SUIT_CLIENT),

    pulsar_test_utils:with_mock(pulsar_socket, internal_getopts,
      fun(_InetM, _Sock, _Opts) -> {error, einval} end,
      fun() ->
        %% now introduce the failure and kill it; the supervisor will restart it
        exit(ClientPid, kill),
        ct:sleep(10_000),
        %% should still be a registered child; if max restart
        %% intensity is reached, will be removed.
        ?assertMatch([_], supervisor:which_children(pulsar_client_sup)),
        ok
      end),

    ok.

%% check that we call per request callbacks
t_per_request_callbacks(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    {ok, _} = application:ensure_all_started(pulsar),
    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    ConsumerOpts = #{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => "pulsar_test_replayq",
        max_consumer_num => 1,
        name => pulsar_test_replayq
    },
    {ok, _Consumers} = pulsar:ensure_supervised_consumers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ConsumerOpts),
    ct:pal("started consumer"),
    ProducerOpts = #{
        batch_size => ?BATCH_SIZE,
        strategy => random,
        retention_period => infinity
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    ct:pal("started producer"),
    {_, ProducerPid} = pulsar_producers:pick_producer(Producers,
                                                      [#{key => <<"k">>, value => <<"v">>}]),
    pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000),
    ct:pal("producer connected"),
    TId = ets:new(results, [public, ordered_set]),
    CallbackFn = fun(Caller, TId0, Res) ->
                   Now = erlang:monotonic_time(),
                   ets:insert(TId0, {Now, Res}),
                   Caller ! {response, Res},
                   ok
                 end,
    Args = [self(), TId],
    pulsar:send(Producers,
                [#{key => <<"k">>, value => integer_to_binary(SeqNo)}
                 || SeqNo <- lists:seq(1, 150)],
                #{callback_fn => {CallbackFn, Args}}),
    receive
        {response, Res} ->
            ct:pal("response: ~p", [Res]),
            ?assertMatch({ok, #{sequence_id := _}}, Res)
    after
        5_000 ->
            ct:fail("no response received")
    end,
    receive
        {response, _Res} ->
            ct:fail("should have invoked the callback only once")
    after
        1_000 ->
            ok
    end,

    ok.

%% Checks that we restart producers if the client is down while producers attempt to use
%% it.
t_client_down_producers_restart(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    {ok, _} = pulsar_test_utils:reset_proxy(ProxyHost, ProxyPort),
    {ok, _} = application:ensure_all_started(pulsar),
    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    ProducersName = ?FUNCTION_NAME,
    ProducerOpts = #{
        name => ProducersName,
        batch_size => ?BATCH_SIZE,
        strategy => random,
        retention_period => infinity
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    Batch = [#{key => <<"k">>, value => <<"v">>}],
    %% pre-condition: everything is fine initially
    ?assertMatch({_, P} when is_pid(P), pulsar_producers:pick_producer(Producers, Batch)),
    %% Now, pulsar becomes unresponsive while `pulsar_producers' is trying to get topic
    %% metadata from it.  It'll timeout and make producers shutdown.
    pulsar_test_utils:enable_failure(down, ProxyHost, ProxyPort),
    ProducersPid0 = whereis(ProducersName),
    ?assert(is_pid(ProducersPid0)),
    MRef0 = monitor(process, ProducersPid0),
    ProducersPid0 ! timeout,
    CallTimeout = 30_000,
    receive
        {'DOWN', MRef0, process, ProducersPid0, Reason0} ->
            ct:pal("shutdown reason: ~p", [Reason0]),
            ok
    after
        CallTimeout + 3_000 ->
            ct:fail("l. ~p : producers didn't shut down", [?LINE])
    end,
    %% ... then, they should eventually restart.
    pulsar_test_utils:heal_failure(down, ProxyHost, ProxyPort),
    ct:sleep(500),
    ProducersPid1 = whereis(ProducersName),
    ?assert(is_pid(ProducersPid1)),
    ?assertNotEqual(ProducersPid0, ProducersPid1),
    ?assert(is_process_alive(ProducersPid1)),
    ?assertMatch({_, P} when is_pid(P), pulsar_producers:pick_producer(Producers, Batch)),

    ok.

t_producers_all_connected(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    {ok, _} = application:ensure_all_started(pulsar),
    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    ProducerOpts = #{
        batch_size => ?BATCH_SIZE,
        strategy => random,
        retention_period => infinity
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    ct:pal("started producers"),
    ProducerPids = pulsar_test_utils:producer_pids(),
    lists:foreach(
     fun(ProducerPid) ->
       pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000)
     end,
     ProducerPids),
    ?retry(
       _Sleep1 = 500,
       _Attempts1 = 20,
       ?assert(pulsar_producers:all_connected(Producers))),
    %% Should respond even if producers are busy.
    lists:foreach(fun sys:suspend/1, ProducerPids),
    ?assert(pulsar_producers:all_connected(Producers)),
    %% If we kill all producers, the empty list should indicate that
    %% they are not connected.
    Refs = maps:from_list([{monitor(process, P), true} || P <- ProducerPids]),
    lists:foreach(fun(P) -> exit(P, kill) end, ProducerPids),
    wait_until_all_dead(Refs, 5_000),
    ?retry(
       _Sleep2 = 500,
       _Attempts2 = 20,
       ?assertNot(pulsar_producers:all_connected(Producers))),
    ok.

t_consumers_all_connected(Config) ->
    PulsarHost = ?config(pulsar_host, Config),
    {ok, _} = application:ensure_all_started(pulsar),
    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),
    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    ConsumerOpts = #{
        cb_init_args => #{send_to => self()},
        cb_module => pulsar_echo_consumer,
        sub_type => 'Shared',
        subscription => "pulsar_test_all_connected",
        max_consumer_num => 1,
        name => pulsar_test_replayq
    },
    {ok, Consumers} = pulsar:ensure_supervised_consumers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ConsumerOpts),
    ct:pal("started consumers"),
    receive {consumer_started, _} -> ok
    after 1_000 -> ct:fail("consumers didn't really start")
    end,
    ConsumerPids =
        [P || {_Name, PS, _Type, _Mods} <- supervisor:which_children(pulsar_consumers_sup),
              P <- element(2, process_info(PS, links)),
              case proc_lib:initial_call(P) of
                  {pulsar_consumer, init, _} -> true;
                  _ -> false
              end],
    lists:foreach(
     fun(ConsumerPid) ->
       pulsar_test_utils:wait_for_state(ConsumerPid, connected, _Retries = 5, _Sleep = 5_000)
     end,
     ConsumerPids),
    ?assert(pulsar_consumers:all_connected(Consumers)),
    %% If we kill all producers, the empty list should indicate that
    %% they are not connected.
    Refs = maps:from_list([{monitor(process, P), true} || P <- ConsumerPids]),
    lists:foreach(fun(P) -> exit(P, kill) end, ConsumerPids),
    wait_until_all_dead(Refs, 5_000),
    ?assertNot(pulsar_consumers:all_connected(Consumers)),
    ok.

%% Checks that, if a topic lookup leads to a redirect to another broker, we dynamically
%% start a new client for it, redo the lookup with the new client and it replies to the
%% original producer.
t_topic_lookup_redirect(Config) ->
    PulsarHost = ?PULSAR_CLONE_HOST,
    {ok, _} = application:ensure_all_started(pulsar),
    ok = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [PulsarHost], #{}),
    ct:pal("started client"),

    %% Only a single endpoint initially.
    ?assertMatch([_], get_worker_pids(?TEST_SUIT_CLIENT)),

    Topic = "persistent://public/default/" ++ atom_to_list(?FUNCTION_NAME),
    TestPid = self(),
    Mod = pulsar_client_worker,
    ok = meck:new(Mod, [passthrough]),
    ok = meck:expect(Mod, handle_response, fun
      ({lookupTopicResponse, Response0} = Msg, State) ->
         case meck:called(Mod, handle_response, [{lookupTopicResponse, '_'}, '_']) of
             true ->
                 TestPid ! {lookup, Response0, State},
                 meck:passthrough([Msg, State]);
             false ->
                 %% First time we lookup, we fake a redirect response
                 Response = Response0#{ response := 'Redirect'
                                      , brokerServiceUrl := ?config(pulsar_host, Config)
                                      , authoritative := true
                                      },
                 TestPid ! {first_lookup, Response, State},
                 meck:passthrough([{lookupTopicResponse, Response}, State])
         end;
      (Msg, State) ->
         meck:passthrough([Msg, State])
    end),

    %% After we start our producers, that should trigger starting a new client that looks
    %% up the topic in the new broker.
    ProducerOpts = #{
        batch_size => ?BATCH_SIZE,
        strategy => random,
        retention_period => infinity
    },
    {ok, _Producers} = pulsar:ensure_supervised_producers(
                        ?TEST_SUIT_CLIENT,
                        Topic,
                        ProducerOpts),
    ct:pal("started producers"),
    ProducerPids = pulsar_test_utils:producer_pids(),
    lists:foreach(
     fun(ProducerPid) ->
       pulsar_test_utils:wait_for_state(ProducerPid, connected, _Retries = 5, _Sleep = 5_000)
     end,
     ProducerPids),

    %% Should have spawned a new client.
    ?assertMatch([_, _], get_worker_pids(?TEST_SUIT_CLIENT)),

    %% Second lookup must use the same `authoritative' value from the redirect response.
    receive
        {lookup, Response, _} ->
            ?assertMatch(#{authoritative := true}, Response)
    after 0 ->
            ct:fail("missing message; mailbox: ~p", [process_info(self(), messages)])
    end,

    ok.

wait_until_all_dead(Refs, _Timeout) when map_size(Refs) =:= 0 ->
    ct:pal("all pids dead"),
    ok;
wait_until_all_dead(Refs, Timeout) ->
    receive
        {'DOWN', Ref, process, _, _} when is_map_key(Ref, Refs) ->
            ct:pal("pid with ref ~p died", [Ref]),
            RemainingRefs = maps:remove(Ref, Refs),
            wait_until_all_dead(RemainingRefs, Timeout)
    after
        Timeout ->
            ct:pal("remaning pids: ~p", [maps:values(Refs)]),
            ct:fail("timeout waiting for pid deaths")
    end.

wait_callbacks(N, Timeout) ->
    wait_callbacks(N, Timeout, []).

wait_callbacks(N, _Timeout, Acc) when N =< 0 ->
    lists:reverse(Acc);
wait_callbacks(N, Timeout, Acc) ->
    receive
        {produce_response, Resp} ->
            wait_callbacks(N - 1, Timeout, [Resp | Acc])
    after
        Timeout ->
            error({missing_callbacks, #{ expected_remaining => N
                                       , so_far => Acc
                                       }})
    end.

wait_until_produced(ExpectedSeqNo, Timeout) ->
    receive
        {sent, ExpectedSeqNo} ->
            ok
    after
        Timeout ->
            error({timeout, ExpectedSeqNo})
    end.

wait_until_consumed(ExpectedPayloads0, Timeout) ->
    case sets:is_empty(ExpectedPayloads0) of
        true -> ok;
        false ->
            receive
                {pulsar_message, _Topic, _Receipt, Payloads} ->
                    PayloadsSet = sets:from_list(Payloads, [{version, 2}]),
                    ExpectedPayloads = sets:subtract(ExpectedPayloads0, PayloadsSet),
                    wait_until_consumed(ExpectedPayloads, Timeout)
            after
                Timeout ->
                    error({missing_messages, lists:sort(sets:to_list(ExpectedPayloads0))})
            end
    end.

get_worker_pids(ClientId) ->
    [ClientsSup] = [ClientsSup
                    || {ClientId0, ClientsSup, _, _} <- supervisor:which_children(pulsar_client_sup),
                       ClientId0 =:= ClientId],
    [WorkerSup] = [WorkerSup
                   || {_, WorkerSup, supervisor, _} <- supervisor:which_children(ClientsSup)],
    lists:map(fun({_, Worker, _, _}) -> Worker end, supervisor:which_children(WorkerSup)).

%%----------------------
%% pulsar callback
%%  #{highest_sequence_id => 18446744073709551615,message_id => #{ack_set => [],entryId => 13,ledgerId => 225},producer_id => 1,sequence_id => 1}
%%----------------------
init(_Topic, Args) ->
    {ok, Args}.
handle_message(_Msg, _Payloads, Loop) ->
    ?tp(test_consumer_handle_message, #{}),
    {ok, 'Individual', Loop}.

producer_callback(_Args) -> ok.

echo_callback(Resp, SendTo) ->
    SendTo ! {produce_response, Resp}.
