%%%%--------------------------------------------------------------------
%%%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [
        t_pass
        , t_conn_pulsar
        , t_produce
        , t_consumer
    ].

init_per_suite(Cfg) ->
    Cfg.

end_per_suite(Args) ->
    io:format("~0p ~0p~n", [?FUNCTION_ARITY, Args]),
    ok.

set_special_configs(Args) ->
    io:format("~0p ~0p~n", [?FUNCTION_ARITY, Args]),
    ok.
%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_pass(Args) ->
    io:format("~0p ~0p~n", [?FUNCTION_ARITY, Args]),
    ok.

t_conn_pulsar(_) ->
    application:ensure_all_started(pulsar),
    Opts = #{},
    {ok, _ClientPid} = pulsar:ensure_supervised_client(?TEST_SUIT_CLIENT, [{"127.0.0.1", 6650}], Opts),
    ok.

t_produce(_) ->
    ProducerOpts = [{batch_size, 100}, {callback, fun ?MODULE:pulsar_callback/1}, {strategy, random}],
    Data = #{key => <<"pulsar">>, value => <<"hello world">>},
    {ok, Producers} = pulsar:ensure_supervised_producers(?TEST_SUIT_CLIENT,
        "persistent://public/default/test", maps:from_list(ProducerOpts)),
    io:format("prducers ~0p ~n", [Producers]),
%%    pulsar:send(Producers, [Data]),
    pulsar:send_sync(Producers, [Data],300),
    ok.

t_consumer(_) ->
    ConsumerOpts = #{
        cb_init_args => init_args,
        cb_module => ?MODULE,
        sub_type => 'Shared',
        subscription => "pulsar_test_suite_subscription",
        max_consumer_num => 1,
        name => pulsar_test_suite_subscription
    },
    Result = pulsar:ensure_supervised_consumers(?TEST_SUIT_CLIENT, "persistent://public/default/test", ConsumerOpts),
    io:format("Result ~0p ~n", [Result]),
    ok.

pulsar_callback(_) -> ok.

%%----------------------
%% pulsar callback
%%----------------------
init(Topic, Args) ->
    io:format("topic ~0p , consumer callback init, args ~0p~n", [Topic, Args]),
    {ok, Args}.
handle_message(Msg, Payloads, CallBackState) ->
    io:format("consumer receive message ~0p ~n", [Msg]),
    lists:foreach(fun(Payload) -> io:format("paylaod: ~0p~n", [Payload]) end, Payloads),
    {ok, 'Individual', CallBackState}.

