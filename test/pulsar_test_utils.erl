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
-module(pulsar_test_utils).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").


populate_proxy(ProxyHost, ProxyPort, FakePulsarPort, PulsarUrl) ->
    {_, {PulsarHost, PulsarPort}} = pulsar_utils:parse_url(PulsarUrl),
    PulsarHostPort = PulsarHost ++ ":" ++ integer_to_list(PulsarPort),
    Url = "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/populate",

    Body = iolist_to_binary(io_lib:format(<<"[{\"name\":\"pulsar\",\"listen\":\"0.0.0.0:~b\","
                                            "\"upstream\":~p,\"enabled\":true}]">>,
                                          [PulsarPort, PulsarHostPort])),
    {ok, {{_, 201, _}, _, _}} = httpc:request(post, {Url, [], "application/json", Body}, [],
                                              [{body_format, binary}]),
    "pulsar://" ++ ProxyHost ++ ":" ++ integer_to_list(FakePulsarPort).

enable_failure(FailureType, ProxyHost, ProxyPort) ->
    case FailureType of
        down -> switch_proxy(off, ProxyHost, ProxyPort);
        timeout -> timeout_proxy(on, ProxyHost, ProxyPort)
    end.

heal_failure(FailureType, ProxyHost, ProxyPort) ->
    case FailureType of
        down -> switch_proxy(on, ProxyHost, ProxyPort);
        timeout -> timeout_proxy(off, ProxyHost, ProxyPort)
    end.

switch_proxy(Switch, ProxyHost, ProxyPort) ->
    Url = "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/pulsar",
    Body = case Switch of
               off -> <<"{\"enabled\":false}">>;
               on  -> <<"{\"enabled\":true}">>
           end,
    {ok, {{_, 200, _}, _, _}} = httpc:request(post, {Url, [], "application/json", Body}, [],
                                              [{body_format, binary}]).

timeout_proxy(on, ProxyHost, ProxyPort) ->
    Url = "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/pulsar/toxics",
    Body = <<"{\"name\":\"timeout\",\"type\":\"timeout\","
             "\"stream\":\"upstream\",\"toxicity\":1.0,"
             "\"attributes\":{\"timeout\":0}}">>,
    {ok, {{_, 200, _}, _, _}} = httpc:request(post, {Url, [], "application/json", Body}, [],
                                              [{body_format, binary}]);
timeout_proxy(off, ProxyHost, ProxyPort) ->
    Url = "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/pulsar/toxics/timeout",
    Body = <<>>,
    {ok, {{_, 204, _}, _, _}} = httpc:request(delete, {Url, [], "application/json", Body}, [],
                                              [{body_format, binary}]).

wait_for_state(_Pid, DesiredState, Retries, _Sleep) when Retries =< 0 ->
    error({didnt_reach_desired_state, DesiredState});
wait_for_state(Pid, DesiredState, Retries, Sleep) ->
    %% the producer may hang for ~ 60 s on the `pulsar_socket:send'
    %% call, which has a `send_timeout' on the socket of 60 s by
    %% default.  we set it to 10 s in the test to make it quicker.
    Timeout = timer:seconds(11),
    try sys:get_state(Pid, Timeout) of
        {DesiredState, _} ->
            ok;
        _ ->
            ct:sleep(Sleep),
            wait_for_state(Pid, DesiredState, Retries - 1, Sleep)
    catch
        exit:{timeout, _} ->
            ct:sleep(Sleep),
            wait_for_state(Pid, DesiredState, Retries - 1, Sleep)
    end.
