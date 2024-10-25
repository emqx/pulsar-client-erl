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

%% Useful when iterating on the tests in a loop, to get rid of all the garbaged printed
%% before the test itself beings.
%% Only actually does anything if the environment variable `CLEAR_SCREEN' is set to `true'
%% and only clears the screen the screen the first time it's encountered, so it's harmless
%% otherwise.
clear_screen() ->
    Key = {?MODULE, clear_screen},
    case {os:getenv("CLEAR_SCREEN"), persistent_term:get(Key, false)} of
        {"true", false} ->
            io:format(standard_io, "\033[H\033[2J", []),
            io:format(standard_error, "\033[H\033[2J", []),
            io:format(standard_io, "\033[H\033[3J", []),
            io:format(standard_error, "\033[H\033[3J", []),
            persistent_term:put(Key, true),
            ok;
        _ ->
            ok
    end.

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

reset_proxy(ProxyHost, ProxyPort) ->
    Url = "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/reset",
    Body = <<>>,
    {ok, {{_, 204, _}, _, _}} = httpc:request(post, {Url, [], "application/json", Body}, [],
                                              [{body_format, binary}]).

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
            ct:pal("~p reached desired ~p state", [Pid, DesiredState]),
            ok;
        State ->
            ct:pal("still not in current state;\n  current: ~p\n  process info: ~p\n  stacktrace: ~p",
                   [State, process_info(Pid), process_info(Pid, current_stacktrace)]),
            ct:sleep(Sleep),
            wait_for_state(Pid, DesiredState, Retries - 1, Sleep)
    catch
        exit:{timeout, _} ->
            ct:pal("timed out making sys request", []),
            ct:sleep(Sleep),
            wait_for_state(Pid, DesiredState, Retries - 1, Sleep)
    end.

with_mock(Mod, FnName, MockedFn, Fun) ->
    ok = meck:new(Mod, [non_strict, no_link, no_history, passthrough]),
    ok = meck:expect(Mod, FnName, MockedFn),
    try
        Fun()
    after
        ok = meck:unload(Mod)
    end.

-spec producer_pids() -> [pid()].
producer_pids() ->
    [P || {_Name, PS, _Type, _Mods} <- supervisor:which_children(pulsar_producers_sup),
          P <- element(2, process_info(PS, links)),
          case proc_lib:initial_call(P) of
              {pulsar_producer, init, _} -> true;
              _ -> false
          end].

get_latest_event(EventRecordTable, EventId) ->
    case get_latest_events(EventRecordTable, EventId) of
        [Last | _] ->
            Last;
        _ ->
            none
    end.

get_latest_events(EventRecordTable, EventId) ->
    case ets:lookup(EventRecordTable, EventId) of
        [{_, Events}] ->
            Events;
        _ ->
            []
    end.

get_current_counter(EventRecordTable, EventId) ->
    lists:foldl(
      fun({_, #{counter_inc := Delta}}, Acc) ->
              Acc + Delta
      end,
      0,
      get_latest_event(EventRecordTable, EventId)
     ).

handle_telemetry_event(
    EventId,
    MetricsData,
    MetaData,
    #{record_table := EventRecordTable}
) ->
    case EventRecordTable =/= none of
        true ->
            do_handle_telemetry_event(EventRecordTable, EventId, MetricsData, MetaData);
        false ->
            ok
    end.

do_handle_telemetry_event(EventRecordTable, EventId, MetricsData, MetaData) ->
    try
        PastEvents = case ets:lookup(EventRecordTable, EventId) of
                         [] -> [];
                         [{_EventId, PE}] -> PE
                     end,
        NewEventList = [ #{metrics_data => MetricsData,
                           meta_data => MetaData} | PastEvents],
        ets:insert(EventRecordTable, {EventId, NewEventList})
    catch
        error:badarg:Stacktrace ->
            ct:pal("<<< error handling telemetry event >>>\n[event id]: ~p\n[metrics data]: ~p\n[meta data]: ~p\n\nStacktrace:\n  ~p\n",
                   [EventId, MetricsData, MetaData, Stacktrace])
    end.

get_telemetry_seq(Table, EventId) ->
    case ets:lookup(Table, EventId) of
       [] -> [];
       [{_, Events}] ->
            lists:reverse(
              [case Data of
                   #{counter_inc := Val} ->
                       Val;
                   #{gauge_set := Val} ->
                       Val;
                   #{gauge_shift := Val} ->
                       Val
               end
               || #{metrics_data := Data} <- Events])
    end.

telemetry_id() ->
    <<"test-telemetry-handler">>.

uninstall_event_logging() ->
    telemetry:detach(telemetry_id()).

install_event_logging() ->
    EventsTable = ets:new(telemetry_events, [public]),
    ok = application:ensure_started(telemetry),
    telemetry:attach_many(
        %% unique handler id
        telemetry_id(),
        telemetry_events(),
        fun ?MODULE:handle_telemetry_event/4,
        #{record_table => EventsTable}
    ),
    timer:sleep(100),
    EventsTable.

telemetry_events() ->
    [
      [pulsar, dropped],
      [pulsar, queuing],
      [pulsar, queuing_bytes],
      [pulsar, inflight]
    ].
