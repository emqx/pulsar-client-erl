%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(pulsar_utils).

-export([ merge_opts/1
        , parse_url/1
        , hostport_from_url/1
        , maybe_enable_ssl_opts/2
        , maybe_add_proxy_to_broker_url_opts/2
        , escape_uri/1
        , wrap_secrets/1
        , unwrap_conn_opts/1
        ]).

-export([collect_send_calls/1]).

merge_opts([Opts1, Opts2]) ->
    proplist_diff(Opts1, Opts2) ++ Opts2;
merge_opts([Opts1 | RemOpts]) ->
    merge_opts([Opts1, merge_opts(RemOpts)]).

hostport_from_url(URL) ->
    case string:split(URL, "://", leading) of
        [HostPort] -> HostPort;
        [_Scheme, HostPort] -> HostPort
    end.

parse_url(URL) when is_list(URL) ->
    case string:split(URL, "://") of
        ["pulsar+ssl", URI] -> {ssl, parse_uri(URI)};
        ["pulsar", URI] -> {tcp, parse_uri(URI)};
        [Scheme, _] -> error({invalid_scheme, Scheme});
        [URL] -> {tcp, parse_uri(URL)}
    end.

parse_uri("") ->
    error(empty_hostname);
parse_uri(URI) ->
    case string:lexemes(URI, ": ") of
        [Host, Port] -> {Host, list_to_integer(Port)};
        [Host] -> {Host, 6650}
    end.

maybe_enable_ssl_opts(tcp, Opts) -> Opts#{enable_ssl => false};
maybe_enable_ssl_opts(ssl, Opts) -> Opts#{enable_ssl => true}.

maybe_add_proxy_to_broker_url_opts(Opts, undefined) ->
    Opts;
maybe_add_proxy_to_broker_url_opts(Opts, ProxyToBrokerUrl) ->
    ConnOpts = maps:get(conn_opts, Opts, #{}),
    ConnOpts1 = ConnOpts#{proxy_to_broker_url => pulsar_utils:hostport_from_url(ProxyToBrokerUrl)},
    Opts#{conn_opts => ConnOpts1}.

collect_send_calls(0) ->
    [];
collect_send_calls(Cnt) when Cnt > 0 ->
    collect_send_calls(Cnt, []).

collect_send_calls(0, Acc) ->
    lists:reverse(Acc);
collect_send_calls(Cnt, Acc) ->
    receive
        {'$gen_cast', {send, Messages}} ->
            collect_send_calls(Cnt - 1, Messages ++ Acc)
    after 0 ->
          lists:reverse(Acc)
    end.

proplist_diff(Opts1, Opts2) ->
    lists:foldl(fun(Opt, Opts1Acc) ->
            Key = case Opt of
                {K, _} -> K;
                K when is_atom(K) -> K
            end,
            proplists:delete(Key, Opts1Acc)
        end, Opts1, Opts2).

%% copied from `edoc_lib' because dialyzer cannot see this private
%% function there.
escape_uri([C | Cs]) when C >= $a, C =< $z ->
    [C | escape_uri(Cs)];
escape_uri([C | Cs]) when C >= $A, C =< $Z ->
    [C | escape_uri(Cs)];
escape_uri([C | Cs]) when C >= $0, C =< $9 ->
    [C | escape_uri(Cs)];
escape_uri([C = $. | Cs]) ->
    [C | escape_uri(Cs)];
escape_uri([C = $- | Cs]) ->
    [C | escape_uri(Cs)];
escape_uri([C = $_ | Cs]) ->
    [C | escape_uri(Cs)];
escape_uri([C | Cs]) when C > 16#7f ->
    %% This assumes that characters are at most 16 bits wide.
    escape_byte(((C band 16#c0) bsr 6) + 16#c0)
	++ escape_byte(C band 16#3f + 16#80)
	++ escape_uri(Cs);
escape_uri([C | Cs]) ->
    escape_byte(C) ++ escape_uri(Cs);
escape_uri([]) ->
    [].

%% copied from `edoc_lib' because dialyzer cannot see this private
%% function there.
%% has a small modification: it uses `=' in place of `%' so that it
%% won't generate invalid paths in windows.
escape_byte(C) when C >= 0, C =< 255 ->
    [$=, hex_digit(C bsr 4), hex_digit(C band 15)].

%% copied from `edoc_lib' because dialyzer cannot see this private
%% function there.
hex_digit(N) when N >= 0, N =< 9 ->
    N + $0;
hex_digit(N) when N > 9, N =< 15 ->
    N + $a - 10.

%% wraps secrets inside connection options to avoid them being stored
%% in plain-text in supervisors child specs and in the process state,
%% only to be printed when there's a crash and restart.
-spec wrap_secrets(#{conn_opts := #{auth_data => term(), any() => term()}, any() => term()}) ->
          #{conn_opts := #{auth_data => function(), any() => term()}, any() => term()}.
wrap_secrets(Opts0 = #{conn_opts := ConnOpts0 = #{auth_data := AuthData}}) ->
    ConnOpts = ConnOpts0#{auth_data := secret:wrap(AuthData)},
    Opts0#{conn_opts := ConnOpts};
wrap_secrets(Opts) ->
    Opts.

-spec unwrap_conn_opts(#{auth_data => function(), any() => term()}) ->
          #{auth_data => term(), any() => term()}.
unwrap_conn_opts(ConnOpts0 = #{auth_data := AuthDataFn})
  when is_function(AuthDataFn, 0) ->
    AuthData = secret:unwrap(AuthDataFn),
    ConnOpts0#{auth_data := AuthData};
unwrap_conn_opts(Opts) ->
    Opts.
