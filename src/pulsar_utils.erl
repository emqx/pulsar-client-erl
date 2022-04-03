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
        , parse_uri/1
        , maybe_enable_ssl_opts/2
        ]).

-export([collect_send_calls/1]).

merge_opts([Opts1, Opts2]) ->
    proplist_diff(Opts1, Opts2) ++ Opts2;
merge_opts([Opts1 | RemOpts]) ->
    merge_opts([Opts1, merge_opts(RemOpts)]).

parse_uri(URI) ->
    case string:split(URI, "://") of
        ["pulsar+ssl", URL] -> {ssl, parse_url(URL)};
        ["pulsar", URL] -> {tcp, parse_url(URL)};
        [Scheme, _] -> error({invalid_scheme, Scheme});
        [URI] -> {tcp, parse_url(URI)}
    end.

parse_url(URL) ->
    case string:lexemes(URL, ": ") of
        [Host, Port] -> {Host, list_to_integer(Port)};
        [Host] -> {Host, 6650}
    end.

maybe_enable_ssl_opts(tcp, Opts) -> Opts#{enable_ssl => false};
maybe_enable_ssl_opts(ssl, Opts) -> Opts#{enable_ssl => true}.

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