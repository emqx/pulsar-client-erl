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

-module(pulsar_socket).

-export([ peername/2
        , tune_buffer/2
        , connect/3
        ]).

-export([ send_connect_packet/3
        , ping/2
        , pong/2
        ]).

-define(SEND_TIMEOUT, 60000).
-define(CONN_TIMEOUT, 30000).

-define(INTERNAL_TCP_OPTS,
    [ binary
    , {packet, raw}
    , {reuseaddr, true}
    , {active, true}
    , {reuseaddr, true}
    ]).

-define(DEF_TCP_OPTS,
    [ {nodelay, true}
    , {send_timeout, ?SEND_TIMEOUT}
    ]).

send_connect_packet(Sock, undefined, Opts) ->
    Mod = tcp_module(Opts),
    Mod:send(Sock, pulsar_protocol_frame:connect());
send_connect_packet(Sock, ProxyToBrokerUrl, Opts) ->
    Mod = tcp_module(Opts),
    Fields = #{proxy_to_broker_url => uri_to_url(ProxyToBrokerUrl)},
    Mod:send(Sock, pulsar_protocol_frame:connect(Fields)).

ping(Sock, Opts) ->
    Mod = tcp_module(Opts),
    Mod:send(Sock, pulsar_protocol_frame:ping()).

pong(Sock, Opts) ->
    Mod = tcp_module(Opts),
    Mod:send(Sock, pulsar_protocol_frame:pong()).

connect(Host, Port, Opts) ->
    TcpMod = tcp_module(Opts),
    {ConnOpts, Timeout} = connect_opts(Opts),
    case TcpMod:connect(Host, Port, ConnOpts, Timeout) of
        {ok, Sock} ->
            tune_buffer(inet_module(Opts), Sock),
            TcpMod:controlling_process(Sock, self()),
            {ok, Sock};
        {error, _} = Error ->
            Error
    end.

connect_opts(Opts) ->
    TcpOpts = maps:get(tcp_opts, Opts, []),
    SslOpts = maps:get(ssl_opts, Opts, []),
    ConnTimeout = maps:get(connect_timeout, Opts, ?CONN_TIMEOUT),
    {pulsar_utils:merge_opts([?DEF_TCP_OPTS, TcpOpts, SslOpts, ?INTERNAL_TCP_OPTS]),
     ConnTimeout}.

peername(Sock, Opts) ->
    Mod = inet_module(Opts),
    Mod:peername(Sock).

tcp_module(Opts) ->
    case maps:find(ssl_opts, Opts) of
        error -> gen_tcp;
        {ok, _} -> ssl
    end.

inet_module(Opts) ->
    case maps:find(ssl_opts, Opts) of
        error -> inet;
        {ok, _} -> ssl
    end.

tune_buffer(InetM, Sock) ->
    {ok, [{recbuf, RecBuf}, {sndbuf, SndBuf}]}
        = InetM:getopts(Sock, [recbuf, sndbuf]),
    InetM:setopts(Sock, [{buffer, max(RecBuf, SndBuf)}]).

uri_to_url(URI) ->
    case string:split(URI, "://", leading) of
        [URI] -> URI;
        [_Scheme, URL] -> URL
    end.
