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

-export([ send_connect/2
        , ping/1
        , pong/1
        ]).

send_connect(Sock, undefined) ->
    gen_tcp:send(Sock, pulsar_protocol_frame:connect());
send_connect(Sock, ProxyToBrokerUrl) ->
    Fields = #{proxy_to_broker_url => uri_to_url(ProxyToBrokerUrl)},
    gen_tcp:send(Sock, pulsar_protocol_frame:connect(Fields)).

ping(Sock) ->
    gen_tcp:send(Sock, pulsar_protocol_frame:ping()).

pong(Sock) ->
    gen_tcp:send(Sock, pulsar_protocol_frame:pong()).

uri_to_url(URI) ->
    case string:split(URI, "://", leading) of
        [URI] -> URI;
        [_Scheme, URL] -> URL
    end.
