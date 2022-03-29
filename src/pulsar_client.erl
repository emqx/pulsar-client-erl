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

-module(pulsar_client).

-behaviour(gen_server).

-export([start_link/3]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([ get_topic_metadata/2
        , lookup_topic/2
        ]).

-export([ get_status/1
        , get_server/1
        ]).

-record(state, {sock, servers, opts, producers = #{}, request_id = 0, requests = #{}, from, last_bin = <<>>}).

-define(TIMEOUT, 60000).
-define(PING_INTERVAL, 30000). %% 30s
-define(PONG_TS, {pulsar_rcvd, pong}).
-define(PONG_TIMEOUT, ?PING_INTERVAL * 2). %% 60s

-define(TCPOPTIONS, [
    binary,
    {packet,    raw},
    {reuseaddr, true},
    {nodelay,   true},
    {active,    true},
    {reuseaddr, true},
    {send_timeout,  ?TIMEOUT}]).

start_link(ClientId, Servers, Opts) ->
    gen_server:start_link({local, ClientId}, ?MODULE, [Servers, Opts], []).

get_topic_metadata(Pid, Topic) ->
    Call = self(),
    gen_server:call(Pid, {get_topic_metadata, Topic, Call}, 30000).

lookup_topic(Pid, PartitionTopic) ->
    gen_server:call(Pid, {lookup_topic, PartitionTopic}, 30000).

get_status(Pid) ->
    gen_server:call(Pid, get_status, 5000).

get_server(Pid) ->
    gen_server:call(Pid, get_server, 5000).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------
init([Servers, Opts]) ->
    State = #state{servers = Servers, opts = Opts},
    case get_sock(Servers, undefined) of
        {error, Reason} ->
            {error, Reason};
        {ok, Sock} ->
            {ok, State#state{sock = Sock}}
    end.

handle_call({get_topic_metadata, Topic, Call}, From, State = #state{sock = Sock,
                                                                    request_id = RequestId,
                                                                    requests = Reqs,
                                                                    producers = Producers,
                                                                    servers = Servers}) ->
    case get_sock(Servers, Sock) of
        {error, Reason} ->
            log_error("get_topic_metadata from pulsar servers failed: ~p", [Reason]),
            {noreply, State};
        {ok, Sock1} ->
            Metadata = topic_metadata(Topic, RequestId),
            gen_tcp:send(Sock1, pulsar_protocol_frame:topic_metadata(Metadata)),
            {noreply, next_request_id(State#state{requests = maps:put(RequestId, {From, Metadata}, Reqs),
                                                  producers = maps:put(Topic, Call, Producers),
                                                  sock = Sock1})}
    end;

handle_call({lookup_topic, PartitionTopic}, From, State = #state{sock = Sock,
                                                                 request_id = RequestId,
                                                                 requests = Reqs,
                                                                 servers = Servers}) ->
    case get_sock(Servers, Sock) of
        {error, Reason} ->
            log_error("lookup_topic from pulsar failed: ~p down", [Reason]),
            {noreply, State};
        {ok, Sock1} ->
            LookupTopic = lookup_topic_cmd(PartitionTopic, RequestId),
            gen_tcp:send(Sock, pulsar_protocol_frame:lookup_topic(LookupTopic)),
            {noreply, next_request_id(State#state{
                requests = maps:put(RequestId, {From, LookupTopic}, Reqs),
                sock = Sock1
            })}
    end;

handle_call(get_status, From, State = #state{sock = undefined, servers = Servers}) ->
    case get_sock(Servers, undefined) of
        {error, _Reason} -> {reply, false, State};
        {ok, Sock} -> {noreply, State#state{from = From, sock = Sock}}
    end;
handle_call(get_status, _From, State) ->
    {reply, not is_pong_longtime_no_received(), State};

handle_call(get_server, From, State = #state{sock = Sock, servers = Servers}) ->
    case get_sock(Servers, Sock) of
        {error, _Reason} -> {reply, {error, no_servers_avaliable}, State};
        {ok, Sock1} ->
            {reply, inet:peername(Sock1), State#state{from = From, sock = Sock1}}
    end;

handle_call(_Req, _From, State) ->
    {reply, ok, State, hibernate}.

handle_cast(_Req, State) ->
    {noreply, State, hibernate}.

handle_info({tcp, _, Bin}, State = #state{last_bin = LastBin}) ->
    parse(pulsar_protocol_frame:parse(<<LastBin/binary, Bin/binary>>), State);

handle_info({tcp_closed, Sock}, State = #state{sock = Sock}) ->
    {noreply, State#state{sock = undefined}, hibernate};

handle_info(ping, State = #state{sock = undefined, servers = Servers}) ->
    case get_sock(Servers, undefined) of
        {error, Reason} ->
            log_error("ping to pulsar servers failed: ~p", [Reason]),
            {noreply, State, hibernate};
        {ok, Sock} ->
            ping(Sock),
            {noreply, State#state{sock = Sock}, hibernate}
    end;
handle_info(ping, State = #state{sock = Sock}) ->
    ping(Sock),
    {noreply, State, hibernate};

handle_info(_Info, State) ->
    log_error("Pulsar_client Receive unknown message:~p", [_Info]),
    {noreply, State, hibernate}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

parse({undefined, Bin}, State) ->
    {noreply, State#state{last_bin = Bin}};
parse({Cmd, <<>>}, State) ->
    handle_response(Cmd, State#state{last_bin = <<>>});
parse({Cmd, LastBin}, State) ->
    State2 = case handle_response(Cmd, State) of
        {_, State1, _} -> State1
    end,
    parse(pulsar_protocol_frame:parse(LastBin), State2).

handle_response({connected, _ConnectedData}, State = #state{from = undefined}) ->
    start_keepalive(),
    {noreply, State, hibernate};

handle_response({connected, _ConnectedData}, State = #state{from = From}) ->
    start_keepalive(),
    gen_server:reply(From, true),
    {noreply, State#state{from = undefined}, hibernate};

handle_response({partitionMetadataResponse, #{error := Reason, message := Msg,
                                        request_id := RequestId, response := 'Failed'}},
                State = #state{requests = Reqs}) ->
    case maps:get(RequestId, Reqs, undefined) of
        {From, #{}} ->
            gen_server:reply(From, {error, #{error => Reason, message => Msg}}),
            {noreply, State#state{requests = maps:remove(RequestId, Reqs)}, hibernate};
        undefined ->
            {noreply, State, hibernate}
    end;

handle_response({partitionMetadataResponse, #{partitions := Partitions,
                                              request_id := RequestId}},
                State = #state{requests = Reqs}) ->
    case maps:get(RequestId, Reqs, undefined) of
        {From, #{topic := Topic}} ->
            gen_server:reply(From, {ok, {Topic, Partitions}}),
            {noreply, State#state{requests = maps:remove(RequestId, Reqs)}, hibernate};
        undefined ->
            {noreply, State, hibernate}
    end;

handle_response({lookupTopicResponse, #{error := Reason, message := Msg,
                                        request_id := RequestId, response := 'Failed'}},
                State = #state{requests = Reqs}) ->
    case maps:get(RequestId, Reqs, undefined) of
        {From, #{}} ->
            gen_server:reply(From, {error, #{error => Reason, message => Msg}}),
            {noreply, State#state{requests = maps:remove(RequestId, Reqs)}, hibernate};
        undefined ->
            {noreply, State, hibernate}
    end;

handle_response({lookupTopicResponse, #{brokerServiceUrl := BrokerServiceUrl,
                                        request_id := RequestId} = Response},
                State = #state{requests = Reqs}) ->
    case maps:get(RequestId, Reqs, undefined) of
        {From, #{}} ->
            gen_server:reply(From, {ok,
                #{ brokerServiceUrl => BrokerServiceUrl
                 , proxy_through_service_url => maps:get(proxy_through_service_url, Response, false)
                 }}),
            {noreply, State#state{requests = maps:remove(RequestId, Reqs)}, hibernate};
        undefined ->
            {noreply, State, hibernate}
    end;

handle_response({ping, #{}}, State = #state{sock = Sock}) ->
    pong(Sock),
    {noreply, State, hibernate};

handle_response({pong, #{}}, State) ->
    pong_received(),
    start_keepalive(),
    {noreply, State, hibernate};

handle_response(_Info, State) ->
    log_error("Client handle_response unknown message:~p~n", [_Info]),
    {noreply, State, hibernate}.

tune_buffer(Sock) ->
    {ok, [{recbuf, RecBuf}, {sndbuf, SndBuf}]}
        = inet:getopts(Sock, [recbuf, sndbuf]),
    inet:setopts(Sock, [{buffer, max(RecBuf, SndBuf)}]).


get_sock(Servers, undefined) ->
    try_connect(Servers);
get_sock(_Servers, Sock) ->
    {ok, Sock}.

try_connect(Servers) ->
    try_connect(Servers, #{}).

try_connect([], Res) ->
    {error, Res};
try_connect([{Host, Port} | Servers], Res) ->
    case gen_tcp:connect(Host, Port, ?TCPOPTIONS, ?TIMEOUT) of
        {ok, Sock} ->
            tune_buffer(Sock),
            gen_tcp:controlling_process(Sock, self()),
            pulsar_socket:send_connect(Sock, undefined),
            {ok, Sock};
        {error, Reason} ->
            try_connect(Servers, Res#{{Host, Port} => Reason})
    end.

topic_metadata(Topic, RequestId) ->
    #{
        topic => Topic,
        request_id => RequestId
    }.

lookup_topic_cmd(Topic, RequestId) ->
    #{
        topic => Topic,
        request_id => RequestId
    }.

next_request_id(State = #state{request_id = 65535}) ->
    State#state{request_id = 1};
next_request_id(State = #state{request_id = RequestId}) ->
    State#state{request_id = RequestId+1}.

log_error(Fmt, Args) -> logger:error("[pulsar-client] " ++ Fmt, Args).

start_keepalive() ->
    erlang:send_after(?PING_INTERVAL, self(), ping).

ping(Sock) ->
    gen_tcp:send(Sock, pulsar_protocol_frame:ping()).

pong(Sock) ->
    gen_tcp:send(Sock, pulsar_protocol_frame:pong()).

pong_received() ->
    _ = erlang:put(?PONG_TS, now_ts()),
    ok.

is_pong_longtime_no_received() ->
    case erlang:get(?PONG_TS) of
        undefined -> false;
        Ts -> now_ts() - Ts > ?PONG_TIMEOUT
    end.

now_ts() ->
    erlang:system_time(millisecond).
