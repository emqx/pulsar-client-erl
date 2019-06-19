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

-include ("PulsarApi_pb.hrl").

-export([start_link/3]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export ([ get_topic_metadata/2
         , lookup_topic/2
         ]).

-record(state, {sock, servers, opts, producers = #{}, request_id = 0, requests = #{}}).

-define(TIMEOUT, 60000).

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
    gen_server:call(Pid, {get_topic_metadata, Topic, Call}).

lookup_topic(Pid, PartitionTopic) ->
    gen_server:call(Pid, {lookup_topic, PartitionTopic}).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------
init([Servers, Opts]) ->
    State = #state{servers = Servers, opts = Opts},
    case try_connect(Servers) of
        error ->
            {error, fail_to_connect_pulser_server};
        Sock ->
            {ok, State#state{sock = Sock}}
    end.

handle_call({get_topic_metadata, Topic, Call}, From, State = #state{sock = Sock,
                                                                    request_id = RequestId,
                                                                    requests = Reqs,
                                                                    producers = Producers}) ->
    Metadata = topic_metadata(Sock, Topic, RequestId),
    {noreply, State#state{requests = maps:put(RequestId, {From, Metadata}, Reqs),
                          producers = maps:put(Topic, Call, Producers)}};

handle_call({lookup_topic, PartitionTopic}, From, State = #state{sock = Sock,
                                                                 request_id = RequestId,
                                                                 requests = Reqs}) ->
    LookupTopic = lookup_topic(Sock, PartitionTopic, RequestId),
    {noreply, State#state{requests = maps:put(RequestId, {From, LookupTopic}, Reqs)}};

handle_call(_Req, _From, State) ->
    {reply, ok, State, hibernate}.

handle_cast(_Req, State) ->
    {noreply, State, hibernate}.

handle_info({tcp, _, Bin}, State) ->
    handle_response(pulsar_protocol_frame:parse(Bin), State);

handle_info(ping, State = #state{sock = Sock}) ->
    ping(Sock),
    {noreply, State, hibernate};

handle_info(_Info, State) ->
    log_error("Receive unknown message:~p~n", [_Info]),
    {noreply, State, hibernate}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

handle_response(#commandconnected{}, State) ->
    start_keepalive(),
    {noreply, next_request_id(State), hibernate};

handle_response(#commandpartitionedtopicmetadataresponse{partitions = Partitions,
                                                         request_id = RequestId},
                State = #state{requests   = Reqs}) ->
    case maps:get(RequestId, Reqs, undefined) of
        {From, #commandpartitionedtopicmetadata{topic = Topic}} ->
            gen_server:reply(From, {Topic, Partitions}),
            {noreply, State#state{requests = maps:remove(RequestId, Reqs)}, hibernate};
        undefined ->
            {noreply, State, hibernate}
    end;

handle_response(#commandlookuptopicresponse{brokerserviceurl = BrokerServiceUrl,
                                            request_id = RequestId},
                State = #state{requests = Reqs}) ->
    case maps:get(RequestId, Reqs, undefined) of
        {From, #commandlookuptopic{}} ->
            gen_server:reply(From, BrokerServiceUrl),
            {noreply, State#state{requests = maps:remove(RequestId, Reqs)}, hibernate};
        undefined ->
            {noreply, State, hibernate}
    end;

handle_response(#commandping{}, State = #state{sock = Sock}) ->
    pong(Sock),
    {noreply, State, hibernate};

handle_response(#commandpong{}, State) ->
    start_keepalive(),
    {noreply, State, hibernate};

handle_response(_Info, State) ->
    log_error("handle_response unknown message:~p~n", [_Info]),
    {noreply, State, hibernate}.

tune_buffer(Sock) ->
    {ok, [{recbuf, RecBuf}, {sndbuf, SndBuf}]}
        = inet:getopts(Sock, [recbuf, sndbuf]),
    inet:setopts(Sock, [{buffer, max(RecBuf, SndBuf)}]).

try_connect([]) ->
    error;
try_connect([{Host, Port} | Servers]) ->
    case gen_tcp:connect(Host, Port, ?TCPOPTIONS, ?TIMEOUT) of
        {ok, Sock} ->
            tune_buffer(Sock),
            gen_tcp:controlling_process(Sock, self()),
            connect(Sock),
            Sock;
        _Error ->
            try_connect(Servers)
    end.

connect(Sock) ->
    Conn = #commandconnect{client_version = "Pulsar-Client-Erlang-v0.0.1",
                           protocol_version = 6},
    gen_tcp:send(Sock, pulsar_protocol_frame:connect(Conn)).

topic_metadata(Sock, Topic, RequestId) ->
    Metadata = #commandpartitionedtopicmetadata{
        topic = Topic,
        request_id = RequestId
    },
    gen_tcp:send(Sock, pulsar_protocol_frame:topic_metadata(Metadata)),
    Metadata.

lookup_topic(Sock, Topic, RequestId) ->
    LookupTopic = #commandlookuptopic{
        topic = Topic,
        request_id = RequestId
    },
    gen_tcp:send(Sock, pulsar_protocol_frame:lookup_topic(LookupTopic)),
    LookupTopic.

start_keepalive() ->
    erlang:send_after(30*1000, self(), ping).

ping(Sock) ->
    gen_tcp:send(Sock, pulsar_protocol_frame:ping()).

pong(Sock) ->
    gen_tcp:send(Sock, pulsar_protocol_frame:pong()).

next_request_id(State = #state{request_id = 65535}) ->
    State#state{request_id = 1};
next_request_id(State = #state{request_id = RequestId}) ->
    State#state{request_id = RequestId+1}.