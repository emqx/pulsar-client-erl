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
        , format_status/1
        , format_status/2
        ]).

-export([ get_topic_metadata/2
        , lookup_topic/2
        , lookup_topic_async/2
        ]).

-export([ get_status/1
        , get_alive_pulsar_url/1
        ]).

-record(state, {sock, servers, opts, producers = #{}, request_id = 0, requests = #{}, from, last_bin = <<>>}).

-export_type([lookup_topic_response/0]).
-type lookup_topic_response() :: {ok, #{ brokerServiceUrl => string()
                                       , proxy_through_service_url => boolean()
                                       }}
                               | {error, #{error => term(), message => term()}}.
%% `gen_server:server_ref()' exists only on OTP 25+
-type server_ref() ::
        pid() |
        (LocalName :: atom()) |
        {Name :: atom(), Node :: atom()} |
        {global, GlobalName :: term()} |
        {via, RegMod :: module(), ViaName :: term()}.

-define(PING_INTERVAL, 30000). %% 30s
-define(PONG_TS, {pulsar_rcvd, pong}).
-define(PONG_TIMEOUT, ?PING_INTERVAL * 2). %% 60s
-define(RECONNECT_TIMEOUT, 5_000).

start_link(ClientId, Servers, Opts) ->
    gen_server:start_link({local, ClientId}, ?MODULE, [Servers, Opts], []).

get_topic_metadata(Pid, Topic) ->
    Call = self(),
    gen_server:call(Pid, {get_topic_metadata, Topic, Call}, 30000).

lookup_topic(Pid, PartitionTopic) ->
    gen_server:call(Pid, {lookup_topic, PartitionTopic}, 30000).

-spec lookup_topic_async(server_ref(), binary()) -> {ok, reference()}.
lookup_topic_async(Pid, PartitionTopic) ->
    Ref = monitor(process, Pid, [{alias, reply_demonitor}]),
    From = {self(), Ref},
    gen_server:cast(Pid, {lookup_topic_async, From, PartitionTopic}),
    {ok, Ref}.

get_status(Pid) ->
    gen_server:call(Pid, get_status, 5000).

get_alive_pulsar_url(Pid) ->
    gen_server:call(Pid, get_alive_pulsar_url, 5000).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------

init([Servers, Opts]) ->
    State = #state{servers = Servers, opts = Opts},
    case get_alive_sock_opts(Servers, undefined, Opts) of
        {error, Reason} ->
            {stop, Reason};
        {ok, {Sock, Opts1}} ->
            {ok, State#state{sock = Sock, opts = Opts1}}
    end.

handle_call({get_topic_metadata, Topic, Call}, From,
        State = #state{
            sock = Sock,
            opts = Opts,
            request_id = RequestId,
            requests = Reqs,
            producers = Producers,
            servers = Servers
        }) ->
    case get_alive_sock_opts(Servers, Sock, Opts) of
        {error, Reason} ->
            log_error("get_topic_metadata from pulsar servers failed: ~p", [Reason]),
            start_reconnect_timer(),
            {reply, {error, no_servers_available}, State};
        {ok, {Sock1, Opts1}} ->
            pulsar_socket:send_topic_metadata_packet(Sock1, Topic, RequestId, Opts1),
            {noreply, next_request_id(State#state{
                requests = maps:put(RequestId, {From, Topic}, Reqs),
                producers = maps:put(Topic, Call, Producers),
                sock = Sock1,
                opts = Opts1
            })}
    end;
handle_call({lookup_topic, Topic}, From,
        State = #state{
            sock = Sock,
            opts = Opts,
            request_id = RequestId,
            requests = Reqs,
            servers = Servers
        }) ->
    case get_alive_sock_opts(Servers, Sock, Opts) of
        {error, Reason} ->
            log_error("lookup_topic from pulsar failed: ~p down", [Reason]),
            start_reconnect_timer(),
            {reply, {error, no_servers_available}, State};
        {ok, {Sock1, Opts1}} ->
            pulsar_socket:send_lookup_topic_packet(Sock1, Topic, RequestId, Opts1),
            {noreply, next_request_id(State#state{
                requests = maps:put(RequestId, {From, Topic}, Reqs),
                sock = Sock1,
                opts = Opts1
            })}
    end;
handle_call(get_status, From, State = #state{sock = undefined, opts = Opts, servers = Servers}) ->
    case get_alive_sock_opts(Servers, undefined, Opts) of
        {error, Reason} ->
            log_error("get_status from pulsar failed: ~p", [Reason]),
            start_reconnect_timer(),
            {reply, false, State};
        {ok, {Sock, Opts1}} ->
            {reply, not is_pong_longtime_no_received(),
                State#state{from = From, sock = Sock, opts = Opts1}}
    end;
handle_call(get_status, _From, State) ->
    {reply, not is_pong_longtime_no_received(), State};
handle_call(get_alive_pulsar_url, From, State = #state{sock = Sock, opts = Opts, servers = Servers}) ->
    case get_alive_sock_opts(Servers, Sock, Opts) of
        {error, _Reason} ->
            start_reconnect_timer(),
            {reply, {error, no_servers_available}, State};
        {ok, {Sock1, Opts1}} ->
            {reply, pulsar_socket:get_pulsar_uri(Sock1, Opts),
                State#state{from = From, sock = Sock1, opts = Opts1}}
    end;
handle_call(_Req, _From, State) ->
    {reply, ok, State, hibernate}.


handle_cast({lookup_topic_async, From, PartitionTopic}, State) ->
    %% re-use the same logic as the call, as the process of looking up
    %% a topic is itself async in the gen_server:call.
    self() ! {'$gen_call', From, {lookup_topic, PartitionTopic}},
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State, hibernate}.


handle_info({Transport, Sock, Bin}, State = #state{sock = Sock, last_bin = LastBin})
        when Transport == tcp; Transport == ssl ->
    {noreply, parse_packet(pulsar_protocol_frame:parse(<<LastBin/binary, Bin/binary>>), State)};
handle_info({Error, Sock, Reason}, State = #state{sock = Sock})
        when Error == ssl_error; Error == tcp_error ->
    log_error("transport layer error: ~p", [Reason]),
    start_reconnect_timer(),
    {noreply, State#state{sock = undefined}, hibernate};

handle_info({Closed, Sock}, State = #state{sock = Sock})
        when Closed == tcp_closed; Closed == ssl_closed ->
    log_error("connection closed by peer", []),
    start_reconnect_timer(),
    {noreply, State#state{sock = undefined}, hibernate};
handle_info(ping, State = #state{sock = undefined, opts = Opts, servers = Servers}) ->
    case get_alive_sock_opts(Servers, undefined, Opts) of
        {error, Reason} ->
            log_error("ping to pulsar servers failed: ~p", [Reason]),
            start_reconnect_timer(),
            {noreply, State, hibernate};
        {ok, {Sock, Opts1}} ->
            pulsar_socket:ping(Sock, Opts1),
            {noreply, State#state{sock = Sock, opts = Opts1}, hibernate}
    end;
handle_info(ping, State = #state{sock = Sock, opts = Opts}) ->
    pulsar_socket:ping(Sock, Opts),
    {noreply, State, hibernate};
handle_info(_Info, State) ->
    log_error("received unknown message: ~p", [_Info]),
    {noreply, State, hibernate}.

terminate(_Reason, #state{sock = undefined}) ->
    ok;
terminate(_Reason, #state{sock = Sock, opts = Opts}) ->
    _ = pulsar_socket:close(Sock, Opts),
    ok.

code_change(_, State, _) ->
    {ok, State}.

format_status(Status) ->
    maps:map(
      fun(state, State0) ->
              censor_secrets(State0);
         (_Key, Value)->
              Value
      end,
      Status).

%% `format_status/2' is deprecated as of OTP 25.0
format_status(_Opt, [_PDict, State0]) ->
    State = censor_secrets(State0),
    [{data, [{"State", State}]}].

censor_secrets(State0 = #state{opts = Opts0 = #{conn_opts := ConnOpts0 = #{auth_data := _}}}) ->
    State0#state{opts = Opts0#{conn_opts := ConnOpts0#{auth_data := "******"}}};
censor_secrets(State) ->
    State.

parse_packet({incomplete, Bin}, State) ->
    State#state{last_bin = Bin};
parse_packet({Cmd, <<>>}, State) ->
    handle_response(Cmd, State#state{last_bin = <<>>});
parse_packet({Cmd, LastBin}, State) ->
    State2 = handle_response(Cmd, State),
    parse_packet(pulsar_protocol_frame:parse(LastBin), State2).

handle_response({connected, _ConnectedData}, State = #state{from = undefined}) ->
    start_keepalive(),
    State;
handle_response({connected, _ConnectedData}, State = #state{from = From}) ->
    start_keepalive(),
    gen_server:reply(From, true),
    State#state{from = undefined};
handle_response({partitionMetadataResponse, #{error := Reason, message := Msg,
                                        request_id := RequestId, response := 'Failed'}},
                State = #state{requests = Reqs}) ->
    case maps:get(RequestId, Reqs, undefined) of
        {From, _} ->
            gen_server:reply(From, {error, #{error => Reason, message => Msg}}),
            State#state{requests = maps:remove(RequestId, Reqs)};
        undefined ->
            State
    end;
handle_response({partitionMetadataResponse, #{partitions := Partitions,
                                              request_id := RequestId}},
                State = #state{requests = Reqs}) ->
    case maps:get(RequestId, Reqs, undefined) of
        {From, Topic} ->
            gen_server:reply(From, {ok, {Topic, Partitions}}),
            State#state{requests = maps:remove(RequestId, Reqs)};
        undefined ->
            State
    end;
handle_response({lookupTopicResponse, #{error := Reason, message := Msg,
                                        request_id := RequestId, response := 'Failed'}},
                State = #state{requests = Reqs}) ->
    case maps:get(RequestId, Reqs, undefined) of
        {From, _} ->
            gen_server:reply(From, {error, #{error => Reason, message => Msg}}),
            State#state{requests = maps:remove(RequestId, Reqs)};
        undefined ->
            State
    end;
handle_response({lookupTopicResponse, #{request_id := RequestId} = Response},
                State = #state{requests = Reqs, opts = Opts}) ->
    case maps:get(RequestId, Reqs, undefined) of
        {From, _} ->
            ServiceURL =
                case {Opts, Response} of
                    {#{enable_ssl := true}, #{brokerServiceUrlTls := BrokerServiceUrlTls}} ->
                        BrokerServiceUrlTls;
                    {#{enable_ssl := true}, #{brokerServiceUrl := BrokerServiceUrl}} ->
                        log_error("SSL enabled but brokerServiceUrlTls is not provided by pulsar,"
                                  " falling back to brokerServiceUrl: ~p", [BrokerServiceUrl]),
                        BrokerServiceUrl;
                    {_, #{brokerServiceUrl := BrokerServiceUrl}} ->
                        %% the 'brokerServiceUrl' is a mandatory field in case the SSL is disabled
                        BrokerServiceUrl
                end,
            gen_server:reply(From, {ok,
                #{ brokerServiceUrl => ServiceURL
                 , proxy_through_service_url => maps:get(proxy_through_service_url, Response, false)
                 }}),
            State#state{requests = maps:remove(RequestId, Reqs)};
        undefined ->
            State
    end;
handle_response({ping, #{}}, State = #state{sock = Sock, opts = Opts}) ->
    pulsar_socket:pong(Sock, Opts),
    State;
handle_response({pong, #{}}, State) ->
    pong_received(),
    start_keepalive(),
    State;
handle_response(_Info, State) ->
    log_error("handle unknown response: ~p", [_Info]),
    State.

get_alive_sock_opts(Servers, undefined, Opts) ->
    try_connect(Servers, Opts);
get_alive_sock_opts(Servers, Sock, Opts) ->
    case pulsar_socket:getstat(Sock, Opts) of
        {ok, _} ->
            {ok, {Sock, Opts}};
        {error, _} ->
            try_connect(Servers, Opts)
    end.

try_connect(Servers, Opts) ->
    do_try_connect(Servers, Opts, #{}).

do_try_connect([], _Opts, Res) ->
    {error, Res};
do_try_connect([URI | Servers], Opts0, Res) ->
    {Type, {Host, Port}} = pulsar_utils:parse_url(URI),
    Opts = pulsar_utils:maybe_enable_ssl_opts(Type, Opts0),
    case pulsar_socket:connect(Host, Port, Opts) of
        {ok, Sock} ->
            pulsar_socket:send_connect_packet(Sock, Opts),
            case wait_for_conn_response(Sock, Opts) of
                {ok, Result} ->
                    {ok, Result};
                {error, Reason} ->
                    ok = close_socket_and_flush_signals(Sock, Opts),
                    do_try_connect(Servers, Opts, Res#{{Host, Port} => Reason})
            end;
        {error, Reason} ->
            do_try_connect(Servers, Opts, Res#{{Host, Port} => Reason})
    end.

wait_for_conn_response(Sock, Opts) ->
    receive
        {Transport, Sock, Bin} when Transport == tcp; Transport == ssl ->
            case pulsar_protocol_frame:parse(Bin) of
                {{connected, _CommandConnected}, <<>>} ->
                    {ok, {Sock, Opts}};
                {{error, CommandError}, <<>>} ->
                    {error, CommandError}
            end;
        {Error, Sock, Reason} when Error == ssl_error; Error == tcp_error ->
            {error, {Error, Reason}};
        {Closed, Sock} when Closed == tcp_closed; Closed == ssl_closed ->
            {error, Closed}
    after
        15000 ->
            {error, wait_connect_response_timeout}
    end.

next_request_id(State = #state{request_id = 65535}) ->
    State#state{request_id = 1};
next_request_id(State = #state{request_id = RequestId}) ->
    State#state{request_id = RequestId+1}.

log_error(Fmt, Args) -> logger:error("[pulsar-client] " ++ Fmt, Args).

%% we use the same ping workflow as it attempts the connection
start_reconnect_timer() ->
    erlang:send_after(?RECONNECT_TIMEOUT, self(), ping).

start_keepalive() ->
    erlang:send_after(?PING_INTERVAL, self(), ping).

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

%% close sockt and flush socket error and closed signals
close_socket_and_flush_signals(Sock, Opts) ->
    _ = pulsar_socket:close(Sock, Opts),
    receive
        {Transport, Sock, _} when Transport == tcp; Transport == ssl ->
            %% race condition
            ok;
        {Error, Sock, _Reason} when Error == ssl_error; Error == tcp_error ->
            ok;
        {Closed, Sock} when Closed == tcp_closed; Closed == ssl_closed ->
            ok
    after
        0 ->
            ok
    end.
