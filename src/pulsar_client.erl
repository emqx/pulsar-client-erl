%% Copyright (c) 2013-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        , format_status/1
        , format_status/2
        ]).

-export([ get_topic_metadata/2
        , lookup_topic/2
        , lookup_topic/3
        , lookup_topic_async/2
        ]).

-export([ get_status/1
        , get_alive_pulsar_url/1
        ]).

-export([try_initial_connection/3]).

%% Internal export for tests
-export([handle_response/2]).

%%--------------------------------------------------------------------
%% Type definitions
%%--------------------------------------------------------------------

-record(state, { client_id
               , sock
               , server
               , opts
               , request_id = 0
               , requests = #{}
               , from
               , last_bin = <<>>
               , extra = #{}
               }).

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
-define(CONN_TIMEOUT, 30000).

%% calls/casts/infos
-record(get_topic_metadata, {topic :: pulsar_client_manager:topic()}).
-record(lookup_topic, {partition_topic, opts = #{}}).
-record(lookup_topic_async, {from, partition_topic, opts = #{}}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(ClientId, Server, Opts) ->
    gen_server:start_link(?MODULE, [ClientId, Server, Opts], []).

get_topic_metadata(Pid, Topic) ->
    gen_server:call(Pid, #get_topic_metadata{topic = Topic}, 30_000).

lookup_topic(Pid, PartitionTopic) ->
    lookup_topic(Pid, PartitionTopic, _Opts = #{}).

lookup_topic(Pid, PartitionTopic, Opts) ->
    gen_server:call(Pid, #lookup_topic{partition_topic = PartitionTopic, opts = Opts}, 30_000).

-spec lookup_topic_async(server_ref(), binary()) -> {ok, reference()}.
lookup_topic_async(Pid, PartitionTopic) ->
    lookup_topic_async(Pid, PartitionTopic, _Opts = #{}).

lookup_topic_async(Pid, PartitionTopic, Opts) ->
    Ref = monitor(process, Pid, [{alias, reply_demonitor}]),
    From = {self(), Ref},
    gen_server:cast(Pid, #lookup_topic_async{from = From, partition_topic = PartitionTopic, opts = Opts}),
    {ok, Ref}.

get_status(Pid) ->
    gen_server:call(Pid, get_status, 5000).

get_alive_pulsar_url(Pid) ->
    gen_server:call(Pid, get_alive_pulsar_url, 5000).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------

init([ClientId, Server, Opts]) ->
    process_flag(trap_exit, true),
    ConnTimeout = maps:get(connect_timeout, Opts, ?CONN_TIMEOUT),
    Parent = self(),
    Pid = spawn_link(fun() -> try_initial_connection(Parent, Server, Opts) end),
    TRef = erlang:send_after(ConnTimeout, self(), timeout),
    Result = wait_for_socket_and_opts(ClientId, Server, Pid, timeout),
    _ = erlang:cancel_timer(TRef),
    exit(Pid, kill),
    receive
        timeout -> ok
    after 0 ->
        ok
    end,
    receive
        {'EXIT', Pid, _Error} -> ok
    after 0 ->
        ok
    end,
    Result.

wait_for_socket_and_opts(ClientId, Server, Pid, LastError) ->
    receive
        {Pid, {ok, {Sock, Opts}}} ->
            State = #state{client_id = ClientId, sock = Sock, server = Server, opts = Opts},
            {ok, State};
        {Pid, {error, Error}} ->
            case contains_authn_error(Error) of
                true ->
                    log_error("authentication error starting pulsar client: ~p", [Error]),
                    {stop, Error};
                false ->
                    wait_for_socket_and_opts(ClientId, Server, Pid, Error)
            end;
        timeout ->
            log_error("timed out when starting pulsar client; last error: ~p", [LastError]),
            {stop, LastError}
    end.

contains_authn_error(#{error := 'AuthenticationError'}) -> true;
contains_authn_error(_Reason) -> false.

handle_call(#get_topic_metadata{topic = Topic}, From,
        State = #state{
            sock = Sock,
            opts = Opts,
            request_id = RequestId,
            requests = Reqs,
            server = Server
        }) ->
    case get_alive_sock_opts(Server, Sock, Opts) of
        {error, Reason} ->
            log_error("get_topic_metadata from pulsar servers failed: ~p", [Reason]),
            Reply = {error, unavailable},
            {stop, {shutdown, Reason}, Reply, State};
        {ok, {Sock1, Opts1}} ->
            pulsar_socket:send_topic_metadata_packet(Sock1, Topic, RequestId, Opts1),
            {noreply, next_request_id(State#state{
                requests = maps:put(RequestId, {From, Topic}, Reqs),
                sock = Sock1,
                opts = Opts1
            })}
    end;
handle_call(#lookup_topic{partition_topic = Topic, opts = ReqOpts}, From,
        State = #state{
            sock = Sock,
            opts = Opts,
            request_id = RequestId,
            requests = Reqs,
            server = Server
        }) ->
    case get_alive_sock_opts(Server, Sock, Opts) of
        {error, Reason} ->
            log_error("lookup_topic from pulsar failed: ~0p down", [Reason]),
            {stop, {shutdown, Reason}, {error, unavailable}, State};
        {ok, {Sock1, Opts1}} ->
            pulsar_socket:send_lookup_topic_packet(Sock1, Topic, RequestId, ReqOpts, Opts1),
            {noreply, next_request_id(State#state{
                requests = maps:put(RequestId, {From, Topic}, Reqs),
                sock = Sock1,
                opts = Opts1
            })}
    end;
handle_call(get_status, From, State = #state{sock = undefined, opts = Opts, server = Server}) ->
    case get_alive_sock_opts(Server, undefined, Opts) of
        {error, Reason} ->
            log_error("get_status from pulsar failed: ~0p", [Reason]),
            {stop, {shutdown, Reason}, false, State};
        {ok, {Sock, Opts1}} ->
            IsHealthy = not is_pong_longtime_no_received(),
            case IsHealthy of
                true ->
                    {reply, IsHealthy, State#state{from = From, sock = Sock, opts = Opts1}};
                false ->
                    {stop, {shutdown, no_pong_received}, IsHealthy, State}
            end
    end;
handle_call(get_status, _From, State) ->
    IsHealthy = not is_pong_longtime_no_received(),
    case IsHealthy of
        true ->
            {reply, IsHealthy, State};
        false ->
            {stop, {shutdown, no_pong_received}, IsHealthy, State}
    end;
handle_call(get_alive_pulsar_url, From, State = #state{sock = Sock, opts = Opts, server = Server}) ->
    case get_alive_sock_opts(Server, Sock, Opts) of
        {error, Reason} ->
            {stop, {shutdown, Reason}, {error, Reason}, State};
        {ok, {Sock1, Opts1}} ->
            URI = pulsar_socket:get_pulsar_uri(Sock1, Opts),
            {reply, URI, State#state{from = From, sock = Sock1, opts = Opts1}}
    end;
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State, hibernate}.

handle_cast(#lookup_topic_async{from = From, partition_topic = PartitionTopic, opts = Opts}, State) ->
    %% re-use the same logic as the call, as the process of looking up
    %% a topic is itself async in the gen_server:call.
    self() ! {'$gen_call', From, #lookup_topic{partition_topic = PartitionTopic, opts = Opts}},
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State, hibernate}.

handle_info({Transport, Sock, Bin}, State = #state{sock = Sock, last_bin = LastBin})
        when Transport == tcp; Transport == ssl ->
    {noreply, parse_packet(pulsar_protocol_frame:parse(<<LastBin/binary, Bin/binary>>), State)};
handle_info({Error, Sock, Reason}, State = #state{sock = Sock})
        when Error == ssl_error; Error == tcp_error ->
    log_error("transport layer error: ~p", [Reason]),
    {stop, {shutdown, Reason}, State#state{sock = undefined}};
handle_info({Closed, Sock}, State = #state{sock = Sock})
        when Closed == tcp_closed; Closed == ssl_closed ->
    log_error("connection closed by peer", []),
    {stop, {shutdown, connection_closed}, State#state{sock = undefined}};
handle_info(ping, State = #state{sock = undefined, opts = Opts, server = Server}) ->
    case get_alive_sock_opts(Server, undefined, Opts) of
        {error, Reason} ->
            log_error("ping to pulsar servers failed: ~p", [Reason]),
            {stop, {shutdown, Reason}, State};
        {ok, {Sock, Opts1}} ->
            pulsar_socket:ping(Sock, Opts1),
            start_check_pong_timeout(),
            {noreply, State#state{sock = Sock, opts = Opts1}, hibernate}
    end;
handle_info(ping, State = #state{sock = Sock, opts = Opts}) ->
    pulsar_socket:ping(Sock, Opts),
    start_check_pong_timeout(),
    {noreply, State, hibernate};
handle_info(check_pong, State) ->
    case is_pong_longtime_no_received() of
        true ->
            {stop, {shutdown, no_pong_received}, State};
        false ->
            {noreply, State, hibernate}
    end;
handle_info(_Info, State) ->
    log_warning("received unknown message: ~p", [_Info]),
    {noreply, State, hibernate}.

terminate(_Reason, #state{sock = undefined}) ->
    ok;
terminate(_Reason, #state{sock = Sock, opts = Opts}) ->
    _ = pulsar_socket:close(Sock, Opts),
    ok.

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
    ?MODULE:handle_response(Cmd, State#state{last_bin = <<>>});
parse_packet({Cmd, LastBin}, State) ->
    State2 = ?MODULE:handle_response(Cmd, State),
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
handle_response({lookupTopicResponse, #{response := 'Redirect'} = Response}, State0) ->
    #state{requests = Requests0} = State0,
    #{request_id := RequestId} = Response,
    case maps:take(RequestId, Requests0) of
        {{From, _Topic}, Requests} ->
            State = State0#state{requests = Requests},
            handle_redirect_lookup_response(State, From, Response);
        error ->
            State0
    end;
handle_response({lookupTopicResponse, #{request_id := RequestId, response := 'Connect'} = Response},
                State = #state{requests = Reqs, opts = Opts}) ->
    case maps:get(RequestId, Reqs, undefined) of
        {From, _} ->
            ServiceURL = get_service_url_from_lookup_response(Response, Opts),
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

get_alive_sock_opts(Server, undefined, Opts) ->
    try_connect(Server, Opts);
get_alive_sock_opts(Server, Sock, Opts) ->
    case pulsar_socket:getstat(Sock, Opts) of
        {ok, _} ->
            {ok, {Sock, Opts}};
        {error, _} ->
            try_connect(Server, Opts)
    end.

try_connect(URI, Opts0) ->
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
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
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

log_error(Fmt, Args) ->
    do_log(error, Fmt, Args).

log_warning(Fmt, Args) ->
    do_log(warning, Fmt, Args).

do_log(Level, Fmt, Args) ->
    logger:log(Level, "[pulsar-client] " ++ Fmt, Args, #{domain => [pulsar, client_worker]}).

%% we use the same ping workflow as it attempts the connection
start_keepalive() ->
    erlang:send_after(?PING_INTERVAL, self(), ping).

start_check_pong_timeout() ->
    erlang:send_after(?PONG_TIMEOUT, self(), check_pong).

%% TODO: use explicit state instead of dictionary
pong_received() ->
    _ = erlang:put(?PONG_TS, now_ts()),
    ok.

%% TODO: use explicit state instead of dictionary
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

try_initial_connection(Parent, Server, Opts) ->
    case get_alive_sock_opts(Server, undefined, Opts) of
        {error, Reason} ->
            %% to avoid a hot restart loop leading to max restart
            %% intensity when pulsar (or the connection to it) is
            %% having a bad day...
            Parent ! {self(), {error, Reason}},
            timer:sleep(100),
            ?MODULE:try_initial_connection(Parent, Server, Opts);
        {ok, {Sock, Opts1}} ->
            pulsar_socket:controlling_process(Sock, Parent, Opts1),
            Parent ! {self(), {ok, {Sock, Opts1}}}
    end.

get_service_url_from_lookup_response(Response, Opts) ->
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
    end.

%% If we receive a response of lookup type `Redirect', we must re-issue the lookup
%% connecting to the returned broker.
%% https://pulsar.apache.org/docs/2.11.x/developing-binary-protocol/#lookuptopicresponse
handle_redirect_lookup_response(State, From, Response) ->
    #state{opts = Opts} = State,
    ServiceURL = get_service_url_from_lookup_response(Response, Opts),
    ReqOpts = #{authoritative => maps:get(authoritative, Response, false)},
    gen_server:reply(From, {redirect, ServiceURL, ReqOpts}),
    State.
