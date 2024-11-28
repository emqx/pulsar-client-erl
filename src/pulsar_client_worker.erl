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

-module(pulsar_client_worker).

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
        , lookup_topic_async/2
        ]).

-export([ get_status/1
        , get_alive_pulsar_url/1
        ]).

-export([try_initial_connection/3]).

%% Internal export for tests
-export([handle_response/2]).

-record(state, { client_id
               , sock
               , server
               , opts
               , producers = #{}
               , request_id = 0
               , requests = #{}
               , from
               , last_bin = <<>>
               , connection_failure_count = 0
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
-define(RECONNECT_TIMEOUT, 5_000).
-define(MAX_CONNECTION_FAILURES, 10).

%% calls/casts/infos
-record(lookup_topic, {partition_topic, opts = #{}}).
-record(lookup_topic_async, {from, partition_topic, opts = #{}}).

start_link(ClientId, Server, Opts) ->
    gen_server:start_link(?MODULE, [ClientId, Server, Opts], []).

get_topic_metadata(Pid, Topic) ->
    Call = self(),
    gen_server:call(Pid, {get_topic_metadata, Topic, Call}, 30_000).

lookup_topic(Pid, PartitionTopic) ->
    gen_server:call(Pid, #lookup_topic{partition_topic = PartitionTopic}, 30_000).

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
    ok = pulsar_client_manager:register_worker(ClientId, self(), Server),
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

contains_authn_error(BrokerToErrorMap) ->
    Iter = maps:iterator(BrokerToErrorMap),
    do_contains_authn_error(Iter).

do_contains_authn_error(Iter) ->
    case maps:next(Iter) of
        {_HostAndPort, #{error := 'AuthenticationError'}, _NIter} ->
            true;
        {_HostAndPort, _Error, NIter} ->
            do_contains_authn_error(NIter);
        none ->
            false
    end.

handle_call({get_topic_metadata, Topic, Call}, From,
        State0 = #state{
            sock = Sock,
            opts = Opts,
            request_id = RequestId,
            requests = Reqs,
            producers = Producers,
            server = Server
        }) ->
    case get_alive_sock_opts(Server, Sock, Opts) of
        {error, Reason} ->
            log_error("get_topic_metadata from pulsar servers failed: ~p", [Reason]),
            Reply = {error, no_servers_available},
            case handle_connection_failure(State0) of
                stop ->
                    {stop, {shutdown, max_connection_failures_reached}, Reply, State0};
                {continue, State} ->
                    start_reconnect_timer(),
                    {reply, Reply, State}
            end;
        {ok, {Sock1, Opts1}} ->
            State = connection_succeeded(State0),
            pulsar_socket:send_topic_metadata_packet(Sock1, Topic, RequestId, Opts1),
            {noreply, next_request_id(State#state{
                requests = maps:put(RequestId, {From, Topic}, Reqs),
                producers = maps:put(Topic, Call, Producers),
                sock = Sock1,
                opts = Opts1
            })}
    end;
handle_call(#lookup_topic{partition_topic = Topic, opts = ReqOpts}, From,
        State0 = #state{
            sock = Sock,
            opts = Opts,
            request_id = RequestId,
            requests = Reqs,
            server = Server
        }) ->
    case get_alive_sock_opts(Server, Sock, Opts) of
        {error, Reason} ->
            log_error("lookup_topic from pulsar failed: ~0p down", [Reason]),
            Reply = {error, no_servers_available},
            case handle_connection_failure(State0) of
                stop ->
                    {stop, {shutdown, max_connection_failures_reached}, Reply, State0};
                {continue, State} ->
                    start_reconnect_timer(),
                    {reply, Reply, State}
            end;
        {ok, {Sock1, Opts1}} ->
            State = connection_succeeded(State0),
            pulsar_socket:send_lookup_topic_packet(Sock1, Topic, RequestId, ReqOpts, Opts1),
            {noreply, next_request_id(State#state{
                requests = maps:put(RequestId, {From, Topic}, Reqs),
                sock = Sock1,
                opts = Opts1
            })}
    end;
handle_call(get_status, From, State0 = #state{sock = undefined, opts = Opts, server = Server}) ->
    case get_alive_sock_opts(Server, undefined, Opts) of
        {error, Reason} ->
            log_error("get_status from pulsar failed: ~0p", [Reason]),
            Reply = false,
            case handle_connection_failure(State0) of
                stop ->
                    {stop, {shutdown, max_connection_failures_reached}, Reply, State0};
                {continue, State} ->
                    start_reconnect_timer(),
                    {reply, Reply, State}
            end;
        {ok, {Sock, Opts1}} ->
            State = connection_succeeded(State0),
            {reply, not is_pong_longtime_no_received(),
                State#state{from = From, sock = Sock, opts = Opts1}}
    end;
handle_call(get_status, _From, State) ->
    {reply, not is_pong_longtime_no_received(), State};
handle_call(get_alive_pulsar_url, From, State0 = #state{sock = Sock, opts = Opts, server = Server}) ->
    case get_alive_sock_opts(Server, Sock, Opts) of
        {error, _Reason} ->
            start_reconnect_timer(),
            Reply = {error, no_servers_available},
            case handle_connection_failure(State0) of
                stop ->
                    {stop, {shutdown, max_connection_failures_reached}, Reply, State0};
                {continue, State} ->
                    start_reconnect_timer(),
                    {reply, Reply, State}
            end;
        {ok, {Sock1, Opts1}} ->
            State = connection_succeeded(State0),
            {reply, pulsar_socket:get_pulsar_uri(Sock1, Opts),
                State#state{from = From, sock = Sock1, opts = Opts1}}
    end;
handle_call(_Req, _From, State) ->
    {reply, ok, State, hibernate}.


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
    start_reconnect_timer(),
    {noreply, State#state{sock = undefined}, hibernate};
handle_info({Closed, Sock}, State = #state{sock = Sock})
        when Closed == tcp_closed; Closed == ssl_closed ->
    log_error("connection closed by peer", []),
    start_reconnect_timer(),
    {noreply, State#state{sock = undefined}, hibernate};
handle_info(ping, State0 = #state{sock = undefined, opts = Opts, server = Server}) ->
    case get_alive_sock_opts(Server, undefined, Opts) of
        {error, Reason} ->
            log_error("ping to pulsar servers failed: ~p", [Reason]),
            case handle_connection_failure(State0) of
                stop ->
                    {stop, {shutdown, max_connection_failures_reached}, State0};
                {continue, State} ->
                    start_reconnect_timer(),
                    {noreply, State, hibernate}
            end;
        {ok, {Sock, Opts1}} ->
            State = connection_succeeded(State0),
            pulsar_socket:ping(Sock, Opts1),
            {noreply, State#state{sock = Sock, opts = Opts1}, hibernate}
    end;
handle_info(ping, State = #state{sock = Sock, opts = Opts}) ->
    pulsar_socket:ping(Sock, Opts),
    {noreply, State, hibernate};
handle_info(_Info, State) ->
    log_error("received unknown message: ~p", [_Info]),
    {noreply, State, hibernate}.

terminate(_Reason, #state{sock = undefined} = State) ->
    unregister_worker(State),
    ok;
terminate(_Reason, #state{sock = Sock, opts = Opts} = State) ->
    _ = pulsar_socket:close(Sock, Opts),
    unregister_worker(State),
    ok.

unregister_worker(#state{client_id = ClientId, server = URL}) ->
    _ = pulsar_client_manager:unregister_worker_async(ClientId, URL),
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
        {{From, Topic}, Requests} ->
            State = State0#state{requests = Requests},
            handle_redirect_lookup_response(State, From, Topic, Response);
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
                    {error, #{{Host, Port} => Reason}}
            end;
        {error, Reason} ->
            {error, #{{Host, Port} => Reason}}
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

do_log(Level, Fmt, Args) ->
    logger:log(Level, "[pulsar-client-worker] " ++ Fmt, Args, #{domain => [pulsar, client_worker]}).

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
handle_redirect_lookup_response(State, From, Topic, Response) ->
    #state{client_id = ClientId, opts = Opts} = State,
    ServiceURL = get_service_url_from_lookup_response(Response, Opts),
    {ok, Sup} = pulsar_client_sup:find_worker_sup(ClientId),
    case pulsar_client_worker_sup:start_worker(Sup, ClientId, ServiceURL, Opts) of
        {error, Reason} ->
            log_error("failed to ensure worker for new url is started;"
                      " reason: ~p ; client_id: ~p ; new url: ~s ; topic: ~p",
                      [Reason, ClientId, ServiceURL, Topic]),
            gen_server:reply(From, {error, {failed_to_start_worker, Reason}}),
            State;
        {ok, WorkerPid} ->
            lookup_topic_redirect_async(WorkerPid, Topic, From, Response),
            State
    end.

lookup_topic_redirect_async(WorkerPid, PartitionTopic, OriginalFrom, Response) ->
    Opts = #{authoritative => maps:get(authoritative, Response, false)},
    gen_server:cast(WorkerPid, #lookup_topic_async{
                                  from = OriginalFrom,
                                  partition_topic = PartitionTopic,
                                  opts = Opts
                                 }),
    ok.

connection_succeeded(State0) ->
    State0#state{connection_failure_count = 0}.

handle_connection_failure(State0) ->
    Count = State0#state.connection_failure_count,
    case Count > ?MAX_CONNECTION_FAILURES of
        true ->
            stop;
        false ->
            {continue, State0#state{connection_failure_count = Count + 1}}
    end.
