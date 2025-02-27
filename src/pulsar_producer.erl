%% Copyright (c) 2013-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(pulsar_producer).

-behaviour(gen_statem).

-include_lib("kernel/include/inet.hrl").
-include("include/pulsar_producer_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([ send/2
        , send/3
        , send_sync/2
        , send_sync/3
        , get_state/1
        ]).

-export([ start_link/4
        , idle/3
        , connecting/3
        , connected/3
        ]).

%% gen_statem API
-export([ callback_mode/0
        , init/1
        , terminate/3
        , format_status/1
        , format_status/2
        ]).

%% replayq API
-export([ queue_item_sizer/1
        , queue_item_marshaller/1
        ]).

-export([handle_response/2]).

%% for testing only
-ifdef(TEST).
-export([make_queue_item/2]).
-endif.

-type statem() :: idle | connecting | connected.
-type sequence_id() :: integer().
-type send_receipt() :: #{ sequence_id := sequence_id()
                         , producer_id := integer()
                         , highest_sequence_id := sequence_id()
                         , message_id := map()
                         , any() => term()
                         }.
-type timestamp() :: integer().
-type callback() :: undefined | mfa() | fun((map()) -> ok) | per_request_callback().
-type callback_input() :: {ok, send_receipt()} | {error, expired}.
-type config() :: #{ replayq_dir := string()
                   , replayq_max_total_bytes => pos_integer()
                   , replayq_seg_bytes => pos_integer()
                   , replayq_offload_mode => boolean()
                   , max_batch_bytes => pos_integer()
                   , producer_name => atom()
                   , clientid => atom()
                   , callback => callback()
                   , batch_size => non_neg_integer()
                   , drop_if_high_mem => boolean()
                   , max_inflight => pos_integer()
                   , retention_period => timeout()
                   }.
-export_type([ config/0
             ]).

-define(RECONNECT_TIMEOUT, 5_000).
-define(LOOKUP_TOPIC_TIMEOUT, 15_000).
-define(GET_ALIVE_PULSAR_URL, 5_000).

-define(MAX_REQ_ID, 4294836225).
-define(MAX_SEQ_ID, 18445618199572250625).

-define(DEFAULT_MAX_INFLIGHT, 10).

-define(DEFAULT_REPLAYQ_SEG_BYTES, 10 * 1024 * 1024).
-define(DEFAULT_REPLAYQ_LIMIT, 2_000_000_000).
-define(DEFAULT_MAX_BATCH_BYTES, 1_000_000).
-define(Q_ITEM(From, Ts, Messages), {From, Ts, Messages}).
-define(INFLIGHT_REQ(QAckRef, FromsToMessages, BatchSize), {inflight_req, QAckRef, FromsToMessages, BatchSize}).
-define(NEXT_STATE_IDLE_RECONNECT(State), {next_state, idle, State#{sock := undefined,
                                                                    sock_pid := undefined},
                                           [{state_timeout, ?RECONNECT_TIMEOUT, do_connect}]}).
-define(buffer_overflow_discarded, buffer_overflow_discarded).
-define(MIN_DISCARD_LOG_INTERVAL, timer:seconds(5)).
-define(PER_REQ_CALLBACK(Fn, Args), {callback, {Fn, Args}}).
-define(SOCK_ERR(SOCK, REASON), {socket_error, SOCK, REASON}).

%% poorman's error handling.
%% this is an extra safety to handle a previously missed tcp/ssl_error or tcp/ssl_closed event
-define(POORMAN(SOCK, EXPR),
        case (EXPR) of
            ok ->
                ok;
            {error, Reason} ->
                _ = self() ! ?SOCK_ERR(SOCK, Reason),
                ok
        end).

%% Calls/Casts/Infos
-record(maybe_send_to_pulsar, {}).

-type state_observer_callback() :: {function(), [term()]}.
-type state() :: #{
    batch_size := non_neg_integer(),
    broker_server := {binary(), pos_integer()},
    callback := undefined | mfa() | fun((map()) -> ok),
    clientid := atom(),
    drop_if_high_mem := boolean(),
    inflight_calls := non_neg_integer(),
    lookup_topic_request_ref := reference() | undefined,
    max_inflight := pos_integer(),
    opts := map(),
    parent_pid := undefined | pid(),
    partitiontopic := string(),
    producer_id := integer(),
    producer_name := atom(),
    proxy_to_broker_url := undefined | string(),
    replayq := replayq:q(),
    replayq_offload_mode := boolean(),
    request_id := integer(),
    requests := #{sequence_id() =>
                      ?INFLIGHT_REQ(
                         replayq:ack_ref(),
                         [{gen_statem:from() | undefined,
                           {timestamp(), [pulsar:message()]}}],
                         _BatchSize :: non_neg_integer()
                        )},
    sequence_id := sequence_id(),
    state_observer_callback := undefined | state_observer_callback(),
    sock := undefined | port(),
    sock_pid := undefined | pid(),
    telemetry_metadata := map()
}.
-type handler_result() :: gen_statem:event_handler_result(statem(), state()).
-type per_request_callback() :: {function(), [term()]}.
-type per_request_callback_int() :: ?PER_REQ_CALLBACK(function(), [term()]).
-type send_opts() :: #{callback_fn => per_request_callback()}.
-export_type([send_opts/0]).

callback_mode() -> [state_functions, state_enter].

start_link(PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts) ->
    SpawnOpts = [{spawn_opt, [{message_queue_data, off_heap}]}],
    gen_statem:start_link(?MODULE, {PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts}, SpawnOpts).

-spec send(gen_statem:server_ref(), [pulsar:message()]) -> {ok, pid()}.
send(Pid, Messages) ->
    send(Pid, Messages, _Opts = #{}).

-spec send(gen_statem:server_ref(), [pulsar:message()], send_opts()) -> {ok, pid()}.
send(Pid, Messages, Opts) ->
    From = case maps:get(callback_fn, Opts, undefined) of
               undefined -> undefined;
               {Fn, Args} when is_function(Fn) -> {callback, {Fn, Args}}
           end,
    erlang:send(Pid, ?SEND_REQ(From, Messages)),
    {ok, Pid}.

-spec send_sync(gen_statem:server_ref(), [pulsar:message()]) ->
          {ok, send_receipt()}
        | {error, producer_connecting
                | producer_disconnected
                | term()}.
send_sync(Pid, Messages) ->
    send_sync(Pid, Messages, 5_000).

-spec send_sync(gen_statem:server_ref(), [pulsar:message()], timeout()) ->
          {ok, send_receipt()}
        | {error, producer_connecting
                | producer_disconnected
                | term()}.
send_sync(Pid, Messages, Timeout) ->
    Caller = self(),
    MRef = erlang:monitor(process, Pid, [{alias, reply_demonitor}]),
    %% Mimicking gen_statem's From, so the reply can be sent with
    %% `gen_statem:reply/2'
    From = {Caller, MRef},
    erlang:send(Pid, ?SEND_REQ(From, Messages)),
    receive
        {MRef, Response} ->
            erlang:demonitor(MRef, [flush]),
            Response;
        {'DOWN', MRef, process, Pid, Reason} ->
            error({producer_down, Reason})
    after
        Timeout ->
            erlang:demonitor(MRef, [flush]),
            receive
                {MRef, Response} ->
                    Response
            after
                0 ->
                    error(timeout)
            end
    end.

-spec get_state(pid()) -> statem().
get_state(Pid) ->
    gen_statem:call(Pid, get_state, 5_000).

%%--------------------------------------------------------------------
%% gen_statem callback
%%--------------------------------------------------------------------

-spec init({string(), string(), string() | undefined, config()}) ->
          gen_statem:init_result(statem(), state()).
init({PartitionTopic, Server, ProxyToBrokerUrl, ProducerOpts0}) ->
    process_flag(trap_exit, true),
    pulsar_utils:set_label({?MODULE, PartitionTopic}),
    {Transport, BrokerServer} = pulsar_utils:parse_url(Server),
    ProducerID = maps:get(producer_id, ProducerOpts0),
    Offload = maps:get(replayq_offload_mode, ProducerOpts0, false),
    ReplayqCfg0 =
        case maps:get(replayq_dir, ProducerOpts0, false) of
            false ->
                #{mem_only => true};
            BaseDir ->
                PartitionTopicPath = escape(PartitionTopic),
                Dir = filename:join([BaseDir, PartitionTopicPath]),
                SegBytes = maps:get(replayq_seg_bytes, ProducerOpts0, ?DEFAULT_REPLAYQ_SEG_BYTES),
                #{dir => Dir, seg_bytes => SegBytes, offload => Offload}
        end,
    MaxTotalBytes = maps:get(replayq_max_total_bytes, ProducerOpts0, ?DEFAULT_REPLAYQ_LIMIT),
    MaxBatchBytes = maps:get(max_batch_bytes, ProducerOpts0, ?DEFAULT_MAX_BATCH_BYTES),
    ReplayqCfg =
        ReplayqCfg0#{ sizer => fun ?MODULE:queue_item_sizer/1
                    , marshaller => fun ?MODULE:queue_item_marshaller/1
                    , max_total_bytes => MaxTotalBytes
                    },
    Q = replayq:open(ReplayqCfg),
    ProducerOpts1 = ProducerOpts0#{max_batch_bytes => MaxBatchBytes},
    %% drop replayq options, now that it's open.
    DropIfHighMem = maps:get(drop_if_high_mem, ProducerOpts1, false),
    MaxInflight = maps:get(max_inflight, ProducerOpts1, ?DEFAULT_MAX_INFLIGHT),
    ProducerOpts = maps:without([ replayq_dir
                                , replayq_seg_bytes
                                , replayq_offload_mode
                                , replayq_max_total_bytes
                                , drop_if_high_mem
                                , max_inflight
                                ],
                                ProducerOpts1),
    StateObserverCallback = maps:get(state_observer_callback, ProducerOpts0, undefined),
    ParentPid = maps:get(parent_pid, ProducerOpts, undefined),
    TelemetryMetadata0 = maps:get(telemetry_metadata, ProducerOpts0, #{}),
    TelemetryMetadata = maps:put(partition_topic, PartitionTopic, TelemetryMetadata0),
    State = #{
        batch_size => maps:get(batch_size, ProducerOpts, 0),
        broker_server => BrokerServer,
        callback => maps:get(callback, ProducerOpts, undefined),
        clientid => maps:get(clientid, ProducerOpts),
        drop_if_high_mem => DropIfHighMem,
        inflight_calls => 0,
        lookup_topic_request_ref => undefined,
        max_inflight => MaxInflight,
        opts => pulsar_utils:maybe_enable_ssl_opts(Transport, ProducerOpts),
        parent_pid => ParentPid,
        partitiontopic => PartitionTopic,
        producer_id => ProducerID,
        producer_name => maps:get(producer_name, ProducerOpts, pulsar_producer),
        proxy_to_broker_url => ProxyToBrokerUrl,
        replayq => Q,
        replayq_offload_mode => Offload,
        request_id => 1,
        requests => #{},
        sequence_id => 1,
        state_observer_callback => StateObserverCallback,
        sock => undefined,
        sock_pid => undefined,
        telemetry_metadata => TelemetryMetadata
    },
    pulsar_metrics:inflight_set(State, 0),
    pulsar_metrics:queuing_set(State, replayq:count(Q)),
    pulsar_metrics:queuing_bytes_set(State, replayq:bytes(Q)),
    {ok, idle, State, [{next_event, internal, do_connect}]}.

%% idle state
-spec idle(gen_statem:event_type(), _EventContent, state()) ->
          handler_result().
idle(enter, _OldState, _State = #{state_observer_callback := StateObserverCallback}) ->
    ?tp(debug, pulsar_producer_state_enter, #{state => ?FUNCTION_NAME, previous => _OldState}),
    notify_state_change(StateObserverCallback, ?FUNCTION_NAME),
    keep_state_and_data;
idle(internal, do_connect, State) ->
    refresh_urls_and_connect(State);
idle(state_timeout, do_connect, State) ->
    refresh_urls_and_connect(State);
idle(state_timeout, lookup_topic_timeout, State0) ->
    log_error("timed out waiting for lookup topic response", [], State0),
    State = State0#{lookup_topic_request_ref := undefined},
    ?NEXT_STATE_IDLE_RECONNECT(State);
idle({call, From}, get_state, _State) ->
    {keep_state_and_data, [{reply, From, ?FUNCTION_NAME}]};
idle({call, From}, _EventContent, _State) ->
    {keep_state_and_data, [{reply, From, {error, unknown_call}}]};
idle(cast, _EventContent, _State) ->
    keep_state_and_data;
idle(info, ?SEND_REQ(_, _) = SendRequest, State0) ->
    State = enqueue_send_requests([SendRequest], State0),
    {keep_state, State};
idle(info, {Ref, Reply}, State0 = #{lookup_topic_request_ref := Ref}) ->
    State = State0#{lookup_topic_request_ref := undefined},
    erlang:demonitor(Ref, [flush]),
    handle_lookup_topic_reply(Reply, State);
idle(info, {'EXIT', ParentPid, Reason}, #{parent_pid := ParentPid}) when is_pid(ParentPid) ->
    {stop, Reason};
idle(info, {'DOWN', Ref, process, _Pid, Reason}, State0 = #{lookup_topic_request_ref := Ref}) ->
    log_error("client down; will retry connection later; reason: ~0p", [Reason], State0),
    State = State0#{lookup_topic_request_ref := undefined},
    try_close_socket(State),
    ?NEXT_STATE_IDLE_RECONNECT(State);
idle(internal, #maybe_send_to_pulsar{}, _State) ->
    %% Stale nudge
    keep_state_and_data;
idle(_EventType, _Event, _State) ->
    keep_state_and_data.

%% connecting state
-spec connecting(gen_statem:event_type(), _EventContent, state()) ->
          handler_result().
connecting(enter, _OldState, _State = #{state_observer_callback := StateObserverCallback}) ->
    ?tp(debug, pulsar_producer_state_enter, #{state => ?FUNCTION_NAME, previous => _OldState}),
    notify_state_change(StateObserverCallback, ?FUNCTION_NAME),
    keep_state_and_data;
connecting(state_timeout, do_connect, State) ->
    refresh_urls_and_connect(State);
connecting(state_timeout, lookup_topic_timeout, State0) ->
    log_error("timed out waiting for lookup topic response", [], State0),
    State = State0#{lookup_topic_request_ref := undefined},
    ?NEXT_STATE_IDLE_RECONNECT(State);
connecting(info, ?SEND_REQ(_, _) = SendRequest, State0) ->
    State = enqueue_send_requests([SendRequest], State0),
    {keep_state, State};
connecting(info, {Ref, Reply}, State0 = #{lookup_topic_request_ref := Ref}) ->
    State = State0#{lookup_topic_request_ref := undefined},
    erlang:demonitor(Ref, [flush]),
    handle_lookup_topic_reply(Reply, State);
connecting(info, {'EXIT', ParentPid, Reason}, #{parent_pid := ParentPid}) when is_pid(ParentPid) ->
    {stop, Reason};
connecting(info, {'DOWN', Ref, process, _Pid, Reason}, State0 = #{lookup_topic_request_ref := Ref}) ->
    log_error("client down; will retry connection later; reason: ~0p", [Reason], State0),
    State = State0#{lookup_topic_request_ref := undefined},
    try_close_socket(State),
    ?NEXT_STATE_IDLE_RECONNECT(State);
connecting(info, {'EXIT', Sock, Reason}, State) when is_port(Sock) ->
    handle_socket_close(connecting, Sock, Reason, State);
connecting(info, {'EXIT', SockPid, Reason}, State) when is_pid(SockPid) ->
    handle_socket_close(connecting, SockPid, Reason, State);
connecting(info, {C, Sock}, State) when C =:= tcp_closed; C =:= ssl_closed ->
    handle_socket_close(connecting, Sock, closed, State);
connecting(info, {E, Sock, Reason}, State) when E =:= tcp_error; E =:= ssl_error ->
    handle_socket_close(connecting, Sock, Reason, State);
connecting(info, ?SOCK_ERR(Sock, Reason), State) ->
    handle_socket_close(connecting, Sock, Reason, State);
connecting(internal, #maybe_send_to_pulsar{}, _State) ->
    %% Stale nudge
    keep_state_and_data;
connecting(_EventType, {Inet, _, Bin}, State) when Inet == tcp; Inet == ssl ->
    Cmd = pulsar_protocol_frame:parse(Bin),
    ?MODULE:handle_response(Cmd, State);
connecting(info, Msg, State) ->
    log_info("[connecting] unknown message received ~p~n  ~p", [Msg, State], State),
    keep_state_and_data;
connecting({call, From}, get_state, _State) ->
    {keep_state_and_data, [{reply, From, ?FUNCTION_NAME}]};
connecting({call, From}, _EventContent, _State) ->
    {keep_state_and_data, [{reply, From, {error, unknown_call}}]};
connecting(cast, _EventContent, _State) ->
   keep_state_and_data.

%% connected state
-spec connected(gen_statem:event_type(), _EventContent, state()) ->
          handler_result().
connected(enter, _OldState, State0 = #{state_observer_callback := StateObserverCallback}) ->
    ?tp(debug, pulsar_producer_state_enter, #{state => ?FUNCTION_NAME, previous => _OldState}),
    notify_state_change(StateObserverCallback, ?FUNCTION_NAME),
    State1 = resend_sent_requests(State0),
    State = maybe_send_to_pulsar(State1),
    {keep_state, State};
connected(state_timeout, do_connect, _State) ->
    keep_state_and_data;
connected(state_timeout, lookup_topic_timeout, State0) ->
    log_error("timed out waiting for lookup topic response", [], State0),
    %% todo: should demonitor reference
    State = State0#{lookup_topic_request_ref := undefined},
    ?NEXT_STATE_IDLE_RECONNECT(State);
connected(info, ?SEND_REQ(_, _) = SendRequest, State0 = #{batch_size := BatchSize}) ->
    ?tp(pulsar_producer_send_req_enter, #{}),
    SendRequests = collect_send_requests([SendRequest], BatchSize),
    State1 = enqueue_send_requests(SendRequests, State0),
    State = maybe_send_to_pulsar(State1),
    ?tp(pulsar_producer_send_req_exit, #{}),
    {keep_state, State};
connected(info, {Ref, Reply}, State0 = #{lookup_topic_request_ref := Ref}) ->
    State = State0#{lookup_topic_request_ref := undefined},
    erlang:demonitor(Ref, [flush]),
    handle_lookup_topic_reply(Reply, State);
connected(info, {'EXIT', ParentPid, Reason}, #{parent_pid := ParentPid}) when is_pid(ParentPid) ->
    {stop, Reason};
connected(info, {'DOWN', Ref, process, _Pid, Reason}, State0 = #{lookup_topic_request_ref := Ref}) ->
    log_error("client down; will retry connection later; reason: ~0p", [Reason], State0),
    State = State0#{lookup_topic_request_ref := undefined},
    try_close_socket(State),
    ?NEXT_STATE_IDLE_RECONNECT(State);
connected(info, {'EXIT', Sock, Reason}, State) when is_port(Sock) ->
    handle_socket_close(connected, Sock, Reason, State);
connected(info, {'EXIT', SockPid, Reason}, State) when is_pid(SockPid) ->
    handle_socket_close(connected, SockPid, Reason, State);
connected(_EventType, {C, Sock}, State) when C =:= tcp_closed; C =:= ssl_closed ->
    handle_socket_close(connected, Sock, closed, State);
connected(_EventType, {E, Sock, Reason}, State) when E =:= tcp_error; E =:= ssl_error ->
    handle_socket_close(connected, Sock, Reason, State);
connected(_EventType, ?SOCK_ERR(Sock, Reason), State) ->
    handle_socket_close(connected, Sock, Reason, State);
connected(_EventType, {Inet, _, Bin}, State) when Inet == tcp; Inet == ssl ->
    Cmd = pulsar_protocol_frame:parse(Bin),
    ?MODULE:handle_response(Cmd, State);
connected(_EventType, ping, State = #{sock_pid := SockPid}) ->
    ok = pulsar_socket_writer:ping_async(SockPid),
    {keep_state, State};
connected(internal, #maybe_send_to_pulsar{}, State0) ->
    State = maybe_send_to_pulsar(State0),
    {keep_state, State};
connected({call, From}, get_state, _State) ->
    {keep_state_and_data, [{reply, From, ?FUNCTION_NAME}]};
connected({call, From}, _EventContent, _State) ->
    {keep_state_and_data, [{reply, From, {error, unknown_call}}]};
connected(cast, _EventContent, _State) ->
    keep_state_and_data;
connected(_EventType, EventContent, State) ->
    ?MODULE:handle_response(EventContent, State).

handle_socket_close(StateName, SockPid, Reason, #{sock_pid := SockPid} = State) ->
    #{sock := Sock} = State,
    handle_socket_close(StateName, Sock, Reason, State);
handle_socket_close(StateName, Sock, Reason, #{sock := Sock} = State) ->
    ?tp("pulsar_socket_close", #{sock => Sock, reason => Reason}),
    log_error("connection_closed at_state: ~p, reason: ~p", [StateName, Reason], State),
    try_close_socket(State),
    ?NEXT_STATE_IDLE_RECONNECT(State);
handle_socket_close(_StateName, _Sock, _Reason, _State) ->
    %% stale close event
    keep_state_and_data.

-spec refresh_urls_and_connect(state()) -> handler_result().
refresh_urls_and_connect(State0) ->
    %% if Pulsar went down and then restarted later, we must issue a
    %% LookupTopic command again after reconnecting.
    %% https://pulsar.apache.org/docs/2.10.x/developing-binary-protocol/#topic-lookup
    %% > Topic lookup needs to be performed each time a client needs
    %% > to create or reconnect a producer or a consumer. Lookup is used
    %% > to discover which particular broker is serving the topic we are
    %% > about to use.
    %% Simply looking up the topic (even from a distinct connection)
    %% will "unblock" the topic so we may send messages to it.  The
    %% producer may be started only after that.
    #{ clientid := ClientId
     , partitiontopic := PartitionTopic
     } = State0,
    ?tp(debug, pulsar_producer_refresh_start, #{}),
    try pulsar_client_manager:lookup_topic_async(ClientId, PartitionTopic) of
        {ok, LookupTopicRequestRef} ->
            State = State0#{lookup_topic_request_ref := LookupTopicRequestRef},
            {keep_state, State, [{state_timeout, ?LOOKUP_TOPIC_TIMEOUT, lookup_topic_timeout}]}
    catch
        exit:{noproc, _} ->
            log_error("client restarting; will retry to lookup topic again later", [], State0),
            ?NEXT_STATE_IDLE_RECONNECT(State0)
    end.

-spec do_connect(state()) -> handler_result().
do_connect(State) ->
    #{ broker_server := {Host, Port}
     , opts := Opts
     , partitiontopic := PartitionTopic
     , proxy_to_broker_url := ProxyToBrokerUrl
     } = State,
    try pulsar_socket_writer:start_link(PartitionTopic, Host, Port, Opts) of
        {ok, {SockPid, Sock}} ->
            Opts1 = pulsar_utils:maybe_add_proxy_to_broker_url_opts(Opts, ProxyToBrokerUrl),
            ?POORMAN(Sock, pulsar_socket:send_connect_packet(Sock, Opts1)),
            {next_state, connecting, State#{sock := Sock, sock_pid := SockPid}};
        {error, Reason} ->
            log_error("error connecting: ~p", [Reason], State),
            try_close_socket(State),
            ?NEXT_STATE_IDLE_RECONNECT(State)
    catch
        Kind:Error:Stacktrace ->
            log_error("exception connecting: ~p -> ~p~n  ~p", [Kind, Error, Stacktrace], State),
            try_close_socket(State),
            ?NEXT_STATE_IDLE_RECONNECT(State)
    end.

format_status(Status) ->
    maps:map(
      fun(data, Data0) ->
              censor_secrets(Data0);
         (_Key, Value)->
              Value
      end,
      Status).

%% `format_status/2' is deprecated as of OTP 25.0
format_status(_Opt, [_PDict, _State0, Data0]) ->
    Data = censor_secrets(Data0),
    [{data, [{"State", Data}]}].

censor_secrets(Data0 = #{opts := Opts0 = #{conn_opts := ConnOpts0 = #{auth_data := _}}}) ->
    Data0#{opts := Opts0#{conn_opts := ConnOpts0#{auth_data := "******"}}};
censor_secrets(Data) ->
    Data.

terminate(_Reason, _StateName, State = #{replayq := Q}) ->
    ok = replayq:close(Q),
    ok = clear_gauges(State, Q),
    ok.

clear_gauges(State, Q) ->
    pulsar_metrics:inflight_set(State, 0),
    maybe_reset_queuing(State, Q),
    ok.

maybe_reset_queuing(State, Q) ->
    case {replayq:count(Q), is_replayq_durable(State, Q)} of
        {0, _} ->
            pulsar_metrics:queuing_set(State, 0),
            pulsar_metrics:queuing_bytes_set(State, 0);
        {_, false} ->
            pulsar_metrics:queuing_set(State, 0),
            pulsar_metrics:queuing_bytes_set(State, 0);
        {_, _} ->
            ok
    end.

is_replayq_durable(#{replayq_offload_mode := true}, _Q) ->
    false;
is_replayq_durable(_, Q) ->
    not replayq:is_mem_only(Q).

-spec handle_response(_EventContent, state()) ->
          handler_result().
handle_response({connected, _ConnectedData}, State0 = #{
        sock := Sock,
        opts := Opts,
        producer_id := ProducerId,
        request_id := RequestId,
        partitiontopic := PartitionTopic
    }) ->
    start_keepalive(),
    ?POORMAN(Sock, pulsar_socket:send_create_producer_packet(Sock, PartitionTopic, RequestId, ProducerId, Opts)),
    {keep_state, next_request_id(State0)};
handle_response({producer_success, #{producer_name := ProName}}, State) ->
    {next_state, connected, State#{producer_name := ProName}};
handle_response({pong, #{}}, _State) ->
    start_keepalive(),
    keep_state_and_data;
handle_response({ping, #{}}, #{sock_pid := SockPid}) ->
    ok = pulsar_socket_writer:pong_async(SockPid),
    keep_state_and_data;
handle_response({close_producer, #{}}, State = #{ partitiontopic := Topic
                                                }) ->
    log_error("Close producer: ~p~n", [Topic], State),
    try_close_socket(State),
    ?NEXT_STATE_IDLE_RECONNECT(State);
handle_response({send_receipt, Resp = #{sequence_id := SequenceId}}, State) ->
    #{ callback := Callback
     , inflight_calls := InflightCalls0
     , requests := Reqs
     , replayq := Q
     } = State,
    ?tp(pulsar_producer_recv_send_receipt, #{receipt => Resp}),
    case maps:get(SequenceId, Reqs, undefined) of
        undefined ->
            _ = invoke_callback(Callback, {ok, Resp}),
            {keep_state, State};
        ?INFLIGHT_REQ(QAckRef, FromsToMessages, BatchSize) ->
            ok = replayq:ack(Q, QAckRef),
            lists:foreach(
              fun({undefined, {_TS, Messages}}) ->
                   BatchLen = length(Messages),
                   _ = invoke_callback(Callback, {ok, Resp}, BatchLen),
                   ok;
                 ({?PER_REQ_CALLBACK(Fn, Args), {_TS, _Messages}}) ->
                   %% No need to count the messages, as we invoke
                   %% per-request callbacks once for the whole batch.
                   _ = invoke_callback({Fn, Args}, {ok, Resp}),
                   ok;
                 ({From, {_TS, _Messages}}) ->
                   gen_statem:reply(From, {ok, Resp})
              end,
              FromsToMessages),
            InflightCalls = InflightCalls0 - BatchSize,
            pulsar_metrics:inflight_set(State, InflightCalls),
            Actions = [{next_event, internal, #maybe_send_to_pulsar{}}],
            NewState = State#{ requests := maps:remove(SequenceId, Reqs)
                             , inflight_calls := InflightCalls
                             },
            {keep_state, NewState, Actions}
    end;
handle_response({error, #{error := Error, message := Msg}}, State) ->
    log_error("Response error:~p, msg:~p~n", [Error, Msg], State),
    try_close_socket(State),
    ?NEXT_STATE_IDLE_RECONNECT(State);
handle_response(Msg, State) ->
    log_error("Receive unknown message:~p~n", [Msg], State),
    keep_state_and_data.

-spec send_batch_payload([{timestamp(), [pulsar:message()]}], sequence_id(), state()) -> ok.
send_batch_payload(Messages, SequenceId, #{
            partitiontopic := Topic,
            producer_id := ProducerId,
            producer_name := ProducerName,
            sock_pid := SockPid,
            opts := Opts
        }) ->
    pulsar_socket_writer:send_batch_async(SockPid, Topic, Messages, SequenceId,
                                          ProducerId, ProducerName, Opts).

start_keepalive() ->
    erlang:send_after(30_000, self(), ping).

next_request_id(State = #{request_id := ?MAX_REQ_ID}) ->
    State#{request_id := 1};
next_request_id(State = #{request_id := RequestId}) ->
    State#{request_id := RequestId + 1}.

next_sequence_id(State = #{sequence_id := ?MAX_SEQ_ID}) ->
    State#{sequence_id := 1};
next_sequence_id(State = #{sequence_id := SequenceId}) ->
    State#{sequence_id := SequenceId + 1}.

-spec log_debug(string(), [term()], state()) -> ok.
log_debug(Fmt, Args, State) ->
    do_log(debug, Fmt, Args, State).

-spec log_info(string(), [term()], state()) -> ok.
log_info(Fmt, Args, State) ->
    do_log(info, Fmt, Args, State).

-spec log_warn(string(), [term()], state()) -> ok.
log_warn(Fmt, Args, State) ->
    do_log(warning, Fmt, Args, State).

-spec log_error(string(), [term()], state()) -> ok.
log_error(Fmt, Args, State) ->
    do_log(error, Fmt, Args, State).

-spec do_log(atom(), string(), [term()], state()) -> ok.
do_log(Level, Format, Args, State) ->
    #{partitiontopic := PartitionTopic} = State,
    logger:log(Level, "[pulsar-producer][~s] " ++ Format,
               [PartitionTopic | Args], #{domain => [pulsar, producer]}).

-spec invoke_callback(callback(), callback_input()) -> ok.
invoke_callback(Callback, Resp) ->
    invoke_callback(Callback, Resp, _BatchLen = 1).

-spec invoke_callback(callback(), callback_input(), non_neg_integer()) -> ok.
invoke_callback(_Callback = undefined, _Resp, _BatchLen) ->
    ok;
invoke_callback({M, F, A}, Resp, BatchLen) ->
    lists:foreach(
      fun(_) ->
        erlang:apply(M, F, [Resp] ++ A)
      end,  lists:seq(1, BatchLen));
invoke_callback(Callback, Resp, BatchLen) when is_function(Callback, 1) ->
    lists:foreach(
      fun(_) ->
        Callback(Resp)
      end,  lists:seq(1, BatchLen));
invoke_callback({Fn, Args}, Resp, _BatchLen) when is_function(Fn), is_list(Args) ->
    %% for per-request callbacks, we invoke it only once, regardless
    %% of how many messages were sent.
    apply(Fn, Args ++ [Resp]).

queue_item_sizer(?Q_ITEM(_CallId, _Ts, _Batch) = Item) ->
    erlang:external_size(Item).

queue_item_marshaller(?Q_ITEM(_, _, _) = I) ->
  term_to_binary(I);
queue_item_marshaller(Bin) when is_binary(Bin) ->
  case binary_to_term(Bin) of
      Item = ?Q_ITEM({Pid, _Tag}, Ts, Msgs) when is_pid(Pid) ->
          case node(Pid) =:= node() andalso erlang:is_process_alive(Pid) of
              true ->
                  Item;
              false ->
                  ?Q_ITEM(undefined, Ts, Msgs)
          end;
      Item ->
          Item
  end.

now_ts() ->
    erlang:system_time(millisecond).

make_queue_item(From, Messages) ->
    ?Q_ITEM(From, now_ts(), Messages).

enqueue_send_requests(Requests, State = #{replayq := Q}) ->
    #{drop_if_high_mem := DropIfHighMem} = State,
    QItems = lists:map(
               fun(?SEND_REQ(From, Messages)) ->
                 make_queue_item(From, Messages)
               end,
               Requests),
    BytesBefore = replayq:bytes(Q),
    NewQ = replayq:append(Q, QItems),
    BytesAfter = replayq:bytes(NewQ),
    pulsar_metrics:queuing_set(State, replayq:count(NewQ)),
    pulsar_metrics:queuing_bytes_set(State, BytesAfter),
    ?tp(pulsar_producer_send_requests_enqueued, #{requests => Requests}),
    Overflow0 = replayq:overflow(NewQ),
    IsHighMemOverflow =
        DropIfHighMem
        andalso replayq:is_mem_only(NewQ)
        andalso load_ctl:is_high_mem(),
    Overflow = case IsHighMemOverflow of
        true ->
            max(Overflow0, BytesAfter - BytesBefore);
        false ->
            Overflow0
    end,
    handle_overflow(State#{replayq := NewQ}, IsHighMemOverflow, Overflow).

-spec handle_overflow(state(), _IsHighMemOverflow :: boolean(), _Overflow :: integer()) -> state().
handle_overflow(State, _IsHighMemOverflow, Overflow) when Overflow =< 0 ->
    %% no overflow
    ok = maybe_log_discard(State, _NumRequestsIncrement = 0),
    State;
handle_overflow(State0 = #{replayq := Q, callback := Callback}, IsHighMemOverflow, Overflow) ->
    BytesMode = case IsHighMemOverflow of
        true -> at_least;
        false -> at_most
    end,
    {NewQ, QAckRef, Items0} =
        replayq:pop(Q, #{bytes_limit => {BytesMode, Overflow}, count_limit => 999999999}),
    ok = replayq:ack(NewQ, QAckRef),
    maybe_log_discard(State0, length(Items0)),
    Items = [{From, Msgs} || ?Q_ITEM(From, _Now, Msgs) <- Items0],
    reply_with_error(Items, Callback, {error, overflow}),
    NumMsgs = length([1 || {_, Msgs} <- Items, _ <- Msgs]),
    pulsar_metrics:dropped_queue_full_inc(State0, NumMsgs),
    pulsar_metrics:queuing_set(State0, replayq:count(NewQ)),
    pulsar_metrics:queuing_bytes_set(State0, replayq:bytes(NewQ)),
    State0#{replayq := NewQ}.

maybe_log_discard(State, Increment) ->
    Last = get_overflow_log_state(),
    #{ count_since_last_log := CountLast
     , total_count := TotalCount
     } = Last,
    case CountLast =:= TotalCount andalso Increment =:= 0 of
        true -> %% no change
            ok;
        false ->
            maybe_log_discard(State, Increment, Last)
    end.

-spec maybe_log_discard(
        state(),
        non_neg_integer(),
        #{ last_log_inst => non_neg_integer()
         , count_since_last_log => non_neg_integer()
         , total_count => non_neg_integer()
         }) -> ok.
maybe_log_discard(State,
                  Increment,
                  #{ last_log_inst := LastInst
                   , count_since_last_log := CountLast
                   , total_count := TotalCount
                   }) ->
    NowInst = now_ts(),
    NewTotalCount = TotalCount + Increment,
    Delta = NewTotalCount - CountLast,
    case NowInst - LastInst > ?MIN_DISCARD_LOG_INTERVAL of
        true ->
            log_warn("replayq dropped ~b overflowed messages", [Delta], State),
            put_overflow_log_state(#{ last_log_inst => NowInst
                                    , count_since_last_log => NewTotalCount
                                    , total_count => NewTotalCount
                                    });
        false ->
            put_overflow_log_state(#{ last_log_inst => LastInst
                                    , count_since_last_log => CountLast
                                    , total_count => NewTotalCount
                                    })
    end.

-spec get_overflow_log_state() -> #{ last_log_inst => non_neg_integer()
                                   , count_since_last_log => non_neg_integer()
                                   , total_count => non_neg_integer()
                                   }.
get_overflow_log_state() ->
    case get(?buffer_overflow_discarded) of
        undefined ->
            #{ last_log_inst => 0
             , count_since_last_log => 0
             , total_count => 0
             };
        Stats = #{} ->
            Stats
    end.

-spec put_overflow_log_state(#{ last_log_inst => non_neg_integer()
                              , count_since_last_log => non_neg_integer()
                              , total_count => non_neg_integer()
                              }) -> ok.
put_overflow_log_state(#{ last_log_inst := _LastInst
                        , count_since_last_log := _CountLast
                        , total_count := _TotalCount
                        } = Stats) ->
    put(?buffer_overflow_discarded, Stats),
    ok.

maybe_send_to_pulsar(State) ->
    #{ replayq := Q
     , requests := Requests
     , max_inflight := MaxInflight
     } = State,
    HasQueued = replayq:count(Q) /= 0,
    HasAvailableInflight = map_size(Requests) < MaxInflight,
    case HasQueued andalso HasAvailableInflight of
        true ->
            do_send_to_pulsar(State);
        false ->
            State
    end.

do_send_to_pulsar(State0) ->
    #{ batch_size := BatchSize
     , inflight_calls := InflightCalls0
     , sequence_id := SequenceId
     , requests := Requests0
     , replayq := Q
     , opts := ProducerOpts
     } = State0,
    MaxBatchBytes = maps:get(max_batch_bytes, ProducerOpts, ?DEFAULT_MAX_BATCH_BYTES),
    {NewQ, QAckRef, Items} = replayq:pop(Q, #{ count_limit => BatchSize
                                             , bytes_limit => MaxBatchBytes
                                             }),
    State1 = State0#{replayq := NewQ},
    pulsar_metrics:queuing_set(State0, replayq:count(NewQ)),
    pulsar_metrics:queuing_bytes_set(State0, replayq:bytes(NewQ)),
    RetentionPeriod = maps:get(retention_period, ProducerOpts, infinity),
    Now = now_ts(),
    {Expired, FromsToMessages} =
       lists:foldr(
         fun(?Q_ITEM(From, Timestamp, Msgs), {Expired, Acc}) ->
           case is_batch_expired(Timestamp, RetentionPeriod, Now) of
             true ->
               {[{From, Msgs} | Expired], Acc};
             false ->
               {Expired, [{From, {Timestamp, Msgs}} | Acc]}
           end
         end,
         {[], []},
         Items),
    reply_expired_messages(Expired, State1),
    pulsar_metrics:dropped_inc(State1, length(Expired)),
    case FromsToMessages of
        [] ->
            %% all expired, immediately ack replayq batch and continue
            ok = replayq:ack(Q, QAckRef),
            maybe_send_to_pulsar(State1);
        [_ | _] ->
            FinalBatch = [Msg || {_From, {_Timestamp, Msgs}} <-
                                     FromsToMessages,
                                 Msg <- Msgs],
            FinalBatchSize = length(FinalBatch),
            send_batch_payload(FinalBatch, SequenceId, State0),
            Requests = Requests0#{SequenceId => ?INFLIGHT_REQ(QAckRef, FromsToMessages, FinalBatchSize)},
            InflightCalls = InflightCalls0 + FinalBatchSize,
            pulsar_metrics:inflight_set(State1, InflightCalls),
            State2 = State1#{requests := Requests, inflight_calls := InflightCalls},
            State = next_sequence_id(State2),
            maybe_send_to_pulsar(State)
    end.

-spec reply_expired_messages([{gen_statem:from() | per_request_callback_int() | undefined,
                               [pulsar:message()]}],
                             state()) -> ok.
reply_expired_messages(Expired, #{callback := Callback}) ->
    reply_with_error(Expired, Callback, {error, expired}).

-spec reply_with_error([{gen_statem:from() | per_request_callback_int() | undefined,
                         [pulsar:message()]}],
                       callback(), {error, expired | overflow}) -> ok.
reply_with_error(Items, Callback, Error) ->
    lists:foreach(
      fun({undefined, Msgs}) ->
              invoke_callback(Callback, Error, length(Msgs));
         ({?PER_REQ_CALLBACK(Fn, Args), _Msgs}) ->
              %% No need to count the messages, as we invoke
              %% per-request callbacks once for the whole batch.
              invoke_callback({Fn, Args}, Error);
         ({From, _Msgs}) ->
              gen_statem:reply(From, Error)
      end,
      Items).

collect_send_requests(Acc, Limit) ->
    Count = length(Acc),
    do_collect_send_requests(Acc, Count, Limit).

do_collect_send_requests(Acc, Count, Limit) when Count >= Limit ->
    lists:reverse(Acc);
do_collect_send_requests(Acc, Count, Limit) ->
    receive
        ?SEND_REQ(_, _) = Req ->
            do_collect_send_requests([Req | Acc], Count + 1, Limit)
    after
        0 ->
            lists:reverse(Acc)
    end.

try_close_socket(#{sock := undefined}) ->
    ok;
try_close_socket(#{sock := Sock, sock_pid := SockPid, opts := Opts}) ->
    %% N.B.: it's important to first close the socket and then terminate the writer
    %% process.  The writer may be blocked in a `send' call, and closing the socket first
    %% will make the call return `einval' immediately, allowing us to terminate it (it
    %% also terminates itself on such `send' errors, but we make sure here).
    _ = pulsar_socket:close(Sock, Opts),
    ok = pulsar_socket_writer:stop(SockPid),
    ok.

resend_sent_requests(State) ->
    ?tp(pulsar_producer_resend_sent_requests_enter, #{}),
    #{ inflight_calls := InflightCalls0
     , requests := Requests0
     , replayq := Q
     , opts := ProducerOpts
     } = State,
    Now = now_ts(),
    RetentionPeriod = maps:get(retention_period, ProducerOpts, infinity),
    {Requests, Dropped} =
        maps:fold(
          fun(SequenceId, ?INFLIGHT_REQ(QAckRef, FromsToMessages, _BatchSize), {AccIn, DroppedAcc}) ->
               {Messages, Expired} =
                  lists:partition(
                    fun({_From, {Ts, _Msgs}}) ->
                      not is_batch_expired(Ts, RetentionPeriod, Now)
                    end,
                    FromsToMessages),
               lists:foreach(
                 fun({From, {_Ts, Msgs}}) ->
                   reply_expired_messages([{From, Msgs}], State)
                 end,
                 Expired),
               Dropped = length(Expired),
               Acc = case Messages of
                   [] ->
                       ?tp(pulsar_producer_resend_all_expired, #{}),
                       ok = replayq:ack(Q, QAckRef),
                       AccIn;
                   [_ | _] ->
                       send_batch_payload([Msg || {_From, {_Ts, Msgs}} <- Messages,
                                                  Msg <- Msgs],
                                          SequenceId, State),
                       AccIn#{SequenceId => ?INFLIGHT_REQ(QAckRef, Messages, length(Messages))}
               end,
               {Acc, DroppedAcc + Dropped}
          end,
          {#{}, 0},
          Requests0),
    InflightCalls = InflightCalls0 - Dropped,
    pulsar_metrics:dropped_inc(State, Dropped),
    pulsar_metrics:inflight_set(State, InflightCalls),
    State#{requests := Requests, inflight_calls := InflightCalls}.

is_batch_expired(_Timestamp, infinity = _RetentionPeriod, _Now) ->
    false;
is_batch_expired(Timestamp, RetentionPeriod, Now) ->
    Timestamp =< Now - RetentionPeriod.

-spec escape(string()) -> binary().
escape(Str) ->
    NormalizedStr = unicode:characters_to_nfd_list(Str),
    iolist_to_binary(pulsar_utils:escape_uri(NormalizedStr)).

-spec handle_lookup_topic_reply(pulsar_client:lookup_topic_response(), state()) -> handler_result().
handle_lookup_topic_reply({error, Error}, State) ->
    log_error("error looking up topic: ~0p", [Error], State),
    try_close_socket(State),
    ?NEXT_STATE_IDLE_RECONNECT(State);
handle_lookup_topic_reply({ok, #{ proxy_through_service_url := true
                                , brokerServiceUrl := NewBrokerServiceURL
                                }}, State0) ->
    #{clientid := ClientId} = State0,
    ?tp(debug, pulsar_producer_lookup_alive_pulsar_url, #{}),
    log_debug("received topic lookup reply: ~0p", [#{proxy_through_service_url => true, broker_service_url => NewBrokerServiceURL}], State0),
    try pulsar_client_manager:get_alive_pulsar_url(ClientId, ?GET_ALIVE_PULSAR_URL) of
        {ok, AlivePulsarURL} ->
            maybe_connect(#{ broker_service_url => NewBrokerServiceURL
                           , alive_pulsar_url => AlivePulsarURL
                           }, State0);
        {error, Reason} ->
            log_error("error getting pulsar alive URL: ~0p", [Reason], State0),
            try_close_socket(State0),
            ?NEXT_STATE_IDLE_RECONNECT(State0)
    catch
        exit:{noproc, _} ->
            log_error("client restarting; will retry to lookup topic later", [], State0),
            try_close_socket(State0),
            ?NEXT_STATE_IDLE_RECONNECT(State0);
        exit:{timeout, _} ->
            log_error("timeout calling client; will retry to lookup topic later", [], State0),
            try_close_socket(State0),
            ?NEXT_STATE_IDLE_RECONNECT(State0)
    end;
handle_lookup_topic_reply({ok, #{ proxy_through_service_url := false
                                , brokerServiceUrl := NewBrokerServiceURL
                                }},
                         State) ->
    log_debug("received topic lookup reply: ~0p",
              [#{proxy_through_service_url => false,
                 broker_service_url => NewBrokerServiceURL}], State),
    maybe_connect(#{ alive_pulsar_url => NewBrokerServiceURL
                   , broker_service_url => undefined
                   }, State).

-spec maybe_connect(#{ alive_pulsar_url := string()
                     , broker_service_url := string() | undefined
                     }, state()) -> handler_result().
maybe_connect(#{ broker_service_url := NewBrokerServiceURL
               , alive_pulsar_url := AlivePulsarURL
               }, State0) ->
    #{ broker_server := OldBrokerServer
     , proxy_to_broker_url := OldBrokerServiceURL
     } = State0,
    {_Transport, NewBrokerServer} = pulsar_utils:parse_url(AlivePulsarURL),
    case {OldBrokerServer, OldBrokerServiceURL} =:= {NewBrokerServer, NewBrokerServiceURL} of
        true ->
            log_debug("connecting to ~0p",
                      [#{broker_server => NewBrokerServer,
                         service_url => NewBrokerServiceURL}], State0),
            do_connect(State0);
        false ->
            %% broker changed; reconnect.
            log_info("pulsar endpoint changed from ~0p to ~0p; reconnecting...",
                      [ #{ broker_server => OldBrokerServer
                         , proxy_url => OldBrokerServiceURL
                         }
                      , #{ broker_server => NewBrokerServer
                         , proxy_url => NewBrokerServiceURL
                         }
                      ],
                      State0),
            try_close_socket(State0),
            State = State0#{
                broker_server := NewBrokerServer,
                proxy_to_broker_url := NewBrokerServiceURL
            },
            ?NEXT_STATE_IDLE_RECONNECT(State)
    end.

-spec notify_state_change(undefined | state_observer_callback(), statem()) -> ok.
notify_state_change(undefined, _ProducerState) ->
    ok;
notify_state_change({Fun, Args}, ProducerState) ->
    _ = apply(Fun, [ProducerState | Args]),
    ok.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

maybe_log_discard_test_() ->
    [ {"no increment, empty dictionary", fun() -> maybe_log_discard(undefined, 0) end}
    , {"fake-last-old",
       fun() ->
         Inst0 = now_ts() - ?MIN_DISCARD_LOG_INTERVAL - 1,
         ok = put_overflow_log_state(#{ last_log_inst => Inst0
                                      , count_since_last_log => 2
                                      , total_count => 2
                                      }),
         ok = maybe_log_discard(#{partitiontopic => <<"partitiontopic">>}, 1),
         Stats = get_overflow_log_state(),
         ?assertMatch(#{count_since_last_log := 3, total_count := 3}, Stats),
         %% greater than the minimum interval because we just logged
         ?assert(maps:get(last_log_inst, Stats) - Inst0 > ?MIN_DISCARD_LOG_INTERVAL),
         ok
       end}
    , {"fake-last-fresh",
       fun() ->
         Inst0 = now_ts(),
         ok = put_overflow_log_state(#{ last_log_inst => Inst0
                                      , count_since_last_log => 2
                                      , total_count => 2
                                      }),
         ok = maybe_log_discard(#{partitiontopic => <<"partitiontopic">>}, 2),
         Stats = get_overflow_log_state(),
         ?assertMatch(#{count_since_last_log := 2, total_count := 4}, Stats),
         %% less than the minimum interval because we didn't log and just accumulated
         ?assert(maps:get(last_log_inst, Stats) - Inst0 < ?MIN_DISCARD_LOG_INTERVAL),
         ok
       end}
    ].
-endif.
