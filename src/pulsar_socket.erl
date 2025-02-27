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
        , connect/3
        , close/2
        , controlling_process/3
        ]).

-export([ send_connect_packet/2
        , send_lookup_topic_packet/5
        , send_topic_metadata_packet/4
        , send_subscribe_packet/7
        , send_set_flow_packet/4
        , send_ack_packet/5
        , send_batch_message_packet/7
        , send_create_producer_packet/5
        , ping/2
        , pong/2
        , getstat/2
        , get_pulsar_uri/2
        ]).

%% exposed ONLY for mocking
-export([internal_getopts/3]).

%% Async socket process
-export([ spawn_link_connect/3
        , send_batch_message_packet_async/7
        , ping_async/1
        , pong_async/1
        ]).
%% `proc_lib' loop
-export([init/2, loop/2, stop/1]).
%% `sys' API
-export([ system_continue/3
        , system_terminate/4
        , system_code_change/4
        , format_status/2
        ]).

-define(SEND_TIMEOUT, 60000).
-define(CONN_TIMEOUT, 30000).

-define(INTERNAL_TCP_OPTS,
    [ binary
    , {packet, 4}
    , {active, true}
    , {reuseaddr, true}
    ]).

-define(DEF_TCP_OPTS,
    [ {nodelay, true}
    , {sndbuf, 1_000_000}
    , {recbuf, 1_000_000}
    , {send_timeout, ?SEND_TIMEOUT}
    ]).

%% Socket process requests
-record(send_batch, {
    topic,
    messages,
    sequence_id,
    producer_id,
    producer_name,
    opts
}).
-record(ping, {}).
-record(pong, {}).
-record(stop, {}).

send_connect_packet(Sock, Opts) ->
    Mod = tcp_module(Opts),
    ConnOpts = opt(conn_opts, Opts, #{}),
    Mod:send(Sock, pulsar_protocol_frame:connect(ConnOpts)).

send_topic_metadata_packet(Sock, Topic, RequestId, Opts) ->
    Mod = tcp_module(Opts),
    Metadata = topic_metadata_cmd(Topic, RequestId),
    Mod:send(Sock, pulsar_protocol_frame:topic_metadata(Metadata)).

send_lookup_topic_packet(Sock, Topic, RequestId, ReqOpts, Opts) ->
    Mod = tcp_module(Opts),
    LookupCmd = lookup_topic_cmd(Topic, RequestId, ReqOpts),
    Mod:send(Sock, pulsar_protocol_frame:lookup_topic(LookupCmd)).

send_subscribe_packet(Sock, Topic, RequestId, ConsumerId, Subscription, SubType, Opts) ->
    Mod = tcp_module(Opts),
    SubInfo = #{
        topic => Topic,
        subscription => Subscription,
        subType => SubType,
        consumer_id => ConsumerId,
        request_id => RequestId
    },
    Mod:send(Sock, pulsar_protocol_frame:create_subscribe(SubInfo)).

send_set_flow_packet(Sock, ConsumerId, FlowSize, Opts) ->
    Mod = tcp_module(Opts),
    FlowInfo = #{
        consumer_id => ConsumerId,
        messagePermits => FlowSize
    },
    Mod:send(Sock, pulsar_protocol_frame:set_flow(FlowInfo)).

send_ack_packet(Sock, ConsumerId, AckType, MsgIds, Opts) ->
    Mod = tcp_module(Opts),
    Ack = #{
        consumer_id => ConsumerId,
        ack_type => AckType,
        message_id => MsgIds
    },
    Mod:send(Sock, pulsar_protocol_frame:ack(Ack)).

send_batch_message_packet(Sock, Topic, Messages, SequenceId, ProducerId, ProducerName, Opts) ->
    Mod = tcp_module(Opts),
    Len = length(Messages),
    SendCmd = message_snd_cmd(Len, ProducerId, SequenceId),
    BatchMsg = batch_message_cmd(Messages, Opts),
    MsgMetadata = batch_message_cmd_metadata(ProducerName, SequenceId, Len),
    pulsar_metrics:send(Topic, length(Messages)),
    Mod:send(Sock, pulsar_protocol_frame:send(SendCmd, MsgMetadata, BatchMsg)).

send_batch_message_packet_async(SockPid, Topic, Messages, SequenceId, ProducerId, ProducerName, Opts) ->
    Req =
        #send_batch{ topic = Topic
                   , messages = Messages
                   , sequence_id = SequenceId
                   , producer_id = ProducerId
                   , producer_name = ProducerName
                   , opts = Opts
                   },
    _ = erlang:send(SockPid, Req),
    ok.

send_create_producer_packet(Sock, Topic, RequestId, ProducerId, Opts) ->
    Mod = tcp_module(Opts),
    Producer = #{
        topic => Topic,
        producer_id => ProducerId,
        request_id => RequestId
    },
    Mod:send(Sock, pulsar_protocol_frame:create_producer(Producer)).

ping(Sock, Opts) ->
    Mod = tcp_module(Opts),
    Mod:send(Sock, pulsar_protocol_frame:ping()).

ping_async(SockPid) ->
    _ = erlang:send(SockPid, #ping{}),
    ok.

pong(Sock, Opts) ->
    Mod = tcp_module(Opts),
    Mod:send(Sock, pulsar_protocol_frame:pong()).

pong_async(SockPid) ->
    _ = erlang:send(SockPid, #pong{}),
    ok.

getstat(Sock, Opts) ->
    InetM = inet_module(Opts),
    InetM:getstat(Sock).

peername(Sock, Opts) ->
    Mod = inet_module(Opts),
    Mod:peername(Sock).

connect(Host, Port, Opts) ->
    TcpMod = tcp_module(Opts),
    {ConnOpts, Timeout} = connect_opts(Opts),
    case TcpMod:connect(Host, Port, ConnOpts, Timeout) of
        {ok, Sock} ->
            case tune_buffer(inet_module(Opts), Sock) of
                ok ->
                    TcpMod:controlling_process(Sock, self()),
                    {ok, Sock};
                Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

spawn_link_connect(Host, Port, Opts) ->
    proc_lib:start_link(?MODULE, init, [self(), #{host => Host, port => Port, opts => Opts}]).

controlling_process(Sock, Pid, Opts) ->
    TcpMod = tcp_module(Opts),
    TcpMod:controlling_process(Sock, Pid),
    ok.

close(Sock, Opts) ->
    try
        TcpMod = tcp_module(Opts),
        TcpMod:close(Sock)
    catch
        _:_ ->
            ok
    end.

get_pulsar_uri(Sock, Opts) ->
    case peername(Sock, Opts) of
        {ok, {IP, Port}} ->
            {ok, pulsar_scheme(Opts) ++ "://" ++ inet:ntoa(IP) ++ ":" ++ integer_to_list(Port)};
        {error, _} = Error ->
            Error
    end.

connect_opts(Opts) ->
    TcpOpts = opt(tcp_opts, Opts, []),
    SslOpts = opt(ssl_opts, Opts, []),
    ConnTimeout = opt(connect_timeout, Opts, ?CONN_TIMEOUT),
    ConnOpts = case opt(enable_ssl, Opts, false) of
        true -> pulsar_utils:merge_opts([?DEF_TCP_OPTS, TcpOpts, SslOpts, ?INTERNAL_TCP_OPTS]);
        false -> pulsar_utils:merge_opts([?DEF_TCP_OPTS, TcpOpts, ?INTERNAL_TCP_OPTS])
    end,
    {ConnOpts, ConnTimeout}.

topic_metadata_cmd(Topic, RequestId) ->
    #{
        topic => Topic,
        request_id => RequestId
    }.

lookup_topic_cmd(Topic, RequestId, ReqOpts) ->
    Authoritative = maps:get(authoritative, ReqOpts, false),
    #{
        topic => Topic,
        request_id => RequestId,
        authoritative => Authoritative
    }.

message_snd_cmd(Len, ProducerId, SequenceId) when is_integer(Len) ->
    #{
        producer_id => ProducerId,
        sequence_id => SequenceId,
        num_messages => Len
    }.

batch_message_cmd(Messages, Opts) ->
    Compression = compression_type(opt(compression, Opts, no_compression)),
    lists:foldl(fun(#{key := Key, value := Msg}, Acc) ->
            Msg1 = maybe_compression(Msg, Compression),
            SMetadata = single_message_metadata(Key, erlang:iolist_size(Msg), Compression),
            SMetadataBin = pulsar_api:encode_msg(SMetadata, 'SingleMessageMetadata'),
            SMetadataBinSize = erlang:iolist_size(SMetadataBin),
            <<Acc/binary, SMetadataBinSize:32, SMetadataBin/binary, Msg1/binary>>
        end, <<>>, Messages).

single_message_metadata(undefined, Size, Compression) ->
    #{ payload_size => Size
     , compression => Compression
     };
single_message_metadata(Key, Size, Compression) ->
    (single_message_metadata(undefined, Size, Compression))#{partition_key => Key}.

batch_message_cmd_metadata(ProducerName, SequenceId, Len) ->
    #{
        producer_name => ProducerName,
        sequence_id => SequenceId,
        publish_time => erlang:system_time(millisecond),
        compression => 'NONE',
        num_messages_in_batch => Len
    }.

compression_type(snappy) ->'SNAPPY';
compression_type(zlib) ->'ZLIB';
compression_type(_) ->'NONE'.

maybe_compression(Bin, 'SNAPPY') ->
    {ok, Compressed} = snappyer:compress(Bin),
    Compressed;
maybe_compression(Bin, 'ZLIB') ->
    zlib:compress(Bin);
maybe_compression(Bin, _) ->
    Bin.

%%=======================================================================================
%% Process loop
%%=======================================================================================

init(OwnerPid, #{host := Host, port := Port, opts := Opts}) ->
    logger:set_process_metadata(#{domain => [pulsar, socket]}),
    set_label({pulsar_socket, OwnerPid}),
    case connect(Host, Port, Opts) of
        {ok, Sock} ->
            controlling_process(Sock, OwnerPid, Opts),
            proc_lib:init_ack(OwnerPid, {ok, {self(), Sock}}),
            link(Sock),
            State = #{sock => Sock, owner => OwnerPid, opts => Opts},
            Debug = sys:debug_options(maps:get(debug, Opts, [])),
            ?MODULE:loop(State, Debug);
        Error ->
            proc_lib:init_ack(OwnerPid, Error),
            exit(normal)
    end.

-if(OTP_RELEASE >= 27).
set_label(Label) -> proc_lib:set_label(Label).
-else.
set_label(_Label) -> ok.
-endif.

stop(SockPid) ->
    _ = erlang:send(SockPid, #stop{}),
    ok.

loop(State, Debug) ->
    Msg = receive X -> X end,
    decode_msg(Msg, State, Debug).

decode_msg({system, From, Msg}, State, Debug) ->
    #{owner := OwnerPid} = State,
    sys:handle_system_msg(Msg, From, OwnerPid, ?MODULE, Debug, State);
decode_msg(Msg, State, [] = Debug) ->
    handle_msg(Msg, State, Debug);
decode_msg(Msg, State, Debug0) ->
    Debug = sys:handle_debug(Debug0, fun print_msg/3, State, Msg),
    handle_msg(Msg, State, Debug).

%% Socket controlling process is parent, so we don't need to handle socket data here.
handle_msg(#send_batch{} = Request, State, Debug) ->
    #send_batch{
       topic = Topic,
       messages = Messages,
       sequence_id = SequenceId,
       producer_id = ProducerId,
       producer_name = ProducerName,
       opts = Opts
    } = Request,
    #{sock := Sock} = State,
    Res = send_batch_message_packet(Sock, Topic, Messages, SequenceId, ProducerId, ProducerName, Opts),
    loop_or_die(Res, State, Debug);
handle_msg(#ping{}, State, Debug) ->
    #{sock := Sock, opts := Opts} = State,
    Res = ping(Sock, Opts),
    loop_or_die(Res, State, Debug);
handle_msg(#pong{}, State, Debug) ->
    #{sock := Sock, opts := Opts} = State,
    Res = pong(Sock, Opts),
    loop_or_die(Res, State, Debug);
handle_msg(#stop{}, State, _Debug) ->
    #{sock := Sock, opts := Opts} = State,
    _ = close(Sock, Opts),
    ok;
handle_msg(Msg, State, Debug) ->
    log_warning("unknow message: ~0p", [Msg], State),
    ?MODULE:loop(State, Debug).

loop_or_die(ok, State, Debug) ->
    ?MODULE:loop(State, Debug);
loop_or_die({error, _} = Error, _State, _Debug) ->
    exit(Error).

print_msg(Device, #send_batch{} = Request, State) ->
  do_print_msg(Device, "send: ~p", [Request], State);
print_msg(Device, #stop{}, State) ->
  do_print_msg(Device, "stop", [], State);
print_msg(Device, Msg, State) ->
  do_print_msg(Device, "unknown msg: ~p", [Msg], State).

do_print_msg(Device, Fmt, Args, _State) ->
  io:format(Device, "[~s] ~p " ++ Fmt ++ "~n",
            [ts(), self()] ++ Args).

ts() ->
  Now = os:timestamp(),
  {_, _, MicroSec} = Now,
  {{Y, M, D}, {HH, MM, SS}} = calendar:now_to_local_time(Now),
  lists:flatten(io_lib:format("~.4.0w-~.2.0w-~.2.0w ~.2.0w:~.2.0w:~.2.0w.~w",
                              [Y, M, D, HH, MM, SS, MicroSec])).

system_continue(_Parent, Debug, State) ->
  ?MODULE:loop(State, Debug).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _Parent, Debug, _Misc) ->
  sys:print_log(Debug),
  exit(Reason).

system_code_change(State, _Module, _Vsn, _Extra) ->
  {ok, State}.

format_status(Opt, Status) ->
  {Opt, Status}.

%%=======================================================================================
%% Helpers
%%=======================================================================================

log_warning(Format, Args, State) ->
    do_log(warning, Format, Args, State).

do_log(Level, Format, Args, State) ->
    #{owner := OwnerPid} = State,
    logger:log(Level, "[pulsar-socket][owner:~p] " ++ Format,
               [OwnerPid | Args],
               #{domain => [pulsar, socket]}).

pulsar_scheme(Opts) ->
    case opt(enable_ssl, Opts, false) of
        false -> "pulsar";
        true -> "pulsar+ssl"
    end.

tcp_module(Opts) ->
    case opt(enable_ssl, Opts, false) of
        false -> gen_tcp;
        true -> ssl
    end.

inet_module(Opts) ->
    case opt(enable_ssl, Opts, false) of
        false -> inet;
        true -> ssl
    end.

%% to allow mocking
internal_getopts(InetM, Sock, Opts) ->
    InetM:getopts(Sock, Opts).

tune_buffer(InetM, Sock) ->
    case ?MODULE:internal_getopts(InetM, Sock, [recbuf, sndbuf]) of
        {ok, Opts} ->
            RecBuf = proplists:get_value(recbuf, Opts),
            SndBuf = proplists:get_value(sndbuf, Opts),
            InetM:setopts(Sock, [{buffer, max(RecBuf, SndBuf)}]),
            ok;
        Error ->
            Error
    end.

opt(Key, Opts, Default) when is_list(Opts) ->
    opt(Key, maps:from_list(Opts), Default);
opt(Key, Opts, Default) ->
    maps:get(Key, Opts, Default).
