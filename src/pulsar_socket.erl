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

pong(Sock, Opts) ->
    Mod = tcp_module(Opts),
    Mod:send(Sock, pulsar_protocol_frame:pong()).

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

%% =======================================================================================
%% Helpers
%% =======================================================================================

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
