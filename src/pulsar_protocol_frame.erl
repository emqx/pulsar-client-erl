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

-module(pulsar_protocol_frame).

-include ("PulsarApi_pb.hrl").

-define(CONNECT, 'CONNECT').
-define(CONNECTED, 'CONNECTED').
-define(PARTITIONED_METADATA, 'PARTITIONED_METADATA').
-define(PARTITIONED_METADATA_RESPONSE, 'PARTITIONED_METADATA_RESPONSE').
-define(LOOKUP, 'LOOKUP').
-define(LOOKUP_RESPONSE, 'LOOKUP_RESPONSE').
-define(PRODUCER, 'PRODUCER').
-define(PRODUCER_SUCCESS, 'PRODUCER_SUCCESS').
-define(SEND, 'SEND').
-define(SEND_RECEIPT, 'SEND_RECEIPT').
-define(PING, 'PING').
-define(PONG, 'PONG').
-define(CLOSE_PRODUCER, 'CLOSE_PRODUCER').
-define(SIMPLE_SIZE, 4).
-define(PAYLOAD_SIZE, 10).
-define(MAGIC_NUMBER, 3585).

-export ([ connect/1
         , topic_metadata/1
         , lookup_topic/1
         , create_producer/1
         , ping/0
         , pong/0
         , serialized_simple_command/1
         , serialized_payload_command/3
         , parse/1
         , send/3
         ]).

connect(CommandConnect) ->
    serialized_simple_command(#basecommand{
        type = ?CONNECT,
        connect = CommandConnect
    }).

topic_metadata(PartitionMetadata) ->
    serialized_simple_command(#basecommand{
        type = ?PARTITIONED_METADATA,
        partitionmetadata = PartitionMetadata
    }).

lookup_topic(LookupTopic) ->
    serialized_simple_command(#basecommand{
        type = ?LOOKUP,
        lookuptopic = LookupTopic
    }).

create_producer(Producer) ->
    serialized_simple_command(#basecommand{
        type = ?PRODUCER,
        producer = Producer
    }).

send(Send, Metadata, BatchPayload) ->
    serialized_payload_command(#basecommand{
        type = ?SEND,
        send = Send
    }, i2b('PulsarApi_pb':encode_messagemetadata(Metadata)), BatchPayload).

ping() ->
    serialized_simple_command(#basecommand{
        type = ?PING,
        ping = #commandping{}
    }).

pong() ->
    serialized_simple_command(#basecommand{
        type = ?PONG,
        pong = #commandpong{}
    }).

parse(<<TotalSize:32, CmdBin:TotalSize/binary, LastBin/binary>>) ->
    Bin = <<TotalSize:32, CmdBin/binary>>,
    BaseCommand = 'PulsarApi_pb':decode_basecommand(Bin),
    Resp = case BaseCommand#basecommand.type of
        ?CONNECTED -> BaseCommand#basecommand.connected;
        ?PARTITIONED_METADATA_RESPONSE -> BaseCommand#basecommand.partitionmetadataresponse;
        ?LOOKUP_RESPONSE -> BaseCommand#basecommand.lookuptopicresponse;
        ?PRODUCER_SUCCESS -> BaseCommand#basecommand.producer_success;
        ?SEND_RECEIPT -> BaseCommand#basecommand.send_receipt;
        ?PING -> #commandping{};
        ?PONG -> #commandpong{};
        ?CLOSE_PRODUCER -> BaseCommand#basecommand.close_producer;
        _ -> unknown
    end,
    {Resp, LastBin};
parse(Bin) ->
    {undefined, Bin}.

serialized_simple_command(BaseCommand) ->
    BaseCommandBin = i2b('PulsarApi_pb':encode_basecommand(BaseCommand)),
    Size = size(BaseCommandBin),
    TotalSize = Size + ?SIMPLE_SIZE,
    <<TotalSize:32, Size:32, BaseCommandBin/binary>>.

serialized_payload_command(BaseCommand, Metadata, BatchPayload) ->
    BaseCommandBin = i2b('PulsarApi_pb':encode_basecommand(BaseCommand)),
    BaseCommandSize = size(BaseCommandBin),
    MetadataSize = size(Metadata),
    Payload = <<MetadataSize:32, Metadata/binary, BatchPayload/binary>>,
    Checksum = crc32cer:nif(Payload),
    TotalSize = BaseCommandSize + size(Payload) + ?PAYLOAD_SIZE,
    <<TotalSize:32, BaseCommandSize:32, BaseCommandBin/binary, ?MAGIC_NUMBER:16, Checksum:32, Payload/binary>>.

i2b(I) when is_list(I) -> iolist_to_binary(I);
i2b(I) -> I.