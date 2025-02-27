%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(pulsar_socket_writer).

-behaviour(gen_server).

%% API
-export([
    start_link/4,
    stop/1,

    send_batch_async/7,
    ping_async/1,
    pong_async/1
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% Calls/Casts/Infos
-record(send_batch, {topic, encoded_packet, num_messages}).
-record(ping, {}).
-record(pong, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(PartitionTopic, Host, Port, Opts) ->
    case pulsar_socket:connect(Host, Port, Opts) of
        {ok, Sock} ->
            Params = #{partition_topic => PartitionTopic, sock => Sock, opts => Opts},
            case gen_server:start_link(?MODULE, Params, []) of
                {ok, SockPid} ->
                    {ok, {SockPid, Sock}};
                Error ->
                    pulsar_socket:close(Sock, Opts),
                    Error
            end;
        Error ->
            Error
    end.

stop(SockPid) when is_pid(SockPid) ->
    link(SockPid),
    exit(SockPid, normal),
    receive
        {'EXIT', SockPid, _} ->
            ok
    after 5_000 ->
            exit(SockPid, kill),
            receive
                {'EXIT', SockPid, _} ->
                    ok
            end
    end.

send_batch_async(SockPid, Topic, Messages, SequenceId, ProducerId, ProducerName, Opts) ->
    {NumMessages, EncodedPacket0} =
        pulsar_socket:encode_send_batch_message_packet(Messages, SequenceId,
                                                       ProducerId, ProducerName, Opts),
    EncodedPacket = iolist_to_binary(EncodedPacket0),
    Req = #send_batch{topic = Topic, encoded_packet = EncodedPacket, num_messages = NumMessages},
    safe_cast(SockPid, Req).

ping_async(SockPid) ->
    safe_cast(SockPid, #ping{}).

pong_async(SockPid) ->
    safe_cast(SockPid, #pong{}).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(#{sock := Sock, partition_topic := PartitionTopic, opts := Opts}) ->
    logger:set_process_metadata(#{domain => [pulsar, socket]}),
    pulsar_utils:set_label({?MODULE, PartitionTopic}),
    State = #{sock => Sock, opts => Opts},
    {ok, State}.

handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(#ping{}, State) ->
    #{sock := Sock, opts := Opts} = State,
    Res = pulsar_socket:ping(Sock, Opts),
    ok_or_die(Res, State);
handle_cast(#pong{}, State) ->
    #{sock := Sock, opts := Opts} = State,
    Res = pulsar_socket:pong(Sock, Opts),
    ok_or_die(Res, State);
handle_cast(#send_batch{} = Req, State) ->
    #{sock := Sock, opts := Opts} = State,
    #send_batch{ topic = Topic
               , num_messages = NumMessages
               , encoded_packet = EncodedPacket} = Req,
    Mod = pulsar_socket:tcp_module(Opts),
    pulsar_metrics:send(Topic, NumMessages),
    Res = Mod:send(Sock, EncodedPacket),
    ok_or_die(Res, State);
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Cast, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

safe_cast(SockPid, Req) ->
    try
        gen_server:cast(SockPid, Req)
    catch
        exit:{noproc, _} ->
            %% Owner will notice process is down later
            ok
    end.

%% Only for casts/infos
ok_or_die(ok, State) ->
    {noreply, State};
ok_or_die(Error, State) ->
    {stop, Error, State}.
