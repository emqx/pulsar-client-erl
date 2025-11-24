#!/usr/bin/env escript
%% -*- erlang -*-
%%--------------------------------------------------------------------
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
%%--------------------------------------------------------------------

-mode(compile).

main([PulsarHost, Topic, NumMessagesStr]) ->
    main([PulsarHost, Topic, NumMessagesStr, "1"]);
main([PulsarHost, Topic, NumMessagesStr, BatchSizeStr]) ->
    %% Set up code paths to find pulsar application
    setup_code_paths(),

    NumMessages = list_to_integer(NumMessagesStr),
    BatchSize = list_to_integer(BatchSizeStr),
    FormatDesc = case BatchSize of
        1 -> "single message format";
        _ -> "batch format with SingleMessageMetadata"
    end,
    io:format("Starting message producer~n"),
    io:format("  Pulsar Host: ~s~n", [PulsarHost]),
    io:format("  Topic: ~s~n", [Topic]),
    io:format("  Messages: ~p~n", [NumMessages]),
    io:format("  Batch Size: ~p (~s)~n", [BatchSize, FormatDesc]),

    {ok, _} = application:ensure_all_started(pulsar),

    ClientId = interop_test_client,
    {ok, _ClientPid} = pulsar:ensure_supervised_client(ClientId, [PulsarHost], #{}),
    io:format("Connected to Pulsar~n"),

    %% Create producer with specified batch_size
    ProducerOpts = #{
        batch_size => BatchSize,
        strategy => first_key_dispatch
    },
    {ok, Producers} = pulsar:ensure_supervised_producers(
        ClientId,
        Topic,
        ProducerOpts
    ),
    io:format("Producer created~n"),

    %% Wait for producer to connect
    timer:sleep(2000),

    %% Produce messages with different keys for key_shared consumption
    Keys = [<<"key1">>, <<"key2">>, <<"key3">>, <<"key4">>, <<"key5">>],
    produce_messages(Producers, Keys, NumMessages, 0),

    io:format("~nProduced ~p messages~n", [NumMessages]),
    io:format("Waiting 2 seconds before cleanup...~n"),
    timer:sleep(2000),

    pulsar:stop_and_delete_supervised_producers(Producers),
    pulsar:stop_and_delete_supervised_client(ClientId),
    application:stop(pulsar),
    io:format("Done~n"),
    ok;
main(_) ->
    io:format("Usage: ~s <pulsar_host> <topic> <num_messages> [batch_size]~n", [escript:script_name()]),
    io:format("Example (single message format): ~s pulsar://localhost:6650 persistent://public/default/interop-test 100~n", [escript:script_name()]),
    io:format("Example (batch format): ~s pulsar://localhost:6650 persistent://public/default/interop-test 100 2~n", [escript:script_name()]),
    halt(1).

setup_code_paths() ->
    %% Get the script directory
    ScriptPath = escript:script_name(),
    ScriptDir = filename:dirname(ScriptPath),
    ProjectRoot = filename:absname(filename:join([ScriptDir, "..", ".."])),

    %% Add _build/default/lib/*/ebin to code path
    LibDir = filename:join([ProjectRoot, "_build", "default", "lib"]),
    case filelib:is_dir(LibDir) of
        true ->
            {ok, Apps} = file:list_dir(LibDir),
            lists:foreach(
                fun(App) ->
                    EbinDir = filename:join([LibDir, App, "ebin"]),
                    case filelib:is_dir(EbinDir) of
                        true ->
                            code:add_patha(EbinDir);
                        false ->
                            ok
                    end
                end,
                Apps);
        false ->
            %% Try alternative path structure
            EbinDir = filename:join([ProjectRoot, "_build", "default", "lib", "pulsar", "ebin"]),
            case filelib:is_dir(EbinDir) of
                true ->
                    code:add_patha(EbinDir);
                false ->
                    io:format("Warning: Could not find pulsar ebin directory. Trying current paths...~n")
            end
    end,

    %% Also add src directory if running from source
    SrcEbin = filename:join([ProjectRoot, "src"]),
    case filelib:is_dir(SrcEbin) of
        true ->
            code:add_patha(SrcEbin);
        false ->
            ok
    end.

produce_messages(_Producers, _Keys, 0, _Count) ->
    ok;
produce_messages(Producers, Keys, Remaining, Count) ->
    Key = lists:nth((Count rem length(Keys)) + 1, Keys),
    Value = iolist_to_binary([<<"message-">>, integer_to_binary(Count)]),
    Message = #{key => Key, value => Value},

    case pulsar:send_sync(Producers, [Message], 5000) of
        {ok, _Receipt} ->
            if Count rem 10 =:= 0 ->
                io:format("Produced message ~p: key=~s, value=~s~n", [Count, Key, Value]);
            true ->
                ok
            end,
            produce_messages(Producers, Keys, Remaining - 1, Count + 1);
        Error ->
            io:format("Error sending message ~p: ~p~n", [Count, Error]),
            timer:sleep(100),
            produce_messages(Producers, Keys, Remaining, Count)
    end.
