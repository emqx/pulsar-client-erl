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

-module(pulsar_client_manager).

-feature(maybe_expr, enable).

-behaviour(gen_server).

-export([start_link/2]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        ]).

-export([ get_topic_metadata/2
        , lookup_topic/2
        , lookup_topic/3
        , lookup_topic_async/2
        ]).

-export([ get_status/1
        , get_alive_pulsar_url/1
        ]).

-export([register_worker/3, unregister_worker_async/2]).


-type server_ref() :: gen_server:server_ref().

-define(PT_WORKERS_KEY(CLIENT_ID), {?MODULE, CLIENT_ID, workers}).

%%--------------------------------------------------------------------
%% Type definitions
%%--------------------------------------------------------------------

%% calls/casts/infos
-record(register_worker, {worker_pid :: pid(), url :: binary()}).
-record(unregister_worker, {url :: binary()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(ClientId, Opts) ->
    gen_server:start_link({local, ClientId}, ?MODULE, [ClientId, Opts], []).

get_topic_metadata(ClientId, Topic) ->
    maybe
        {ok, WorkerPid} ?= pick_random_worker(ClientId),
        pulsar_client_worker:get_topic_metadata(WorkerPid, Topic)
    end.

lookup_topic(ClientId, PartitionTopic) ->
    maybe
        {ok, WorkerPid} ?= pick_random_worker(ClientId),
        pulsar_client_worker:lookup_topic(WorkerPid, PartitionTopic)
    end.

lookup_topic(ClientId, PartitionTopic, BrokerServiceURL) ->
    maybe
        {ok, WorkerPid} ?= get_worker_for_service_url(ClientId, BrokerServiceURL),
        pulsar_client_worker:lookup_topic(WorkerPid, PartitionTopic)
    end.

-spec lookup_topic_async(server_ref(), binary()) -> {ok, reference()} | {error, no_servers_available}.
lookup_topic_async(ClientId, PartitionTopic) ->
    maybe
        {ok, WorkerPid} ?= pick_random_worker(ClientId),
        pulsar_client_worker:lookup_topic_async(WorkerPid, PartitionTopic)
    end.

get_status(ClientId) ->
    maybe
        WorkerPids = maps:values(get_workers(ClientId)),
        true ?= length(WorkerPids) > 0,
        lists:all(fun pulsar_client_worker:get_status/1, WorkerPids)
    end.

get_alive_pulsar_url(ClientId) ->
    maybe
        %% TODO: return all urls?
        {ok, WorkerPid} ?= pick_random_worker(ClientId),
        pulsar_client_worker:get_alive_pulsar_url(WorkerPid)
    end.

register_worker(ClientId, WorkerPid, ServiceURL0) ->
    ServiceURL = bin(ServiceURL0),
    gen_server:call(ClientId, #register_worker{worker_pid = WorkerPid, url = ServiceURL}, infinity).

unregister_worker_async(ClientId, ServiceURL0) ->
    ServiceURL = bin(ServiceURL0),
    gen_server:cast(ClientId, #unregister_worker{url = ServiceURL}).

%%--------------------------------------------------------------------
%% `gen_server' API
%%--------------------------------------------------------------------

init([ClientId, _Opts]) ->
    process_flag(trap_exit, true),
    persistent_term:put(?PT_WORKERS_KEY(ClientId), #{}),
    State = #{client_id => ClientId},
    {ok, State}.

handle_call(#register_worker{worker_pid = WorkerPid, url = URL}, _From, State) ->
    #{client_id := ClientId} = State,
    handle_register_worker(ClientId, WorkerPid, URL),
    {reply, ok, State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State, hibernate}.

handle_cast(#unregister_worker{url = URL}, State) ->
    #{client_id := ClientId} = State,
    handle_unregister_worker(ClientId, URL),
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State, hibernate}.

handle_info(_Info, State) ->
    log_error("received unknown message: ~p", [_Info]),
    {noreply, State, hibernate}.

terminate(_Reason, #{client_id := ClientId}) ->
    persistent_term:erase(?PT_WORKERS_KEY(ClientId)),
    ok.

log_error(Fmt, Args) ->
    do_log(error, Fmt, Args).

do_log(Level, Fmt, Args) ->
    logger:log(Level, "[pulsar-client] " ++ Fmt, Args, #{domain => [pulsar, client]}).

get_worker_for_service_url(ClientId, BrokerServiceURL0) ->
    BrokerServiceURL = bin(BrokerServiceURL0),
    case persistent_term:get(?PT_WORKERS_KEY(ClientId), undefined) of
        #{BrokerServiceURL := Worker} ->
            {ok, Worker};
        _ ->
            {error, worker_not_found}
    end.

get_workers(ClientId) ->
    persistent_term:get(?PT_WORKERS_KEY(ClientId), #{}).

pick_random_worker(ClientId) ->
    case get_workers(ClientId) of
        Workers when map_size(Workers) == 0 ->
            {error, no_servers_available};
        Workers ->
            {_, Worker} = hd(lists:sort([{rand:uniform(), W} || W <- maps:values(Workers)])),
            {ok, Worker}
    end.

handle_register_worker(ClientId, WorkerPid, URL) ->
    Workers0 = persistent_term:get(?PT_WORKERS_KEY(ClientId)),
    Workers = Workers0#{URL => WorkerPid},
    persistent_term:put(?PT_WORKERS_KEY(ClientId), Workers),
    ok.

handle_unregister_worker(ClientId, URL) ->
    case persistent_term:get(?PT_WORKERS_KEY(ClientId)) of
        #{URL := _} = Workers0 ->
            Workers = maps:remove(URL, Workers0),
            persistent_term:put(?PT_WORKERS_KEY(ClientId), Workers),
            ok;
        _ ->
            ok
    end.

bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Bin) when is_binary(Bin) -> Bin.
