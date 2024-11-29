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

-export([start_link/3]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        ]).

-export([ get_topic_metadata/3
        , lookup_topic/3
        , lookup_topic_async/2
        ]).

-export([ get_status/2
        , get_alive_pulsar_url/2
        ]).

%% For tests/introspection.
-export([get_workers/2]).

-export_type([topic/0]).

%%--------------------------------------------------------------------
%% Type definitions
%%--------------------------------------------------------------------

-type client_id() :: atom().
-type url() :: string().
-type topic() :: string().
-type partition_topic() :: string().

-define(client_id, client_id).
-define(initial_opts, initial_opts).
-define(seed_urls, seed_urls).
-define(workers, workers).
-define(no_servers_available, no_servers_available).

-type state() :: #{
    ?client_id := client_id(),
    ?initial_opts := map(),
    ?seed_urls := [url()],
    ?workers := #{url() => pid(), pid() => url()}
}.

%% calls/casts/infos
-record(get_topic_metadata, {topic :: topic()}).
-record(lookup_topic, {deadline :: infinity | integer(), partition_topic :: partition_topic()}).
-record(lookup_topic_async, {from, partition_topic :: partition_topic()}).
-record(get_status, {}).
-record(get_alive_pulsar_url, {}).
-record(get_workers, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(ClientId, Servers, Opts) ->
    Args = #{
      client_id => ClientId,
      seed_urls => Servers,
      initial_opts => Opts
    },
    gen_server:start_link({local, ClientId}, ?MODULE, Args, []).

get_topic_metadata(ClientId, Topic, Timeout) ->
    gen_server:call(ClientId, #get_topic_metadata{topic = Topic}, Timeout).

lookup_topic(ClientId, PartitionTopic, Timeout) ->
    %% We use a deadline here because `pulsar_producers' and `pulsar_consumers' call this
    %% with a timeout, and we want to avoid polluting their mailboxes and logs with stale
    %% responses.
    Deadline = deadline(Timeout),
    gen_server:call(
      ClientId,
      #lookup_topic{deadline = Deadline, partition_topic = PartitionTopic},
      Timeout).

-spec lookup_topic_async(gen_server:server_ref(), string()) ->
          {ok, reference()} | {error, ?no_servers_available}.
lookup_topic_async(ClientId, PartitionTopic) ->
    Ref = monitor(process, ClientId, [{alias, reply_demonitor}]),
    From = {self(), Ref},
    gen_server:cast(ClientId, #lookup_topic_async{from = From, partition_topic = PartitionTopic}),
    {ok, Ref}.

get_status(ClientId, Timeout) ->
    try
        gen_server:call(ClientId, #get_status{}, Timeout)
    catch
        exit:{noproc, _} ->
            false;
        exit:{timeout, _} ->
            false
    end.

get_alive_pulsar_url(ClientId, Timeout) ->
    gen_server:call(ClientId, #get_alive_pulsar_url{}, Timeout).

get_workers(ClientId, Timeout) ->
    gen_server:call(ClientId, #get_workers{}, Timeout).

%%--------------------------------------------------------------------
%% `gen_server' API
%%--------------------------------------------------------------------

-spec init(map()) -> {ok, state()}.
init(Args) ->
    #{
      client_id := ClientId,
      seed_urls := SeedURLs,
      initial_opts := Opts
    } = Args,
    process_flag(trap_exit, true),
    State0 = #{
        ?client_id => ClientId,
        ?initial_opts => Opts,
        ?seed_urls => SeedURLs,
        ?workers => #{}
    },
    case spawn_any_and_wait_connected(State0) of
        {{ok, _}, State} ->
            {ok, State};
        {{error, _} = Error, _State} ->
            {stop, Error}
    end.

handle_call(#get_workers{}, _From, State) ->
    #{?workers := Workers} = State,
    {reply, Workers, State};
handle_call(#get_status{}, _From, State0) ->
    {Reply, State} = handle_get_status(State0),
    {reply, Reply, State};
handle_call(#get_alive_pulsar_url{}, _From, State0) ->
    {Reply, State} = handle_get_alive_pulsar_url(State0),
    {reply, Reply, State};
handle_call(#get_topic_metadata{topic = Topic}, _From, State0) ->
    {Reply, State} = handle_get_topic_metadata(State0, Topic),
    {reply, Reply, State};
handle_call(#lookup_topic{deadline = Deadline, partition_topic = PartitionTopic}, From, State0) ->
    {Reply, State} = handle_lookup_topic_async(State0, PartitionTopic),
    maybe
        true ?= is_within_deadline(Deadline),
        gen_server:reply(From, Reply)
    end,
    {noreply, State};
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State, hibernate}.

handle_cast(#lookup_topic_async{from = From, partition_topic = PartitionTopic}, State0) ->
    {Reply, State} = handle_lookup_topic_async(State0, PartitionTopic),
    gen_server:reply(From, Reply),
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State, hibernate}.

handle_info({'EXIT', Pid, Reason}, State0) ->
    State = handle_worker_down(State0, Pid, Reason),
    {noreply, State};
handle_info(_Info, State) ->
    log_error("received unknown message: ~p", [_Info]),
    {noreply, State, hibernate}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal fns
%%--------------------------------------------------------------------

handle_get_status(State0) ->
    case alive_workers(State0) of
        [] ->
            case spawn_any_and_wait_connected(State0) of
                {{ok, _}, State} ->
                    {true, State};
                {{error, _}, State} ->
                    {false, State}
            end;
        [_ | _] ->
            %% Clients shut themselves down if no pong received after a timeout.
            {true, State0}
    end.

handle_get_alive_pulsar_url(State0) ->
    WorkerPids = alive_workers(State0),
    Fun = fun pulsar_client:get_alive_pulsar_url/1,
    Args = [],
    case get_first_successful_call(WorkerPids, Fun, Args) of
        {ok, URI} ->
            {{ok, URI}, State0};
        {error, ?no_servers_available} ->
            maybe
                {{ok, Pid}, State} ?= spawn_any_and_wait_connected(State0),
                Res = get_first_successful_call([Pid], Fun, Args),
                {Res, State}
            end;
        {error, _} = Res ->
            {Res, State0}
    end.

handle_get_topic_metadata(State0, Topic) ->
    WorkerPids = alive_workers(State0),
    Fun = fun pulsar_client:get_topic_metadata/2,
    Args = [Topic],
    case get_first_successful_call(WorkerPids, Fun, Args) of
        {ok, _} = Res ->
            {Res, State0};
        {error, ?no_servers_available} ->
            maybe
                {{ok, Pid}, State} ?= spawn_any_and_wait_connected(State0),
                Res = get_first_successful_call([Pid], Fun, Args),
                {Res, State}
            end;
        {error, _} = Res ->
            {Res, State0}
    end.

handle_lookup_topic_async(State0, PartitionTopic) ->
    WorkerPids = alive_workers(State0),
    Fun = fun pulsar_client:lookup_topic/2,
    Args = [PartitionTopic],
    case get_first_successful_call(WorkerPids, Fun, Args) of
        {redirect, ServiceURL, Opts} ->
            handle_redirect_lookup(State0, ServiceURL, Opts, PartitionTopic);
        {ok, _} = Res ->
            {Res, State0};
        {error, ?no_servers_available} ->
            handle_lookup_topic_async_fresh_worker(State0, PartitionTopic);
        {error, _} = Res ->
            {Res, State0}
    end.

handle_lookup_topic_async_fresh_worker(State0, PartitionTopic) ->
    maybe
        {{ok, Pid}, State} ?= spawn_any_and_wait_connected(State0),
        Fun = fun pulsar_client:lookup_topic/2,
        Args = [PartitionTopic],
        case get_first_successful_call([Pid], Fun, Args) of
            {redirect, ServiceURL, Opts} ->
                handle_redirect_lookup(State0, ServiceURL, Opts, PartitionTopic);
            Res ->
                {Res, State}
        end
    else
        {Error, State1} ->
            {Error, State1}
    end.

handle_redirect_lookup(State0, ServiceURL, Opts, PartitionTopic) ->
    case find_alive_worker(State0, ServiceURL) of
        {ok, Pid} ->
            try pulsar_client:lookup_topic(Pid, PartitionTopic, Opts) of
                {redirect, ServiceURL, _Opts} ->
                    %% Should not respond with this, since we've just used this server.
                    %% Replying error to avoid loop
                    {{error, pulsar_unstable}, State0};
                {redirect, OtherServiceURL, OtherOpts} ->
                    handle_redirect_lookup(State0, OtherServiceURL, OtherOpts, PartitionTopic);
                Res ->
                    {Res, State0}
            catch
                exit:{timeout, _} ->
                    {{error, timeout}, State0};
                exit:{noproc, _} ->
                    %% race; retry
                    timer:sleep(10),
                    handle_redirect_lookup(State0, ServiceURL, Opts, PartitionTopic)
            end;
        error ->
            maybe
                {{ok, _Pid}, State} ?= spawn_any_and_wait_connected(State0, [ServiceURL]),
                handle_redirect_lookup(State, ServiceURL, Opts, PartitionTopic)
            end
    end.

handle_worker_down(State0, Pid, Reason) ->
    #{?workers := Workers0} = State0,
    maybe
        {ok, URL} ?= find_url_by_worker_pid(Workers0, Pid),
        log_info("worker for ~0s down: ~0p", [URL, Reason]),
        {_Pid, Workers} = maps:take(URL, Workers0),
        State0#{?workers := Workers}
    else
        error -> State0
    end.

find_url_by_worker_pid(Workers, Pid) ->
    MURL = [URL || {URL, Pid0} <- maps:to_list(Workers), Pid0 =:= Pid],
    case MURL of
        [] ->
            error;
        [URL] ->
            {ok, URL}
    end.

find_alive_worker(State, URL) ->
    #{?workers := Workers} = State,
    URLKey = bin(URL),
    maybe
        #{URLKey := Pid} ?= Workers,
        true ?= is_process_alive(Pid),
        {ok, Pid}
    else
        _ -> error
    end.

alive_workers(State) ->
    #{?workers := Workers} = State,
    Pids = maps:values(Workers),
    lists:filter(fun is_process_alive/1, Pids).

get_first_successful_call(WorkerPids, Fun, Args) ->
    pulsar_utils:foldl_while(
      fun(WorkerPid, Acc) ->
        try
            {halt, apply(Fun, [WorkerPid | Args])}
        catch
            _:_ ->
                {cont, Acc}
        end
      end,
      {error, ?no_servers_available},
      WorkerPids).

%% Takes the list of seed URLs and attempts to have at most one worker alive and connected
%% to it.
-spec spawn_any_and_wait_connected(state()) -> {{ok, pid()} | {error, term()}, state()}.
spawn_any_and_wait_connected(State0) ->
    #{?seed_urls := SeedURLs} = State0,
    spawn_any_and_wait_connected(State0, SeedURLs).

spawn_any_and_wait_connected(State0, SeedURLs) ->
    #{ ?client_id := ClientId
     , ?initial_opts := Opts
     , ?workers := Workers0
     } = State0,
    Res =
        pulsar_utils:foldl_while(
          fun(URL, _Acc) ->
            case pulsar_client:start_link(ClientId, URL, Opts) of
                {error, _} = Error ->
                    {cont, Error};
                {ok, Pid} ->
                    {halt, {ok, {URL, Pid}}}
            end
          end,
          {error, ?no_servers_available},
          SeedURLs),
    %% We currently don't use `ignore'.
    case Res of
        {ok, {URL, Pid}} ->
            Workers = Workers0#{bin(URL) => Pid},
            State = State0#{workers := Workers},
            {{ok, Pid}, State};
        {error, Reason} ->
            {{error, Reason}, State0}
    end.

log_error(Fmt, Args) ->
    do_log(error, Fmt, Args).

log_info(Fmt, Args) ->
    do_log(info, Fmt, Args).

do_log(Level, Fmt, Args) ->
    logger:log(Level, "[pulsar-client-manager] " ++ Fmt, Args, #{domain => [pulsar, client_manager]}).

bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Bin) when is_binary(Bin) -> Bin.

deadline(infinity) -> infinity;
deadline(Timeout) when Timeout > 0 -> now_ts() + Timeout.

is_within_deadline(infinity) -> true;
is_within_deadline(Deadline) -> now_ts() < Deadline.

now_ts() -> erlang:system_time(millisecond).
