%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(pulsar_relup).

-include("src/pulsar_producer_internal.hrl").

-define(REPLAYQ_VSN, {0, 7, 0}).

-type parsed_version() :: { _Major :: pos_integer()
                          , _Minor :: pos_integer()
                          , _Patch :: pos_integer()
                          }.

%% used in appups.
-export([ producer_pids/0
        , suspend_producers/0
        , resume_producers/0
        , change_producers_up/2
        , change_producers_down/2
        , post_producer_code_load/2
        , collect_and_downgrade_send_requests/0
        , collect_send_requests/2
        ]).

%% API for use in other modules.
-export([is_before_replayq/1]).

-ifdef(TEST).
-export([ post_producer_code_load/3
        , change_producers_up/3
        , change_producers_down/3
        ]).
-endif.

-spec is_before_replayq(parsed_version()) -> boolean().
is_before_replayq(ToVsn) ->
    ToVsn < ?REPLAYQ_VSN.

-spec producer_pids() -> [pid()].
producer_pids() ->
    [P || {_Name, PS, _Type, _Mods} <- supervisor:which_children(pulsar_producers_sup),
          P <- element(2, process_info(PS, links)),
          case proc_lib:initial_call(P) of
              {pulsar_producer, init, _} -> true;
              _ -> false
          end].

-spec suspend_producers() -> ok.
suspend_producers() ->
    lists:foreach(fun sys:suspend/1, producer_pids()).

-spec resume_producers() -> ok.
resume_producers() ->
    lists:foreach(fun sys:resume/1, producer_pids()).

-spec change_producers_up(CurVsn :: parsed_version(), FromVsn :: parsed_version()) -> ok.
change_producers_up(CurVsn, FromVsn) ->
    change_producers_up(producer_pids(), CurVsn, FromVsn).

-spec change_producers_up([pid()], ToVsn :: parsed_version(), FromVsn :: parsed_version()) -> ok.
change_producers_up(ProducerPIDs, CurVsn, FromVsn) ->
    Extra = #{from_version => FromVsn, to_version => CurVsn},
    lists:foreach(
      fun(P) ->
        sys:change_code(P, pulsar_producer, CurVsn, Extra, 30_000)
      end,
      ProducerPIDs).

-spec change_producers_down(FromVsn :: parsed_version(), ToVsn :: parsed_version()) -> ok.
change_producers_down(CurVsn, ToVsn) ->
    change_producers_down(producer_pids(), CurVsn, ToVsn).

-spec change_producers_down([pid()], CurVsn :: parsed_version(), FromVsn :: parsed_version()) -> ok.
change_producers_down(ProducerPIDs, CurVsn, ToVsn) ->
    Extra = #{from_version => CurVsn, to_version => ToVsn},
    lists:foreach(
      fun(P) ->
        sys:change_code(P, pulsar_producer, {down, ToVsn}, Extra, 30_000)
      end,
      ProducerPIDs).

-spec post_producer_code_load(CurVsn :: parsed_version(), FromVsn :: parsed_version()) -> ok.
post_producer_code_load(CurVsn, ToVsn) ->
    post_producer_code_load(producer_pids(), CurVsn, ToVsn).

-spec post_producer_code_load([pid()], CurVsn :: parsed_version(), FromVsn :: parsed_version()) -> ok.
post_producer_code_load(ProducerPIDs, _CurVsn, ToVsn) ->
    case is_before_replayq(ToVsn) of
        true ->
            %% we should call this after the old code is loaded; at that
            %% point, external code calling `pulsar_producer:send{,_async}'
            %% will stop producing messages in the new format.
            lists:foreach(
              fun(P) ->
                sys:replace_state(P, fun(Data) ->
                  pulsar_relup:collect_and_downgrade_send_requests(),
                  Data
                end)
              end,
              ProducerPIDs);
        false ->
            ok
    end.

-spec collect_and_downgrade_send_requests() -> ok.
collect_and_downgrade_send_requests() ->
    SendRequests = pulsar_relup:collect_send_requests(_Acc = [], _Limit = 10_000),
    lists:foreach(
      fun(?SEND_REQ(_From = undefined, Messages)) ->
              self() ! {'$gen_cast', {send, Messages}};
         (?SEND_REQ(From, Messages)) ->
              self() ! {'$gen_call', From, {send, Messages}}
      end,
      SendRequests).

-spec collect_send_requests([?SEND_REQ(gen_statem:from() | undefined, [pulsar:message()])],
                            non_neg_integer()) ->
          [?SEND_REQ(gen_statem:from() | undefined, [pulsar:message()])].
collect_send_requests(Acc, Limit) ->
    Count = length(Acc),
    do_collect_send_requests(Acc, Count, Limit).

-spec do_collect_send_requests([?SEND_REQ(gen_statem:from() | undefined, [pulsar:message()])],
                               non_neg_integer(),
                               non_neg_integer()) ->
          [?SEND_REQ(gen_statem:from() | undefined, [pulsar:message()])].
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
