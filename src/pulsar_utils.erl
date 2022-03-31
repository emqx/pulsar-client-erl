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

-module(pulsar_utils).

-export([ merge_opts/1
        ]).

merge_opts([Opts1, Opts2]) ->
    proplist_diff(Opts1, Opts2) ++ Opts2;
merge_opts([Opts1 | RemOpts]) ->
    merge_opts([Opts1, merge_opts(RemOpts)]).

proplist_diff(Opts1, Opts2) ->
    lists:foldl(fun(Opt, Opts1Acc) ->
            Key = case Opt of
                {K, _} -> K;
                K when is_atom(K) -> K
            end,
            proplists:delete(Key, Opts1Acc)
        end, Opts1, Opts2).
