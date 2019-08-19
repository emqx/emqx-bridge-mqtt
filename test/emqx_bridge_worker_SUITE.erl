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

-module(emqx_bridge_worker_SUITE).

-export([ all/0
        , init_per_suite/1
        , end_per_suite/1]).
-export([ t_rpc/1
        , t_mqtt/1
        , t_mngr/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(wait(For, Timeout), emqx_ct_helpers:wait_for(?FUNCTION_NAME, ?LINE, fun() -> For end, Timeout)).

receive_messages(Count) ->
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count-1, [Msg|Msgs]);
        _Other ->
            receive_messages(Count, Msgs)
    after 100 ->
        Msgs
    end.

all() -> [ t_rpc
         , t_mqtt
         , t_mngr
         ].

init_per_suite(Config) ->
    case node() of
        nonode@nohost -> net_kernel:start(['emqx@127.0.0.1', longnames]);
        _ -> ok
    end,
    ok = application:set_env(gen_rpc, tcp_client_num, 1),
    emqx_ct_helpers:start_apps([emqx_bridge_mqtt]),
    emqx_logger:set_log_level(error),
    [{log_level, error} | Config].

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_bridge_mqtt]).

t_mngr(Config) when is_list(Config) ->
    Subs = [{<<"a">>, 1}, {<<"b">>, 2}],
    Cfg = #{address => node(),
            forwards => [<<"mngr">>],
            connect_module => emqx_bridge_rpc,
            mountpoint => <<"forwarded">>,
            subscriptions => Subs,
            start_type => auto},
    Name = ?FUNCTION_NAME,
    {ok, Pid} = emqx_bridge_worker:start_link(Name, Cfg),
    try
        ?assertEqual([<<"mngr">>], emqx_bridge_worker:get_forwards(Name)),
        ?assertEqual(ok, emqx_bridge_worker:ensure_forward_present(Name, "mngr")),
        ?assertEqual(ok, emqx_bridge_worker:ensure_forward_present(Name, "mngr2")),
        ?assertEqual([<<"mngr">>, <<"mngr2">>], emqx_bridge_worker:get_forwards(Pid)),
        ?assertEqual(ok, emqx_bridge_worker:ensure_forward_absent(Name, "mngr2")),
        ?assertEqual(ok, emqx_bridge_worker:ensure_forward_absent(Name, "mngr3")),
        ?assertEqual([<<"mngr">>], emqx_bridge_worker:get_forwards(Pid)),
        ?assertEqual({error, no_remote_subscription_support},
                     emqx_bridge_worker:ensure_subscription_present(Pid, <<"t">>, 0)),
        ?assertEqual({error, no_remote_subscription_support},
                     emqx_bridge_worker:ensure_subscription_absent(Pid, <<"t">>)),
        ?assertEqual(Subs, emqx_bridge_worker:get_subscriptions(Pid))
    after
        ok = emqx_bridge_worker:stop(Pid)
    end.

%% A loopback RPC to local node
t_rpc(Config) when is_list(Config) ->
    Cfg = #{address => node(),
            forwards => [<<"t_rpc/#">>],
            connect_module => emqx_bridge_rpc,
            mountpoint => <<"forwarded">>,
            start_type => auto},
    {ok, Pid} = emqx_bridge_worker:start_link(?FUNCTION_NAME, Cfg),
    ClientId = <<"ClientId">>,
    try
        {ok, ConnPid} = emqtt:start_link([{client_id, ClientId}]),
        {ok, _Props} = emqtt:connect(ConnPid),
        {ok, _Props, [1]} = emqtt:subscribe(ConnPid, {<<"forwarded/t_rpc/one">>, ?QOS_1}),
        timer:sleep(100),
        {ok, _PacketId} = emqtt:publish(ConnPid, <<"t_rpc/one">>, <<"hello">>, ?QOS_1),
        timer:sleep(100),
        ?assertEqual(1, length(receive_messages(1))),
        emqtt:disconnect(ConnPid)
    after
        ok = emqx_bridge_worker:stop(Pid)
    end.

%% Full data loopback flow explained:
%% mqtt-client ----> local-broker ---(local-subscription)--->
%% bridge(export) --- (mqtt-connection)--> local-broker ---(remote-subscription) -->
%% bridge(import) --> mqtt-client
t_mqtt(Config) when is_list(Config) ->
    SendToTopic = <<"t_mqtt/one">>,
    SendToTopic2 = <<"t_mqtt/two">>,
    Mountpoint = <<"forwarded/${node}/">>,
    ForwardedTopic = emqx_topic:join(["forwarded", atom_to_list(node()), SendToTopic]),
    ForwardedTopic2 = emqx_topic:join(["forwarded", atom_to_list(node()), SendToTopic2]),
    Cfg = #{address => "127.0.0.1:1883",
            forwards => [SendToTopic],
            connect_module => emqx_bridge_mqtt,
            mountpoint => Mountpoint,
            username => "user",
            clean_start => true,
            client_id => "bridge_aws",
            keepalive => 60000,
            max_inflight => 32,
            password => "passwd",
            proto_ver => mqttv4,
            queue => #{replayq_dir => "data/t_mqtt/",
                       replayq_seg_bytes => 10000,
                       batch_bytes_limit => 1000,
                       batch_count_limit => 10
                      },
            reconnect_delay_ms => 1000,
            ssl => false,
            %% Consume back to forwarded message for verification
            %% NOTE: this is a indefenite loopback without mocking emqx_bridge_worker:import_batch/2
            subscriptions => [{ForwardedTopic, _QoS = 1}],
            start_type => auto},
    Tester = self(),
    Ref = make_ref(),
    meck:new(emqx_bridge_worker, [passthrough, no_history]),
    meck:expect(emqx_bridge_worker, import_batch, 3,
                fun(Batch, AckFun, _IfRecordMetrics) ->
                        Tester ! {publish, {Ref, Batch}},
                        AckFun()
                end),
    {ok, Pid} = emqx_bridge_worker:start_link(?FUNCTION_NAME, Cfg),
    ClientId = <<"client-1">>,
    try
        ?assertEqual([{ForwardedTopic, 1}], emqx_bridge_worker:get_subscriptions(Pid)),
        ok = emqx_bridge_worker:ensure_subscription_present(Pid, ForwardedTopic2, _QoS = 1),
        ok = emqx_bridge_worker:ensure_forward_present(Pid, SendToTopic2),
        ?assertEqual([{ForwardedTopic, 1},
                      {ForwardedTopic2, 1}],
                     emqx_bridge_worker:get_subscriptions(Pid)),
        {ok, ConnPid} = emqtt:start_link([{client_id, ClientId}]),
        {ok, _Props} = emqtt:connect(ConnPid),
        %% message from a different client, to avoid getting terminated by no-local
        Max = 100,
        Msgs = lists:seq(1, Max),
        lists:foreach(fun(I) ->
                          {ok, _PacketId} = emqtt:publish(ConnPid, SendToTopic, integer_to_binary(I), ?QOS_1)
                      end, Msgs),
        ?assertEqual(100, length(receive_messages(200))),
        emqtt:disconnect(ConnPid)
    after
        ok = emqx_bridge_worker:stop(Pid),
        meck:unload(emqx_bridge_worker)
    end.
