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

%% @doc This module implements EMQX Bridge transport layer on top of MQTT protocol

-module(emqx_bridge_mqtt_actions).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-import(emqx_rule_utils, [str/1]).

-export([ on_resource_create/2
        , on_get_resource_status/2
        , on_resource_destroy/2
        ]).

%% Callbacks of ecpool Worker
-export([connect/1]).

-export([subscriptions/1]).

-export([on_action_create_data_to_mqtt_broker/2]).

-define(RESOURCE_TYPE_MQTT, 'bridge_mqtt').
-define(RESOURCE_TYPE_RPC, 'bridge_rpc').

-define(RESOURCE_CONFIG_SPEC_MQTT,
        #{address => #{order => 1,
                       type => string,
                       required => true,
                       default => <<"127.0.0.1:1883">>,
                       title => #{en => <<"Broker Address">>,
                                  zh => <<"桥接地址"/utf8>>},
                       description => #{en => <<"The MQTT Remote IP Address">>,
                                        zh => <<"远程 MQTT Broker 的 IP 地址"/utf8>>
                                       }
                      },
          proto_ver => #{order => 2,
                         type => string,
                         required => false,
                         default => <<"mqttv4">>,
                         enum => [<<"mqttv3">>, <<"mqttv4">>, <<"mqttv5">>],
                         title => #{en => <<"Protocol Version">>,
                                    zh => <<"协议版本"/utf8>>},
                         description => #{en => <<"Protocol version for MQTT bridge"
                                                  "(Only for bridge with MQTT protocol)">>,
                                          zh => <<"用于 MQTT 桥接的 MQTT 协议版本"
                                                  "（仅适用于 MQTT 协议桥接）"/utf8>>
                                         }
                        },
          pool_size => #{order => 4,
                         type => number,
                         required => true,
                         default => 4,
                         title => #{en => <<"Pool Size">>, zh => <<"连接池大小"/utf8>>},
                         description => #{en => <<"Size of MQTT/RPC Connection Pool">>,
                                          zh => <<"客户端连接池大小"/utf8>>}
                        },
          username => #{order => 5,
                        type => string,
                        required => false,
                        default => <<"user">>,
                        title => #{en => <<"MQTT Username">>, zh => <<"MQTT 用户名"/utf8>>},
                        description => #{en => <<"Username for connecting to MQTT Broker"
                                                 "(Only for bridge with MQTT protocol)">>,
                                         zh => <<"用于桥接 MQTT Broker 的 Clean Start 值"
                                                 "（仅适用于 MQTT 协议桥接）"/utf8>>}
                       },
          password => #{order => 6,
                        type => string,
                        required => false,
                        default => <<"passwd">>,
                        title => #{en => <<"MQTT Password">>, zh => <<"MQTT 密码"/utf8>>},
                        description => #{en => <<"Password for connecting to MQTT Broker"
                                                 "(Only for bridge with MQTT protocol)">>,
                                         zh => <<"用于桥接 MQTT Broker 的密码"
                                                 "（仅适用于 MQTT 协议桥接）"/utf8>>}
                       },
          mountpoint => #{order => 7,
                          type => string,
                          required => true,
                          default => <<"bridge/aws/${node}/">>,
                          title => #{en => <<"Bridge MountPoint">>,
                                     zh => <<"桥接挂载点"/utf8>>},
                          description => #{en => <<"MountPoint for bridge topic<br/>"
                                                   "Example: The topic of messages sent to `topic1` on local node"
                                                   "will be transformed to `bridge/aws/${node}/topic1`">>,
                                           zh => <<"桥接主题的挂载点<br/>"
                                                   "示例: 本地节点向 `topic1` 发消息，远程桥接节点的主题"
                                                   "会变换为 `bridge/aws/${node}/topic1`"/utf8>>
                                          }
                         },
          keepalive => #{order => 8,
                         type => string,
                         required => true,
                         default => <<"60s">> ,
                         title => #{en => <<"Ping interval">>,
                                    zh => <<"桥接的心跳间隔"/utf8>>},
                         description => #{en => <<"Ping interval">>,
                                          zh => <<"桥接的心跳间隔"/utf8>>}
                        },
          reconnect_interval => #{order => 9,
                                  type => string,
                                  required => false,
                                  default => <<"30s">>,
                                  title => #{en => <<"Reconnect Interval">>,
                                             zh => <<"重连间隔"/utf8>>},
                                  description => #{en => <<"Start type of the bridge<br/>"
                                                           "Enum: auto, manual">>,
                                                   zh => <<"桥接的启动类型<br/>"
                                                           "启动类型: auto, manual"/utf8>>}
                                 },
          retry_interval => #{order => 10,
                              type => string,
                              required => false,
                              default => <<"20s">>,
                              title => #{en => <<"Retry interval">>,
                                         zh => <<"重传间隔"/utf8>>},
                              description => #{en => <<"Retry interval for bridge QoS1 message delivering">>,
                                               zh => <<"QoS1 消息重传间隔"/utf8>>}
                             },
          ssl => #{order => 11,
                   type => string,
                   required => true,
                   default => <<"off">>,
                   title => #{en => <<"Bridge ssl">>,
                              zh => <<"Bridge ssl"/utf8>>
                             },
                   description => #{en => <<"The switch which used to enable ssl connection of the bridge">>,
                                    zh => <<"用于启用 bridge ssl 连接的开关"/utf8>>
                                   }
                  },
          cacertfile => #{order => 12,
                          type => string,
                          required => false,
                          default => <<"etc/certs/cacert.pem">>,
                          title => #{en => <<"CA certificates">>,
                                     zh => <<"CA 证书"/utf8>>},
                          description => #{en => <<"The file path of the CA certificates">>,
                                           zh => <<"CA 证书所在路径"/utf8>>
                                          }
                         },
          certfile => #{order => 13,
                        type => string,
                        required => false,
                        default => <<"etc/certs/client-cert.pem">>,
                        title => #{en => <<"SSL Certfile">>,
                                   zh => <<"SSL 证书"/utf8>>},
                        description => #{en => <<"The file path of the client certfile">>,
                                         zh => <<"客户端证书文件所在路径"/utf8>>
                                        }
                       },
          keyfile => #{order => 14,
                       type => string,
                       required => false,
                       default => <<"etc/certs/client-key.pem">>,
                       title => #{en => <<"SSL Keyfile">>,
                                  zh => <<"SSL 密钥文件"/utf8>>},
                       description => #{en => <<"The file path of the client keyfile">>,
                                        zh => <<"客户端密钥文件所在路径"/utf8>>
                                       }
                      },
          ciphers => #{order => 15,
                       type => string,
                       required => false,
                       default => <<"ECDHE-ECDSA-AES256-GCM-SHA384,ECDHE-RSA-AES256-GCM-SHA384">>,
                       title => #{en => <<"SSL Ciphers">>,
                                  zh => <<"SSL 密码算法"/utf8>>},
                       description => #{en => <<"SSL Ciphers used by the bridge">>,
                                        zh => <<"用于桥接的 SSL 密码算法"/utf8>>}
                      }
         }).

-define(RESOURCE_CONFIG_SPEC_RPC,
        #{address => #{order => 1,
                       type => string,
                       required => true,
                       default => <<"emqx2@127.0.0.1">>,
                       title => #{en => <<"EMQX Node Name">>,
                                  zh => <<"EMQX 节点名称"/utf8>>},
                       description => #{en => <<"EMQX remote NodeName">>,
                                        zh => <<"远程 EMQX 节点名称 "/utf8>>
                                       }
                      },
          mountpoint => #{order => 2,
                          type => string,
                          required => true,
                          default => <<"bridge/aws/${node}/">>,
                          title => #{en => <<"Bridge MountPoint">>,
                                     zh => <<"桥接挂载点"/utf8>>},
                          description => #{en => <<"MountPoint for bridge topic<br/>"
                                                   "Example: The topic of messages sent to `topic1` on local node"
                                                   "will be transformed to `bridge/aws/${node}/topic1`">>,
                                           zh => <<"桥接主题的挂载点<br/>"
                                                   "示例: 本地节点向 `topic1` 发消息，远程桥接节点的主题"
                                                   "会变换为 `bridge/aws/${node}/topic1`"/utf8>>
                                          }
                         },
          pool_size => #{order => 3,
                         type => number,
                         required => true,
                         default => 4,
                         title => #{en => <<"Pool Size">>, zh => <<"连接池大小"/utf8>>},
                         description => #{en => <<"Size of MQTT/RPC Connection Pool">>,
                                          zh => <<"客户端连接池大小"/utf8>>} 
                        }
         }).

-define(ACTION_PARAM_RESOURCE,
        #{type => string,
          required => true,
          title => #{en => <<"Resource ID">>, zh => <<"资源 ID"/utf8>>},
          description => #{en => <<"Bind a resource to this action">>,
                           zh => <<"给动作绑定一个资源"/utf8>>
                          }
         }).

-resource_type(#{name => ?RESOURCE_TYPE_MQTT,
                 create => on_resource_create,
                 status => on_get_resource_status,
                 destroy => on_resource_destroy,
                 params => ?RESOURCE_CONFIG_SPEC_MQTT,
                 title => #{en => <<"MQTT Bridge">>, zh => <<"MQTT Bridge"/utf8>>},
                 description => #{en => <<"MQTT Message Bridge">>, zh => <<"MQTT 消息桥接"/utf8>>}
                }).

-resource_type(#{name => ?RESOURCE_TYPE_RPC,
                 create => on_resource_create,
                 status => on_get_resource_status,
                 destroy => on_resource_destroy,
                 params => ?RESOURCE_CONFIG_SPEC_RPC,
                 title => #{en => <<"RPC Bridge">>, zh => <<"RPC Bridge"/utf8>>},
                 description => #{en => <<"EMQX RPC Bridge">>, zh => <<"EMQX RPC 消息桥接"/utf8>>}
                }).

-rule_action(#{name => data_to_mqtt_broker,
               for => '$any',
               types => [?RESOURCE_TYPE_MQTT, ?RESOURCE_TYPE_RPC],
               create => on_action_create_data_to_mqtt_broker,
               params => #{'$resource' => ?ACTION_PARAM_RESOURCE},
               title => #{en => <<"Data bridge to MQTT Broker">>,
                          zh => <<"桥接数据到 MQTT Broker"/utf8>>
                         },
               description => #{en => <<"Store Data to Kafka">>,
                                zh => <<"桥接数据到 MQTT Broker"/utf8>>
                               }
              }).

on_resource_create(ResId, Params) ->
    ?LOG(info, "Initiating Resource ~p, ResId: ~p", [?RESOURCE_TYPE_MQTT, ResId]),
    {ok, _} = application:ensure_all_started(ecpool),
    Options = options(Params),
    PoolName = pool_name(ResId),
    start_resource(ResId, PoolName, Options),
    case test_resource_status(PoolName) of
        true -> ok;
        false ->
            on_resource_destroy(ResId, #{<<"pool">> => PoolName}),
            error({{?RESOURCE_TYPE_MQTT, ResId}, connection_failed})
    end,
    #{<<"pool">> => PoolName}.

start_resource(ResId, PoolName, Options) ->
    case ecpool:start_sup_pool(PoolName, ?MODULE, Options) of
        {ok, _} ->
            ?LOG(info, "Initiated Resource ~p Successfully, ResId: ~p", [?RESOURCE_TYPE_MQTT, ResId]);
        {error, {already_started, _Pid}} ->
            on_resource_destroy(ResId, #{<<"pool">> => PoolName}),
            start_resource(ResId, PoolName, Options);
        {error, Reason} ->
            ?LOG(error, "Initiate Resource ~p failed, ResId: ~p, ~p", [?RESOURCE_TYPE_MQTT, ResId, Reason]),
            on_resource_destroy(ResId, #{<<"pool">> => PoolName}),
            error({{?RESOURCE_TYPE_MQTT, ResId}, create_failed})
    end.

test_resource_status(PoolName) ->
    IsConnected = fun(Worker) ->
                          case ecpool_worker:client(Worker) of
                              {ok, Bridge} ->
                                  try emqx_bridge_worker:status(Bridge) of
                                      connected -> true;
                                      _ -> false
                                  catch _Error:_Reason ->
                                          false
                                  end;
                              {error, _} ->
                                  false
                          end
                  end,
    Status = [IsConnected(Worker) || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    lists:any(fun(St) -> St =:= true end, Status).

-spec(on_get_resource_status(ResId::binary(), Params::map()) -> Status::map()).
on_get_resource_status(_ResId, #{<<"pool">> := PoolName}) ->
    IsAlive = test_resource_status(PoolName),
    #{is_alive => IsAlive}.

on_resource_destroy(ResId, #{<<"pool">> := PoolName}) ->
    ?LOG(info, "Destroying Resource ~p, ResId: ~p", [?RESOURCE_TYPE_MQTT, ResId]),
        case ecpool:stop_sup_pool(PoolName) of
            ok ->
                ?LOG(info, "Destroyed Resource ~p Successfully, ResId: ~p", [?RESOURCE_TYPE_MQTT, ResId]);
            {error, Reason} ->
                ?LOG(error, "Destroy Resource ~p failed, ResId: ~p, ~p", [?RESOURCE_TYPE_MQTT, ResId, Reason]),
                error({{?RESOURCE_TYPE_MQTT, ResId}, destroy_failed})
        end.

on_action_create_data_to_mqtt_broker(_Id, #{<<"pool">> := PoolName}) ->
    ?LOG(info, "Initiating Action ~p.", [?FUNCTION_NAME]),
    fun(Msg, _Env = #{id := Id, from := From, flags := Flags,
                      topic := Topic, timestamp := TimeStamp}) ->
            BrokerMsg = #message{id = Id,
                                 qos = 1,
                                 from = From,
                                 flags = Flags,
                                 topic = Topic,
                                 payload = format_data(Msg),
                                 timestamp = TimeStamp},
            ecpool:with_client(PoolName, fun(BridgePid) ->
                                             BridgePid ! {dispatch, rule_engine, BrokerMsg}
                                         end)
    end.

format_data(Msg) ->
    jsx:encode(maps:merge(Msg, base_data())).

base_data() ->
    #{node => node(), ts => emqx_time:now_ms()}.

tls_versions() ->
    ['tlsv1.2','tlsv1.1', tlsv1].

ciphers(Ciphers) ->
    string:tokens(str(Ciphers), ", ").

subscriptions(Subscriptions) ->
    scan_binary(<<"[", Subscriptions/binary, "].">>).

is_node_addr(Addr0) ->
    Addr = binary_to_list(Addr0),
    case string:tokens(Addr, "@") of
        [_NodeName, _Hostname] -> true;
        _ -> false
    end.

scan_binary(Bin) ->
    TermString = binary_to_list(Bin),
    scan_string(TermString).

scan_string(TermString) ->
    {ok, Tokens, _} = erl_scan:string(TermString),
    {ok, Term} = erl_parse:parse_term(Tokens),
    Term.

connect(Options) ->
    emqx_bridge_worker:start_link(Options).

pool_name(ResId) ->
    list_to_atom("bridge_mqtt:" ++ str(ResId)).

options(Options) ->
    GetD = fun(Key, Default) -> maps:get(Key, Options, Default) end,
    Get = fun(Key) -> GetD(Key, undefined) end,
    Address = Get(<<"address">>),
    [{max_inflight_batches, 32},
     {mountpoint, str(Get(<<"mountpoint">>))},
     {queue, #{batch_bytes_limit => 1048576000,
               batch_count_limit => 32,
               replayq_dir => pool,
               replayq_seg_bytes => 10485760}},
     {start_type, auto},
     {if_record_metrics, false},
     {pool_size, GetD(<<"pool_size">>, 1)}
    ] ++ case is_node_addr(Address) of
             true ->
                 [{address, binary_to_atom(Get(<<"address">>), utf8)},
                  {connect_module, emqx_bridge_rpc}];
             false ->
                 [{address, binary_to_list(Address)},
                  {clean_start, true},
                  {client_id, undefined},
                  {connect_module, emqx_bridge_mqtt},
                  {keepalive, cuttlefish_duration:parse(str(Get(<<"keepalive">>)), s)},
                  {username, str(Get(<<"username">>))},
                  {password, str(Get(<<"password">>))},
                  {proto_ver, mqtt_ver(Get(<<"proto_ver">>))},
                  {reconnect_delay_ms, cuttlefish_duration:parse(str(Get(<<"reconnect_interval">>)), ms)},
                  {retry_interval, cuttlefish_duration:parse(str(Get(<<"retry_interval">>)), ms)},
                  {ssl, cuttlefish_flag:parse(str(Get(<<"ssl">>)))},
                  {ssl_opts, [{versions, tls_versions()},
                              {ciphers, ciphers(Get(<<"ciphers">>))},
                              {keyfile, str(Get(<<"keyfile">>))},
                              {certfile, str(Get(<<"certfile">>))},
                              {cacertfile, str(Get(<<"cacertfile">>))}
                             ]}]
         end.

mqtt_ver(ProtoVer) ->
    case ProtoVer of
       <<"mqttv3">> -> v3;
       <<"mqttv4">> -> v4;
       <<"mqttv5">> -> v5;
       _ -> v4
   end.
