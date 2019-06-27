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

-export([on_action_create_data_to_mqtt_broker/2]).

-define(RESOURCE_TYPE_MQTT, 'bridge_mqtt').

-define(RESOURCE_CONFIG_SPEC,
        #{name => #{order => 1,
                    type => string,
                    required => true,
                    default => <<"aws">>,
                    title => #{en => <<"MQTT Bridge Name">>,
                               zh => <<"MQTT 桥接名称"/utf8>>
                              }
                   },
          address => #{order => 2,
                       type => string,
                       required => true,
                       default => <<"127.0.0.1:1883">>,
                       title => #{en => <<"MQTT Bridge Address">>,
                                  zh => <<"MQTT 桥接地址"/utf8>>},
                       description => #{en => <<"IP Address or Host Name of the MQTT remote host<br/>"
                                                "Note: When the address is the format like `emqx@127.0.0.1`"
                                                "Bridge will be created with rpc, and the rpc bridge is only"
                                                "for emqx node">>,
                                        zh => <<"远程 MQTT Broker 的 IP 地址或主机名<br/>"
                                                "注：当地址名为 `emqx@127.0.0.1` 这一形式时，"
                                                "桥接会以 rpc 的形式来创建， rpc 桥接仅限于 emqx 节点"/utf8>>
                                       }
                      },
          proto_ver => #{order => 3,
                         type => string,
                         required => false,
                         default => <<"MQTTv4">>,
                         title => #{en => <<"Protocol Version">>,
                                    zh => <<"协议版本"/utf8>>},
                         description => #{en => <<"Protocol version for MQTT bridge"
                                                  "(Only for bridge with MQTT protocol)">>,
                                          zh => <<"用于 MQTT 桥接的 MQTT 协议版本"
                                                  "（仅适用于 MQTT 协议桥接）"/utf8>>
                                         }
                        },
          client_id => #{order => 4,
                         type => string,
                         required => false,
                         default => <<"bridge_aws">>,
                         title => #{en => <<"Client ID">>,
                                    zh => <<"客户端 ID"/utf8>>},
                         description => #{en => <<"Client Id for connecting to MQTT Broker"
                                                  "(Only for bridge with MQTT protocol)">>,
                                          zh => <<"用于桥接 MQTT Broker 的 Client Id"
                                                  "（仅适用于 MQTT 协议桥接）"/utf8>>} 
                        },
          clean_start => #{order => 5,
                           type => boolean,
                           required => false,
                           default => true,
                           title => #{en => <<"Clean Start">>,
                                      zh => <<"Clean Start"/utf8>>},
                           description => #{en => <<"Clean Start Value for connecting to MQTT Broker"
                                                    "(Only for bridge with MQTT protocol)">>,
                                            zh => <<"用于桥接 MQTT Broker 的 Clean Start 值"
                                                    "（仅适用于 MQTT 协议桥接）"/utf8>>} 
                          },
          username => #{order => 6,
                        type => string,
                        required => false,
                        default => <<"user">>,
                        title => #{en => <<"MQTT Username">>, zh => <<"MQTT 用户名"/utf8>>},
                        description => #{en => <<"Username for connecting to MQTT Broker"
                                                 "(Only for bridge with MQTT protocol)">>,
                                         zh => <<"用于桥接 MQTT Broker 的 Clean Start 值"
                                                 "（仅适用于 MQTT 协议桥接）"/utf8>>}
                       },
          password => #{order => 7,
                        type => string,
                        required => false,
                        default => <<"passwd">>,
                        title => #{en => <<"MQTT Password">>, zh => <<"MQTT 密码"/utf8>>},
                        description => #{en => <<"Password for connecting to MQTT Broker"
                                                 "(Only for bridge with MQTT protocol)">>,
                                         zh => <<"用于桥接 MQTT Broker 的密码"
                                                 "（仅适用于 MQTT 协议桥接）"/utf8>>}          
                       },
          mountpoint => #{order => 8,
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
          forwards => #{order => 9,
                        type => string,
                        required => true,
                        default => <<"topic1/#,topic2/#">>,
                        title => #{en => <<"Bridge Forwards Topics">>,
                                   zh => <<"桥接转发主题"/utf8>>},
                        description => #{en => <<"Forwards for the bridge<br/>"
                                                 "Example: topic1/#, topic2/#">>,
                                         zh => <<"用于桥接的转发主题<br/>"
                                                 "示例: topic1/#, topic2/#"/utf8>>
                                        }
                       },
          ssl => #{order => 10,
                   type => string,
                   required => true,
                   default => <<"off">>,
                   title => #{en => <<"The switch of the bridge ssl">>,
                              zh => <<"Bridge 启用 ssl 连接开关"/utf8>>
                             },
                   description => #{en => <<"The switch which used to enable ssl connection of the bridge">>,
                                    zh => <<"用于启用 bridge ssl 连接的开关"/utf8>>
                                   }
                  },
          cacertfile => #{order => 11,
                          type => string,
                          required => false,
                          default => <<"etc/certs/cacert.pem">>,
                          title => #{en => <<"PEM-encoded CA certificates of the bridge">>,
                                     zh => <<"用于 ssl 加密桥接的 PEM 编码的 CA 证书"/utf8>>},
                          description => #{en => <<"The file path of the CA certificates">>,
                                           zh => <<"CA 证书所在路径"/utf8>>
                                          }
                         },
          certfile => #{order => 12,
                        type => string,
                        required => false,
                        default => <<"etc/certs/client-cert.pem">>,
                        title => #{en => <<"Client SSL Certfile of the bridge">>,
                                   zh => <<"用于 SSL 加密桥接的客户端证书"/utf8>>},
                        description => #{en => <<"The file path of the client certfile">>,
                                         zh => <<"客户端证书文件所在路径"/utf8>>
                                        }
                       },
          keyfile => #{order => 13,
                       type => string,
                       required => false,
                       default => <<"etc/certs/client-key.pem">>,
                       title => #{en => <<"Client SSL Keyfile of the bridge">>,
                                  zh => <<"用于 SSL 加密桥接的客户端密钥文件"/utf8>>},
                       description => #{en => <<"The file path of the client keyfile">>,
                                        zh => <<"客户端密钥文件所在路径"/utf8>>
                                       }
                      },
          ciphers => #{order => 14,
                       type => string,
                       required => false,
                       default => <<"ECDHE-ECDSA-AES256-GCM-SHA384,ECDHE-RSA-AES256-GCM-SHA384">>,
                       title => #{en => <<"SSL Ciphers used by the bridge">>,
                                  zh => <<"用于桥接的 SSL 密码算法"/utf8>>},
                       description => #{en => <<"SSL Ciphers used by the bridge">>,
                                        zh => <<"用于桥接的 SSL 密码算法"/utf8>>}
                      },
          psk_ciphers => #{order => 15,
                           type => string,
                           required => false,
                           default => <<"PSK-AES128-CBC-SHA,PSK-AES256-CBC-SHA,PSK-3DES-EDE-CBC-SHA,PSK-RC4-SHA">>,
                           title => #{en => <<"Ciphers for TLS PSK">>,
                                      zh => <<"用于 TLS PSK 的加密算法"/utf8>>},
                           description => #{en => <<"Ciphers for TLS PSK<br/>"
                                                    "Note: ciphers and psk_ciphers cannot be"
                                                    "configured at the same time.">>,
                                            zh => <<"用于 TLS PSK 的加密算法<br/>"
                                                    "注意: 普通的密码算法和 psk 密码算法不能同时配置"/utf8>>}
                          },
          tls_versions => #{order => 16,
                            type => string,
                            required => false,
                            default => <<"tlsv1.2,tlsv1.1,tlsv1">>,
                            title => #{en => <<"Ping interval of a down bridge">>,
                                       zh => <<"桥接的心跳间隔"/utf8>>},
                            description => #{en => <<"Ping interval of a down bridge">>,
                                             zh => <<"桥接的心跳间隔"/utf8>>}
                           },
          keepalive => #{order => 17,
                         type => string,
                         required => true,
                         default => <<"60s">> ,
                         title => #{en => <<"Ping interval of a down bridge">>,
                                    zh => <<"桥接的心跳间隔"/utf8>>},
                         description => #{en => <<"Ping interval of a down bridge">>,
                                          zh => <<"桥接的心跳间隔"/utf8>>}
                        },
          subscriptions => #{order => 18,
                             type => string,
                             required => false,
                             default => <<"{\"cmd/topic1\", 1}, {\"cmd/topic2\", 1}">> ,
                             title => #{en => <<"The list of Bridge Subscriptions">>,
                                        zh => <<"桥接订阅列表"/utf8>>},
                             description => #{en => <<"The list of Bridge Subscriptions">>,
                                              zh => <<"桥接订阅列表"/utf8>>}
                            },
          reconnect_interval => #{order => 20,
                                  type => string,
                                  required => false,
                                  default => <<"30s">>,
                                  title => #{en => <<"Bridge Reconnect Interval">>,
                                             zh => <<"桥接重连间隔"/utf8>>},
                                  description => #{en => <<"Start type of the bridge<br/>"
                                                           "Enum: auto, manual">>,
                                                   zh => <<"桥接的启动类型<br/>"
                                                           "启动类型: auto, manual"/utf8>>}
                                 },
          retry_interval => #{order => 21,
                              type => string,
                              required => false,
                              default => <<"20s">>,
                              title => #{en => <<"Retry interval for bridge QoS1 message delivering">>,
                                         zh => <<"QoS1 消息重传间隔"/utf8>>},
                              description => #{en => <<"Retry interval for bridge QoS1 message delivering">>,
                                               zh => <<"QoS1 消息重传间隔"/utf8>>}
                             },
          max_inflight_batches => #{order => 22,
                                    type => number,
                                    required => false,
                                    default => 32,
                                    title => #{en => <<"Inflight size">>,
                                               zh => <<"Inflight 大小"/utf8>>},
                                    description => #{en => <<"Used to resend batch messages">>,
                                                     zh => <<"用于批量重传消息的 Inflight 大小"/utf8>>}
                                   },
          queue_replayq_dir => #{order => 23,
                                 type => string,
                                 required => false,
                                 default => 32,
                                 title => #{en => <<"batch_count_limit for replay queue">>,
                                            zh => <<"用于 replay 队列的 batch_count_limit 值"/utf8>>},
                                 description => #{en => <<"Max number of messages to collect in a batch">>,
                                                  zh => <<"一次 batch 所收集的最大数量的消息数"/utf8>>}
                                },
          queue_batch_count_limit => #{order => 24,
                                       type => number,
                                       required => false,
                                       default => 32,
                                       title => #{en => <<"batch_count_limit for replay queue">>,
                                                  zh => <<"用于 replay 队列的 batch_count_limit 值"/utf8>>},
                                       description => #{en => <<"Max number of messages to collect in a batch">>,
                                                        zh => <<"一次 batch 所收集的最大数量的消息数"/utf8>>}
                                      },
          queue_batch_bytes_limit => #{order => 25,
                                       type => string,
                                       required => false,
                                       default => <<"1000MB">>,
                                       title => #{en => <<"batch_bytes_limit for replay queue">>,
                                                  zh => <<"用于 replay 队列的 batch_bytes_limit 值"/utf8>>},
                                       description => #{en => <<"Max number of bytes to collect in a batch">>,
                                                        zh => <<"一次 batch 所收集的最大数量的字节数"/utf8>>}
                                      },
          queue_seg_bytes => #{order => 26,
                               type => string,
                               required => false,
                               default => <<"10MB">>,
                               title => #{en => <<"Replayq segment size">>,
                                          zh => <<"Replayq segment 大小"/utf8>>},
                               description => #{en => <<"Replayq segment size which used to determine"
                                                        "the size of each log file">>,
                                                zh => <<"用于确定每个 log 文件大小的 Replayq segment 大小"/utf8>>}
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
                 params => ?RESOURCE_CONFIG_SPEC,
                 title => #{en => <<"MQTT Broker">>, zh => <<"MQTT Broker"/utf8>>},
                 description => #{en => <<"MQTT Resource">>, zh => <<"MQTT 资源"/utf8>>}
                }).

-rule_action(#{name => data_to_mqtt_broker,
               for => '$any',
               types => [?RESOURCE_TYPE_MQTT],
               create => on_action_create_data_to_mqtt_broker,
               params => #{'$resource' => ?ACTION_PARAM_RESOURCE,
                           forward => #{type => string,
                                        required => true,
                                        title => #{en => <<"Forward Topic">>,
                                                   zh => <<"转发主题"/utf8>>},
                                        description => #{en => <<"Forward Topic">>,
                                                         zh => <<"转发主题"/utf8>>}
                                       }
                          },
               title => #{en => <<"Data bridge to MQTT Broker">>,
                          zh => <<"桥接数据到 MQTT Broker"/utf8>>
                         },
               description => #{en => <<"Store Data to Kafka">>,
                                zh => <<"桥接数据到 MQTT Broker"/utf8>>
                               }
              }).

on_resource_create(ResId, #{<<"name">> := Name,
                            <<"address">> := Address,
                            <<"proto_ver">> := ProtoVer,
                            <<"client_id">> := ClientId,
                            <<"clean_start">> := CleanStart,
                            <<"username">> := Username,
                            <<"password">> := Password,
                            <<"mountpoint">> := MountPoint,
                            <<"forwards">> := Forwards,
                            <<"ssl">> := SslFlag,
                            <<"cacertfile">> := Cacertfile,
                            <<"certfile">> := Certfile,
                            <<"keyfile">> := Keyfile,
                            <<"ciphers">> := Ciphers,
                            <<"psk_ciphers">> := PskCiphers,
                            <<"tls_versions">> := TlsVersions,
                            <<"keepalive">> := KeepAlive,
                            <<"subscriptions">> := Subscriptions,
                            <<"reconnect_interval">> := ReconnectInterval,
                            <<"retry_interval">> := RetryInterval,
                            <<"max_inflight_batches">> := MaxInflightBatches,
                            <<"queue_replayq_dir">> := QueueReplayqDir,
                            <<"queue_batch_count_limit">> := QueueBatchCountLimit,
                            <<"queue_batch_bytes_limit">> := QueueBatchBytesLimit,
                            <<"queue_seg_bytes">> := QueueSegBytes
                           }) ->
    ?LOG(info, "Initiating Resource ~p, ResId: ~p", [?RESOURCE_TYPE_MQTT, ResId]),
    BridgeName = str(Name),
    Options = [{address, case is_node_addr(Address) of
                             true -> erlang:list_to_atom(Address);
                             false -> Address
                         end},
               {clean_start, CleanStart},
               {client_id, ClientId},
               {connect_module, case is_node_addr(Address) of
                                    true -> emqx_bridge_rpc;
                                    false -> emqx_bridge_mqtt
                                end},
               {forwards, string:tokens(str(Forwards), ", ")},
               {keepalive, cuttlefish_duration:parse(KeepAlive, s)},
               {max_inflight_batches, MaxInflightBatches},
               {mountpoint, str(MountPoint)},
               {username, str(Username)},
               {password, str(Password)},
               {proto_ver, case ProtoVer of
                               <<"mqttv3">> -> v3;
                               <<"mqttv4">> -> v4;
                               <<"mqttv5">> -> v5;
                               _ -> v4
                           end},
               {queue, #{batch_bytes_limit => QueueBatchBytesLimit,
                         batch_count_limit => QueueBatchCountLimit,
                         replayq_dir => QueueReplayqDir,
                         replayq_seg_bytes => QueueSegBytes}},
               {reconnect_delay_ms, cuttlefish_duration:parse(ReconnectInterval, ms)},
               {retry_interval, cuttlefish_duration:parse(RetryInterval, ms)},
               {ssl, cuttlefish_flag:parse(SslFlag)},
               {ssl_opts,
                [{versions, tls_versions(TlsVersions)},
                 {ciphers, ciphers(Ciphers)},
                 {user_lookup_fun, {fun emqx_psk:lookup/3, <<>>}},
                 {keyfile, str(Keyfile)},
                 {ciphers, psk_ciphers(PskCiphers)},
                 {certfile, str(Certfile)},
                 {cacertfile, str(Cacertfile)}
                ]},
               {start_type, auto},
               {subscriptions, subscriptions(Subscriptions)}
              ],
    emqx_bridge_sup:create_bridge(BridgeName, Options),
    #{<<"bridge_name">> => BridgeName}.

-spec(on_get_resource_status(ResId::binary(), Params::map()) -> Status::map()).
on_get_resource_status(_ResId, #{<<"bridge_name">> := BridgeName}) ->
    IsAlive = try emqx_bridge:status(BridgeName) of
                      connected -> true;
                      _ -> false
              catch _Error:_Reason ->
                      false
              end,
    #{is_alive => IsAlive}.

on_resource_destroy(ResId, #{<<"bridge_name">> := BridgeName}) ->
    ?LOG(info, "Destroying Resource ~p, ResId: ~p", [?RESOURCE_TYPE_MQTT, ResId]),
    case emqx_bridge_sup:drop_bridge(BridgeName) of
        ok ->
            ?LOG(info, "Destroyed Resource ~p Successfully, ResId: ~p", [?RESOURCE_TYPE_MQTT, ResId]);
        {error, Reason} ->
            ?LOG(error, "Destroy Resource ~p failed, ResId: ~p, ~p", [?RESOURCE_TYPE_MQTT, ResId, Reason]),
            error({{?RESOURCE_TYPE_MQTT, ResId}, destroy_failed})
    end.

on_action_create_data_to_mqtt_broker(_Id, #{<<"bridge_name">> := BridgeName, <<"forward">> := Forward}) ->
    ?LOG(info, "Initiating Action ~p, Exchange: ~p", [?FUNCTION_NAME]),
    ok = emqx_bridge:ensure_forward_present(BridgeName, Forward),
    fun(Msg, _Env) ->
            BrokerMsg = emqx_message:make(rule_action, Forward, format_data(Msg)),
            emqx_broker:publish(BrokerMsg)
    end.

format_data(Msg) ->
    jsx:encode(maps:merge(Msg, base_data())).

base_data() ->
    #{node => node(), ts => emqx_time:now_ms()}.

tls_versions(Versions) ->
    case string:tokens(str(Versions), ", ") of
        Tokens when is_list(Tokens) ->
            [erlang:list_to_atom(Token) || Token <- Tokens];
        _Tokens ->
            []
    end.

ciphers(Ciphers) ->
    string:tokens(Ciphers, ", ").

subscriptions(Subscriptions) ->
    scan_binary(<<"[", Subscriptions/binary, "].">>).

psk_ciphers(PskCiphers) ->
    lists:map(
      fun("PSK-AES128-CBC-SHA") -> {psk, aes_128_cbc, sha};
         ("PSK-AES256-CBC-SHA") -> {psk, aes_256_cbc, sha};
         ("PSK-3DES-EDE-CBC-SHA") -> {psk, '3des_ede_cbc', sha};
         ("PSK-RC4-SHA") -> {psk, rc4_128, sha}
      end, string:tokens(str(PskCiphers), ", ")).
    
is_node_addr(Addr) ->
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

