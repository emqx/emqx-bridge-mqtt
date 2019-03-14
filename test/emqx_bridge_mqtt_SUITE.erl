-module(emqx_bridge_mqtt_SUITE).

-compile(export_all).


all() ->
    [test_sample].

%%------------------------------------------------------------------------------
%%  Test Cases
%%------------------------------------------------------------------------------

test_subscribe(_Config) ->
    ok.

test_sample(_Config) ->
    ok.


%%------------------------------------------------------------------------------
%% Setups Testcase
%%------------------------------------------------------------------------------

init_per_testcase(_TestCase, Config) ->
    NewConfig = generate_config(emqx_bridge_mqtt),
    lists:foreach(fun set_app_env/1, NewConfig),
    %% case TestCase of
    %%     test_message_expiry ->
    %%         application:set_env(emqx_retainer, expiry_interval, 0),
    %%         application:set_env(emqx_retainer, expiry_timer_interval, 0);
    %%     test_expiry_timer ->
    %%         application:set_env(emqx_retainer, expiry_interval, 2000),
    %%         application:set_env(emqx_retainer, expiry_timer_interval, 1000);    % 1000ms
    %%     test_subscribe_topics ->
    %%         application:set_env(emqx_retainer, expiry_interval, 0),
    %%         application:set_env(emqx_retainer, expiry_timer_interval, 0)
    %% end,
    application:ensure_all_started(emqx_bridge_mqtt),
    Config.

end_per_testcase(_TestCase, _Config) ->
    application:stop(emqx_bridge_mqtt).

%%------------------------------------------------------------------------------
%% SUITE Setups
%%------------------------------------------------------------------------------

init_per_suite(Config) ->
    [run_setup_steps(App) || App <- [emqx]],
    %% Change some env
    lager:set_loglevel(lager_console_backend, debug),
    Config.

end_per_suite(_Config) ->
    emqx:shutdown().

run_setup_steps(App) ->
    NewConfig = generate_config(App),
    lists:foreach(fun set_app_env/1, NewConfig),
    application:ensure_all_started(App),
    ct:log("Applications: ~p", [application:loaded_applications()]).

generate_config(emqx) ->
    Schema = cuttlefish_schema:files([local_path(["deps", "emqx", "priv", "emqx.schema"])]),
    Conf = conf_parse:file([local_path(["deps", "emqx", "etc", "emqx.conf"])]),
    cuttlefish_generator:map(Schema, Conf);

generate_config(emqx_bridge_mqtt) ->
    Schema = cuttlefish_schema:files([local_path(["priv", "emqx_bridge_mqtt.schema"])]),
    Conf = conf_parse:file([local_path(["etc", "emqx_bridge_mqtt.conf"])]),
    cuttlefish_generator:map(Schema, Conf).

local_path(Components, Module) ->
    filename:join([get_base_dir(Module) | Components]).

get_base_dir(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

get_base_dir() ->
    get_base_dir(?MODULE).

local_path(Components) ->
    local_path(Components, ?MODULE).

set_app_env({App, Lists}) ->
    lists:foreach(fun({acl_file, _Var}) ->
                      application:set_env(App, acl_file, local_path(["deps", "emqx", "etc", "acl.conf"]));
                     ({plugins_loaded_file, _Var}) ->
                      application:set_env(App, plugins_loaded_file, local_path(["deps","emqx", "test", "emqx_SUITE_data", "loaded_plugins"]));
                     ({Par, Var}) ->
                      application:set_env(App, Par, Var)
                  end, Lists).

