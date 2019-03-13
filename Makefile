.PHONY: tests

PROJECT = emqx_bridge_mqtt
PROJECT_DESCRIPTION = EMQ X MQTT Bridge
PROJECT_VERSION = 0.0.1

NO_AUTOPATCH = cuttlefish

BUILD_DEPS = emqx cuttlefish
dep_emqx        = git git@github.com:emqx/emqx-enterprise master
dep_cuttlefish  = git https://github.com/emqtt/cuttlefish develop

DEPS = emqttc

TEST_DEPS = emqttc
dep_emqttc = git https://github.com/emqtt/emqttc.git master

ERLC_OPTS += +debug_info
ERLC_OPTS += +'{parse_transform, lager_transform}'

TEST_ERLC_OPTS += +debug_info
TEST_ERLC_OPTS += +'{parse_transform, lager_transform}'

EUNIT_OPTS = verbose

COVER = true

PLT_APPS = sasl asn1 ssl syntax_tools runtime_tools crypto xmerl os_mon inets public_key ssl lager compiler mnesia

DIALYZER_DIRS := ebin/
DIALYZER_OPTS := --verbose --statistics -Werror_handling \
                 -Wrace_conditions #-Wunmatched_returns

include $(if $(ERLANG_MK_FILENAME),$(ERLANG_MK_FILENAME),erlang.mk)

app.config::
	./deps/cuttlefish/cuttlefish -l info -e etc/ -c etc/emqx_bridge_mqtt.conf -i priv/emqx_bridge_mqtt.schema -d data
