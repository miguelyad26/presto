#!/usr/bin/env bash

source ${BASH_SOURCE%/*}/../common/compose-commons.sh

TEMPTO_CONFIG_YAML_DEFAULT="${PRODUCT_TESTS_ROOT}/conf/tempto/tempto-configuration-for-docker-kerberos.yaml"
export TEMPTO_CONFIG_YAML=$(canonical_path ${TEMPTO_CONFIG_YAML:-${TEMPTO_CONFIG_YAML_DEFAULT}})

docker-compose \
-f ${BASH_SOURCE%/*}/../common/base.yml \
-f ${BASH_SOURCE%/*}/../common/hive.yml \
-f ${BASH_SOURCE%/*}/../common/presto.yml \
-f ${BASH_SOURCE%/*}/../common/kerberos.yml \
-f ${BASH_SOURCE%/*}/../common/jdbc_db.yml \
-f ${BASH_SOURCE%/*}/docker-compose.yml \
"$@"
