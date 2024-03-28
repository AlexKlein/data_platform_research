#!/bin/bash

BASEDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
MIGRATION_DIR="${BASEDIR}/migrations"
VERSION="$(date +%s)"

MIGRATION_FILE="${MIGRATION_DIR}/V${VERSION}__please_update_description.sql"
touch $MIGRATION_FILE
if [ $? == 0 ]
    then
        echo "Created a new migration file: ${MIGRATION_FILE}, please change its name"
    else
        echo "Could not create migration file ${MIGRATION_FILE}"
fi
