#!/bin/bash
# Set var and validate return value is not empty
CI_TAG=$(git show-ref --tags | grep $(git rev-parse HEAD) | awk -F/ '{print $3}')
# count amount of characters in the variable
validate=${#CI_TAG}
if [[ $validate -lt 1 ]]
then
    CI_TAG=$(git tag --points-at HEAD)
    validate=${#CI_TAG}
    if [[ $CI_TAG -lt 1 ]]
    then
        CI_TAG=$(git rev-parse --short HEAD)
    fi
fi

# Printing value for return
echo $CI_TAG
