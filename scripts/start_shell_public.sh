#!/usr/bin/env bash

MY_IP=$(curl v4.ifconfig.co)

export IP=$MY_IP
make shell
