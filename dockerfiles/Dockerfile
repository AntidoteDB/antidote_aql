FROM erlang:19

MAINTAINER Pedro Lopes <pms.lopes@campus.fct.unl.pt>

ENV AQL_REL "/opt/AQL"
ENV HANDOFF_PORT "8099"
ENV PB_PORT "8087"
ENV PB_IP "0.0.0.0"
ENV PBSUB_PORT "8086"
ENV LOGREADER_PORT "8085"
ENV RING_STATE_DIR "data/ring"
ENV PLATFORM_DATA_DIR "data"
ENV NODE_NAME "aql@127.0.0.1"
ENV SHORT_NAME "false"
ENV DEBUG_MODE "false"
ENV AQL_REPO "https://github.com/AntidoteDB/antidote_aql.git"
ENV AQL_BRANCH "master"

#AQL
RUN cd /usr/src \
  && git clone $AQL_REPO \
  && cd AQL \
  && git checkout $AQL_BRANCH \
  && make release \
  && cp -r _build/default/rel/aql /opt/AQL \
  && cp -r erl_scripts /opt/AQL/erl_scripts \
  && cp -r scripts /opt/AQL/scripts \
  && rm -rf /usr/src/AQL

RUN chmod a+x /opt/AQL/erl_scripts/create_cluster.erl \
  && chmod a+x /opt/AQL/erl_scripts/populate_index.erl

ADD ./start_and_attach.sh /opt/
ADD ./entrypoint.sh /

RUN chmod a+x /opt/start_and_attach.sh \
  && chmod a+x /entrypoint.sh

# Distributed Erlang Port Mapper
EXPOSE 4368
# Ports for Antidote
EXPOSE 8085 8086 8087 8099

# Antidote RPC
EXPOSE 9100

VOLUME /opt/AQL/data

ENTRYPOINT ["/entrypoint.sh", "/opt/start_and_attach.sh"]
