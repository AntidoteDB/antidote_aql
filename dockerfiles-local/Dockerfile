FROM antidotedb/antidote

MAINTAINER Pedro Lopes <pms.lopes@campus.fct.unl.pt>

ENV AQL_REPO "git://github.com/pedromslopes/AQL.git"
ENV AQL_BRANCH "new_features"
ENV AQL_NAME "aql@127.0.0.1"

#AQL
RUN cd /usr/src \
  && git clone $AQL_REPO \
  && cd AQL \
  && git checkout $AQL_BRANCH \
  && make compile \
  && cp -R ../AQL /opt/

ADD ./start_and_attach.sh /opt/

ENTRYPOINT ["/entrypoint.sh", "/opt/start_and_attach.sh"]
