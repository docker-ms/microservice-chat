FROM node:6.11.0-alpine

# 'libc6-compat' is needed by 'grpc'.
RUN apk --no-cache add --update libc6-compat

#
## Set up deployment key and npm install production dependencies.
#
RUN mkdir -p /root/.ssh/ /app/
COPY ./package.json /app/
COPY ./deployment_key/id_dsa /root/.ssh/

ARG RUN_NPM_INSTALL

RUN apk --no-cache add --update --virtual build-dependencies git openssh \
  && touch /root/.ssh/known_hosts \
  && ssh-keyscan bitbucket.org >> /root/.ssh/known_hosts \
  && chmod 600 -R /root/.ssh/ \
  && cd /app/ \
  && npm install --only=production \
  && apk del build-dependencies

# Copy necessary source code.
COPY . /app/

ARG PORTS_END

EXPOSE 53547-${PORTS_END}

CMD ["node", "/app/app.js"]


