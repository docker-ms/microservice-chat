## Description

- This is the 'chat' microservice, its responsibilities will be:

    1. All the chatting related logic should come here.

## Note

1. We are not using 'master' branch.

2. Any new created branch please indicate its 'target' and 'based point' in the branch name explicitly.

## How to build this image, and then push to our private registry

  ```
  # Build your image.
  docker build \
    --no-cache=true \
    --pull=true \
    --compress=false \
    --rm=true \
    --force-rm=true \
    --build-arg PORTS_END=53547 \
    --tag chat-dev-leonard-1:0.0.1 \
    .

  # Tag your image.
  docker tag chat-dev-leonard-1:0.0.1 micro02.sgdev.vcube.com:65300/chat-dev-leonard-1:0.0.1

  # Login to the corresponding registry.
  docker login micro02.sgdev.vcube.com:65300

  # Push your image to the registry.
  docker push micro02.sgdev.vcube.com:65300/chat-dev-leonard-1:0.0.1
  ```

## Run this project at your localhost

  ```
  #
  ## -n to specify whether you need to run npm install.
  ## -w to specify the owner of this env.
  #
  source run.sh -n -w leonard
  ```


