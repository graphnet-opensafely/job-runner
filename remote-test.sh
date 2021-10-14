#!/bin/bash

set -a
source .env.local
set +a

# run separately:
python -m jobrunner.sync
python -m jobrunner.run

#eval $(minikube docker-env)
# docker build -t opensafely-job-runner:latest .
# docker build -t cohortextractor:latest -f ../cohort-extractor/Dockerfile ../cohort-extractor/.
# docker tag cohortextractor:latest ghcr.io/opensafely-core/cohortextractor:latest

# method 1: not working due to buildkit not available
#az acr build --registry ccbidevdsacr --image opensafely-job-runner:latest -f Dockerfile .
#az acr build --registry ccbidevdsacr --target job-runner-tools-graphnet --image opensafely-job-runner-tools:latest -f docker/Dockerfile .

# method 2: build locally and push it
docker login ccbidevdsacr.azurecr.io

docker build --target job-runner-tools-graphnet -t ccbidevdsacr.azurecr.io/opensafely-job-runner-tools:latest -f docker/Dockerfile .
docker push ccbidevdsacr.azurecr.io/opensafely-job-runner-tools:latest

