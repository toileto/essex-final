container_registry ?= asia.gcr.io/essex-thesis-project/laminar
project_id ?= essex-thesis-project
deployment ?= simulation

_require_project:
ifndef project_id
	$(error project_id must be defined.)
endif

_require_env:
ifndef deployment
	$(error deployment must be defined.)
endif

_require_container_registry:
ifndef container_registry
	$(error container_registry must be defined.)
endif

_require_table:
ifndef table
	$(error table must be defined.)
endif

_require_start_date:
ifndef start_date
	$(error start_date must be defined.)
endif

_require_end_date:
ifndef end_date
	$(error end_date must be defined.)
endif

build_version ?= $(shell bash -e deployments/misc/get_release_version.sh)
base_docker_image ?= $(container_registry)/base
base_docker_tag ?= 1.0.1
live_docker_image ?= $(container_registry)/live
live_docker_tag ?= $(build_version)
python_executable ?= python3
gsm_project_id ?= $(project_id)
gsm_secret_id ?= laminar-service-account-$(deployment)
gsm_version_id ?= 1


# Tests
test_pyflakes_src:
	docker run --rm -e build_version=$(build_version) -v $(shell pwd):/workspace \
	--entrypoint pyflakes eeacms/pyflakes:py3 /workspace/laminar/

test_pyflakes_tests:
	docker run --rm -e build_version=$(build_version) -v $(shell pwd):/workspace \
	--entrypoint pyflakes eeacms/pyflakes:py3 /workspace/tests/

test_unit: _require_container_registry
	docker run --rm -v $(shell pwd):/workspace -e PYTHONPATH=/workspace -e build_version=$(build_version) \
	--entrypoint pytest $(base_docker_image):$(base_docker_tag) \
	--cov=laminar --cov-config=/workspace/.coveragerc --cov-report term-missing \
	/workspace/tests/unit

tests: _require_container_registry test_pyflakes_src test_pyflakes_tests test_unit

# Builds
build_wheel:
	build_version=$(build_version) $(python_executable) setup.py sdist bdist_wheel
	test -e dist/laminar-$(build_version)-py3-none-any.whl

docker_base_build: _require_container_registry
	docker build -f deployments/docker/base/Dockerfile \
	-t $(base_docker_image):$(base_docker_tag) .

docker_live_build: _require_container_registry
	docker build -f deployments/docker/live/Dockerfile \
	--build-arg base_docker_image=$(base_docker_image) \
	--build-arg base_docker_tag=$(base_docker_tag) \
	--build-arg build_version=$(build_version) \
	-t $(live_docker_image):$(live_docker_tag) .

docker_build: _require_container_registry docker_base_build docker_live_build

docker_base_push: _require_container_registry
	docker push $(base_docker_image):$(base_docker_tag)

docker_live_push: _require_container_registry
	docker push $(live_docker_image):$(live_docker_tag)

docker_push: _require_container_registry docker_base_push docker_live_push

builds: _require_container_registry build_wheel docker_live_build docker_live_push

# GCP Service account
download_credentials: _require_project _require_env
	$(python_executable) deployments/misc/get_credentials.py download-service-account \
	--gsm_project_id=$(gsm_project_id) \
	--gsm_secret_id=$(gsm_secret_id) \
	--gsm_version_id=$(gsm_version_id) \
	--deployment=$(deployment)

# Dataflow jobs
run_streaming_dataflow_job: _require_env _require_container_registry
	docker run --rm --network host \
	-e GOOGLE_APPLICATION_CREDENTIALS=/workspace/service_account.$(deployment).json -e build_version=$(build_version) \
	-v /tmp:/tmp -v $(shell pwd):/workspace:ro \
	--entrypoint /usr/local/bin/laminar $(live_docker_image):$(live_docker_tag) run-streaming-pipeline \
	--deployment_config_path=/workspace/deployments/configs/streaming.$(deployment).yaml \
	--dataflow_container=$(live_docker_image):$(live_docker_tag) \
	--dryrun=false

run_batch_dataflow_job: _require_env _require_container_registry _require_table _require_start_date _require_end_date
	docker run --rm --network host \
	-e GOOGLE_APPLICATION_CREDENTIALS=/workspace/service_account.$(deployment).json -e build_version=$(build_version) \
	-v /tmp:/tmp -v $(shell pwd):/workspace:ro \
	--entrypoint /usr/local/bin/laminar $(live_docker_image):$(live_docker_tag) run-batch-pipeline \
	--deployment_config_path=/workspace/deployments/configs/batch.$(deployment).yaml \
	--dataflow_container=$(live_docker_image):$(live_docker_tag) \
	--table=$(table) \
	--start_date=$(start_date) \
	--end_date=$(end_date) \
	--dryrun=false

drain_dataflow_job: _require_env _require_container_registry
	docker run --rm --network host \
	-e GOOGLE_APPLICATION_CREDENTIALS=/workspace/service_account.$(deployment).json \
	-e PYTHONPATH=/workspace -e build_version=$(build_version) -v /tmp:/tmp -v $(shell pwd):/workspace:ro \
	--entrypoint $(python_executable) $(base_docker_image):$(base_docker_tag) /workspace/deployments/misc/job_drainer.py stop-dataflow-jobs \
	--deployment_config_path=/workspace/deployments/configs/streaming.$(deployment).yaml

wait_running_dataflow_job: _require_env _require_container_registry
	docker run --rm --network host \
	-e GOOGLE_APPLICATION_CREDENTIALS=/workspace/service_account.$(deployment).json \
	-e PYTHONPATH=/workspace -e build_version=$(build_version) -v /tmp:/tmp -v $(shell pwd):/workspace:ro \
	--entrypoint $(python_executable) $(base_docker_image):$(base_docker_tag) /workspace/deployments/misc/job_drainer.py wait-dataflow-jobs \
	--deployment_config_path=/workspace/deployments/configs/streaming.$(deployment).yaml

deploy_streaming_dataflow_job: _require_project _require_env _require_container_registry download_credentials drain_dataflow_job run_streaming_dataflow_job wait_running_dataflow_job
deploy_batch_dataflow_job: _require_project _require_env _require_container_registry _require_table _require_start_date _require_end_date download_credentials run_batch_dataflow_job


publish_data:
	docker run -it --rm \
	--network host \
	-e GOOGLE_CLOUD_PROJECT=$(project_id) \
	-e GCP_PUBSUB_TOPIC=data-processing-source-simulation \
	-e GOOGLE_APPLICATION_CREDENTIALS=/workspace/service_account.$(deployment).json \
	-v /tmp:/tmp \
	-v $(shell pwd):/workspace:ro \
	--entrypoint $(python_executable) \
	$(base_docker_image):$(base_docker_tag) \
	/workspace/publisher/publish_data.py