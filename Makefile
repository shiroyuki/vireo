PY=python3
PIP=pip3
G3_CLI=cd sample && vireo
LXC_IMAGE_TAG=shiroyuki/vireo
LXC_RUN_OPTS=
LXC_RUN_ARGS=
SAMPLE_IMAGE=vireo/sampleobserver
SAMPLE_SERVER:=amqp://guest:guest@127.0.0.1:5672/%2F

package:
	@rm -v dist/*
	@$(PY) setup.py sdist

install:
	@$(PIP) install -IU --force-reinstall dist/*

# Do not use bdist_wheel if there is a command to install.
release: package
	# @$(PY) setup.py sdist upload
	twine upload dist/*

# Build the test image.
docker-image:
	@docker build -t $(LXC_IMAGE_TAG) .

# Run the test image.
docker-run: docker-image
	@docker run -it --rm $(LXC_RUN_OPTS) $(LXC_IMAGE_TAG) $(LXC_RUN_ARGS)

# Run TERM the test image.
docker-term: docker-image
	@docker run -it --rm -w /opt/vireo -v `pwd`:/opt/vireo $(LXC_RUN_OPTS) $(LXC_IMAGE_TAG) bash

clean:
	@rm -rf build dist *.egg-info;

sample-build:
	docker build $(BUILD_EXT) -t $(SAMPLE_IMAGE) .

sample-run: sample-build
	docker run -it --rm -t $(SAMPLE_IMAGE) g3 sample.observe -d -b $(SAMPLE_SERVER)
