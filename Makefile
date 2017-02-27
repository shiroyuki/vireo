PY=python3
PIP=pip3
G3_CLI=cd sample && vireo
LXC_IMAGE_TAG=shiroyuki/vireo
LXC_RUN_OPTS=
LXC_RUN_ARGS=

package:
	@rm -v dist/*
	@$(PY) setup.py sdist

install:
	@$(PIP) install -IU --force-reinstall dist/*

# Do not use bdist_wheel if there is a command to install.
release:
	@$(PY) setup.py sdist upload

# Build the test image.
docker-image:
	@docker build -t $(LXC_IMAGE_TAG) .

# Run the test image.
docker-run: docker-image
	@docker run -it --rm $(LXC_RUN_OPTS) $(LXC_IMAGE_TAG) $(LXC_RUN_ARGS)

clean:
	@rm -rf build dist *.egg-info;
