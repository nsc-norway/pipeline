# Resources directory #

## Open_Emails.*

AppleScript and bash script for preparing email messages.

These are not managed as part of the normal deployment, they should be installed separately.
Currently referred to by explicit paths, the installation may need to change to support other
sites than OUS.

## pip and src

For compat with pipeline host, use python 3.9 for the below. E.g. use a conda env.

    conda create -n pipeline python=3.9
	conda activate pipeline


Files for building an environment for the pipeline, for use on RHEL9.
Downloading:

	cd downloadpip
	pip download -r requirements.txt

To download for the RHEL9 platform used on the prod servers:

	docker run -ti --rm -v $PWD/downloadpip:/downloadpip \
		registry.access.redhat.com/ubi9/ubi \
		bash -c "\
			yum install -y python3-pip && \
			cd /downloadpip && \
			pip download -r requirements.txt
		"

Creating environment: This must be done with the correct absolute path to use on the system
(i.e. the venv can't be moved to a different path).

	python3 -m venv pipenv
	source pipenv/bin/activate
	pip install downloadpip/*.whl

