docker run -ti --rm --platform=linux/amd64 -v $PWD/resources:/resources -v $PWD/virtualenv:/virtualenv \
    registry.access.redhat.com/ubi9/ubi \
    /bin/bash -c " \
        yum install -y python-pip && \
        python3 -m venv /virtualenv && \
        source /virtualenv/bin/activate && \
        pip install -r /resources/requirements.txt"
