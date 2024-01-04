docker run -ti --rm --platform=linux/amd64 -v $PWD/resources:/resources -v $PWD/virtualenv:/virtualenv \
    registry.access.redhat.com/ubi9/ubi \
    /bin/bash -c " \
        yum install -y wget gcc make zlib-devel perl && \
        mkdir /tmp/build && cd /tmp/build && \
        tar xzf /resources/src/openssl-1.0.2d.tar.gz && cd openssl-1.0.2d && \
        ./config && make && make install && cd .. &&\
        tar xf /resources/src/Python-2.7.tgz && cd Python-2.7/ \
        ./configure && make && make install && cd .. \
        wget https://bootstrap.pypa.io/pip/2.7/get-pip.py && \
        python2.7 get-pip.py && \
        pip2 install virtualenv && \
        virtualenv -p /usr/bin/python2.7 /virtualenv && \
        source /virtualenv/bin/activate && \
        pip install /pip/*.whl"
