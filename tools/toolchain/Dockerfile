FROM fedora:31
ADD ./install-dependencies.sh ./
ADD ./seastar/install-dependencies.sh ./seastar/
ADD ./tools/toolchain/system-auth ./
RUN dnf -y install 'dnf-command(copr)' \
    && dnf -y install ccache \
    && dnf -y install gdb \
    && dnf -y install devscripts debhelper fakeroot file rpm-build python2-pystache \
    && /bin/bash -x ./install-dependencies.sh && dnf -y update && dnf clean all \
    && echo 'ALL ALL=(ALL:ALL) NOPASSWD: ALL' >> /etc/sudoers \
    && cp system-auth /etc/pam.d \
    && echo 'Defaults !requiretty' >> /etc/sudoers

RUN dnf -y remove thrift
RUN yum -y install automake libtool flex bison pkgconfig gcc-c++ boost-devel libevent-devel zlib-devel python-devel ruby-devel openssl-devel

RUN dnf -y install boost-static
RUN git clone https://github.com/apache/thrift.git
RUN cd thrift && git reset --hard cecee50308fc7e6f77f55b3fd906c1c6c471fa2f && ./bootstrap.sh && ./configure --without-java && sudo make install -j10
RUN dnf -y install strace

RUN git clone https://github.com/scylladb/scylla-jmx
RUN cd scylla-jmx && mvn --file scylla-jmx-parent/pom.xml package
RUN chmod 1777 /tmp


ENV LIBRARY_PATH=/home/sme/parquet4seastar/build/:/thrift/lib/cpp/.libs/:${LIBRARY_PATH}
ENV LD_LIBRARY_PATH=/home/sme/parquet4seastar/build/:/thrift/lib/cpp/.libs/:${LD_LIBRARY_PATH}

CMD /bin/bash
