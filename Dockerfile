FROM rustlang/rust:nightly

WORKDIR .
COPY / /
#COPY jmdict jmdict

RUN ls -al src

RUN apt-get update
RUN apt-get install -y numactl

# RUN cargo install --path . --force
# RUN cd bin
RUN cd bin;cargo install
# Make port 3000 available to the world outside this container
EXPOSE 3000

#ENV LoadingType=Disk
ENV RUST_BACKTRACE=full
#ENV RUST_LOG=server=info,veloci=info,measure_time=debug
#ENV measure_time=debug


#CMD ["numactl --interleave=all server"]
