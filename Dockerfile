FROM rustlang/rust:nightly

WORKDIR .
COPY src src
COPY Cargo.toml .
COPY index.html .
COPY json_converter json_converter
#COPY jmdict jmdict

RUN ls -al

RUN apt-get update
RUN apt-get install -y numactl

RUN cargo install

# Make port 3000 available to the world outside this container
EXPOSE 3000

#ENV LoadingType=Disk
ENV RUST_BACKTRACE=full
#ENV RUST_LOG=server=info,search_lib=info,measure_time=debug
#ENV measure_time=debug


#CMD ["numactl --interleave=all server"]
