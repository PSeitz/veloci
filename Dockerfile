FROM rustlang/rust:nightly

WORKDIR .
COPY src src
COPY Cargo.toml .
COPY index.html .
COPY json_converter json_converter
#COPY jmdict jmdict

RUN ls -al

RUN cargo install

# Make port 3000 available to the world outside this container
EXPOSE 3000

#ENV LoadingType=Disk
ENV RUST_BACKTRACE=full
ENV search_lib=info
ENV measure_time=debug

CMD ["server"]

