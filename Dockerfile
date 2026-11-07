# Build: the toolchain pinned in rust-toolchain.toml, on Debian bookworm.
FROM rust:1.99.1-bookworm@sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c AS build
# Use the image's toolchain as is: rust-toolchain.toml would make rustup
# download rustfmt and clippy, which a build does not need.
ENV RUSTUP_TOOLCHAIN=1.99.1
WORKDIR /src
COPY . .
RUN cargo build --release --locked && cp target/release/ciwatch /ciwatch

# Run: glibc and CA certificates only, as the unprivileged nonroot user.
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=build /ciwatch /usr/local/bin/ciwatch
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ciwatch"]
CMD ["--config", "/etc/ciwatch/ciwatch.toml"]
