FROM rust as build
COPY . /usr/src/gu-blender-mediator
RUN cargo install --path /usr/src/gu-blender-mediator 

FROM ubuntu
COPY --from=build /usr/local/cargo/bin/gu-blender-mediator /usr/local/bin/gu-blender-mediator
ENV DAV_URL="http://gw-dav-storage:55011"
ENV GW_URL="http://gw:55001/"
ENV GU_HUB_ADDR=hub:61622
ENV RUST_LOG=info
ENTRYPOINT gu-blender-mediator --dav $DAV_URL --gw GW_URL

