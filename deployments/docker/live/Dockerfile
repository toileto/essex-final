ARG base_docker_image
ARG base_docker_tag
FROM $base_docker_image:$base_docker_tag

# Install laminar
ARG build_version
COPY dist/laminar-$build_version-py3-none-any.whl /laminar/dist/laminar-$build_version-py3-none-any.whl
RUN python -m pip install /laminar/dist/laminar-$build_version-py3-none-any.whl

# Smoke test and verifications
RUN which python
RUN which laminar
RUN pip check

# Copy files from official SDK image, including script/dependencies.
COPY --from=apache/beam_python3.8_sdk:2.41.0 /opt/apache/beam /opt/apache/beam

# Set the entrypoint to Apache Beam SDK launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]