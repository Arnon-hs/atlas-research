# syntax=docker/dockerfile:1.18@sha256:dabfc0969b935b2080555ace70ee69a5261af8a8f1b4df97b9e7fbcf6722eddf
FROM python:3.11.15-slim@sha256:90744cff8f32887f075c47d747a173ff333e9e98801667af93c357fa9f5e28ff

ARG VCS_REF

LABEL org.opencontainers.image.source="https://github.com/Arnon-hs/atlas-research" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.title="Atlas Research offline worker" \
      org.opencontainers.image.revision="${VCS_REF}"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY dist/atlasrepo_research-*.whl /tmp/atlas-research-dist/

RUN test "${#VCS_REF}" -eq 40 \
    && case "${VCS_REF}" in *[!0-9a-f]*) exit 64 ;; *) true ;; esac \
    && set -- /tmp/atlas-research-dist/*.whl \
    && test "$#" -eq 1 \
    && wheel_sha256="$(sha256sum "$1" | cut -d ' ' -f 1)" \
    && test "${#wheel_sha256}" -eq 64 \
    && install -d -m 0755 /usr/local/share/atlas-research \
    && printf '%s\n%s\n' "${VCS_REF}" "${wheel_sha256}" \
      > /usr/local/share/atlas-research/source-provenance \
    && chmod 0444 /usr/local/share/atlas-research/source-provenance \
    && python -m pip install --no-cache-dir --no-deps "$1" \
    && rm -rf /tmp/atlas-research-dist \
    && groupadd --gid 65532 atlas-research \
    && useradd --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin atlas-research \
    && install -d -m 0700 -o 65532 -g 65532 /work

USER 65532:65532
WORKDIR /work

ENTRYPOINT ["atlas-research"]
CMD ["--help"]
