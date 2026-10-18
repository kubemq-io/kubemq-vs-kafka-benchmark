# OpenMessaging Benchmark at a pinned commit, built from source. Nothing in OMB is patched.
# Stage 1 runs on the build machine's own architecture (Java bytecode is portable);
# stage 2 is the amd64 runtime image the GKE nodes pull.
ARG OMB_COMMIT
FROM --platform=$BUILDPLATFORM maven:3.9.9-eclipse-temurin-17@sha256:f58d59b6273e785ac0a4477f6e9b5ba1d7731c75b906c0f7b34076f1851318cc AS build
ARG OMB_COMMIT
ADD https://github.com/openmessaging/benchmark/archive/${OMB_COMMIT}.tar.gz /src.tgz
RUN mkdir /src && tar -xzf /src.tgz -C /src --strip-components=1
WORKDIR /src
# spotbugs is skipped: at this commit OMB fails its own lint gate (EI_EXPOSE_REP in
# MessageSizeDistribution). A report-only check; it changes no compiled code.
RUN mvn -B -q install -DskipTests -Dspotless.check.skip=true -Dlicense.skip=true -Dspotbugs.skip=true
RUN mkdir /benchmark && tar -xzf package/target/openmessaging-benchmark-*-SNAPSHOT-bin.tar.gz -C /benchmark --strip-components=1

FROM eclipse-temurin:17-jre@sha256:34d6aaf0fa4ef553c470234ec23c32c7d5c0f370c2bd4b0bf8a16838052d85f0
ARG OMB_COMMIT
LABEL org.opencontainers.image.source=https://github.com/openmessaging/benchmark \
      org.opencontainers.image.revision=${OMB_COMMIT}
COPY --from=build /benchmark /benchmark
WORKDIR /benchmark
CMD ["/bin/bash"]
