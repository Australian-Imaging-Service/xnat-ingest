FROM ubuntu:22.04

# Install build dependencies
RUN apt-get update && apt-get install -y \
  python3-pip \
  wget \
  git \
  mrtrix3 \
  && rm -rf /var/lib/apt/lists/*

# Add application code
ADD . /app

# Install application
RUN pip3 install /app

# Set application entrypoint to docker entrypoint
ENTRYPOINT ["xnat-ingest"]
