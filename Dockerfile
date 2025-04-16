FROM ubuntu:24.04

# Install build dependencies
RUN apt-get update && apt-get install -y \
  python3-pip \
  python3-venv \
  pipx \
  wget \
  git \
  mrtrix3 \
  curl \
  zip

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
  && unzip awscliv2.zip \
  && ./aws/install

# Add application code
ADD . /app

# Install pipx and then install application using pipx so "xnat-ingest" is on PATH
RUN pipx ensurepath \
  && pipx install /app

# Set application entrypoint to docker entrypoint
ENTRYPOINT ["xnat-ingest"]
