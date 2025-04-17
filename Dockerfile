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

RUN pipx ensurepath

# Pre-install some dependencies before adding the application to use the docker cache
RUN pipx install \
    click >=8.1 \
    discord \
    fileformats-medimage>=0.10.1 \
    fileformats-medimage-extras>=0.10.1 \
    pydicom>=2.3.1 \
    tqdm>=4.64.1 \
    boto3 \
    natsort \
    paramiko \
    xnat \
    frametree \
    frametree-xnat

# Add application code
ADD . /app

# Install pipx and then install application using pipx so "xnat-ingest" is on PATH
RUN pipx install /app

# Set application entrypoint to docker entrypoint
ENTRYPOINT ["xnat-ingest"]
