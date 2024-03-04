FROM ubuntu:22.04

# Install build dependencies
RUN apt-get update && apt-get install -y \
  python3-pip \
  wget \
  git \
  && rm -rf /var/lib/apt/lists/*

# Install Miniconda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
  bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/conda && \
  rm Miniconda3-latest-Linux-x86_64.sh

# Add Miniconda to PATH
ENV PATH="/opt/conda/bin:${PATH}"

# Install MRtrix3
RUN  conda install -c mrtrix3 mrtrix3

# Add application code
ADD . /app

# Install application
RUN pip3 install /app

# Set application entrypoint to docker entrypoint
ENTRYPOINT ["xnat-ingest"]
