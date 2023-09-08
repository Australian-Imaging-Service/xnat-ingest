FROM ubuntu:22.04

# Install build dependencies
RUN apt-get update && apt-get install -y \
  python3-pip \
  git \
  dcmtk \
  && rm -rf /var/lib/apt/lists/*

# Add application code
ADD . /app

RUN pip3 install /app


# Set application entrypoint to docker entrypoint
ENTRYPOINT ["xnat-siemens-export-upload"]
