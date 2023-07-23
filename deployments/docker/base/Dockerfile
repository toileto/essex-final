FROM python:3.8.13-slim-buster

# Install requirements
ADD requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# Install build-essential to use "make" commands
RUN apt-get update && apt-get install -y build-essential

# Install docker
RUN apt-get update && apt-get install -y ca-certificates curl gnupg lsb-release
RUN mkdir -p /etc/apt/keyrings
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
RUN echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
