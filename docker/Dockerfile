FROM python:3.10-slim-buster
LABEL maintainer="astenger"
HEALTHCHECK NONE

RUN apt-get update && \
    apt-get install -y build-essential cmake libpng-dev pkg-config git

RUN useradd --create-home --shell /bin/bash dockeruser
USER dockeruser
WORKDIR /home/dockeruser

COPY requirements.txt .
RUN pip install -r requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/home/dockeruser/.local/bin"
ENV PATH="${PATH}:/home/dockeruser/.local/bin"

RUN mkdir input
RUN mkdir output

COPY process_script.py .

ENTRYPOINT ["python", "-u", "process_script.py"]
