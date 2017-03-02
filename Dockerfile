FROM python:latest

VOLUME /opt/vireo
WORKDIR /opt/vireo

RUN pip install gallium

ADD . /opt/vireo
RUN pip install .

CMD g3 sample.observe
