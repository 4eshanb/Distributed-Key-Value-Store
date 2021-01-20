FROM python

MAINTAINER esbharga

WORKDIR /usr/src/app

COPY key_value_replicas.py .

RUN pip3 install flask 
RUN pip3 install requests

EXPOSE 8085

CMD [ "python3", "./key_value_replicas.py" ]

