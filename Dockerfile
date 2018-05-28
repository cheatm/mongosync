FROM registry.docker-cn.com/library/python

ADD . /mongosync
WORKDIR /mongosync

RUN pip install -r requirements.txt
RUN python setup.py install

CMD mongosync oplog