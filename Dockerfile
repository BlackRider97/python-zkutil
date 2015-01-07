FROM dockerfile/python

RUN pip install kazoo==2.0

RUN pip install pyyaml

VOLUME /python-zkutil

WORKDIR /python-zkutil

CMD python zkutil.py
 
