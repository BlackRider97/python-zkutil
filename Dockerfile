FROM dockerfile/python

RUN apt-get install -y git

RUN git clone https://github.com/BlackRider97/python-zkutil.git 

RUN pip install kazoo==2.0

RUN pip install pyyaml

WORKDIR python-zkutil

CMD python zkutil.py

ENTRYPOINT python zkutil.py 
