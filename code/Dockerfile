FROM stevwonder/docker-spark
COPY project.py /src/
RUN  chmod a+rx /src/*.py

COPY *.csv /data/
CMD ["/opt/spark/bin/spark-submit", "/src/project.py", "/data/DEMO_H.csv,/data/DR1IFF_H.csv"]
