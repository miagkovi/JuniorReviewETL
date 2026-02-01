# JuniorReviewETL
Junior big data engineer PeEx review 

Launch the container:

```bash
docker-compose run spark-etl /bin/bash
```

Execute the etl script manually:

```bash
pytest tests/ && /opt/spark/bin/spark-submit --master local[*] src/app.py
```

Check files:

```bash
ls -l output/
cat etl_process.log
```