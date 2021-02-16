# Exam-bluepi

* All Service
    *   Kubernetes Engine (Airflow)            # Cluster -> asia-northeast2-airflow-af25a695-gke
    *   Compute Engine (Jupyter)               # Instance-e2-jupyter-bluepi
    *   CloudBuild (CI/CD)                     # Trigger airflow-bluepi-deploy
    *   Storage                                # Datalake-bluepi-bucket, asia-northeast2-airflow-af25a695-bucket
    *   BigQuery                               # ( Data Warehouse ) de-exam-anucha:dw_bluepi.user_log
    *   Postgres                               # ( Data source ) user_log

* Code Structure
```
    .
    ├── credential
    ├── dags
    │   ├── bluepi
    │       ├── crm (project1)
    │       │   ├── env (other config)
    │       │   ├── src (etl helper function)
    │       │   ├── dag_de_anucha_exam.py (DAG)
    │       ├── sale (project2)
    │           ├── env (other config)
    │           ├── src (code function)
    │           ├── other.py
    ├── logs
    ├── .gitignore
    ├── airflow.cfg
    ├── cloudbuild
    ├── env_var.json
    ├── README.md
    └── requirements.txt
```


* Architecture

    ![picture](images/work-flow.png)

* PySpark
    *   Compute Engine ( Instance-e2-jupyter-bluepi )
        *   PySpark Environment
            ```python
                os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
                os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'
                os.environ['SPARK_HOME'] = '/home/spark/spark-3.0.1'
                os.environ['PYTHONPATH'] = '/home/spark/spark-3.0.1/python/lib/py4j-0.10.9-src.zip:/home/spark/spark-3.0.1/python/:'
            ```

* Jupyter Notebook
    *   Compute Engine ( Instance-e2-jupyter-bluepi )
        *   Environment
            ```sh
                su - jupyter
                cd workspace/
                source workspace/bin/activate
            ```
        *  Status service
            ```sh
                systemctl status jupyter
            ```
        *   Example
            ```s
                notebook/PySpark.ipynb
                notebook/Getdata-final.ipynb
            ```
* CloudBuild
    *   trigger name : airflow-bluepi-deploy
    *   Cloudbuild.yaml
        ```yaml
            steps:
                - name: 'docker.io/library/python:3.7'
                id: Environment
                entrypoint: /bin/sh
                args:
                - -c
                - 'pip install -r requirements.txt'
                - name: gcr.io/google.com/cloudsdktool/cloud-sdk
                id: Deploy
                entrypoint: bash
                args: [ '-c', 'if [ "$BRANCH_NAME" == "main" ]; then echo "$BRANCH_NAME" && gsutil -m rsync -d -r ./dags gs://${_COMPOSER_BUCKET}/dags; else echo "Working on $BRANCH_NAME"; fi']
                substitutions:
                    _COMPOSER_BUCKET: asia-northeast2-airflow-af25a695-bucket
        ```
    *   Environment
        ```py
            'pip install -r requirements.txt'
        ```
    *   Deploy
         ```sh
            if [ "$BRANCH_NAME" == "main" ]; then echo "$BRANCH_NAME" && gsutil -m rsync -d -r ./dags gs://${_COMPOSER_BUCKET}/dags; else echo "Working on $BRANCH_NAME"; fi
        ```

* Storage
    *  Bucket
    ```py
        TODAY =  date.today().strftime('%Y-%m-%d')
        HOURS = strftime("%H%M%S", gmtime())
        PROJECT_ID = 'de-exam-anucha'
        DESTINATION_BUCKET = 'datalake-bluepi-bucket'
        DESTINATION_DIRECTORY = 'bluepi/crm'
        DATABASE = 'postgres'
        TABLE_NAME = 'user_log'
        DATASET_NAME = 'dw_bluepi'

        backet=f"gs://{PROJECT_ID}/{DESTINATION_DIRECTORY}/{DATABASE}/dt={YYY-MM-DD}/{TABLE_NAME}-{TODAY}.csv"
    ```

* BigQuery (DW)
    ```py
        PROJECT_ID = 'de-exam-anucha'
        DATASET_NAME = 'dw_bluepi'
        TABLE_NAME = 'user_log'
    ```

    ```sql
        SELECT * FROM `de-exam-anucha.dw_bluepi.user_log` LIMIT 1000
    ```

*  Output

    | id                      |  user_id                               |  action |  success |  created_at           |  updated_at
    | ----------------------- |  ------------------------------------  |  -----  |  -----  |  -------------------  |  ---------------------|
    | 4b796e06-3178-4133-ad30 |  55514cf0-3026-404f-8ea3-f41b00bdf6b5  |  login  |  false  |  2020-02-16 18:46:05  |  2020-02-16 18:46:05  |
    | 8745cacb-f8aa-4294-b824 |  55514cf0-3026-404f-8ea3-f41b00bdf6b5  |  login  |  false  |  2020-02-16 18:46:06  |  2020-02-16 18:46:06  |
    | 9b44ab5c-8516-4507-a46f |  55514cf0-3026-404f-8ea3-f41b00bdf6b5  |  login  |  false  |  2021-02-10 05:11:18  |  2021-02-10 05:11:18  |
