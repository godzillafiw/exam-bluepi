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
    │       │   ├── src (code function)
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

* Spark
    *   Compute Engine ( Instance-e2-jupyter-bluepi )
        *   Install
            ```sh
                sudo apt update
                sudo apt install python3-pip python3-dev
                sudo -H pip3 install --upgrade pip
                sudo -H pip3 install virtualenv
            ```

* Notebook
    ```
        notebook/PySpark.ipynb
        notebook/Getdata-final.ipynb
    ```

* Example
```sql
    SELECT * FROM `de-exam-anucha.dw_bluepi.user_log` LIMIT 1000
```
*  Output

    | id                      |  user_id                               |  action |  success |  created_at           |  updated_at
    | ----------------------- |  ------------------------------------  |  -----  |  -----  |  -------------------  |  ---------------------|
    | 4b796e06-3178-4133-ad30 |  55514cf0-3026-404f-8ea3-f41b00bdf6b5  |  login  |  false  |  2020-02-16 18:46:05  |  2020-02-16 18:46:05  |
    | 8745cacb-f8aa-4294-b824 |  55514cf0-3026-404f-8ea3-f41b00bdf6b5  |  login  |  false  |  2020-02-16 18:46:06  |  2020-02-16 18:46:06  |
    | 9b44ab5c-8516-4507-a46f |  55514cf0-3026-404f-8ea3-f41b00bdf6b5  |  login  |  false  |  2021-02-10 05:11:18  |  2021-02-10 05:11:18  |


