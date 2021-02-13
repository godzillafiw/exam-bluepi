""" add additional DAGs folders """
import os
from airflow.models import DagBag


dags_dirs= [dir[0] for x in os.walk('~/dags/bluepi')]
#dags_dirs = ['~/docker-airflow/dags/dags_scgp/DAGS_CIP/', '/docker-airflow/dags/dags_scgp/DAGS_FC/']

for dir in dags_dirs:
   dag_bag = DagBag(os.path.expanduser(dir))

   if dag_bag:
      for dag_id, dag in dag_bag.dags.items():
         globals()[dag_id] = dag