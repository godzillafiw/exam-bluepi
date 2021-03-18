""" add additional DAGs folders """
import os
from airflow.models import DagBag

# Loop add dag in path dags/bluepi
dags_dirs= [dir[0] for x in os.walk('~/dags/bluepi')]

for dir in dags_dirs:
   dag_bag = DagBag(os.path.expanduser(dir))

   if dag_bag:
      for dag_id, dag in dag_bag.dags.items():
         globals()[dag_id] = dag  
         sdfsdfsdf