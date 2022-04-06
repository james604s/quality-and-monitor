# quality-and-monitor

### Env Local
```
pipenv --python 3.9 install
pipenv install <pkg>
pipenv shell <enter env>
```

### Env Docker
```
OS: Debian GNU/Linux 10 (buster)
Python: 3.9
Airflow: 2.1.2
MongoDB: 4
PostgreSQL: 13
Redis: latest
```

### Folder Structure
```
├── config                         // airflow設定檔
├── plugins
├── dags
│   ├── crawler_scripts      
│   ├── util                      // 可利用func彙整         
│   ├── activities_dag.py         // DAG
│   ├── ...
├── logs                          // .gitignore
├── travel-jump-docker-addition   // Dockerfile
├── travel-jump-docker-dev        // docker-compose 開發用
├── travel-jump-docker-official
├── wiki
├── .gitignore
├── airflow.sh
└── Readme.md
```

### ge folder
```

great_expectations.yml contains the main configuration of your deployment.
The expectations directory stores all your Expectations as JSON files. If you want to store them somewhere else, you can change that later.

The plugins/ directory holds code for any custom plugins you develop as part of your deployment.
The uncommitted/ directory contains files that shouldn’t live in version control. It has a .gitignore configured to exclude all its contents from version control. The main contents of the directory are:
uncommitted/config_variables.yml, which holds sensitive information, such as database credentials and other secrets.
uncommitted/data_docs, which contains Data Docs generated from Expectations, Validation Results, and other metadata.
uncommitted/validations, which holds Validation Results generated by Great Expectations.
```