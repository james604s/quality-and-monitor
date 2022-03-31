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