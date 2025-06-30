# Project Structure

## Directory Overview

```
projeto_etl_spark/
├── 📁 astro/                  # Apache Airflow environment
│   ├── 📁 dags/               # Airflow DAGs
│   │   ├── main.py            # Main ETL pipeline DAG
│   │   └── utils/             # DAG utilities
│   ├── 📁 include/            # Shared resources
│   │   ├── sql/               # SQL queries
│   │   └── spark/             # Spark jobs
│   ├── 📁 plugins/            # Airflow plugins
│   ├── 📁 tests/              # Unit and integration tests
│   ├── 📄 Dockerfile          # Custom Airflow image
│   ├── 📄 requirements.txt    # Python dependencies
│   └── 📄 .env                # Environment variables
│
├── 📁 data/                   # Data processing scripts
│   ├── create_tables.py       # Database schema creation
│   ├── faker_data.py          # Synthetic data generation
│   └── spark_transformations/ # Spark transformation logic
│
├── 📁 iac/                    # Infrastructure as Code
│   ├── main.tf                # Terraform main configuration
│   ├── variables.tf           # Terraform variables
│   ├── outputs.tf             # Terraform outputs
│   └── modules/               # Terraform modules
│
├── 📁 docs/                   # Project documentation
│   ├── index.md               # Documentation home
│   ├── architecture.md        # System architecture
│   ├── etl_pipeline.md        # Pipeline details
│   └── ...                    # Other documentation
│
├── 📁 notebooks/              # Jupyter notebooks
│   ├── exploration/           # Data exploration
│   └── analysis/              # Data analysis
│
├── 📁 scripts/                # Utility scripts
│   ├── setup.sh               # Environment setup
│   └── deploy.sh              # Deployment script
│
├── 📄 pyproject.toml          # Poetry configuration
├── 📄 poetry.lock             # Locked dependencies
├── 📄 mkdocs.yml              # Documentation config
├── 📄 .gitignore              # Git ignore rules
├── 📄 .env.example            # Environment template
├── 📄 README.md               # Project readme
└── 📄 LICENSE                 # Project license
```

## Key Components

### **Airflow Directory (`astro/`)**

The Astronomer-based Airflow setup containing:

- **DAGs**: Python files defining workflow orchestration
- **Include**: Shared resources like SQL queries and Spark jobs
- **Plugins**: Custom Airflow operators and hooks
- **Tests**: Test coverage for DAGs and tasks

### **Data Directory (`data/`)**

Contains data processing scripts:

- **Schema Management**: Scripts to create and manage database schemas
- **Data Generation**: Tools for creating synthetic test data
- **Transformations**: Spark transformation logic organized by layer

### **Infrastructure (`iac/`)**

Terraform configuration for Azure resources:

- **Main Configuration**: Core infrastructure definition
- **Variables**: Configurable parameters
- **Modules**: Reusable infrastructure components

### **Documentation (`docs/`)**

Comprehensive project documentation:

- **Architecture**: System design and components
- **Guides**: Installation, configuration, and usage guides
- **API Reference**: Technical documentation

## File Naming Conventions

### **Python Files**
- Snake_case for modules: `data_processor.py`
- Classes use PascalCase: `DataProcessor`
- Functions use snake_case: `process_data()`

### **Documentation**
- Lowercase with underscores: `quick_start.md`
- Descriptive names indicating content

### **Configuration**
- Standard names: `.env`, `requirements.txt`
- Environment-specific: `.env.development`, `.env.production`

## Code Organization

### **DAG Structure**

```python
# astro/dags/main.py
from airflow import DAG
from airflow.operators.python import PythonOperator

def extract_data():
    """Extract data from source"""
    pass

def transform_data():
    """Transform data"""
    pass

def load_data():
    """Load data to destination"""
    pass

with DAG('etl_pipeline', ...) as dag:
    extract = PythonOperator(task_id='extract', python_callable=extract_data)
    transform = PythonOperator(task_id='transform', python_callable=transform_data)
    load = PythonOperator(task_id='load', python_callable=load_data)
    
    extract >> transform >> load
```

### **Spark Job Structure**

```python
# data/spark_transformations/bronze_processing.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class BronzeProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def process(self, input_path: str, output_path: str):
        """Process bronze layer data"""
        df = self.spark.read.csv(input_path)
        df_processed = self._apply_transformations(df)
        df_processed.write.format("delta").save(output_path)
    
    def _apply_transformations(self, df):
        """Apply bronze layer transformations"""
        return df.withColumn("processing_date", current_date())
```

## Development Workflow

### **1. Feature Development**
```bash
# Create feature branch
git checkout -b feature/new-transformation

# Make changes
# Add tests
# Update documentation

# Commit changes
git add .
git commit -m "feat: add new transformation logic"
```

### **2. Testing**
```bash
# Run unit tests
pytest astro/tests/

# Run integration tests
pytest astro/tests/integration/

# Test DAG
airflow dags test medallion_architecture_etl
```

### **3. Documentation**
```bash
# Build documentation locally
mkdocs serve

# View at http://localhost:8000
```

## Best Practices

### **Code Quality**
- Follow PEP 8 style guide
- Use type hints
- Write docstrings for all functions/classes
- Maintain test coverage > 80%

### **Version Control**
- Use conventional commits
- Keep commits atomic
- Write descriptive commit messages
- Review code before merging

### **Documentation**
- Keep documentation up-to-date
- Include examples in docstrings
- Document design decisions
- Maintain changelog

### **Security**
- Never commit secrets
- Use environment variables
- Implement proper access controls
- Regular security audits