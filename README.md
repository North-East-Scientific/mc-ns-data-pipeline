# MasterControl & NetSuite ETL Pipeline

This repository contains an enterprise-grade ETL framework to extract, transform, and load data from **MasterControl** and **NetSuite** into a centralized **PostgreSQL** warehouse. It supports both **bulk** and **incremental** loads and provides transformation scripts for metric generation and attribute derivation.

---

## 🔧 Features

- ✅ Extract data from MasterControl APIs
- ✅ Extract transactional and reference data from NetSuite
- ✅ Support for bulk and checkpointed incremental loads
- ✅ Transformation pipelines for metrics and derived attributes
- ✅ Load processed data into PostgreSQL
- ✅ Logging, checkpointing, and audit tracking
- ✅ Modular structure with standalone tasks
