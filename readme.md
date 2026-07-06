# Mining Company — Data Platform Architecture

A design deliverable: a scalable, orchestrated data platform for a mining company,
handling both batch and streaming data — ingestion, processing, storage, governance,
and BI/ML — proposed as a **Lambda architecture on GCP**.

**Stack:** GCP (Dataflow / Apache Beam · Pub/Sub · Cloud Storage · BigQuery · Composer / Airflow · Dataplex) · Terraform

## Data sources

Surveyed with the company's analytics team:

- **Sistema ERP:** SAP con una base de datos Oracle.
- **Sistema de Producción:** App desarrollada "in house" con una base de datos Postgres.
- **Fuentes externas:** un proveedor que realiza algunas mediciones de la calidad de las rocas le deja todos sus análisis en un bucket de AWS S3 con archivos Avro.
- **Mediciones en tiempo real:** Utilizan +100 sensores de mediciones de vibración por toda la mina para detectar movimiento del suelo y se podrían utilizar para predecir posibles derrumbes.
- **Apps Mobile:** La empresa cuenta con una app mobile donde trackean todos los issues pendientes con maquinaria de la mina.

## Design decisions

Key decisions below (cloud vs on-prem, ETL vs ELT, tooling, storage model, governance,
IaC), answering the requirements of a scalable, robust, automatically orchestrated
platform with security, quality and data lineage across batch and real-time data.


**1. ¿Utilizarían infraestructura on premise o en la nube?**

En primera instancia, se evaluaría la infraestructura actual de la empresa y las políticas establecidas para alinear el flujo de trabajo con la infraestructura existente. 

Sin embargo, si se tiene la libertad de elegir, recomendaría utilizar una **infraestructura en la nube** por las siguientes razones:

**Escalabilidad y mantenimiento:** La nube permite escalar la infraestructura de forma rápida y sencilla para adaptarse a las necesidades cambiantes de almacenamiento y procesamiento de datos. 

El proveedor de la nube se encarga del mantenimiento de la infraestructura, liberando tiempo y recursos para enfocarse en el know how del negocio.

Además no tenemos que 

**Integración de herramientas:** La nube ofrece una amplia gama de herramientas pre-configuradas y listas para usar, lo que reduce el tiempo y esfuerzo dedicados a la configuración y gestión de herramientas para ingesta, transformación y carga de datos.

**Resiliencia de datos:** La nube proporciona redundancia y mecanismos de recuperación ante desastres para garantizar la disponibilidad y seguridad de los datos.

**Costos reducidos:** El modelo de pago por uso de la nube permite optimizar los gastos, pagando solo por los recursos que se utilizan.

**Seguridad:** La nube ofrece mejor protección que en on premise. 

### Enfoque de procesamiento de datos

**2. ¿ETL o ELT? ¿Por qué?**

Se implementará una **arquitectura Lambda** que combina una capa de **Streaming** y una capa de **Batch** para procesar y analizar datos de manera eficiente.

**Capa de Streaming:**

En la capa de Streaming se utilizará el enfoque **ELT (Extract, Load, Transform)** para ingerir **mediciones en tiempo real** y **mensajes de la aplicación móvil** por las siguientes razones:

* **Velocidad de acceso a la información:** La arquitectura ELT permite una ingesta y almacenamiento rápidos de datos sin transformaciones previas, garantizando un acceso inmediato a la información crítica, como lecturas de sensores de vibración y mensajes de incidencias.

* **Complejidad reducida:** Al evitar pipelines y transformaciones complejas en la ingesta inicial, ELT simplifica el acceso a los datos, especialmente para datos en tiempo real que requieren una respuesta inmediata.

**Capa de Batch:**

En la capa de Batch se utilizará ETL para ingerir datos del **sistema ERP**, **sistemas de producción** y **fuentes externas** y almacenarlos en un **Data Lake**. Este Data Lake servirá como repositorio centralizado de datos sin procesar, permitiendo su posterior utilización para:

* **Modelos de Machine Learning (ML) e Inteligencia Artificial (AI):** Los datos sin procesar en el Data Lake pueden ser utilizados para entrenar y desarrollar modelos de ML y AI que aporten valor a la organización.

* **Transformación y almacenamiento en un Data Warehouse (DW) para análisis:** Los datos del Data Lake pueden transformarse y almacenarse en un DW para su análisis y consulta posterior, permitiendo a los usuarios realizar análisis complejos y obtener insights valiosos.

### Herramientas

**3. ¿Qué herramienta/s utilizarían para ETL/ELT?**

* **Capa de Streaming:** Apache Beam en Dataflow.
* **Capa de Batch:** Apache Beam en Dataflow.

**Orquestación:** Apache Airflow en Composer. 

**4. ¿Qué herramienta/s utilizarían para ingestar estos datos?**

* **Capa de Streaming:** Pub/Sub.
* **Capa de Batch:** Operadores de Airflow en Composer: 
  * PostgresToGCSOperator: para los datos provenientes del sistema de producción. 
  * OracleToGCSOperator: para los datos provenientes de SAP. 
  * S3ToGCSOperator: para las fuentes externas adicionales. 

**5. ¿Qué herramienta/s utilizarían para almacenar estos datos?**

* Staging: Google Cloud Storage.
* Data Lake: Google Cloud Storage. 
* Data Warehouse: BigQuery.

**6. ¿Cómo guardarán la información, OLTP o OLAP?**

**OLAP (On-Line Analytical Processing)** será la forma de almacenar la información de manera desnormalizada y en distintas tablas, ya que se utilizará para la inteligencia empresarial y la toma de decisiones. 

**7. ¿Qué herramienta/s utilizarían para Data Governance?**

Google Cloud Data Plex.

**8. ¿Data Warehouse, Data Lake o Lake House?**

Lake House. Se almacenarán los datos en crudo provenientes de la capa de Streaming y de Batch en un bucket de Google Cloud Storage, este será el Data Lake. Como Data Warehouse se utilizará Big Query para almacenar los datos luego de las tranformaciones. 

**9. ¿Qué tipo de información gestionarán, estructurada, semi estructurada, no estructurada?**

Se gestionará:

**Data estructurada:** Los datos provenientes del sistema ERP, el sistema de producción y fuentes externas. 

**Data semi estructurada:** Los datos provenientes de las mediciones en tiempo real y de las apps mobile, ambas (JSON).

**10. ¿Con qué herramienta podrían desplegar toda la infraestructura de datos?**

Se puede despelagar toda la infraestructura de datos con Terraform, Terraform es una herramienta de infraestructura como código (IaC), que permite definir, provisionar y gestionar la infraestructura de forma declarativa. 

# Representación de la arquitectura 

La arquitectura se plantea de la siguiente manera 

![img/arq.png](img/arq.png)