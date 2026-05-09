# Data Lake Académico UNAD — Trabajo de grado

Implementación de un Data Lake serverless tipo Medallion (Bronze / Silver / Gold)
sobre AWS para la integración de datos académicos de la **Universidad Nacional
Abierta y a Distancia (UNAD)** con fuentes gubernamentales colombianas (Datos
Abiertos, SPADIES, SNIES). El objetivo es reducir la latencia de consolidación
de información de 8 días hábiles a menos de 30 minutos y soportar analítica
descriptiva y predictiva (deserción estudiantil) sobre un punto único de verdad.

Trabajo de grado del programa de Ingeniería de Sistemas (UNAD), línea de
**Ciencia de Datos y Sistemas Complejos**.

---

## Tabla de contenido

1. [Arquitectura](#1-arquitectura)
2. [Estructura del repositorio](#2-estructura-del-repositorio)
3. [Convenciones](#3-convenciones)
4. [Pre-requisitos](#4-pre-requisitos)
5. [Despliegue paso a paso](#5-despliegue-paso-a-paso)
6. [Verificación post-despliegue](#6-verificación-post-despliegue)
7. [Probar el pipeline](#7-probar-el-pipeline)
8. [Habilitar el scheduler diario](#8-habilitar-el-scheduler-diario-opcional)
9. [Costos esperados](#9-costos-esperados)
10. [Solución de problemas comunes](#10-solución-de-problemas-comunes)
11. [Limpieza completa](#11-limpieza-completa)
12. [Apache Superset (visualización transitoria)](#12-apache-superset-visualización-transitoria)
13. [Referencias](#13-referencias)

---

## 1. Arquitectura

| Capa | Servicio AWS | Responsabilidad |
|---|---|---|
| Trigger | EventBridge Scheduler | Cron diario que dispara la State Machine (DISABLED por defecto) |
| Orquestación | Step Functions | Coordina ingesta paralela + Glue jobs + Crawlers |
| Extracción | Lambda (Python 3.11, arm64) | UNAD vía POST + Datos Abiertos vía Socrata API paginada |
| Almacenamiento | S3 | 4 buckets: `raw` (Bronze), `silver`, `gold`, `athena-results` |
| Transformación | Glue Jobs (PySpark, G.1X) | `raw_to_silver` y `silver_to_gold` con bookmarks habilitados |
| Catálogo | Glue Data Catalog + Crawlers | Registro automático de tablas Silver y Gold |
| Consumo SQL | Athena | WorkGroup con cifrado SSE-KMS y cutoff 1 GB/query |
| Cifrado | KMS | Llave única con rotación habilitada para todo el lake |
| Seguridad | IAM | 4 roles con principio de menor privilegio |
| IaC | CloudFormation | Stack maestro con 7 stacks anidados |
| Observabilidad | CloudWatch | Logs estructurados, métricas custom, alarmas |

**Fuentes de datos**:
- **UNAD** (microdato estudiantil por periodo, CSV CP1252) → ingesta automática vía Lambda.
- **Datos Abiertos Colombia** (Sisbén IV, MinTIC, Cobertura MEN, Renta Ciudadana, Indicadores DNP) → ingesta automática vía Lambda paginada.
- **SNIES** (graduados E.S., XLSX/XLSB) → carga **manual** al bucket Bronze.
- **SPADIES** (% beneficiarios ICETEX, CSV pivot UTF-8 BOM) → carga **manual** al bucket Bronze.

---

## 2. Estructura del repositorio

```
DataLake-Academico/
├── README.md                          # Este documento
├── .gitignore                         # Excluye datasets, secretos y build artifacts
├── IaC/                               # CloudFormation modular
│   ├── bootstrap.yaml                 # Bucket de artefactos (deploy 1ª vez)
│   ├── master.yaml                    # Stack maestro orquestador
│   ├── parameters.yaml                # Defaults para CLI
│   ├── template-kms.yaml              # Llave KMS y alias
│   ├── template-s3.yaml               # 4 buckets de datos
│   ├── template-iam.yaml              # 4 roles de servicio
│   ├── template-lambdas.yaml          # 2 funciones Lambda + alarmas
│   ├── template-glue.yaml             # 2 DBs, 2 jobs, 2 crawlers
│   ├── template-step.yaml             # State Machine ETL
│   ├── template-eventbridge.yaml      # Schedule diario (DISABLED)
│   ├── template-athena.yaml           # WorkGroup
│   └── .cfnlintrc                     # Configuración de lint
└── src/
    ├── glue_jobs/
    │   ├── raw_to_silver.py           # Bronze → Silver (4 fuentes)
    │   └── silver_to_gold.py          # Silver → Gold (fact + dims + target)
    └── lambdas/
        ├── ingestion_unad/
        │   ├── lambda_function.py     # Extractor UNAD
        │   └── test_local.py          # Smoke-test offline (DRY_RUN)
        └── ingestion_gobierno/
            ├── lambda_function.py     # Extractor Datos Abiertos paginado
            └── test_local.py          # Smoke-test offline (DRY_RUN)
```

---

## 3. Convenciones

**Nombrado de recursos**: `data-lake-academico-{descriptor}`

El `Environment` (dev/qa/prod) **no entra en el nombre** del recurso, se conserva
únicamente como **Tag** en cada recurso para multi-ambiente futuro y para reportes
de costos en AWS Cost Explorer.

Ejemplos:
- Buckets S3: `data-lake-academico-raw-{accountId}`, `data-lake-academico-silver-{accountId}`, `data-lake-academico-gold-{accountId}`, `data-lake-academico-athena-results-{accountId}`, `data-lake-academico-artifacts-{accountId}`
- Lambdas: `data-lake-academico-extract-unad`, `data-lake-academico-extract-gobierno`
- Glue Jobs: `data-lake-academico-raw-to-silver`, `data-lake-academico-silver-to-gold`
- Glue Crawlers: `data-lake-academico-crawler-silver`, `data-lake-academico-crawler-gold`
- Glue Databases: `data_lake_academico_silver`, `data_lake_academico_gold` (sin guiones, restricción de Glue)
- Step Function: `data-lake-academico-etl-pipeline`
- EventBridge Schedule: `data-lake-academico-daily-trigger`
- Athena WorkGroup: `data-lake-academico-workgroup`
- KMS Alias: `alias/data-lake-academico-kms`
- IAM Roles: `data-lake-academico-{lambda,glue,step,events}-role`
- Stack names: `data-lake-academico-bootstrap` y `data-lake-academico` (master)

**Particionamiento Bronze**:
- `unad/extracted_at=YYYY-MM-DD/periodo=XXXX/year=YYYY/month=MM/day=DD/estudiantes.csv`
- `gobierno/dataset_id=ID/extracted_at=YYYY-MM-DD/year=YYYY/month=MM/day=DD/part-NNNNNNN.json`

**Cifrado**: SSE-KMS en los 4 buckets de datos, SSE-S3 en el bucket de artefactos, TLS obligatorio (deny insecure transport).

---

## 4. Pre-requisitos

### 4.1 Cuenta AWS

- Una cuenta AWS activa con método de pago configurado.
- Acceso a la región **us-east-1** (los nombres y servicios usados están alineados a esta región y al Free Tier amplio).

### 4.2 Usuario IAM (NO usar root)

> **Importante**: nunca uses las credenciales del usuario root para operaciones programáticas. Si tienes access keys del root, elimínalas en **IAM → Configuración de cuenta → Administración del acceso raíz**.

Crea un IAM user dedicado al despliegue:

1. **IAM → Personas → Crear persona**
2. Nombre: `infra_admin` (o el que prefieras)
3. **Adjuntar políticas directamente → AdministratorAccess**
4. Una vez creado: pestaña **Credenciales de seguridad → Crear clave de acceso → Tipo: Command Line Interface (CLI)**.
5. Copia el **Access Key ID** y el **Secret Access Key**. El secret se muestra una sola vez.

> **Crítico**: el Secret Access Key **nunca debe pegarse en chats, correos, repositorios git ni capturas de pantalla**. Trátalo como una contraseña bancaria. Si se expone por error, desactívalo inmediatamente y genera otro.

### 4.3 Herramientas locales

| Herramienta | Versión mínima | Instalación (macOS) |
|---|---|---|
| AWS CLI | v2 | `brew install awscli` |
| Python | 3.10+ | preinstalado en macOS recientes |
| zip | cualquiera | preinstalado |
| Git | cualquiera | `brew install git` |

Configura el CLI con las credenciales del IAM user:

```bash
aws configure
# AWS Access Key ID:     <Access Key ID del IAM user>
# AWS Secret Access Key: <Secret del IAM user>
# Default region name:   us-east-1
# Default output format: json
```

Configura SSE por defecto para que los `aws s3 cp` cumplan la política de los buckets:

```bash
aws configure set s3.sse AES256
```

Verifica la identidad activa:

```bash
aws sts get-caller-identity
```

Debe responder con `"Arn": "arn:aws:iam::<ACCOUNT_ID>:user/infra_admin"`. **Si responde con `:root`, no continúes**: rota credenciales primero.

### 4.4 Cuotas de servicio

Las cuentas nuevas de AWS tienen `Lambda Concurrent Executions = 10`. El despliegue ya está parametrizado para no reservar concurrencia, así que **no requiere ajuste**. Si quieres garantizar concurrencia (e.g. en `prod`), pide aumento de cuota en **Service Quotas → Lambda → Concurrent executions** y modifica `template-lambdas.yaml` para añadir `ReservedConcurrentExecutions`.

---

## 5. Despliegue paso a paso

Tiempo total estimado: **30 minutos**.

### Fase 1 — Variables de sesión

Posicionate en `src/IaC/` y exporta variables:

```bash
cd <RUTA_DEL_REPO>/DataLake-Academico/src/IaC

export AWS_REGION=us-east-1
export PROJECT=data-lake-academico
export ENV=dev
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export ARTIFACTS_BUCKET="${PROJECT}-artifacts-${ACCOUNT_ID}"
export RAW_BUCKET="${PROJECT}-raw-${ACCOUNT_ID}"
export STATE_MACHINE_ARN="arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:${PROJECT}-etl-pipeline"

# Paths absolutos (ajusta a tu repo)
export PROJECT_ROOT="$(pwd)/../.."
export IAC_DIR="${PROJECT_ROOT}/src/IaC"
export LAMBDAS_DIR="${PROJECT_ROOT}/src/lambdas"
export GLUE_DIR="${PROJECT_ROOT}/src/glue_jobs"

echo "ACCOUNT_ID=${ACCOUNT_ID}"
echo "ARTIFACTS_BUCKET=${ARTIFACTS_BUCKET}"
echo "RAW_BUCKET=${RAW_BUCKET}"
```

### Fase 2 — Bootstrap del bucket de artefactos

Despliega el stack que crea el bucket donde subiremos plantillas hijas, ZIPs de Lambdas y scripts de Glue.

```bash
aws cloudformation deploy \
  --stack-name "${PROJECT}-bootstrap" \
  --template-file bootstrap.yaml \
  --parameter-overrides ProjectName="${PROJECT}" Environment="${ENV}" \
  --region "${AWS_REGION}" \
  --no-fail-on-empty-changeset
```

Tarda ~30 segundos. Verifica:

```bash
aws s3 ls "s3://${ARTIFACTS_BUCKET}" --region "${AWS_REGION}"
aws cloudformation describe-stacks \
  --stack-name "${PROJECT}-bootstrap" \
  --query 'Stacks[0].Outputs' \
  --region "${AWS_REGION}"
```

### Fase 3 — Empaquetar y subir artefactos

Sube 12 archivos al bucket: 8 plantillas hijas, 2 scripts de Glue, 2 ZIPs de Lambdas.

```bash
# 3.1 Borrar ZIPs viejos para no subir basura por error
rm -f /tmp/extract_unad.zip /tmp/extract_gobierno.zip

# 3.2 Verificar que el código fuente existe
ls -l "${LAMBDAS_DIR}/ingestion_unad/lambda_function.py"
ls -l "${LAMBDAS_DIR}/ingestion_gobierno/lambda_function.py"

# 3.3 Empaquetar lambdas usando subshells (no muta el cwd)
( cd "${LAMBDAS_DIR}/ingestion_unad"     && zip -q -r /tmp/extract_unad.zip     lambda_function.py )
( cd "${LAMBDAS_DIR}/ingestion_gobierno" && zip -q -r /tmp/extract_gobierno.zip lambda_function.py )

# 3.4 Confirmar contenido de los ZIPs (debe verse lambda_function.py con tamaño > 0)
unzip -l /tmp/extract_unad.zip
unzip -l /tmp/extract_gobierno.zip

# 3.5 Subir lambdas
aws s3 cp /tmp/extract_unad.zip      "s3://${ARTIFACTS_BUCKET}/lambdas/extract_unad.zip"      --sse AES256
aws s3 cp /tmp/extract_gobierno.zip  "s3://${ARTIFACTS_BUCKET}/lambdas/extract_gobierno.zip"  --sse AES256

# 3.6 Subir scripts Glue
aws s3 cp "${GLUE_DIR}/raw_to_silver.py"   "s3://${ARTIFACTS_BUCKET}/glue/raw_to_silver.py"   --sse AES256
aws s3 cp "${GLUE_DIR}/silver_to_gold.py"  "s3://${ARTIFACTS_BUCKET}/glue/silver_to_gold.py"  --sse AES256

# 3.7 Subir las 8 plantillas hijas
for tpl in template-kms.yaml template-s3.yaml template-iam.yaml \
           template-lambdas.yaml template-glue.yaml template-step.yaml \
           template-eventbridge.yaml template-athena.yaml; do
  aws s3 cp "${tpl}" "s3://${ARTIFACTS_BUCKET}/cfn/${tpl}" --sse AES256
done

# 3.8 Verificación final: deben aparecer 12 archivos
aws s3 ls "s3://${ARTIFACTS_BUCKET}/" --recursive
```

> **Importante**: si modificas alguna plantilla hija o los scripts/Lambdas en el futuro, debes **re-subirlos** a S3 antes de hacer `aws cloudformation deploy` del master, de lo contrario CloudFormation seguirá leyendo la versión anterior.

### Fase 4 — Despliegue del master stack

```bash
aws cloudformation deploy \
  --stack-name "${PROJECT}" \
  --template-file master.yaml \
  --parameter-overrides \
      ProjectName="${PROJECT}" \
      Environment="${ENV}" \
      ArtifactsBucketName="${ARTIFACTS_BUCKET}" \
      ArtifactsBucketUrl="https://${ARTIFACTS_BUCKET}.s3.${AWS_REGION}.amazonaws.com/cfn" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "${AWS_REGION}" \
  --no-fail-on-empty-changeset
```

Tarda **15–20 minutos**. La CLI no muestra progreso en vivo. En otra terminal puedes seguirlo:

```bash
watch -n 15 'aws cloudformation describe-stacks \
  --stack-name "'"${PROJECT}"'" \
  --query "Stacks[0].StackStatus" \
  --region "'"${AWS_REGION}"'" --output text'
```

Estados esperados: `REVIEW_IN_PROGRESS` → `CREATE_IN_PROGRESS` → `CREATE_COMPLETE`.

### Fase 5 — Cargar SNIES y SPADIES al bucket Bronze

SNIES y SPADIES son descarga manual desde los portales de origen (no tienen API estable). Los archivos del repo (`data/snies/`, `data/spadies/`) ya están descargados:

```bash
aws s3 sync "${PROJECT_ROOT}/data/snies/"   "s3://${RAW_BUCKET}/snies/"   --region "${AWS_REGION}" --sse aws:kms
aws s3 sync "${PROJECT_ROOT}/data/spadies/" "s3://${RAW_BUCKET}/spadies/" --region "${AWS_REGION}" --sse aws:kms
```

Verifica:

```bash
aws s3 ls "s3://${RAW_BUCKET}/snies/"   --region "${AWS_REGION}"
aws s3 ls "s3://${RAW_BUCKET}/spadies/" --region "${AWS_REGION}"
```

---

## 6. Verificación post-despliegue

```bash
# Outputs del master (URLs y nombres reales de recursos)
aws cloudformation describe-stacks \
  --stack-name "${PROJECT}" \
  --query 'Stacks[0].Outputs' \
  --region "${AWS_REGION}" --output table

# 4 buckets de datos
aws s3 ls --region "${AWS_REGION}" | grep "${PROJECT}-${ENV}"

# 2 Lambdas
aws lambda list-functions --region "${AWS_REGION}" \
  --query "Functions[?starts_with(FunctionName, '${PROJECT}-${ENV}-lambda')].FunctionName" \
  --output table

# State Machine
aws stepfunctions list-state-machines --region "${AWS_REGION}" \
  --query "stateMachines[?contains(name, '${PROJECT}-${ENV}')].name" \
  --output table

# Glue databases (deben existir; las tablas se crean al correr los jobs)
aws glue get-databases --region "${AWS_REGION}" \
  --query "DatabaseList[?starts_with(Name, '${PROJECT}_${ENV}')].Name" \
  --output table
```

---

## 7. Probar el pipeline

### 7.1 Smoke-test individual de Lambdas

UNAD (un periodo):
```bash
aws lambda invoke \
  --function-name "${PROJECT}-extract-unad" \
  --payload '{"periodo":"2034","tipo":"1","nivel":"2"}' \
  --cli-binary-format raw-in-base64-out \
  --region "${AWS_REGION}" \
  /tmp/unad_response.json && cat /tmp/unad_response.json
```

Gobierno (1 página de Renta Ciudadana, dataset pequeño):
```bash
aws lambda invoke \
  --function-name "${PROJECT}-extract-gobierno" \
  --payload '{"dataset_id":"6v4n-7ahj","limit":50000,"max_pages":1}' \
  --cli-binary-format raw-in-base64-out \
  --region "${AWS_REGION}" \
  /tmp/gob_response.json && cat /tmp/gob_response.json
```

Verifica los archivos en raw:
```bash
aws s3 ls "s3://${RAW_BUCKET}/unad/"     --recursive --region "${AWS_REGION}"
aws s3 ls "s3://${RAW_BUCKET}/gobierno/" --recursive --region "${AWS_REGION}"
```

### 7.2 Ejecución end-to-end de la State Machine

```bash
aws stepfunctions start-execution \
  --state-machine-arn "${STATE_MACHINE_ARN}" \
  --name "smoke-test-$(date +%Y%m%d-%H%M%S)" \
  --input '{
    "unadPeriods":[{"periodo":"2034"},{"periodo":"2035"}],
    "gobiernoDatasets":[
      {"dataset_id":"6v4n-7ahj","limit":50000,"max_pages":1},
      {"dataset_id":"ji8i-4anb","limit":50000,"max_pages":1},
      {"dataset_id":"nkjx-rsq7","limit":50000,"max_pages":1}
    ]
  }' \
  --region "${AWS_REGION}"
```

Devuelve un `executionArn`. Tarda **5–10 minutos** (ingesta + 2 Glue jobs + 2 crawlers). Síguelo en la consola: **Step Functions → data-lake-academico-etl-pipeline → Executions** y haz clic sobre la última.

### 7.3 Tablas en el catálogo Glue

```bash
aws glue get-tables --database-name "data_lake_academico_silver" \
  --region "${AWS_REGION}" --query 'TableList[].Name' --output table

aws glue get-tables --database-name "data_lake_academico_gold" \
  --region "${AWS_REGION}" --query 'TableList[].Name' --output table
```

### 7.4 Primera consulta Athena

Consola: **Athena → Editor → WorkGroup `data-lake-academico-workgroup` → Database `data_lake_academico_gold`** y ejecuta:

```sql
SELECT
    departamento_residencia_norm,
    COUNT(*) AS estudiantes,
    AVG(edad) AS edad_prom,
    AVG(estrato_social) AS estrato_prom,
    SUM(desertion_t1) AS desertores
FROM data_lake_academico_gold.fact_estudiante_periodo
WHERE desertion_t1 IS NOT NULL
GROUP BY departamento_residencia_norm
ORDER BY estudiantes DESC
LIMIT 20;
```

---

## 8. Habilitar el scheduler diario (opcional)

EventBridge se desplegó **DISABLED** para no consumir recursos en desarrollo. Para activarlo (corre el pipeline diariamente a las 7 am Bogotá):

```bash
aws scheduler update-schedule \
  --name "${PROJECT}-daily-trigger" \
  --group-name "${PROJECT}-events-group" \
  --schedule-expression "cron(0 7 * * ? *)" \
  --schedule-expression-timezone "America/Bogota" \
  --state ENABLED \
  --flexible-time-window 'Mode=OFF' \
  --target Arn=${STATE_MACHINE_ARN},RoleArn=arn:aws:iam::${ACCOUNT_ID}:role/service-role/${PROJECT}-events-role \
  --region "${AWS_REGION}"
```

Para desactivarlo: cambia `--state DISABLED`.

---

## 9. Costos esperados

Estimación con la cuenta en Free Tier para `dev`:

| Servicio | Uso típico | Costo estimado |
|---|---|---|
| S3 | ~5 GB | $0 (Free Tier 5 GB) |
| Lambda | ~50 invocaciones/día | $0 (1 M gratis) |
| Glue | ~10 DPU-hora/mes | $0 (10 DPU-hora gratis primer mes) |
| Step Functions | ~30 transiciones/ejecución | $0 (4000 gratis) |
| Athena | <100 MB escaneado | $0 (1 TB casi gratis) |
| KMS | 1 llave | ~$1 USD/mes (sin Free Tier) |
| EventBridge | DISABLED | $0 |
| **Total mes 1** | | **~$1 USD** |

> Recomendación: configura un **AWS Budgets alert** en USD 5 para que te avise si algo se sale de control.

---

## 10. Solución de problemas comunes

### 10.1 `AccessDenied ... explicit deny in a resource-based policy` al subir a S3

**Causa**: la política del bucket exige el header `x-amz-server-side-encryption: AES256` en cada `PutObject`. AWS CLI no lo envía por defecto.

**Fix**:
```bash
aws configure set s3.sse AES256
# O añade el flag a cada upload:
aws s3 cp <archivo> s3://... --sse AES256
```

### 10.2 El caller del CLI es `root` y no `infra_admin`

**Causa**: en `aws configure` configuraste las access keys del root account.

**Fix**:
1. Crea access keys del IAM user `infra_admin` (sección 4.2).
2. Reconfigura: `aws configure` con las nuevas credenciales.
3. Verifica: `aws sts get-caller-identity` debe responder `user/infra_admin`.
4. Borra las access keys del root en **IAM → Configuración de cuenta → Administración del acceso raíz**.

### 10.3 Stack en `ROLLBACK_COMPLETE`, no se puede actualizar

**Causa**: el primer intento de creación falló. CloudFormation no permite `update` sobre un stack en ese estado.

**Fix**: identifica la causa, parchea, borra y redespliega:
```bash
# 1) Encuentra el motivo
aws cloudformation describe-stack-events \
  --stack-name "${PROJECT}" \
  --region "${AWS_REGION}" \
  --query "StackEvents[?ResourceStatus=='CREATE_FAILED'].[LogicalResourceId, ResourceStatusReason]" \
  --output table

# 2) Si requiere editar plantilla hija, hazlo y re-súbela:
aws s3 cp template-X.yaml "s3://${ARTIFACTS_BUCKET}/cfn/template-X.yaml" --sse AES256

# 3) Borra el stack fallido y redespliega
aws cloudformation delete-stack --stack-name "${PROJECT}" --region "${AWS_REGION}"
aws cloudformation wait stack-delete-complete --stack-name "${PROJECT}" --region "${AWS_REGION}"
# Luego repite Fase 4.
```

### 10.4 `Specified ReservedConcurrentExecutions decreases account's UnreservedConcurrentExecution below its minimum value of [10]`

**Causa**: cuotas iniciales de Lambda en cuentas nuevas son de 10. Reservar concurrencia agota el pool.

**Fix**: la plantilla actual ya no reserva concurrencia. Si tu copia local tiene `ReservedConcurrentExecutions`, elimínala de `template-lambdas.yaml` y re-sube la plantilla.

### 10.5 El `for tpl in template-*.yaml` no sube los archivos

**Causa**: el `cwd` no es `src/IaC/` (los `cd -` previos te dejaron en otro lado).

**Fix**: usa paths absolutos o vuelve a `cd "${IAC_DIR}"` antes del bucle. Para empaquetar Lambdas usa subshells `( cd ... && zip ... )` para no mutar el `cwd` del shell padre.

### 10.6 `zip error: Nothing to do!`

**Causa**: el `cwd` donde corrió `zip` no contiene `lambda_function.py`. Aun así el `aws s3 cp` posterior puede subir un ZIP viejo de `/tmp` por error.

**Fix**: borra los ZIPs viejos antes de empaquetar:
```bash
rm -f /tmp/extract_unad.zip /tmp/extract_gobierno.zip
( cd "${LAMBDAS_DIR}/ingestion_unad"     && zip -q -r /tmp/extract_unad.zip     lambda_function.py )
( cd "${LAMBDAS_DIR}/ingestion_gobierno" && zip -q -r /tmp/extract_gobierno.zip lambda_function.py )
unzip -l /tmp/extract_unad.zip      # Confirma que el ZIP no está vacío
unzip -l /tmp/extract_gobierno.zip
```

### 10.7 `BucketAlreadyExists` durante el bootstrap

**Causa**: el nombre `data-lake-academico-artifacts-{accountId}` ya está tomado en otra cuenta o en otra región de la misma cuenta.

**Fix**: cambia `Environment` a otro valor (`dev2`, `qa`, etc.) en la Fase 1 antes de redesplegar.

---

## 11. Limpieza completa

> **Importante**: borra primero los objetos en buckets, luego los stacks. CloudFormation no borra buckets que contengan objetos.

```bash
# 1) Vaciar buckets de datos (incluye versiones, ya que tienen versionado activado)
for b in raw silver gold athena-results artifacts; do
  bucket="${PROJECT}-${ENV}-s3-${b}-${ACCOUNT_ID}"
  echo "Vaciando ${bucket}..."
  aws s3 rm "s3://${bucket}" --recursive --region "${AWS_REGION}" 2>/dev/null
  aws s3api delete-objects --bucket "${bucket}" --region "${AWS_REGION}" \
    --delete "$(aws s3api list-object-versions --bucket "${bucket}" --region "${AWS_REGION}" \
      --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null)" 2>/dev/null || true
  aws s3api delete-objects --bucket "${bucket}" --region "${AWS_REGION}" \
    --delete "$(aws s3api list-object-versions --bucket "${bucket}" --region "${AWS_REGION}" \
      --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' --output json 2>/dev/null)" 2>/dev/null || true
done

# 2) Borrar stacks en orden inverso al de creación (master primero, bootstrap al final)
aws cloudformation delete-stack --stack-name "${PROJECT}" --region "${AWS_REGION}"
aws cloudformation wait stack-delete-complete --stack-name "${PROJECT}" --region "${AWS_REGION}"

aws cloudformation delete-stack --stack-name "${PROJECT}-bootstrap" --region "${AWS_REGION}"
aws cloudformation wait stack-delete-complete --stack-name "${PROJECT}-bootstrap" --region "${AWS_REGION}"

echo "Infraestructura completamente eliminada."
```

> La KMS Key queda en estado `Pending Deletion` por 7 días por seguridad. Durante ese plazo, si quieres redesplegar con el mismo `Environment`, recupera la key con `aws kms cancel-key-deletion`. Si dejas que se elimine, simplemente desplegar de nuevo creará una key nueva.

---

## 12. Apache Superset (visualización transitoria)

Sub-stack opcional que despliega **Apache Superset** sobre una instancia **EC2** para construir dashboards y reportes contra la capa Gold vía Athena. Es una solución transitoria mientras se construye la web app definitiva del trabajo de grado. Costo aproximado: **~$12-15/mes 24/7** (t4g.small + EBS gp3 20 GB + EIP), o **~$5-7/mes** si apagas la instancia fuera de horas de trabajo.

> No se activa por defecto. Se controla con el parámetro `EnableSuperset=true` al desplegar el master stack.

### 12.1 Arquitectura del sub-stack

```
master.yaml (CloudFormation)
   └── SupersetStack [Condition: DeploySuperset]
         ├── IAM::ManagedPolicy        data-lake-academico-superset-policy (Athena+Glue+S3 silver/gold)
         ├── IAM::Role                 data-lake-academico-superset-role (assume EC2; +SSM Session Manager)
         ├── IAM::InstanceProfile      data-lake-academico-superset-profile
         ├── EC2::SecurityGroup        SSH 22 + UI 8088 desde AllowedCidr
         ├── EC2::Instance             ubuntu_22_04 ARM64 (t4g.small) con UserData
         ├── EC2::EIP                  IP elastica estatica
         └── EC2::EIPAssociation       attach EIP a la instance
```

**Por qué EC2 y no Lightsail**: cuentas AWS en *Free Plan* (introducido en 2025) no tienen acceso a Lightsail. EC2 t-class sí está incluido. Adicionalmente, EC2 nos permite usar **IAM Instance Role** en lugar de access keys: el container Docker hereda credenciales temporales del role vía Instance Metadata Service, así no hay secretos en `.env` ni en SecretsManager, ni paso manual de configuración post-deploy.

El UserData de la instancia:
1. Instala Docker + plugin compose
2. Construye una imagen derivada de `apache/superset:<versión>` con `pyathena[pandas,sqlalchemy]==3.7.0`
3. Levanta Superset con SQLite metadata en volumen Docker `superset_home`
4. Inicializa la metadata DB y crea el usuario admin con el password pasado por parámetro
5. Deja `/opt/superset/.env` solo con `AWS_DEFAULT_REGION` y vars Superset (sin keys; las saca del IMDS)

### 12.2 Permisos IAM del rol `data-lake-academico-superset-role`

| Servicio | Acciones permitidas | Recursos |
|---|---|---|
| Athena | `StartQueryExecution`, `Get*`, `Stop*`, `BatchGet*` | WorkGroup `data-lake-academico-workgroup` + `datacatalog/AwsDataCatalog` |
| Glue Catalog | `Get*`, `BatchGetPartition`, `SearchTables` (solo lectura) | DBs `data_lake_academico_silver` y `_gold` + sus tablas |
| S3 Silver/Gold | `GetObject`, `ListBucket` (solo lectura) | `silver-${acc}/*` y `gold-${acc}/*` |
| S3 Athena Results | `GetObject`, `PutObject`, `Abort/ListMultipart`, `ListBucket` | `athena-results-${acc}/*` |
| KMS | `Decrypt`, `GenerateDataKey`, `DescribeKey` | KMS Key del proyecto |
| SSM Session Manager | (managed) `AmazonSSMManagedInstanceCore` | global (necesario para SSM) |

**Sin acceso a Bronze/Raw** — el rol no puede leer microdatos UNAD originales, solo las tablas Silver/Gold ya pseudonimizadas.

### 12.3 Despliegue

**Pre-requisitos**:
- Tu IP pública (solo esa IP podrá llegar al puerto 8088 y SSH).
- VPC + Subnet pública donde lanzar la EC2 (típicamente la default VPC de la cuenta).
- (Opcional) Un EC2 KeyPair existente si quieres SSH; sin él, usas SSM Session Manager.

```bash
# 1) Obtener tu IP pública
export MY_IP=$(curl -s https://checkip.amazonaws.com)
echo "Mi IP: ${MY_IP}/32"

# 2) Identificar VPC default y una subnet pública
export SUPERSET_VPC_ID=$(aws ec2 describe-vpcs \
  --filters Name=is-default,Values=true \
  --query 'Vpcs[0].VpcId' --output text --region "${AWS_REGION}")
export SUPERSET_SUBNET_ID=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=${SUPERSET_VPC_ID}" "Name=default-for-az,Values=true" \
  --query 'Subnets[0].SubnetId' --output text --region "${AWS_REGION}")
echo "VPC: ${SUPERSET_VPC_ID}  Subnet: ${SUPERSET_SUBNET_ID}"

# 3) (Opcional) Crear un KeyPair si quieres SSH; saltar si solo usarás SSM
#    aws ec2 create-key-pair --key-name "${PROJECT}-superset-key" --region "${AWS_REGION}" \
#      --query 'KeyMaterial' --output text > "${PROJECT}-superset-key.pem"
#    chmod 600 "${PROJECT}-superset-key.pem"
export SUPERSET_KEYPAIR=""    # vacío = sin SSH, solo SSM Session Manager

# 4) Definir un password fuerte para el admin Superset
#    (12-64 chars; alfanum + ! @ # % ^ & * ( ) _ + = - [ ] { } < > . , ? / : ; | ~)
printf 'Superset admin password: '
read -s SUPERSET_ADMIN_PASS
echo
export SUPERSET_ADMIN_PASS

# 5) Subir templates al bucket de artefactos
aws s3 cp "${IAC_DIR}/template-superset.yaml" \
  "s3://${ARTIFACTS_BUCKET}/cfn/template-superset.yaml" --sse AES256
aws s3 cp "${IAC_DIR}/master.yaml" \
  "s3://${ARTIFACTS_BUCKET}/cfn/master.yaml" --sse AES256

# 6) Deploy con EnableSuperset=true
aws cloudformation deploy \
  --stack-name "${PROJECT}" \
  --template-file "${IAC_DIR}/master.yaml" \
  --parameter-overrides \
      ProjectName="${PROJECT}" \
      Environment="${ENV}" \
      ArtifactsBucketName="${ARTIFACTS_BUCKET}" \
      ArtifactsBucketUrl="https://${ARTIFACTS_BUCKET}.s3.${AWS_REGION}.amazonaws.com/cfn" \
      EnableSuperset=true \
      SupersetVpcId="${SUPERSET_VPC_ID}" \
      SupersetSubnetId="${SUPERSET_SUBNET_ID}" \
      SupersetSshKeyPairName="${SUPERSET_KEYPAIR}" \
      SupersetAllowedCidr="${MY_IP}/32" \
      SupersetAdminPassword="${SUPERSET_ADMIN_PASS}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "${AWS_REGION}" \
  --no-fail-on-empty-changeset
```

CloudFormation reporta `CREATE_COMPLETE` en ~3-5 min (recursos AWS). El UserData tarda **~5-8 min adicionales** descargando Docker e imágenes; el endpoint web no responde hasta que termine.

### 12.4 Verificar que el bootstrap terminó

```bash
# 1) Obtener la IP pública
export SUPERSET_IP=$(aws cloudformation describe-stacks \
  --stack-name "${PROJECT}" --region "${AWS_REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='SupersetPublicIp'].OutputValue" --output text)
echo "Superset IP: ${SUPERSET_IP}"

# 2) Obtener el Instance ID
export SUPERSET_INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Project,Values=${PROJECT}" "Name=tag:Name,Values=${PROJECT}-superset" \
            "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].InstanceId' --output text --region "${AWS_REGION}")
echo "Instance ID: ${SUPERSET_INSTANCE_ID}"

# 3) Verificar el archivo marker via SSM Session Manager (sin SSH)
aws ssm start-session --target "${SUPERSET_INSTANCE_ID}" --region "${AWS_REGION}"
# Dentro de la sesión:
sudo ls -la /var/log/superset-bootstrap-done   # si existe -> bootstrap OK
sudo tail -20 /var/log/user-data.log           # ver progreso del UserData
exit
```

> Para usar SSM Session Manager localmente necesitas el plugin: `brew install --cask session-manager-plugin`.

### 12.5 Acceso a la UI

```
http://<SUPERSET_IP>:8088
Usuario: admin
Password: el que pasaste en SupersetAdminPassword
```

### 12.6 Acceso al servidor de Superset (administración por EC2)

Esta sección documenta cómo conectarse a la instancia EC2 que hospeda Superset, inspeccionar el container Docker, ver logs y operar el servicio. La forma recomendada de acceso es **AWS SSM Session Manager** (no requiere SSH ni KeyPair); SSH es opcional si proporcionaste `SupersetSshKeyPairName` al deploy.

#### 12.6.1 Pre-requisitos en tu máquina local

```bash
# Plugin SSM Session Manager para AWS CLI (instalación una sola vez)
brew install --cask session-manager-plugin

# Verificar
session-manager-plugin
# Debe imprimir el banner de uso/version
```

Si no usas Homebrew, descarga el `.pkg` oficial desde la documentación de AWS Systems Manager.

#### 12.6.2 Obtener IDs de la instancia y la IP pública

```bash
# Instance ID
export SUPERSET_INSTANCE_ID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=${PROJECT}-superset" \
            "Name=instance-state-name,Values=running" \
  --query 'Reservations[0].Instances[0].InstanceId' \
  --output text --region "${AWS_REGION}")

# IP pública (Elastic IP attached)
export SUPERSET_IP=$(aws cloudformation describe-stacks \
  --stack-name "${PROJECT}" --region "${AWS_REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='SupersetPublicIp'].OutputValue" \
  --output text)

echo "Instance: ${SUPERSET_INSTANCE_ID}"
echo "IP:       ${SUPERSET_IP}"
echo "URL UI:   http://${SUPERSET_IP}:8088"
```

#### 12.6.3 Conexión interactiva por SSM Session Manager

```bash
aws ssm start-session --target "${SUPERSET_INSTANCE_ID}" --region "${AWS_REGION}"
```

Esto abre una shell `sh-5.1$` en la instancia como user `ssm-user`. Para volverte a un shell de Ubuntu con `sudo` disponible:

```bash
sudo bash      # ahora corres como root con bash
# o:
sudo -u ubuntu -i   # cambia al usuario ubuntu (default de la AMI)
```

Salir: `exit` (cierra la sesión SSM).

#### 12.6.4 Comando único sin sesión interactiva (`send-command`)

Útil para automatización o cuando no quieres abrir shell interactiva:

```bash
CMD_ID=$(aws ssm send-command \
  --instance-ids "${SUPERSET_INSTANCE_ID}" \
  --document-name "AWS-RunShellScript" \
  --parameters 'commands=["docker ps","docker logs superset --tail 20"]' \
  --region "${AWS_REGION}" \
  --query 'Command.CommandId' --output text)

sleep 3
aws ssm get-command-invocation \
  --command-id "${CMD_ID}" \
  --instance-id "${SUPERSET_INSTANCE_ID}" \
  --region "${AWS_REGION}" \
  --query 'StandardOutputContent' --output text
```

#### 12.6.5 Conexión por SSH (opcional, requiere KeyPair)

Solo aplica si pasaste `SupersetSshKeyPairName=<tu-keypair>` al deploy y descargaste el `.pem`:

```bash
chmod 600 ~/.ssh/${PROJECT}-superset-key.pem
ssh -i ~/.ssh/${PROJECT}-superset-key.pem ubuntu@${SUPERSET_IP}
```

**Tunnel SSH para acceder a la UI sin abrir 8088 al mundo** (útil cuando estás en una red distinta a la del SG):

```bash
ssh -L 8088:localhost:8088 -i ~/.ssh/${PROJECT}-superset-key.pem ubuntu@${SUPERSET_IP}
# Mantén la sesión abierta y abre en tu navegador http://localhost:8088
```

#### 12.6.6 Comandos Docker útiles dentro de la instancia

Una vez dentro vía SSM o SSH, todos los comandos asumen `cd /opt/superset` o el path absoluto al `docker-compose.yml`.

| Acción | Comando |
|---|---|
| Estado del container | `sudo docker ps` |
| Estado por compose | `sudo docker compose -f /opt/superset/docker-compose.yml ps` |
| Logs últimos 100 | `sudo docker logs superset --tail 100` |
| Logs en streaming | `sudo docker logs superset -f` |
| Filtrar errores | `sudo docker logs superset --tail 500 2>&1 \| grep -iE "error\|exception"` |
| Reiniciar Superset | `sudo docker compose -f /opt/superset/docker-compose.yml restart superset` |
| Detener | `sudo docker compose -f /opt/superset/docker-compose.yml stop` |
| Iniciar | `sudo docker compose -f /opt/superset/docker-compose.yml up -d` |
| Recrear (tras cambio en `.env`) | `sudo docker compose -f /opt/superset/docker-compose.yml up -d --force-recreate` |
| Consumo recursos | `sudo docker stats --no-stream` |
| Espacio en disco | `df -h /` y `sudo du -sh /var/lib/docker` |
| RAM y swap | `free -h` |
| Verificar marker bootstrap | `sudo ls -la /var/log/superset-bootstrap-done` |
| Log completo del UserData | `sudo less /var/log/user-data.log` |

#### 12.6.7 Comandos administrativos de Superset (CLI Flask App Builder)

```bash
# Listar usuarios registrados
sudo docker compose -f /opt/superset/docker-compose.yml exec superset \
  superset fab list-users

# Crear un usuario adicional (rol Admin/Alpha/Gamma/Public)
sudo docker compose -f /opt/superset/docker-compose.yml exec superset \
  superset fab create-user --role Admin --username analista \
    --firstname Analista --lastname Datos \
    --email analista@datalake.local --password '<password-fuerte>'

# Resetear el password del admin (si lo olvidaste)
sudo docker compose -f /opt/superset/docker-compose.yml exec superset \
  superset fab reset-password --username admin --password '<nuevo-password>'

# Versión de Superset corriendo
sudo docker compose -f /opt/superset/docker-compose.yml exec superset \
  superset --version
```

#### 12.6.8 Backup y restore de la metadata DB (SQLite)

Los dashboards, charts y conexiones viven en `/app/superset_home/superset.db` dentro del container, sobre el volumen Docker `superset_superset_home` (persiste en EBS).

```bash
# 1) BACKUP — copiar el SQLite al host y descargarlo a tu Mac
sudo docker cp superset:/app/superset_home/superset.db \
  /tmp/superset_$(date +%Y%m%d).db

# (Salir de la sesión SSM y descargar via SSM también)
exit
aws ssm start-session --target "${SUPERSET_INSTANCE_ID}" --region "${AWS_REGION}" \
  --document-name AWS-StartPortForwardingSession  # avanzado, ver doc AWS
# Más simple: usar `aws s3 cp` desde la instance al bucket de artefactos:
sudo aws s3 cp /tmp/superset_$(date +%Y%m%d).db \
  s3://${ARTIFACTS_BUCKET}/backups/superset/

# 2) RESTORE — pasar el archivo a la instance y copiarlo dentro del container
sudo aws s3 cp s3://${ARTIFACTS_BUCKET}/backups/superset/superset_YYYYMMDD.db /tmp/
sudo docker cp /tmp/superset_YYYYMMDD.db superset:/app/superset_home/superset.db
sudo docker compose -f /opt/superset/docker-compose.yml restart superset
```

#### 12.6.9 Editar configuración del container

Si necesitas cambiar variables de entorno (ej. activar feature flags de Superset):

```bash
sudo nano /opt/superset/.env
# Aplicar cambios:
sudo docker compose -f /opt/superset/docker-compose.yml up -d --force-recreate
```

Si necesitas cambiar la imagen Docker o agregar drivers extras:

```bash
sudo nano /opt/superset/Dockerfile
# Por ejemplo, agregar otro driver SQLAlchemy:
# RUN pip install --no-cache-dir 'redshift-connector==2.x' 'sqlalchemy-redshift==0.x'

sudo docker compose -f /opt/superset/docker-compose.yml build
sudo docker compose -f /opt/superset/docker-compose.yml up -d
```

#### 12.6.10 Actualizar la IP del operador en el Security Group

Si tu IP pública cambió (red distinta, ISP), no podrás llegar al puerto 8088. Hay dos formas:

**Opción A — Re-deploy del CFN** (preferida, queda registrado en IaC):

```bash
export NEW_IP=$(curl -s https://checkip.amazonaws.com)
# Re-corre el comando completo de aws cloudformation deploy de §12.3
# pero con SupersetAllowedCidr="${NEW_IP}/32"
```

**Opción B — Quick fix manual con AWS CLI** (no queda en IaC, mejor solo para emergencias):

```bash
SG_ID=$(aws ec2 describe-security-groups \
  --filters "Name=tag:Project,Values=${PROJECT}" \
            "Name=group-name,Values=${PROJECT}-superset-sg" \
  --query 'SecurityGroups[0].GroupId' --output text --region "${AWS_REGION}")

# Quitar la regla vieja (sustituye OLD_IP)
aws ec2 revoke-security-group-ingress --group-id "${SG_ID}" \
  --protocol tcp --port 22 --cidr OLD_IP/32 --region "${AWS_REGION}"
aws ec2 revoke-security-group-ingress --group-id "${SG_ID}" \
  --protocol tcp --port 8088 --cidr OLD_IP/32 --region "${AWS_REGION}"

# Agregar la nueva
NEW_IP=$(curl -s https://checkip.amazonaws.com)
aws ec2 authorize-security-group-ingress --group-id "${SG_ID}" \
  --protocol tcp --port 22 --cidr "${NEW_IP}/32" --region "${AWS_REGION}"
aws ec2 authorize-security-group-ingress --group-id "${SG_ID}" \
  --protocol tcp --port 8088 --cidr "${NEW_IP}/32" --region "${AWS_REGION}"
```

#### 12.6.11 Diagnóstico rápido cuando "no carga"

| Síntoma | Comando de diagnóstico | Causa típica |
|---|---|---|
| Browser timeout en `http://IP:8088` | `aws ec2 describe-instances --instance-ids ${SUPERSET_INSTANCE_ID} --query 'Reservations[0].Instances[0].State.Name'` | Instance stopped (start-instances) |
| Browser "Connection refused" | SSM → `sudo docker ps` | Container detenido (compose up -d) |
| "Connection reset" | SSM → `sudo docker logs superset --tail 50` | Bootstrap aún en curso, o crash interno |
| HTTP 200 pero error en SQL Lab | SSM → `sudo docker logs superset \| grep -iE "denied\|forbidden"` | Falta permiso IAM (ampliar managed policy) |
| Dashboards/usuarios desaparecieron | SSM → `sudo docker volume ls` | Volumen Docker borrado (restore desde backup) |
| Cambió tu IP pública | `curl -s https://checkip.amazonaws.com` vs SG existente | Actualizar SG (sección 12.6.10) |

### 12.7 Conexión a Athena dentro de Superset

Una vez logueado, ir a **Settings → Database Connections → + Database**, elegir **Amazon Athena** (no "Amazon Athena (legacy)") y usar este SQLAlchemy URI:

```
awsathena+rest://@athena.us-east-1.amazonaws.com/default?s3_staging_dir=s3%3A%2F%2Fdata-lake-academico-athena-results-<ACCOUNT_ID>%2Fqueries%2F&work_group=data-lake-academico-workgroup
```

- **Display Name**: `DataLake (Athena)`.
- Reemplaza `<ACCOUNT_ID>` por tu account real (ej. `462035739083`).
- El path `/default` no fija un schema único — permite que SQL Lab y Datasets descubran ambos `data_lake_academico_silver` y `data_lake_academico_gold` en el dropdown.
- **No lleva access keys** — PyAthena/boto3 detectan que están corriendo en EC2 y obtienen credenciales temporales del **Instance Metadata Service** (IMDSv2) usando el rol `data-lake-academico-superset-role` attached a la instancia. Click **Test Connection** → debe responder OK.

> Nota: si vas a SQL Lab y el dropdown SCHEMA aparece vacío con error "There was an error loading the schemas", es que el resource scope de la managed policy no permite enumerar databases. El template ya usa `datacatalog/*` para los actions de discovery (`athena:ListDatabases`, etc.), así que esto solo aplicaría si lo modificaste manualmente.

### 12.8 Reporte básico de validación (queries de ejemplo)

Una vez configurada la conexión Athena, este flujo construye un dashboard mínimo que valida end-to-end el data lake. Demuestra el cruce UNAD ↔ datos abiertos del gobierno y se puede ampliar con más charts.

**Flujo recomendado**: usar **SQL Lab → CREATE CHART** para que cada query se vuelva un *dataset virtual* directo, sin tener que crear datasets persistentes. Más rápido para reportes one-shot.

#### Chart 1 — Tabla "Cobertura socioeconómica por periodo"

En **SQL → SQL Lab** (database `DataLake (Athena)`, schema `data_lake_academico_gold`):

```sql
SELECT 
    periodo_codigo,
    COUNT(id) as total_estudiantes,
    COUNT(tasa_informalidad_mpio) as estudiantes_con_datos_sisben,
    ROUND(AVG(promedio_ipm_mpio), 2) as indice_pobreza_promedio,
    COUNT(desertion_t1) as registros_con_target_desercion
FROM "data_lake_academico_gold"."fact_estudiante_periodo"
GROUP BY periodo_codigo
ORDER BY periodo_codigo;
```

- **RUN** → cuando aparezcan resultados, **CREATE CHART**.
- **Choose chart type**: `Table`.
- **QUERY MODE**: `Raw Records`.
- **COLUMNS**: las 5 columnas en orden.
- **ORDERING**: `periodo_codigo` ASC, **ROW LIMIT** 50.
- **CREATE CHART** → **SAVE** con nombre `Cobertura socioeconómica por periodo`.

#### Chart 2 — Bar Chart "Top 5 municipios por cantidad de estudiantes"

Nueva pestaña en SQL Lab, mismo database/schema:

```sql
SELECT 
    municipio_residencia,
    COUNT(id) as numero_estudiantes,
    ROUND(AVG(tasa_informalidad_mpio), 4) as informalidad_promedio
FROM "data_lake_academico_gold"."fact_estudiante_periodo"
WHERE codigo_divipola IS NOT NULL
GROUP BY municipio_residencia
ORDER BY numero_estudiantes DESC
LIMIT 5;
```

- **CREATE CHART** → tipo `Bar Chart` (o `Mixed Chart` si quieres una segunda métrica con eje secundario).
- **QUERY MODE**: `Raw Records`. **X-AXIS**: `municipio_residencia`. **METRICS**: `numero_estudiantes`.
- **SORT BY METRIC**: `numero_estudiantes` Descending. **ROW LIMIT**: 5.
- **CREATE CHART** → **SAVE** con nombre `Top 5 municipios por cantidad de estudiantes`.

#### Combinar en un dashboard

- **Dashboards → + Dashboard** → título: `Reporte básico — Cobertura DataLake UNAD`.
- Panel derecho **CHARTS** → arrastra ambos charts al canvas → redimensiona → **SAVE**.

**Resultado esperado** (validado en deploy real):

| Chart | Filas | Insight |
|---|---|---|
| Cobertura socioeconómica | 10 periodos (2024 I/II/III, 2025 I/II/III, …) | ~62% de los estudiantes tienen datos Sisbén cruzados, IPM promedio ≈ 0.32 |
| Top 5 municipios | Valledupar, Florencia, Cali, Sogamoso, Soacha | Concentración geográfica con tasas de informalidad entre 64-86% |

> Si quieres filtros interactivos (`periodo`, `escuela`) que afecten múltiples charts a la vez, hay que migrar de "datasets virtuales de SQL Lab" a **datasets persistentes** sobre `fact_estudiante_periodo`. Esto se hace en **Datasets → + Dataset** seleccionando la tabla, y luego construyendo los charts con métricas/dimensiones del dataset (no SQL custom). Los filtros del dashboard solo descubren datasets persistentes.

### 12.9 Apagar / prender / destruir (control de costos)

EC2 factura por hora corrida. Tienes tres palancas:

```bash
# OPCIÓN A: Apagar la instancia (mantiene EBS + EIP, factura solo storage ~$1.6/mes)
aws ec2 stop-instances --instance-ids "${SUPERSET_INSTANCE_ID}" --region "${AWS_REGION}"

# Reanudar
aws ec2 start-instances --instance-ids "${SUPERSET_INSTANCE_ID}" --region "${AWS_REGION}"
# Nota: la EIP se mantiene attached aunque la instance esté stopped, pero AWS
# cobra ~$3.6/mes por una EIP no asociada a una instance running. Si vas a
# tener la instance stopped por más de 1 hora, considera la opción B.

# OPCIÓN B: Destruir todo el sub-stack (ahorra 100% del costo Superset)
aws cloudformation deploy \
  --stack-name "${PROJECT}" \
  --template-file "${IAC_DIR}/master.yaml" \
  --parameter-overrides \
      ProjectName="${PROJECT}" \
      Environment="${ENV}" \
      ArtifactsBucketName="${ARTIFACTS_BUCKET}" \
      ArtifactsBucketUrl="https://${ARTIFACTS_BUCKET}.s3.${AWS_REGION}.amazonaws.com/cfn" \
      EnableSuperset=false \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "${AWS_REGION}"
# Destruye instance + EIP + SG + IAM role + ManagedPolicy. Pierdes los
# dashboards (están en EBS que se borra). Para preservarlos, ver opción C.

# OPCIÓN C: Snapshot del EBS antes de destruir
EBS_VOLUME_ID=$(aws ec2 describe-instances --instance-ids "${SUPERSET_INSTANCE_ID}" \
  --query 'Reservations[0].Instances[0].BlockDeviceMappings[0].Ebs.VolumeId' \
  --output text --region "${AWS_REGION}")
aws ec2 create-snapshot --volume-id "${EBS_VOLUME_ID}" \
  --description "${PROJECT}-superset $(date +%Y%m%d)" \
  --tag-specifications "ResourceType=snapshot,Tags=[{Key=Project,Value=${PROJECT}}]" \
  --region "${AWS_REGION}"
# Después corre la opción B. El snapshot persiste (~$0.05/GB/mes).
```

### 12.10 Limitaciones conocidas

- Single-container con **SQLite metadata**: sin alertas async, sin scheduled exports, sin Celery worker. Para 1-5 users concurrentes alcanza. Si crece, migrar a `docker-compose-non-dev.yml` oficial (Postgres + Redis + worker) y subir a `t4g.medium` (4 GB).
- **Sin HTTPS** por defecto: tráfico al puerto 8088 va en plaintext. Para defensa de tesis, agregar Caddy + dominio con Let's Encrypt, o usar SSH tunnel local (`ssh -L 8088:localhost:8088 ubuntu@${SUPERSET_IP}`).
- **Persistencia**: los dashboards viven en SQLite dentro del volumen Docker `superset_home` sobre EBS gp3. Backups vía snapshot EBS (opción C).
- **UserData no soporta `cfn-signal`** sin recursos extras: CloudFormation reporta CREATE_COMPLETE antes de que el container esté operativo. Verificar `/var/log/superset-bootstrap-done` antes de intentar abrir la UI.
- **Acceso a IMDS desde el container**: requiere `HttpPutResponseHopLimit: 2` en `MetadataOptions` (ya configurado en el template). Sin esto, PyAthena no obtiene credenciales del role.

---

## 13. Referencias

- **Documento de tesis**: ``
- **Diagrama de arquitectura**: ``
- **Fuentes de datos públicas**:
  - Datos Abiertos Colombia: https://www.datos.gov.co
  - SPADIES (MEN): https://spadies.mineducacion.gov.co
  - SNIES (MEN): https://hecaa.mineducacion.gov.co/consultaspublicas/
  - Portal Datos UNAD: https://datos.unad.edu.co
- **Marco normativo**:
  - Ley 1581 de 2012 — Protección de datos personales
  - Ley 1712 de 2014 — Transparencia y acceso a la información pública
  - CONPES 3920 — Política Nacional de Explotación de Datos
- **Documentación AWS clave**:
  - CloudFormation Nested Stacks: https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/using-cfn-nested-stacks.html
  - Glue PySpark: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python.html
  - Step Functions ASL: https://docs.aws.amazon.com/step-functions/latest/dg/concepts-amazon-states-language.html
