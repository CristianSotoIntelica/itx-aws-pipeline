#!/bin/bash
set -e
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
ok()   { echo -e "${GREEN}  OK${NC} $1"; }
warn() { echo -e "${YELLOW}  WARN${NC} $1"; }
info() { echo -e "${BLUE}  -->${NC} $1"; }
fail() { echo -e "${RED}  ERROR${NC} $1"; exit 1; }

REGION=${AWS_REGION:-"us-east-1"}
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ENV=${ENVIRONMENT:-"dev"}

# Tags corporativos requeridos por Intelica IT (Hildebrando Nunez)
TAG_ENV="Environment=dev"
TAG_PROJECT="Project=datalake-itx"
TAGS_IAM="Key=Environment,Value=dev Key=Project,Value=datalake-itx"
TAGS_LAMBDA="Environment=dev,Project=datalake-itx"
TAGS_JSON='[{"Key":"Environment","Value":"dev"},{"Key":"Project","Value":"datalake-itx"}]'
TAGS_GLUE='{"Environment":"dev","Project":"datalake-itx"}'

S3_LANDING="itx-landing-${ENV}"
S3_STAGING="itx-staging-${ENV}"
S3_OPERATIONAL="itx-operational-${ENV}"
S3_ARCHIVE="itx-archive-${ENV}"
S3_REFERENCE="itx-reference-${ENV}"
LAYER_NAME="itx-pandas-pyarrow"
SF_NAME="itx-main-orchestrator"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=============================================="
echo " ITX AWS Pipeline - Deploy"
echo " Cuenta:   $ACCOUNT_ID"
echo " Region:   $REGION"
echo " Ambiente: $ENV"
echo " Tags:     $TAG_ENV | $TAG_PROJECT"
echo "=============================================="

echo -e "\n${BLUE}[1/8] Creando S3 Buckets...${NC}"
for bucket in $S3_LANDING $S3_STAGING $S3_OPERATIONAL $S3_ARCHIVE $S3_REFERENCE; do
  if aws s3api head-bucket --bucket $bucket 2>/dev/null; then
    warn "$bucket ya existe"
  else
    if [ "$REGION" = "us-east-1" ]; then
      aws s3api create-bucket --bucket $bucket --region $REGION
    else
      aws s3api create-bucket --bucket $bucket --region $REGION \
        --create-bucket-configuration LocationConstraint=$REGION
    fi
    aws s3api put-bucket-tagging --bucket $bucket \
      --tagging "TagSet=[{Key=Environment,Value=dev},{Key=Project,Value=datalake-itx}]"
    ok "$bucket creado y taggeado"
  fi
done
aws s3 cp $ROOT_DIR/glue/scripts/calculate.py s3://$S3_STAGING/scripts/calculate.py
aws s3 cp $ROOT_DIR/glue/scripts/interchange.py s3://$S3_STAGING/scripts/interchange.py
ok "Scripts Glue subidos"

echo -e "\n${BLUE}[2/8] Creando IAM Roles...${NC}"
create_role() {
  local ROLE_NAME=$1; local SERVICE=$2
  if aws iam get-role --role-name $ROLE_NAME 2>/dev/null; then
    warn "$ROLE_NAME ya existe"
  else
    TRUST="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"${SERVICE}\"},\"Action\":\"sts:AssumeRole\"}]}"
    aws iam create-role --role-name $ROLE_NAME --assume-role-policy-document "$TRUST" \
      --tags $TAGS_IAM > /dev/null
    ok "$ROLE_NAME creado"
  fi
}
create_role "itx-lambda-router-role"          "lambda.amazonaws.com"
create_role "itx-lambda-transform-role"       "lambda.amazonaws.com"
create_role "itx-lambda-extract-role"         "lambda.amazonaws.com"
create_role "itx-lambda-clean-role"           "lambda.amazonaws.com"
create_role "itx-lambda-store-role"           "lambda.amazonaws.com"
create_role "itx-lambda-archive-role"         "lambda.amazonaws.com"
create_role "itx-stepfunctions-role"          "states.amazonaws.com"
create_role "itx-glue-calculate-role"         "glue.amazonaws.com"
create_role "itx-glue-interchange-role"       "glue.amazonaws.com"
create_role "itx-glue-crawler-reference-role" "glue.amazonaws.com"
create_role "itx-glue-crawler-ebgr-role"      "glue.amazonaws.com"
for role in itx-lambda-router-role itx-lambda-transform-role itx-lambda-extract-role itx-lambda-clean-role itx-lambda-store-role itx-lambda-archive-role; do
  aws iam attach-role-policy --role-name $role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole 2>/dev/null
  aws iam attach-role-policy --role-name $role --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess 2>/dev/null
  aws iam attach-role-policy --role-name $role --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess 2>/dev/null
done
for role in itx-glue-calculate-role itx-glue-interchange-role itx-glue-crawler-reference-role itx-glue-crawler-ebgr-role; do
  aws iam attach-role-policy --role-name $role --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole 2>/dev/null
  aws iam attach-role-policy --role-name $role --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess 2>/dev/null
done
aws iam attach-role-policy --role-name itx-stepfunctions-role --policy-arn arn:aws:iam::aws:policy/AWSLambda_FullAccess 2>/dev/null
ok "Roles y politicas configurados"
info "Esperando propagacion IAM..."
sleep 10

echo -e "\n${BLUE}[3/8] Publicando Lambda Layer...${NC}"
LAYER_FILE="$ROOT_DIR/layers/itx-pandas-pyarrow/layer.zip"
[ -f "$LAYER_FILE" ] || fail "No se encontro layer.zip en $LAYER_FILE"
LAYER_ARN=$(aws lambda publish-layer-version \
  --layer-name $LAYER_NAME \
  --description "pandas y pyarrow para ITX" \
  --zip-file fileb://$LAYER_FILE \
  --compatible-runtimes python3.11 \
  --region $REGION \
  --query "LayerVersionArn" --output text)
ok "Layer publicado: $LAYER_ARN"

echo -e "\n${BLUE}[4/8] Creando tablas DynamoDB...${NC}"
create_table() {
  local TABLE=$1; local KEY=$2; local ATTR=$3
  if aws dynamodb describe-table --table-name $TABLE --region $REGION 2>/dev/null; then
    warn "$TABLE ya existe"
  else
    aws dynamodb create-table --table-name $TABLE \
      --attribute-definitions $ATTR --key-schema $KEY \
      --billing-mode PAY_PER_REQUEST \
      --tags $TAGS_IAM \
      --region $REGION > /dev/null
    ok "$TABLE creada"
  fi
}
create_table "itx-file-control"  "AttributeName=file_id,KeyType=HASH"    "AttributeName=file_id,AttributeType=S"
create_table "itx-file-pattern"  "AttributeName=pattern_id,KeyType=HASH" "AttributeName=pattern_id,AttributeType=S"
create_table "itx-visa-fields"   "AttributeName=field_id,KeyType=HASH"   "AttributeName=field_id,AttributeType=S"
create_table "itx-client"        "AttributeName=client_id,KeyType=HASH"  "AttributeName=client_id,AttributeType=S"
for table in itx-file-control itx-file-pattern itx-visa-fields itx-client; do
  aws dynamodb wait table-exists --table-name $table --region $REGION
  ok "$table activa"
done

echo -e "\n${BLUE}[5/8] Desplegando Lambdas...${NC}"
deploy_lambda() {
  local FN=$1; local ROLE=$2; local HANDLER=$3; local TIMEOUT=$4; local MEMORY=$5
  local TMP_ZIP="/tmp/${FN}.zip"
  cd $ROOT_DIR/lambdas/${FN}/src && zip -q -r $TMP_ZIP . && cd $ROOT_DIR
  ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE}"
  if aws lambda get-function --function-name $FN --region $REGION 2>/dev/null; then
    aws lambda update-function-code --function-name $FN \
      --zip-file fileb://$TMP_ZIP --region $REGION > /dev/null
    warn "$FN actualizado"
  else
    aws lambda create-function --function-name $FN \
      --runtime python3.11 --role $ROLE_ARN --handler $HANDLER \
      --zip-file fileb://$TMP_ZIP --timeout $TIMEOUT --memory-size $MEMORY \
      --layers $LAYER_ARN \
      --tags "$TAGS_LAMBDA" \
      --region $REGION > /dev/null
    ok "$FN creado"
  fi
}
deploy_lambda "itx-router"       "itx-lambda-router-role"    "handler.lambda_handler" 60  256
deploy_lambda "itx-transform"    "itx-lambda-transform-role" "handler.lambda_handler" 900 3008
deploy_lambda "itx-extract"      "itx-lambda-extract-role"   "handler.lambda_handler" 900 3008
deploy_lambda "itx-clean"        "itx-lambda-clean-role"     "handler.lambda_handler" 900 3008
deploy_lambda "itx-archive-file" "itx-lambda-archive-role"   "handler.lambda_handler" 60  256
warn "itx-store omitido - pendiente de implementacion"

info "Configurando variables de entorno..."
aws lambda update-function-configuration --function-name itx-router --region $REGION \
  --environment "Variables={STEP_FUNCTION_ARN=arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${SF_NAME},DYNAMODB_TABLE_FILE_CONTROL=itx-file-control,DYNAMODB_TABLE_FILE_PATTERN=itx-file-pattern,S3_BUCKET_LANDING=${S3_LANDING}}" > /dev/null
ok "itx-router vars OK"
aws lambda update-function-configuration --function-name itx-transform --region $REGION \
  --environment "Variables={S3_BUCKET_LANDING=${S3_LANDING},S3_BUCKET_STAGING=${S3_STAGING},CHUNK_SIZE_MB=16,FLUSH_BATCH_SIZE=200000}" > /dev/null
ok "itx-transform vars OK"
aws lambda update-function-configuration --function-name itx-extract --region $REGION \
  --environment "Variables={S3_BUCKET_STAGING=${S3_STAGING},DYNAMODB_FIELD_DEFINITION=itx-visa-fields,EXTRACT_CHUNK_SIZE=100000}" > /dev/null
ok "itx-extract vars OK"
aws lambda update-function-configuration --function-name itx-clean --region $REGION \
  --environment "Variables={S3_BUCKET_STAGING=${S3_STAGING},DYNAMODB_FIELD_DEFINITION=itx-visa-fields,CLEAN_CHUNK_SIZE=100000}" > /dev/null
ok "itx-clean vars OK"
aws lambda update-function-configuration --function-name itx-archive-file --region $REGION \
  --environment "Variables={S3_BUCKET_LANDING=${S3_LANDING},S3_BUCKET_ARCHIVE=${S3_ARCHIVE}}" > /dev/null
ok "itx-archive-file vars OK"

echo -e "\n${BLUE}[6/8] Configurando S3 Event trigger...${NC}"
ROUTER_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:itx-router"
aws lambda add-permission --function-name itx-router --statement-id s3-trigger \
  --action lambda:InvokeFunction --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::${S3_LANDING} --source-account $ACCOUNT_ID \
  --region $REGION 2>/dev/null || warn "Permiso S3 ya existe"
aws s3api put-bucket-notification-configuration --bucket $S3_LANDING \
  --notification-configuration "{\"LambdaFunctionConfigurations\":[{\"Id\":\"TriggerRouterOnUpload\",\"LambdaFunctionArn\":\"${ROUTER_ARN}\",\"Events\":[\"s3:ObjectCreated:*\"]}]}"
ok "S3 trigger: $S3_LANDING -> itx-router"

echo -e "\n${BLUE}[7/8] Desplegando Step Functions...${NC}"
SF_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/itx-stepfunctions-role"
ASL=$(cat $ROOT_DIR/step-functions/asl.json)
SF_ARN="arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:${SF_NAME}"
if aws stepfunctions describe-state-machine --state-machine-arn $SF_ARN --region $REGION 2>/dev/null; then
  aws stepfunctions update-state-machine --state-machine-arn $SF_ARN \
    --definition "$ASL" --role-arn $SF_ROLE_ARN --region $REGION > /dev/null
  warn "$SF_NAME actualizado"
else
  aws stepfunctions create-state-machine --name $SF_NAME \
    --definition "$ASL" --role-arn $SF_ROLE_ARN \
    --tags "$TAGS_JSON" \
    --region $REGION > /dev/null
  ok "$SF_NAME creado"
fi

echo -e "\n${BLUE}[8/8] Creando Glue Jobs y Crawlers...${NC}"
GCALC_ROLE="arn:aws:iam::${ACCOUNT_ID}:role/itx-glue-calculate-role"
GINTC_ROLE="arn:aws:iam::${ACCOUNT_ID}:role/itx-glue-interchange-role"
GCREF_ROLE="arn:aws:iam::${ACCOUNT_ID}:role/itx-glue-crawler-reference-role"
GCEBGR_ROLE="arn:aws:iam::${ACCOUNT_ID}:role/itx-glue-crawler-ebgr-role"
aws glue create-job --name itx-calculate --role $GCALC_ROLE \
  --command "Name=glueetl,ScriptLocation=s3://${S3_STAGING}/scripts/calculate.py,PythonVersion=3" \
  --glue-version "4.0" --number-of-workers 2 --worker-type G.1X \
  --default-arguments '{"--enable-metrics":"true","--enable-job-insights":"true","--job-language":"python","--enable-continuous-cloudwatch-log":"true"}' \
  --tags "$TAGS_GLUE" \
  --region $REGION 2>/dev/null && ok "itx-calculate creado" || warn "itx-calculate ya existe"
aws glue create-job --name itx-interchange --role $GINTC_ROLE \
  --command "Name=glueetl,ScriptLocation=s3://${S3_STAGING}/scripts/interchange.py,PythonVersion=3" \
  --glue-version "4.0" --number-of-workers 4 --worker-type G.2X \
  --default-arguments '{"--job-bookmark-option":"job-bookmark-disable","--enable-job-insights":"true","--job-language":"python","--enable-continuous-cloudwatch-log":"true"}' \
  --tags "$TAGS_GLUE" \
  --region $REGION 2>/dev/null && ok "itx-interchange creado" || warn "itx-interchange ya existe"
aws glue create-database --database-input "{\"Name\":\"itx_reference\"}" \
  --region $REGION 2>/dev/null && ok "DB itx_reference creada" || warn "itx_reference ya existe"
aws glue create-database --database-input "{\"Name\":\"ebgr_visa_staging\"}" \
  --region $REGION 2>/dev/null && ok "DB ebgr_visa_staging creada" || warn "ebgr_visa_staging ya existe"
aws glue create-crawler --name itx-crawler-reference --role $GCREF_ROLE \
  --database-name itx_reference \
  --targets "{\"S3Targets\":[{\"Path\":\"s3://${S3_REFERENCE}/\"}]}" \
  --tags "$TAGS_GLUE" \
  --region $REGION 2>/dev/null && ok "itx-crawler-reference creado" || warn "ya existe"
aws glue create-crawler --name itx-crawler-ebgr-staging --role $GCEBGR_ROLE \
  --database-name ebgr_visa_staging \
  --targets "{\"S3Targets\":[{\"Path\":\"s3://${S3_STAGING}/\"}]}" \
  --tags "$TAGS_GLUE" \
  --region $REGION 2>/dev/null && ok "itx-crawler-ebgr-staging creado" || warn "ya existe"

echo ""
echo "=============================================="
echo " DEPLOY COMPLETADO"
echo "=============================================="
echo " Cuenta:        $ACCOUNT_ID"
echo " Tags:          Environment=dev | Project=datalake-itx"
echo "----------------------------------------------"
echo " S3 Buckets     : 5"
echo " IAM Roles      : 11"
echo " Lambda Layer   : itx-pandas-pyarrow"
echo " DynamoDB       : 4 tablas"
echo " Lambdas        : 5 (itx-store pendiente)"
echo " S3 Trigger     : itx-landing -> itx-router"
echo " Step Functions : itx-main-orchestrator"
echo " Glue Jobs      : itx-calculate (G.1X·2w), itx-interchange (G.2X·4w)"
echo " Glue Databases : itx_reference, ebgr_visa_staging"
echo " Glue Crawlers  : itx-crawler-reference, itx-crawler-ebgr-staging"
echo "----------------------------------------------"
echo " PENDIENTE:"
echo " - Implementar itx-store"
echo " - Cargar datos en $S3_REFERENCE"
echo " - Poblar DynamoDB: itx-file-pattern, itx-visa-fields, itx-client"
echo " - Ejecutar crawlers"
echo " - Testing end-to-end"
echo "=============================================="