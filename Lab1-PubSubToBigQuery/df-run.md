## Set up environment variables
export PROJECT_ID=$(gcloud config get-value project) \
export REGION='europe-west4' \
export PIPELINE_FOLDER=gs://${PROJECT_ID} \
export MAIN_CLASS_NAME=gcp_lab1.PubSubToBigQuery \
export RUNNER=DataflowRunner \
export LAB_ID=13 \
export TOPIC=projects/${PROJECT_ID}/topics/uc1-input-topic-13 \

## Download dependencies listed in pom.xml
mvn clean dependency:resolve

## Execute the pipeline
mvn compile exec:java \
-Dexec.mainClass=gcp_lab1.PubSubToBigQuery \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--subTopic=projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-13 \
--tableName=uc1_13.account \
--runner=DataflowRunner \
--project=nttdata-c4e-bde \
--jobName=usecase1-labid-$LAB_ID \
--region=europe-west4 \
--serviceAccount=c4e-uc1-sa-$LAB_ID@nttdata-c4e-bde.iam.gserviceaccount.com \
--maxNumWorkers=1 \
--workerMachineType=n1-standard-1 \
--gcpTempLocation=gs://c4e-uc1-dataflow-temp-$LAB_ID/temp \
--stagingLocation=gs://c4e-uc1-dataflow-temp-$LAB_ID/staging \
--subnetwork=regions/europe-west4/subnetworks/subnet-uc1-$LAB_ID \
--streaming=true"

## Message Body
{"id": N, "name": "name1", "surname": "name2"}

## extras
projects/${PROJECT_ID}/topics/uc1-input-topic-13
--inputTopic=$TOPIC \
projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-13
--allowedLateness=0 \
