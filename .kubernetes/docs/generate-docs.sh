echo "Current working directory is:" && pwd

echo "Creating directories..."

cd ./queries

mkdir ./credentials-dev

mkdir ./credentials-prod

mkdir /tmp

echo "Mounting files from env..."

bash -c "echo $1  > /tmp/credentials.json"

ls /tmp | grep credentials.json

# bash -c "echo $1  > ./credentials-prod/prod.json"

# echo """
# default:
#   target: dev
#   outputs:
#     dev:
#       type: bigquery
#       method: service-account
#       project: rj-smtr
#       dataset: dbt
#       location: US
#       threads: 2
#       keyfile: /tmp/credentials.json
#     prod:
#       type: bigquery
#       method: service-account
#       project: rj-smtr
#       dataset: dbt
#       location: US
#       threads: 2
#       keyfile: $PWD/credentials-prod/prod.json""" > profiles/profiles.yml

# ls ./profiles

echo "Generating docs static files"

dbt docs generate --profiles-dir .