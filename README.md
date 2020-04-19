# avid-covider-pipelines

[avid-covider](https://github.com/hasadna/avid-covider) data processing pipelines

## What are we doing here?

The avid-covider project aims to support gathering daily information on the spread of the corona virus.

The avid-covider pipelines aggregate and analyze that data.

The frameworks we're using to accomplish all of this are called `dataflows` and `datapackage-pipelines`.
These frameworks allow us to write simple 'pipelines', each consisting of a set of predefined processing steps.
Most of the pipelines use of a set of common building-blocks, and some custom processors - mainly custom scrapers for exotic sources.

`dataflows` is used for writing processing flows individual sources.

`datapackage-pipelines` runs the flows, combines the results and creates aggregated datasets.

## Quickstart on `dataflows`

The recommended way to start is by reading the README of `dataflows`[here](https://github.com/datahq/dataflows)

Then, try to write a very simple pipeline - just to test your understanding. A good task for that would be:
- Load the first page of ynet/youtube/reddit/
- Scrape the list of items from that page
- Set proper datatypes for the fields (e.g. for title, dates etc)
- Dump the results into a csv file or an sqlite file

 ## Quickstart on `datapackage-pipelines`

The recommended way to start is by reading the README of `datapackage-pipelines`[here](https://github.com/frictionlessdata/datapackage-pipelines)- 
it's a bit long, so at least read the beginning and skim the rest.

## What's currenty running?

To see what's the current processing status of each pipeline you will need a username/password to access the [dashboard](https://avid-covider-pipelines.odata.org.il/).

## Installation

### Installing Python 3.6+

We recommend using [pyenv](https://github.com/pyenv/pyenv) for managing your installed python versions.

On Ubuntu, use these commands:

```bash
sudo apt-get install git python-pip make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev
sudo pip install virtualenvwrapper

git clone https://github.com/yyuu/pyenv.git ~/.pyenv
git clone https://github.com/yyuu/pyenv-virtualenvwrapper.git ~/.pyenv/plugins/pyenv-virtualenvwrapper

echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
echo 'pyenv virtualenvwrapper' >> ~/.bashrc

exec $SHELL
```

On OSX, you can run

```bash
brew install pyenv
echo 'eval "$(pyenv init -)"' >> ~/.bash_profile
```

After installation, running:

```bash
pyenv install 3.6.1
pyenv global 3.6.1
```

Will set your Python version to 3.6.1

### Installing requirements

```bash
$ pip install -r requirements.txt
```

That should be enough to work on single steps of the pipelines using dataflows

### Running dataflows

```
python3 -m preprocessing.get_raw_data
```

### Installation of the full pipelines system

Install system requirements:

```
sudo apt-get install build-essential python3-dev libxml2-dev libxslt1-dev libleveldb-dev libspatialindex-c4v5
```

The full system requires a private repositry called COVID19-ISRAEL

You should have a clone of this repository in `../COVID19-ISRAEL` relative to the pipelines directory

Install COVID19-ISRAEL preprocessing requirements:

```
pip install -r ../COVID19-ISRAEL/requirements-preprocess.txt
```

Install additional requirements:

```
pip install -r requirements-full.txt
```

### Running a Pipeline

```bash
$ dpp run ./preprocess
```

### Advanced Topics

#### Deployment

Access to the COVID19-ISRAEL repository

* Generate an SSH key: https://help.github.com/en/github/authenticating-to-github/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#generating-a-new-ssh-key
* Add the key as a deploy key to the repository
* Mount the private key to a directory inside the pipelines container
* Set environment variables:
  * `COVID19_ISRAEL_PRIVATE_KEY_FILE=/path/to/private-key`
  * `COVID19_ISRAEL_REPOSITORY=GitHubUser/GitHubRepo`

Access to Google Drive

* Create a Google service account
* Mount the service account secret json to a directory
* Set environment variable:
  * `GOOGLE_SERVICE_ACCOUNT_FILE=/path/to/secret_service_account`

Access to Google API

* Mount the `google_api_key.txt` file into a path in the container
* Set environment variable:
  * `GOOGLE_API_KEY_FILE=/path/to/google_api_key.txt`

You should create a persistent volume to store the COVID19-ISRAEL repository and data files:

* Mount the volume at `/COVID19-ISRAEL`

Deployment:

Run the docker container from Dockerfile with the above environment variables and paths

#### Testing The full pipelines system

##### Initial installation (should be done once) 

Place secret files under a single path:

```
export SECRETS_PATH=/path/to/all/secrets
```

You should have the following files under that path:

```
ls -lah $SECRETS_PATH/covid19_israel_github_private_key $SECRETS_PATH/secret_service_account $SECRETS_PATH/google_api_key.txt
```

Set the repository env var:

```
echo 'export COVID19_ISRAEL_REPOSITORY=GitHubUser/GitHubRepo' > $SECRETS_PATH/.env
```

Create persistent volumes:

```
mkdir -p .covid19-israel-volume
mkdir -p data
```

Add secret files for corona_data_collector to CDC_SECRETS_PATH:

```
export CDC_SECRETS_PATH=/path/to/corona_data_collector_secrets
ls -lah $CDC_SECRETS_PATH/certs/client-cert.pem $CDC_SECRETS_PATH/certs/client-key.pem $CDC_SECRETS_PATH/certs/server-ca.pem
```

Secret environment variables required for corona_data_collector:

```
echo "
export CDC_SECRETS_PATH=$CDC_SECRETS_PATH
export CORONA_DATA_COLLECTOR_DB_PASS=
export CORONA_DATA_COLLECTOR_GPS_URL_KEY=
export CORONA_DATA_COLLECTOR_TELEGRAM_TOKEN=
" >> $SECRETS_PATH/.env
```

##### Running

```
export SECRETS_PATH=/path/to/all/secrets &&\
source $SECRETS_PATH/.env &&\
docker build -t avid-covider-pipelines . &&\
docker run -it \
  -v $SECRETS_PATH:/secrets \
  -v $CDC_SECRETS_PATH:/cdc_secrets \
  -e COVID19_ISRAEL_PRIVATE_KEY_FILE=/secrets/covid19_israel_github_private_key \
  -e COVID19_ISRAEL_REPOSITORY \
  -e GOOGLE_SERVICE_ACCOUNT_FILE=/secrets/secret_service_account \
  -e GOOGLE_API_KEY_FILE=/secrets/google_api_key.txt \
  -e CORONA_DATA_COLLECTOR_DB_PASS -e CORONA_DATA_COLLECTOR_GPS_URL_KEY -e CORONA_DATA_COLLECTOR_TELEGRAM_TOKEN \
  -e CORONA_DATA_COLLECTOR_SECRETS_PATH=/cdc_secrets \
  -e CORONA_DATA_COLLECTOR_GPS_PATH=/pipelines/data/corona_data_collector/gps_data.json \
  -v `pwd`/.covid19-israel-volume:/COVID19-ISRAEL \
  -v `pwd`/data:/pipelines/data \
  -p 5000:5000 \
  avid-covider-pipelines server
```

Access the dashboard at http://localhost:5000
