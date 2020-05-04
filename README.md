# Waze CCP on GCP

![banner](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/banner.png)

This is not an officially supported Google product, though support will be provided on a best-effort basis.

Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[https://www.apache.org/licenses/LICENSE-2.0](https://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Introduction

This AppEngine sample application is designed to process your Waze CCP JSON Feed into; BigQuery GIS  tables for analysis, Google Cloud Storage as GeoJSON for use in desktop or web GIS applications,  and, optionally into [Carto](https://carto.com/) for advanced spatial visualization.

Join the [Group](https://groups.google.com/d/forum/waze-ccp-gcp) for updates and discussion

### Google Cloud Products Used

- Google AppEngine
- Google Cloud Datastore
- Google BigQuery
- Google Cloud Storage
- Google Cloud Tasks

## Getting Started

### Step 1: Create and Configure your Project

![New Project Screen](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/1.png)

We will need the Project ID later.

### Step 2: Configure BigQuery

#### 1. Enable the BigQuery API

![Enable BigQuery API](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/2.png)

#### 2. Go to BigQuery UI and Create a Dataset

![Create BigQuery Dataset](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/3.png)

We will need the Dataset ID later, in SQL format with ``project-id.dataset_id`` - for example:

![View Datasets in BigQuery UI](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/4.png)

This would translate as:

    waze-ccp-os.waze_ccp

We'll refer to this as ``BQDATASET``

#### Step 3. Go to Cloud Storage UI and Create a Bucket

![Cloud Storage UI](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/5.png)

From here on out, we'll refer to your Bucket as ``GCSPATH``

#### Step 4. Create a Cloud Tasks Queue

Open Cloud Shell in the Google Cloud Platform Console, and run:

    gcloud tasks queues create waze-ccp-tasks

You can change the name of the queue if necessary.  You may be prompted to enable the Tasks API, and create an App Engine application.

We will refer to the queue name as ``CLOUD_TASKS_QUEUE`` and the location in which the queue is located as ``CLOUD_TASKS_LOCATION``.  View the location by running:

    gcloud tasks queues describe waze-ccp-tasks

Which will return (for example):

    name: projects/project-id/locations/us-central1/queues/waze-ccp-tasks

#### Step 5. Download source code and Update Variables to Match your Project

##### (i). Clone This Source Code

Using Cloud Shell, download the source code - by default, this will download code to ``waze-ccp-gcp``:

    git clone https://github.com/google/waze-ccp-gcp.git

##### (ii). Update Variables Source Code

Run the ``generate_config.py`` - this will do two things:

- Create a specific configuration file in ``instance/wazeccp.cfg`` which contains a custom GUID
- Create the cron.yaml, with a custom GUID

The GUID is just a way to create a non-guessable URL for the handler that Cron will call to update the Waze data every 10 minutes.

Now edit the ``instance/wazeccp.cfg`` file, and update the placeholder values with the ones from your project.  (Note, edit the config file in the instance folder, **not** the template configuration file in the root)

For example:

    WAZE_URL="https://my.waze.url/"
    BQDATASET="waze-gcp-ccp.waze_ccp"
    GCSPATH="waze-gcp-ccp-os"
    CLOUD_TASKS_QUEUE="waze-ccp-tasks"
    CLOUD_TASKS_LOCATION="us-central1"
    GUID="xxxxx-xxxxx-xxxxx-xxxxx"

Note that you do not need to change the GUID, as this was set by the ``generate_config.py`` script.

##### (iii). Deploy your Application to AppEngine

From Cloud Shell, use gcloud to deploy the app:

    cd waze-gcp-ccp
    gcloud app deploy app.yaml

Also, deploy the cron job that will periodically update data:

    gcloud app deploy cron.yaml

#### Step 6. Secure your Application with Identity Aware Proxy

Even though you generated a GUID to serve as the URL path that AppEngine's Cron accesses to cause a data update, someone could discover it and maliciously hit that URL, and, they could also hit the /newCase/ endpoint. In order to prevent unwanted use of these URLs, you will enable IAP and lock down access to the application only to approved users (or just you).

When you go to IAP settings for your project, you'll first have to set up a Credentials Screen (Oath2).
Set the Application Type to "Internal".

![Configure OAuth Consent Screen](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/7.png)

Then, under IAP - turn the IAP on for the AppEngine app:

![Enable IAP For App Engine](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/8.png)

You can verify that IAP is working by visiting [https://{project-name}.appspot.com](https://{project-name}.appspot.com) in an Incognito browser. You should be redircted to your OAuth2 Credentials Screen, which shows that the IAP is working and protecting the entire application.

#### Step 7. Create your New Case Study

Simply visit ![https://{project-name}.appspot.com/newCase/?name={your-case-name}]

You should just see a confirmation page (which includes the Datastore UID), and no 500 errors if everything worked correctly.

To confirm the Case Study was crated, you can visit Datastore and confirm the Entity you expect to see is there.

![Datastore view of caseModel kind](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/10.png)

Within 10 minutes, the Cron job described in cron.yaml will call [https://{project-name}.appspot.com/{guid}/cron](https://{project-name}.appspot.com/{guid}/cron) and will start populating the tables in BigQuery.

*Note - the cron function of AppEngine is internal so it is automatically inscope for IAP purposes.

#### Step 7. Investigating the Waze Data

##### BigQuery

In BigQuery, you should see the three tables (alerts, jams, irregularities) under your ``BQDATASET`` - these tables have a suffix of the UID assigned to the caseModel entity in Datastore

![BigQuery Tables](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/11.png)

These tables will contain all the **unique** elements from your **{waze-url}**

![BigQuery Unique Events View](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/12.png)

##### Data Studio

Once you have a few days worth of data, you can start experimenting with building Data Studio dashboards.

![Datastudio Example](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/6.png)

If you come up with something interesting, be sure to share with the group: <waze-ccp-on-gcp@googlegroups.com>
