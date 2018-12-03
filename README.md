![banner](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/banner.png)
# Waze CCP on GCP
Copyright 2018 Google LLC. This software is provided as-is, without warranty or representation for any use or purpose. Your use of it is subject to your agreements with Google.

### Introduction

This AppEngine sample application is desgined to process your Waze CCP JSON Feed into; BigQuery GIS  tables for analysis, Google Cloud Storage as GeoJSON for use in desktop or web GIS applications,  and, optionally into [Carto](https://carto.com/) for advanced spatial visualization. 

Join the [Group](https://groups.google.com/d/forum/waze-ccp-gcp) for updates and discussion

##### Google Cloud Products Used:
- Google AppEngine
- Google Cloud Datastore
- Google BigQuery
- Google Cloud Storage

### Getting Started

##### Step 1: Create and Configure your Project

![Step1](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/1.png)

From here on out, we'll refer to your Project ID as **{project-id}** and Project Name as **{project-name}**

##### Step 2: Configure BigQuery

###### 1. Enable the BigQuery API

![Step2](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/2.png)

###### 2. Go to BigQuery UI and Create a Dataset

![Step2](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/3.png)

From here on out, we'll refer to your Dataset as **{bqDataset}**

![Step2b](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/4.png)

##### Step 3. Go to Cloud Storage UI and Create a Bucket

![Step3](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/5.png)

From here on out, we'll refer to your Bucket as **{gcsPath}**

##### Step 4. Download source code, Install Dependencies, and Update Variables to Match your Project.
###### 1.Clone The Source Code from the CE / PSO Repo: 
```gcloud source repos clone waze-ccp-gcp --project=cloud-ce-pso-shared-code ```
###### 2. Update Variables Source Code: 
First, genereate a [GUID](https://www.guidgenerator.com/). This will be referred to as **{guid}** and its just a way to create a non-guessable URL for the handler that Cron will call to update the Waze data every 10 minutes. 

- In app.yaml 
  - Line 1: Change **{project-name}** to your **{project-name}**
  - Line 23: Change **{guid}** to your **{guid}** 
- In cron.yaml 
  - Line 3: Change **{guid}** to your **{guid}**
- In main.py
  - Line 15: Change **{waze-url}** to your Waze CCP URL
  - Line 24: Change **{gcsPath}** to your **{gcsPath}** 
  - Line 27: Change **{bqDataset}** to your **{bqDataset}**
  -  Line 859: Change **{guid}** to your **{guid}**

###### 3. Install Dependencies to /lib folder: 
This application utilizes Google-provided Python libraries that are not part of AppEngine Standard, but are easily installed using the vendor library method. Becaue these libraries update frequently and themselves install additional dependencies, you will use the requirements.txt file provided and pip to install them. 

From the terminal, change directories to where you cloned the source code. 
``` cd {your-app-folder} ```
Next, use pip to install the required libraries to the /lib folder. 
``` pip install -r requirements.txt -t lib/ ```
The special file "appengine_config.py" uses the Vendor library to include any dependencies located in the /lib folder.

##### Step 5. Deploy your Application to AppEngine

###### 1. Using gcloud, Switch Project to your New Project:
``` gcloud config set project {project-name} ```
###### 2. Using gcloud, Switch Project to your New Project:
``` gcloud app create ```

![Step5](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/9.png)

###### 3. Using gcloud, Deploy your Application:

```gcloud app deploy {your-app-folder}/app.yaml```

###### 4. Secure your Application with Identity Aware Proxy:
Even though you generated a GUID to serve as the URL path that AppEngine's Cron accesses to cause a data update, someone could discover it and maliciously hit that URL, and, they could also hit the /newCase/ endpoint. In order to prevent unwanted use of these URLs, you will enable IAP and lock down access to the application only to approved users (or just you). 

When you go to IAP settings for your project, you'll first have to set up a Credentials Screen (Oath2). 
Set the Application Type to "Internal".
![Step5b](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/7.png)

Then, under IAP - turn the IAP on for the AppEngine app: 

![Step5c](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/8.png)

You can verify that IAP is working by visiting https://{project-name}.appspot.com in an Incognito browser. You should be redircted to your OAuth2 Credentials Screen, which shows that the IAP is working and protecting the entire application. 

##### Step 6. Creeate your New Case Study

Simply visit https://{project-name}.appspot.com/newCase/?name={your-case-name} 
You should just see a blank page rendered, and no 500 errors if everything worked correctly. 
To confirm the Case Study was crated, you can visit Datastore and confirm the Entity you expect to see is there. 
![Step6](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/10.png)

Within 10 minutes, the Cron job described in cron.yaml will call https://{project-name}.appspot.com/{guid}/ and will start populating the tables in BigQuery. *Note - the cron function of AppEngine is internal so it is automatically inscope for IAP purposes. 
##### Step 7. Investigating the Waze Data
###### BigQuery:
In BigQuery, you should see the three tables (alerts, jams, irregularities) under your **{bqDataset}**
![Step7a](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/11.png)
These tables will contain all the **unique** elements from your **{waze-url}**
![Step7a](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/12.png)
###### Data Studio:
Once you have a few days worth of data, you can start experimenting with building Data Studio dashboards.
![Step7a](https://storage.googleapis.com/waze-ccp-gcp-os/readmeimages/6.png)
If you some up with something interesting, be sure to share with the group: <waze-ccp-on-gcp@googlegroups.com>