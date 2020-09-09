 # INHABIT - make an informed move!
![image](https://user-images.githubusercontent.com/43352808/85424319-9fa80a80-b52c-11ea-9759-182d42f20a2c.png)
##  Motivation
Around 40 million Americans move every year, which is roughly 14% of the population. When people move to a new place, they look for opportunities, healthy life, education, transportation. Increasingly people are also thinking about the environment, and they rightly do so because it impacts our life tremendously. The World Health Organization estimates around 4 million people die every year prematurely because of air pollution. Along these lines, weather has been linked to mental health.
The motivation of this app is to provide all the environment related trends, so that people can make an informed move. It can also be used to check weather and air pollution trends before visiting or moving to a new place.

## Description
### Datasets:
The project uses two publicly available data sets, OpenAQ and NOAA weather data. The Open AQ data consists of air quality data from various sensors in the world. NOAA weather data contains information such as temperature, solar radiation, and soil information in the US. More details can be found in the data processing section. These data sets are >300 GB and are stored in Amazon S3 bucket.

![DEtools_github](https://user-images.githubusercontent.com/43352808/86197055-71689300-bb09-11ea-9bb1-f1f399349370.PNG)
### Processing:
The data is processed using Apache Spark. Apache Spark has been installed on an AWS EC2 cluster with one master instance (4 cores and 16 GB RAM) and three worker instances (4 vCPUS and 16 GB RAM). Faster processing, availability of resources to learn the software, and ease of using led to the selection of this tool.

### Code details:
The code is designed to read data from S3 bucket into a large data frame. The data has been cleaned to allow only:
1. Entries with valid latitude and longitude information.
2. Valid sensor values.
3. Drop any duplicate values. 

### Interesting observations in data set:
* OpenAQ data set comes from sources across the world hence the units for the same parameter are inconsistent depending on the source of sensor (For example, NO2 units for some sources can be ppm or Micrograms/m3).
* The update values for the sensor varies depending on the sources. Some sensors are updated once a day and some of them are updated 4-5 times a day. 
* OpenAQ has only PM related data in 2013 and 2014. From 2015, there is data related to all air quality parameters.
* NOAA weather data schema is different from Open AQ data, transformation of data has been performed and then merged with AQ data.

### Optimizing the code:
The code initially took 2 hours to read data, some optimization techniques like providing the static schema for the datasets and expanding computation capability of spark cluster brought the reading time to under 6 min.
I am currently working on repartitioning techniques to see if any other optimizations are possible. I will post those details shortly.

### Storage: 
Processed data is stored in a PostgreSQL database on an EC2 instance. PostgreSQL has been chosen because of the advanced functionality to query geographical data through the PostGIS extension. There are approximately 23 million records being stored in a table. Having index to columns to the queries frequently searched sped up the run time. 

### Visualization:
The Web app is designed using Dash by Plotly. The web app has few basic interactive features: 
An overview map that shows sensors available across the world. A user can select specific parameter and look at availability of sensors:  
![maps_Save](https://user-images.githubusercontent.com/43352808/85491919-32769280-b589-11ea-91cc-e8601f74e004.png)
Fields to get latitude and longitude or US ZIP code information from user:

![coordinate_selection](https://user-images.githubusercontent.com/43352808/85492008-589c3280-b589-11ea-839b-b6590c5b19ee.PNG)
Scatter plot that shows trends for the sensor data against time and overview map will zoom into the location:
![Scatterplot](https://user-images.githubusercontent.com/43352808/85492336-de1fe280-b589-11ea-9261-801e456d895c.PNG)

Distance to the nearest sensor in order to understand the limitations of the information provided:


![distance capture](https://user-images.githubusercontent.com/43352808/85492406-fa238400-b589-11ea-9833-e6964d491bbb.PNG)

The overview map is being cached for getting faster loading. Exceptions have been placed to catch any incorrect ZIP codes.

### Scheduler

Air flow is used as scheduler to download data from openaq and NOAA sources on a daily basis and run spark job

## Next Steps:
* Integrating with new data sources like water quality, hospitals, transportation etc. can make it a one-stop app to check all the factors.
* Instead of having trends, come up with a score for air pollution to make trends more intuitive to user.
* Machine learning models to predict possibility of natural calamities like floods, droughts, hurricanes etc. will be a good extension to this project.
