# iot-end-to-end


This is an end-to-end IoT solution simulating high velocity data emitted from smart meters and analyzed in Azure. 
this project is divided into 3 parts:

Part1: We will design an architecture, filtering a subset of the telemetry data for real-time visualization on the hot path.
We are fetching data from iot hub and seeing the average of temp in last 5 mins as a real-time report in powerbi.

Part2: Storing all the data in long-term storage for the cold path.
We are fetching data from iot hub and  transforming the data into a proper table via ADB dumping it in a storage account.


Part3: We are performing anamoly detection in real-time.
We are fetching data from iot hub and performing anamoly detection via Stream Analytics Query. Pushing data into a event hub which connects to a azure function.
This Azure Function  


We will be using :

Azure App Services
Azure Blob Storage
Azure Databricks
Azure SQL Database
Azure Stream Analytics
Device Provisioning Service
IoT Hub
Power BI Desktop
Visual Studio

complete architecture:


![image](https://user-images.githubusercontent.com/66850958/227395340-3bfd5cdb-b9e2-48a4-9443-69963aa88d1f.png)


Step1: Run the smart meter simulation solution file on your local PC. The files have been added to this repo.

![image](https://user-images.githubusercontent.com/66850958/227395480-2afa031c-a14e-4e1e-8d50-2c29e68d57f8.png)


Step2: Create a resource group. In that, create a IoT Hub and Azure IoT Hub Device Provisioning Service (DPS). 
NOTE: Go to shared access policies, select iothubwoner and copy&save the primary key and connection string for future use.
Also, copy the ID Scope of DPS service.


![image](https://user-images.githubusercontent.com/66850958/227395688-5341d2a0-93be-4ab2-ada4-db7bb4aaeb40.png)



![image](https://user-images.githubusercontent.com/66850958/227395630-07036be2-3e6c-496c-848c-fe104b692bea.png)


Step3: Create a databricks workspace and spin a single-node cluster(free trial).


![image](https://user-images.githubusercontent.com/66850958/227395975-d75c0629-8f98-47b3-9cf7-1871609cd24b.png)


Step4: Go to DPS, click to link IoT Hubs and connect to above made IoT Hub.


![image](https://user-images.githubusercontent.com/66850958/227396718-a558fa38-ca03-4aa5-a838-bdac45554018.png)


Step5: Go to manage enrollment group, add a group and click save. Copy the primary key of the earlier made group and save it.

Step6: Copy the DPS mgmnt groyp primary key and ID scope in simulation window, click the meter and click register. It will turn blue i.e it is registered. 
After that, hit connect.


![image](https://user-images.githubusercontent.com/66850958/227398591-213a5bc2-11a8-41ed-b2cf-9d77f29f7a1f.png)




Real-time visualization on the hot path


Step7: Create a steam analytics job. create a input with iot hub and a output for powerbi. this will give real-time reporting on your data.
NOTE: As i don't have a work account for powerbi, I will just list the steps.

Steps for powerbi:
1. Write a sql query in stream analytics job as and save it:
      select
            avg(temp) as Average, id
      into 
           [powerbi]  (output name)
      from
          [iothub]  ( input name)
         GROUP BY TUMBLING(minute,5), id;
         
   Explanation: This query fetches the streaming data in 5 minute non-overlapping windows. Aggregration function is applied which averages the last 5 min 
   temp data, displays the output as Average with id column grouped by id.
   
   
   ![image](https://user-images.githubusercontent.com/66850958/227402005-78b9529c-ca73-4613-82e4-e8df73ccf1aa.png)

   
2. Start the job. You will see in the monitor tab, data starts to flow in.

3. Create a dashboard in powerbi by selecting id on x-axis and sum of average on y-axis. this gives a real-time report based on the streaming data 
   coming from iot hub.


![image](https://user-images.githubusercontent.com/66850958/227402437-826ad580-989d-4a62-b89a-843f046fe17a.png)



Storing data in long-term storage for the cold path.


Step8: Create a storage account. Create another stream analytics job. Repeat the above step, make an input from iothub and a output to the storage account
(choose CSV as serialization format).

![image](https://user-images.githubusercontent.com/66850958/227403424-c080157d-1c47-4e21-b0f4-aa9fa39664c8.png)


Successful output in blob storage with proper folder format.


![image](https://user-images.githubusercontent.com/66850958/227403849-7102b40d-3366-4540-9fca-8401f36213fc.png)


Step9: Create a notebook (mytranspy.ipynb) in databricks workspace, transforming file into a useful table and loading it as a table.


![image](https://user-images.githubusercontent.com/66850958/227419796-77ed3935-3c53-4860-9588-80733e68a243.png)



For anamoly detection, send a mail to user if a anamoly is detected.


Step10: Create a event hub namespace, then a event hub. Create a new stream analytics job, input will be iothub and output will be this event hub.

![image](https://user-images.githubusercontent.com/66850958/227428941-9cd43e56-ce44-4c32-b7e1-b3f9cfaa31ff.png)


SQL Query to analyse anamoly:
WITH SmootheningStep AS
(
    SELECT
        System.Timestamp() as time, id,
        AVG(CAST(temperature as float)) as temp
    FROM myiot
    GROUP BY TUMBLINGWINDOW(second, 30),id
),
AnomalyDetectionStep AS
(
    SELECT
    time,
    temp,id,
    AnomalyDetection_SpikeAndDip(temp, 100, 120, 'spikesanddips') 
        OVER(PARTITION BY id LIMIT DURATION(second, 120)) as SpikeAndDipScores
    FROM SmootheningStep
), AnamolyDetectionFinal as (
SELECT
    time,
    temp,id,
    CAST(GetRecordPropertyValue(SpikeAndDipScores, 'Score') AS FLOAT) As
    SpikeAndDipScore,
    CAST(GetRecordPropertyValue(SpikeAndDipScores, 'IsAnomaly') AS BIGINT) AS
    IsSpikeAndDipAnomaly
FROM AnomalyDetectionStep )
select id,time,
    temp, SpikeAndDipScore, IsSpikeAndDipAnomaly into huboutput
    from AnamolyDetectionFinal where IsSpikeAndDipAnomaly=0


Step11: Create a function app with a event hub trigger. Go to integration and change the input from above event hub.

![image](https://user-images.githubusercontent.com/66850958/227429186-9a48799f-b523-4d78-b4f1-8f5b1d14bc65.png)


![image](https://user-images.githubusercontent.com/66850958/227429488-b9ed529e-4ae5-441e-9c02-1e31a578d434.png)


Step11: Create a logic app to send an email in case an anamoly is generated.


![image](https://user-images.githubusercontent.com/66850958/227430912-a75a6ee6-b757-449a-a5e7-b1b2f6a786cb.png)


Step12: We get a successful email about the anamoly detection.


![image](https://user-images.githubusercontent.com/66850958/227431161-68ee8a55-3a94-40b3-9e27-783fabbb9def.png)







       
         




