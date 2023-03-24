# iot-end-to-end


This is an end-to-end IoT solution simulating high velocity data emitted from smart meters and analyzed in Azure. We will design an architecture, filtering a subset of the telemetry data for real-time visualization on the hot path, and storing all the data in long-term storage for the cold path.

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


Step8: Create a storage account. Create another stream analytics job. Repeat the above step, make an input from iothub and a output to the storage account.

![image](https://user-images.githubusercontent.com/66850958/227403424-c080157d-1c47-4e21-b0f4-aa9fa39664c8.png)


Successful output in blob storage with proper folder format.


![image](https://user-images.githubusercontent.com/66850958/227403849-7102b40d-3366-4540-9fca-8401f36213fc.png)




       
         




