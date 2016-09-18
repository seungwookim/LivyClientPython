# LivyClientPython : 'requests' wrapper class for 'Livy-Spark'  
  It's Python client util class for Livy-Spark. Livy support submitting pyspark remotly but I couldn't find 
  comfortable spark-sql support features. Livy's session, statemets API process wasn't comfortable for simple
  Spark sql usage purpose. simply purpose of this util is to provide direct spark-sql methods for python client.  

# Usage *[(more)](http://hugrypiggykim.com)*

   - set up for Livy server Info & save file path
     will be changed to see property files 
   ```python
      self.host = "http://481bf68ee6d9:8998"
      self.hdfs_path = "/home/dev/hadoop/data_frame"
   ```
   
   - declare Class with maximum session number 
   ```python
      livy_client = LivyParqClientManager(2)
   ```
   
   - create session
   ```python
      livy_client.create_session()
   ```   
   
   - create tables
   ```python
      livy_client.create_table("xxxx", "[{'name':'Andy', 'univ':'snu'},{'name':'Kim', 'univ':'snu'} ]")
   ```     
   
   - select tables
   ```python
      livy_client.query_data("xxxx", "select * from xxxx")

   ```    
