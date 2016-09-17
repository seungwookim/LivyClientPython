import json, requests, textwrap, time, random

# json object hooker class
class JsonObject:
  def __init__(self, d):
    self.__dict__ = d

#json_data = json.loads(data, object_hook=JsonObject)
#requests.get('http://httpbin.org', hooks=dict(response=print_url))
#list(filter(lambda x:x=='idle' ,self.alive_sess_obj))

class LivyHiveClientManager:
    def __init__(self, s_num):
        self.max_sess_num = s_num
        self.host = "http://481bf68ee6d9:8998"
        self.hdfs_path = "/home/dev/hadoop"
        self.headers = {'Content-Type': 'application/json'}
        self.alive_sess_obj = None
        self.alive_sess_cnt = None
        self.alive_sess_list = []
        self.alive_sess_state = []
        self.avail_sess_list = []

    def create_session(self):
        """
        create session, get session id form return, run long code with that session
        :return:
        """
        self.check_alive_sessions()
        if(self.max_sess_num < self.alive_sess_cnt):
            print("exceed max session number")
            return False

        data = {'kind': 'pyspark',
                "name": "tensormsa",
                "executorCores": 1,
                "executorMemory": "512m",
                "driverCores": 1,
                "driverMemory": "512m"}
        r = requests.post(self.host + "/sessions", data=json.dumps(data), headers=self.headers)
        return r.json()['id']

    def check_alive_sessions(self):
        """
        check alive sessions info
        :return:
        """
        self.alive_sess_list[:] = []
        self.alive_sess_cnt = 0
        self.alive_sess_obj = None
        resp = requests.get(self.host + "/sessions/" , headers=self.headers)
        self.alive_sess_obj = json.loads(resp.content,  object_hook=JsonObject)
        self.alive_sess_cnt = len(self.alive_sess_obj.sessions)

        if(self.alive_sess_cnt > 0):
            for i in range(0 , self.alive_sess_cnt):
                self.alive_sess_list.append(self.alive_sess_obj.sessions[i].id)

    def get_available_sess_id(self):
        """
        get random one available (state is idle) session
        :return:
        """
        self.avail_sess_list[:] = []

        resp = requests.get(self.host + "/sessions/" , headers=self.headers)
        self.alive_sess_obj = json.loads(resp.content,  object_hook=JsonObject)
        self.alive_sess_cnt = len(self.alive_sess_obj.sessions)

        if(self.alive_sess_cnt > 0):
            for i in range(0 , self.alive_sess_cnt):
                if(self.alive_sess_obj.sessions[i].state == 'idle'):
                    self.avail_sess_list.append(self.alive_sess_obj.sessions[i].id)
        print("list of available sessions : {0} " .format(self.avail_sess_list))

    def delete_all_sessions(self):
        """
        delete all sessions
        :return:
        """
        print(self.alive_sess_list)
        for sess_id in self.alive_sess_list:
            print(sess_id)
            r = requests.delete(self.host + "/sessions/" + str(sess_id), headers=self.headers)
            print(r.json())

    def print_all(self):
        """
        delete all sessions
        :return:
        """
        print("host : {0}".format(self.host))
        print("headers : {0}".format(self.headers))
        print("alive_sess_obj : {0}".format(self.alive_sess_obj))
        print("alive_sess_cnt : {0}".format(self.alive_sess_cnt))
        print("alive_sess_list : {0}".format(self.alive_sess_list))


    def create_table_parq(self, table_name, json_data):
        """
        action for create table with json request
        :return:
        """

        self.get_available_sess_id()

        json_data = [{"name":"Andy", "univ":"snu"}]
        data = {
            'code': ''.join(['from pyspark.sql import SQLContext, DataFrameWriter, DataFrame, HiveContext\n',
                             'sqlContext = SQLContext(sc)\n',
                             'df_writer = sqlContext.createDataFrame(', str(json_data)  ,').write\n',
                             'df_writer.parquet("' , str(self.hdfs_path), "/", table_name , '", mode="overwrite", partitionBy=None)'
                             #'df_writer.saveAsTable("' , table_name , '",format="parquet", mode="overwrite" , partitionBy=None)'
                             # 'df_writer = DataFrameWriter(df)\n',
                             # 'df_writer'
                             #'df.write.format("parquet").save("' ,str(self.hdfs_path), "/", table_name , '.parquet")\n'
                             # 'result = df_writer.saveAsTable("' ,
                             # table_name ,'", format="parquet", mode="overwrite"',
                             # ', path="' ,str(self.hdfs_path), "/", table_name, ' " ' ,
                             # ')\n',
                             # 'result'
                             ])
        }

        print("request codes : {0} ".format(data))
        resp = requests.post(self.host + "/sessions/" + str(min(self.avail_sess_list)) + "/statements", data=json.dumps(data), headers=self.headers)
        temp_resp = json.loads(resp.content, object_hook=JsonObject)
        result = livy_client.get_response(str(min(self.avail_sess_list)), temp_resp.id)
        print("result : {0} ".format(result))


    def create_table_hive(self, table_name, json_data):
        """
        action for create table with json request
        :return:
        """

        self.get_available_sess_id()

        json_data = [{"name":"Andy", "univ":"snu"}]
        data = {
            'code': ''.join(['from pyspark.sql import SQLContext, DataFrameWriter, DataFrame, HiveContext\n',
                             'hiveContext = HiveContext(sc)\n',
                             'df_writer = hiveContext.createDataFrame(', str(json_data)  ,').write\n',
                             'df_writer.saveAsTable("' , table_name , '",format="parquet", mode="overwrite" , partitionBy=None)'
                             ])
        }

        print("request codes : {0} ".format(data))
        resp = requests.post(self.host + "/sessions/" + str(min(self.avail_sess_list)) + "/statements", data=json.dumps(data), headers=self.headers)
        temp_resp = json.loads(resp.content, object_hook=JsonObject)
        result = livy_client.get_response(str(min(self.avail_sess_list)), temp_resp.id)
        print("result : {0} ".format(result))

    def get_response(self, session_id, statements_id):
        """
        retry till running finished
        :return:
        """
        resp = requests.get(self.host + "/sessions/" + str(session_id) + "/statements/" + str(statements_id), headers=self.headers)
        response_obj = json.loads(resp.content, object_hook=JsonObject)

        if(response_obj.state == 'running'):
            time.sleep(1)
            return self.get_response(session_id, statements_id)
        else:
            print(resp.json())
            return resp.json()

        #print("response : {0}".format(self.response_obj.statements[len(self.response_obj.statements)-1].output.evalue))


    def query_sql(self, query_str):
        """
        get data from hive table
        :return:
        """

        self.get_available_sess_id()

        json_data = [{"name":"Andy", "univ":"snu"}]
        data = {
            'code': ''.join(['from pyspark.sql import HiveContext\n',
                             'hiveContext = HiveContext(sc)\n',
                             'result = hiveContext.sql("' , str(query_str) ,'")\n'
                             'result'
                             ])
        }

        print("request codes : {0} ".format(data))
        resp = requests.post(self.host + "/sessions/" + str(min(self.avail_sess_list)) + "/statements", data=json.dumps(data), headers=self.headers)
        temp_resp = json.loads(resp.content, object_hook=JsonObject)
        result = livy_client.get_response(str(min(self.avail_sess_list)), temp_resp.id)
        print("result : {0} ".format(result))


    def query_sql_test(self):
        """
        action for create table with json request
        :return:
        """

        self.get_available_sess_id()

        data = {
            'code': ''.join(['from pyspark.sql import SQLContext\n',
                             'sqlContext = SQLContext(sc)\n',
                             'df = sqlContext.read.load("/home/dev/spark/examples/src/main/resources/users.parquet")\n',
                             'df.registerAsTable("users")',
                             'result = sqlContext.sql("SELECT * FROM users").collect()',
                             'result'
                             ])
        }
        print("request codes : {0} ".format(data))
        resp = requests.post(self.host + "/sessions/" + str(min(self.avail_sess_list)) + "/statements", data=json.dumps(data), headers=self.headers)
        temp_resp = json.loads(resp.content, object_hook=JsonObject)
        result = livy_client.get_response(str(min(self.avail_sess_list)), temp_resp.id)
        print("result : {0} ".format(result))

livy_client = LivyClientManager(2)

#livy_client.create_session()
#livy_client.check_alive_sessions()
#livy_client.delete_all_sessions()
#livy_client.get_available_sess_id()
#livy_client.create_table_parq("abcd", None)
livy_client.create_table_hive("abcd", None)
#livy_client.get_response(8, 102)
#livy_client.print_all()
#livy_client.query_sql("select * from abcd")
#livy_client.query_sql_test()