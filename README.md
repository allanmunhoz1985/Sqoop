![image](https://user-images.githubusercontent.com/82824988/118039363-a3ac6900-b346-11eb-9157-84525efa7d26.png)

########################################################################## 
#Inicio do Script da Batch Layer
########################################################################## 
# analisando o banco de dados
#Loga no mysql e analisa a tabela customers
mysql --user=root --password=cloudera
use retail_db
select distinct customer_city from customers;
select distinct customer_state from customers;

#teste de sqoop, depois vamos apagar estes dados
sqoop import --connect jdbc:mysql://localhost/retail_db --table customers --username root --password cloudera --check-column customer_id --incremental append --last-value 0 -m 1

#verifica diretório e arquivo criados pelo sqoop no HDFS
sudo hdfs dfs -ls /user/cloudera
sudo hdfs dfs -ls /user/cloudera/customers

#visualiza o arquivo
sudo hdfs dfs -cat /user/cloudera/customers/part-m-00000

#apaga os dados, foi só um teste
sudo hdfs dfs -rm -r /user/cloudera/

#vamos criar um job para o sqoop, este job cria a batch layer
#parametro -D sqoop.metastore.client.record.password=true para permitir a senha no script
sqoop job -D sqoop.metastore.client.record.password=true --create batchlayer -- import --connect jdbc:mysql://localhost/retail_db --table customers --username root --password cloudera --check-column customer_id --incremental append --last-value 0 -m 1 

#listamos jobs
sqoop job --list

#mostra detalhes do job
sqoop job --show batchlayer

#executa o job
sqoop job --exec batchlayer

#olhamos o ultimo inserido
sudo hdfs dfs -ls /user/cloudera/customers/
sudo hdfs dfs -cat /user/cloudera/customers/part-m-00000

#voltamos ao mysql
#confirmamos o numero do ultimo cliente
select max(customer_id) from customers;

#inserimos cliente de numero 12436
insert into customers (customer_id,customer_fname, customer_lname,customer_city,customer_state) values (12436,"Fernando","Amaral","Apopka","FL");

#verificamos o cliente
select max(customer_id) from customers;

#executa o job da batchlayer
sqoop job --exec batchlayer

#olhamos a atualização no HDFS
sudo hdfs dfs -ls /user/cloudera/customers/
sudo hdfs dfs -cat /user/cloudera/customers/part-m-00001

#agora vamos ao Hive criar o DW
beeline
!connect jdbc:hive2://

#opcional, porém recomendado, mudamos o engine do Hive para Spark
#se você tiver problemas de travamento da VM, volte o parâmetro para mr
set hive.execution.engine;
set hive.execution.engine=spark;
set hive.execution.engine;

#verificamos e criamos o banco de dados
show databases;
create database customers;
use customers;

#criamos a tabela master, imutável
create table customers_master(customer_id int, customer_fname string, customer_lname string, customer_email string, customer_password string, customer_street string,  customer_city string, customer_state string, customer_zipcode string) row format delimited fields terminated by ',' location '/user/cloudera/customers/';

#ver registros
select * from customers_master;

#fazer nova inserção no mysql, rodar o job e mostrar que o hive atualiza automaticamente
insert into customers (customer_id,customer_fname, customer_lname,customer_city,customer_state) values (12437,"Irene","Ebling","Apopka","FL");

#shell do linux
sqoop job --exec batchlayer
#hive
select * from customers_master;

#agora vamos criar as duas batchviews no hive
#testando as consultas
select count(customer_state), customer_state from customers_master group by customer_state ;
select count(customer_city), customer_city from customers_master group by customer_city ;

#criando as views
create view if not exists bv_customer_state as select count(customer_state) soma, customer_state from customers_master group by customer_state sort by soma desc;
create view if not exists bv_customer_city as select count(customer_city) soma, customer_city from customers_master group by customer_city sort by soma desc;

#testando as views
select * from bv_customer_state;
select * from bv_customer_city;

sqoop job -D sqoop.metastore.client.record.password=true --create speedlayer -- import --connect jdbc:mysql://localhost/retail_db --table customers --username root --password cloudera --check-column customer_id --incremental append --last-value 12436 -m 1 --target-dir /user/cloudera/customerspeed/

#speed layer
#criamos um novo job para a speed layer
#definimos target dir para armazenar em outro local
#depois na automatização, o arquivo será apagado e apenas novos registros serão gravados 
#definimos ultimo valor com 12436, para manter um registro e importar
sqoop job -D sqoop.metastore.client.record.password=true --create speedlayer -- import --connect jdbc:mysql://localhost/retail_db --table customers --username root --password cloudera --check-column customer_id --incremental append --last-value 12436 -m 1 --target-dir /user/cloudera/customerspeed/

#executa
sqoop job --exec speedlayer

#ver os dados,
sudo hdfs dfs -ls /user/cloudera/customerspeed/
sudo hdfs dfs -cat /user/cloudera/customerspeed/part-m-00000

#voltamos ao hive, criamos as real-time views
#criamos a tabela que vai servidr de base para as real time views
create table customers_sl(customer_id int, customer_fname string, customer_lname string, customer_email string, customer_password string, customer_street string,  customer_city string, customer_state string, customer_zipcode string) row format delimited fields terminated by ',' location '/user/cloudera/customersspeed/';

#criamos as views real time
create view if not exists customer_state_rtv as select count(customer_state) soma, customer_state from customers_sl group by customer_state sort by soma desc;
create view if not exists customer_city_rtv as select count(customer_city) soma, customer_city from customers_sl group by customer_city sort by soma desc;

# union que deve ser a "soma" da batch view com real time view
select sum(soma) soma,  customer_state from  (  select soma,customer_state from bv_customer_state  union all   select soma,customer_state from customer_state_rtv) q1  group by customer_state sort by soma desc;
select sum(soma) soma,  customer_city from  (  select soma,customer_city from bv_customer_city  union all   select soma,customer_city from customer_city_rtv) q1  group by customer_city sort by soma desc;

#como sabemos que a consulta está somando batch view com real time view?
# a soma de florida foi 377
#vamos olhar o total de florida em batch view e real time view
select * from bv_customer_state;
select * from customer_state_rtv;

#será que as real time views atualizam automaticamente como as batch views? vamos fazer um teste
#mysql 
insert into customers (customer_id,customer_fname, customer_lname,customer_city,customer_state) values (12438,"Pedro","Antonio","Apopka","FL");
#executamos job da speed layer
sqoop job --exec speedlayer
# verificamos a real time view
select * from customer_state_rtv;

##########################################################################
#teste finais
########################################################################## 
#observar a contagem de FL
select * from bv_customer_state;
select * from customer_state_rtv;

insert into customers (customer_id,customer_fname, customer_lname,customer_city,customer_state) values (12439,"Ana","Maria","Apopka","FL");
insert into customers (customer_id,customer_fname, customer_lname,customer_city,customer_state) values (12440,"José","Maia","Apopka","FL");

./batch

select * from bv_customer_state;
select * from customer_state_rtv;

insert into customers (customer_id,customer_fname, customer_lname,customer_city,customer_state) values (12441,"Carla","Aparecida","Apopka","FL");
insert into customers (customer_id,customer_fname, customer_lname,customer_city,customer_state) values (12442,"Maria","Rosa","Apopka","FL");

./speed

select * from bv_customer_state;
select * from customer_state_rtv;
