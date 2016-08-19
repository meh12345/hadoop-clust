#!/usr/bin/python2
import os
import subprocess
import commands
while True:
			os.system('clear');
			os.system('tput setaf 3')
			print "\n\n\n\t\t\t\t------------ welcome to hadoop world-------------\n\n\n"                			
			os.system('tput setaf 2')
			print """
					press 1 :	to start the namenode 
					press 2 :	to start the datanode
					press 3 :       to check all nodes 
					press 4 :       to start the jobtracker
					press 5 :	to start the tasktracker
					press 6 :	to start the client
					press 7 :	to put a file
					press 8 :	to put a job 
					press 9	:	to list active trackers
					press 10:	to list all files in hdfs
					press 11:	to enter or leave  the safe mode
					press 12:	to check the map-reduce portal
					press 13:	to check hdfs admin portal 
					press 14:	to exit	
			
			"""
			os.system('tput setaf 0')
			as1=int(raw_input("enter your choice : "))
			if as1 == 1:
				os.chdir('/')				
				#os.system('mkdir /asd')      
				os.chdir('/etc/hadoop')
				f = open('/etc/hadoop/hdfs-site.xml', 'w+')
				as2=raw_input("enter the file name which you want to give : ")				
				f.write('''<?xml version="1.0"?>					
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>dfs.name.dir</name>
<value>/'''+as2+'''</value>
</property>
</configuration>''')
				f.close()
				as3=commands.getoutput("ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{print  $1}'")
				f1=open('/etc/hadoop/core-site.xml','w+')
				f1.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://'''+as3+''':9001</value>
</property>
</configuration>
					''')
				f1.close()
				os.system('hadoop-daemon.sh stop namenode')
				#ch=os.system('hadoop namenode -format')				
				#print ch
				os.system('hadoop-daemon.sh start namenode')
				#print ch
				#ch=os.system('hadoop dfsadmin -report')
				#print ch
				#ch=os.system('hadoop fs -ls /')
			if as1 == 2:
				os.system('scp 192.168.10.133:/etc/hadoop/hdfs-site.xml  /root/Desktop')
				f = open('/root/Desktop/hdfs-site.xml', 'w+')
				as4=raw_input("enter the file name which you want to give : ")				
				f.write('''<?xml version="1.0"?>					
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>dfs.name.dir</name>
<value>/'''+as4+'''</value>
</property>
</configuration>''')
				f.close()
				os.system('scp /root/Desktop/hdfs-site.xml  192.168.10.133:/etc/hadoop')
				os.system('scp 192.168.10.133:/etc/hadoop/core-site.xml  /root/Desktop')
				as5=commands.getoutput("ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{print  $1}'")
				f1=open('/root/Desktop/core-site.xml','w+')
				f1.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://'''+as5+''':9001</value>
</property>
</configuration>''')
				f1.close()
				os.system('scp /root/Desktop/core-site.xml  192.168.10.133:/etc/hadoop')
				os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.133  hadoop-daemon.sh stop datanode ')
				os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.133  hadoop-daemon.sh start datanode ')
				os.system('scp 192.168.10.138:/etc/hadoop/hdfs-site.xml  /root/Desktop')
				f = open('/root/Desktop/hdfs-site.xml', 'w+')
				as4=raw_input("enter the file name which you want to give : ")				
				f.write('''<?xml version="1.0"?>					
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>dfs.name.dir</name>
<value>/'''+as4+'''</value>
</property>
</configuration>''')
				f.close()
				os.system('scp /root/Desktop/hdfs-site.xml  192.168.10.138:/etc/hadoop')
				os.system('scp 192.168.10.138:/etc/hadoop/core-site.xml  /root/Desktop')
				as5=commands.getoutput("ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{print  $1}'")
				f1=open('/root/Desktop/core-site.xml','w+')
				f1.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://'''+as5+''':9001</value>
</property>
</configuration>''')
				f1.close()
				os.system('scp /root/Desktop/core-site.xml  192.168.10.138:/etc/hadoop')
				os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.138  hadoop-daemon.sh stop datanode ')
				os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.138  hadoop-daemon.sh start datanode ')
				#os.chdir('/')
				#os.system('mkdir asd1')
				#os.chdir('/etc/hadoop')
				#os.system('vim core-site.xml')
				#os.system('hadoop-daemon.sh start datanode')	
				#os.system('ssh 192.168.10.137')
				#os.chdir('/')
				#os.system('mkdir asd1')
				#os.chdir('/etc/hadoop')
				#os.system('vim core-site.xml')
				#os.system('hadoop-daemon.sh start datanode')										
				#print ch
				#ch=os.system('hadoop dfsadmin -report')
				#print ch
				#ch=os.system('hadoop fs -ls /')
			if as1 == 3:
				os.system("hadoop dfsadmin -report | grep 'Datanodes available'")

			if as1 == 4:
				os.system('scp 192.168.10.143:/etc/hadoop/core-site.xml  /root/Desktop')
				f = open('/root/Desktop/core-site.xml', 'w+')
				as7=commands.getoutput("ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{print  $1}'")				
				f.write('''<?xml version="1.0"?>					
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://'''+as7+''':9001</value>
</property>
</configuration>''')
				f.close()
				os.system('scp /root/Desktop/core-site.xml  192.168.10.143:/etc/hadoop')
				os.system('scp 192.168.10.143:/etc/hadoop/mapred-site.xml  /root/Desktop')
				as5=commands.getoutput(" sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.143 ifconfig eth2 | grep 'inet addr:' | cut -d: -f2 | awk '{print  $1}'")
				f1=open('/root/Desktop/mapred-site.xml','w+')
				f1.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>mapred.job.tracker</name>
<value>'''+as5+''':9002</value>
</property>
</configuration>''')
				f1.close()
				os.system('scp /root/Desktop/mapred-site.xml  192.168.10.143:/etc/hadoop')
				os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.143  hadoop-daemon.sh stop jobtracker ')
	                        os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.143  hadoop-daemon.sh start jobtracker')		
			if as1 == 5:
				os.system('scp 192.168.10.136:/etc/hadoop/mapred-site.xml  /root/Desktop')
				as5=commands.getoutput("sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.143 ifconfig eth2 | grep 'inet addr:' | cut -d: -f2 | awk '{print  $1}'")
				f1=open('/root/Desktop/mapred-site.xml','w+')
				f1.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>mapred.job.tracker</name>
<value>'''+as5+''':9002</value>
</property>
</configuration>''')
				f1.close()
				os.system('scp /root/Desktop/mapred-site.xml  192.168.10.136:/etc/hadoop')
				os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.136  hadoop-daemon.sh stop tasktracker ')
	                        os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.136  hadoop-daemon.sh start tasktracker')	
				os.system('scp 192.168.10.133:/etc/hadoop/mapred-site.xml  /root/Desktop')
				as5=commands.getoutput("sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.143 ifconfig eth2 | grep 'inet addr:' | cut -d: -f2 | awk '{print  $1}'")
				f1=open('/root/Desktop/mapred-site.xml','w+')
				f1.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>mapred.job.tracker</name>
<value>'''+as5+''':9002</value>
</property>
</configuration>''')
				f1.close()
				os.system('scp /root/Desktop/mapred-site.xml  192.168.10.133:/etc/hadoop')
				os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.133  hadoop-daemon.sh stop tasktracker ')
	                        os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.133  hadoop-daemon.sh start tasktracker')
				os.system('scp 192.168.10.138:/etc/hadoop/mapred-site.xml  /root/Desktop')
				as5=commands.getoutput("sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.143 ifconfig eth2	 | grep 'inet addr:' | cut -d: -f2 | awk '{print  $1}'")
				f1=open('/root/Desktop/mapred-site.xml','w+')
				f1.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>mapred.job.tracker</name>
<value>'''+as5+''':9002</value>
</property>
</configuration>''')
				f1.close()
				os.system('scp /root/Desktop/mapred-site.xml  192.168.10.138:/etc/hadoop')
				os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.138  hadoop-daemon.sh stop tasktracker ')
	                        os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.138  hadoop-daemon.sh start tasktracker')					
			elif as1 == 6:
				os.system('scp 192.168.10.144:/etc/hadoop/core-site.xml  /root/Desktop')
				f = open('/root/Desktop/core-site.xml', 'w+')
				as7=commands.getoutput("ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{print  $1}'")				
				f.write('''<?xml version="1.0"?>					
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>fs.default.name</name>
<value>hdfs://'''+as7+''':9001</value>
</property>
</configuration>''')
				f.close()
				os.system('scp /root/Desktop/core-site.xml  192.168.10.144:/etc/hadoop')
				os.system('scp 192.168.10.144:/etc/hadoop/mapred-site.xml  /root/Desktop')
				as5=commands.getoutput(" sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.143 ifconfig eth2 | grep 'inet addr:' | cut -d: -f2 | awk '{print  $1}'")
				f1=open('/root/Desktop/mapred-site.xml','w+')
				f1.write('''<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>
<property>
<name>mapred.job.tracker</name>
<value>'''+as5+''':9002</value>
</property>
</configuration>''')
				f1.close()
				os.system('scp /root/Desktop/mapred-site.xml  192.168.10.144:/etc/hadoop')
			elif as1 == 7:
				os.system('cd /root/Desktop')
				ch1=raw_input('enter the  file name which you want to put in the clusture \n') 				
				tr=os.system('hadoop fs -put '+ch1+' /')
				if tr == 0:
					print "file succesfully uploaded "
				else:
					print "file not uploaded "
			elif as1 == 8:
				ch2=raw_input('enter the  file name which you want to count the words \n')
				ch3=raw_input('enter the  output folder  in which you want to store result \n')
				os.system('sshpass -p redhat ssh -o StrictHostKeyChecking=no root@192.168.10.144 hadoop jar  /usr/share/hadoop/hadoop-examples-1.2.1.jar  wordcount    /'+ch2+' /'+ch3)												
			elif as1 == 9:
				os.system('hadoop job -list-active-trackers')
			elif as1 == 10:
				os.system('hadoop fs -ls /')
			elif as1 == 11:
				print """
						press 1 :	to enter  the safemode 
						press 2 :	to leave the  safemode
					 	press 3 :	to  get   the   status
				      """
				ch5=int(raw_input('enter the  choice'))
				if ch5 == 1 :
  					os.system('hadoop dfsadmin -safemode  enter')
				elif ch5 == 2 :
					os.system('hadoop dfsadmin -safemode leave') 					
				elif ch5 == 3 :
					os.system('hadoop dfsadmin -safemode get')
			elif as1 == 12:
				os.system('firefox 192.168.10.143:50030/jobtracker.jsp')
			elif as1 == 13:
				os.system('firefox 192.168.10.130:50070/dfshealth.jsp')										  	 
			elif as1 == 14:
				exit()

			else:
				print "enter the correct choice\n"
				

	                raw_input("\t\tpress enter to continue\n");


