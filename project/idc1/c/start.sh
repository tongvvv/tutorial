#####################################################
#启动数据中心后台服务程序的脚本。
#####################################################

#检查服务程序是否超时，配置在/etc/rc.local中由root用户执行
#/project/tools1/bin/procctl 30 /project/tools1/bin/checkproc

#压缩数据中心后台服务程序的备份日志
/project/tools1/bin/procctl 300 /project/tools1/bin/gzipfiles /log/idc "*.log.20*" 0.04

#生成用于测试的全国气象站点观测的分钟数据
/project/tools1/bin/procctl 60 /project/idc1/bin/crtsurfdata /project/idc1/ini/stcode.ini /tmp/idc/surfdata /log/idc/crtsurfdata.log xml,json,csv

#清理原始的全国气象站点观测的分钟数据目录/tmp/idc/surfdata的历史数据文件
/project/tools1/bin/procctl 300 /project/tools1/bin/deletefiles /tmp/idc/surfdata "*" 0.04

#采集全国气象站点观测的分钟数据
/project/tools1/bin/procctl 30 /project/tools1/bin/ftpgetfiles /log/idc/ftpgetfiles_surfdata.log "<host>101.34.83.118:21</host><mode>1</mode><username>tw</username><password>123456</password><localpath>/idcdata/surfdata</localpath><remotepath>/tmp/idc/surfdata</remotepath><matchname>SURF_ZH*.xml</matchname><listfilename>/idcdata/ftplist/ftpgetfiles_surfdata.list</listfilename><ptype>1</ptype><okfilename>/idcdata/ftplist/ftpgetfiles_surfdata.xml</okfilename><checkmtime>true</checkmtime><timeout>80</timeout><pname>ftpgetfiles_surfdata</pname>" 

#清理采集的原始的全国气象站点观测的分钟数据目录/idcdata/surfdata的历史数据文件
/project/tools1/bin/procctl 300 /project/tools1/bin/deletefiles /idcdata/surfdata "*" 0.04

#上传全国气象站点观测的分钟数据
/project/tools1/bin/procctl 30 /project/tools1/bin/ftpputfiles /log/idc/ftpputfiles_surfdata.log "<host>101.34.83.118:21</host><mode>1</mode><username>tw</username><password>123456</password><localpath>/tmp/idc/surfdata</localpath><remotepath>/tmp/ftpputest</remotepath><matchname>SURF_ZH*.xml</matchname><ptype>1</ptype><okfilename>/idcdata/ftplist/ftpputfiles_surfdata.xml</okfilename><timeout>80</timeout><pname>ftpputfiles_surfdata</pname>" 

#清理上传的原始的全国气象站点观测的分钟数据目录/tmp/ftpputest的历史数据文件
/project/tools1/bin/procctl 300 /project/tools1/bin/deletefiles /tmp/ftpputest "*" 0.04

#Tcp文件传输的服务程序
/project/tools1/bin/procctl 10 /project/tools1/bin/fileserver 5005 /log/idc/fileserver.log

#把目录/tmp/ftpputest中的文件上传到/tmp/tcpputest目录中
/project/tools1/bin/procctl 20 /project/tools1/bin/tcpputfiles /log/idc/tcpputfiles.log "<ip>101.34.83.118</ip><port>5005</port><ptype>1</ptype><clientpath>/tmp/ftpputest</clientpath><andchild>true</andchild><matchname>*.xml,*.csv,*.json</matchname><srvpath>/tmp/tcpputest</srvpath><timevl>10</timevl><timeout>50</timeout><pname>tcpputfiles_surfdata</pname>" 

#把目录/tmp/tcpputest中的文件下载到/tmp/tcpgetest目录中
/project/tools1/bin/procctl 20 /project/tools1/bin/tcpgetfiles /log/idc/tcpgetfiles.log "<ip>101.34.83.118</ip><port>5005</port><ptype>1</ptype><srvpath>/tmp/tcpputest</srvpath><andchild>true</andchild><matchname>*.xml,*.csv,*json</matchname><clientpath>/tmp/tcpgetest</clientpath><timevl>10</timevl><timeout>50</timeout><pname>tcpgetfiles_surfdata</pname>" 

#清理采集的全国气象站点分钟观测数据目录/tmp/tcpgetest中的历史文件
/project/tools1/bin/procctl 300 /project/tools1/bin/deletefiles /tmp/tcpgetest "*" 0.04

#把全国站点参数数据保存到数据库表中
/project/tools1/bin/procctl 200 /project/idc1/bin/obtcodetodb /project/idc1/ini/stcode.ini "101.34.83.118,root,admin,mysql,3306" utf8 /log/idc/obtcodetodb.log

#把全国气象站点分钟观测数据保存到数据库的T_ZHOBTMIND表中，数据只插入，不更新
/project/tools1/bin/procctl 10 /project/idc1/bin/obtmindtodb /idcdata/surfdata  "101.34.83.118,root,admin,mysql,3306" utf8   /log/idc/obtmindtodb.log

#清理表T_ZHOBTMIND中30分钟之前的数据，防止磁盘空间被撑满
#/project/tools1/bin/procctl 120 /project/tools1/bin/execsql /project/idc1/sql/cleardata.sql "101.34.83.118,root,admin,mysql,3306" utf8 /log/idc/execsql.log

#每隔一小时把数据从T_OBTCODE表中抽取出来
/project/tools1/bin/procctl 3600 /project/tools1/bin/dminingmysql /log/idc/dminingmysql_ZHOBTCODE.log "<connstr>101.34.83.118,root,admin,mysql,3306</connstr><charset>utf8</charset><selectsql>select obtid,cityname,provname,lat,lon,height from T_ZHOBTCODE</selectsql><fieldstr> obtid,cityname,provname,lat,lon,height</fieldstr><fieldlen>10,30,30,10,10,10</fieldlen><bfilename>ZHOBTCODE</bfilename><efilename>HYCZ</efilename><outpath>/idcdata/dmindata</outpath><timeout>10</timeout><pname>dminingmysql_ZHOBTCODE</pname>" 

#每隔30秒把数据从T_OBTMIND表中抽取出来
/project/tools1/bin/procctl 30 /project/tools1/bin/dminingmysql /log/idc/dminingmysql_ZHOBTMIND.log "<connstr>101.34.83.118,root,admin,mysql,3306</connstr><charset>utf8</charset><selectsql>select obtid,date_format(ddatetime,'%%Y-%%m-%%d %%H:%%i:%%s'),t,p,u,wd,wf,r,vis,keyid from T_ZHOBTMIND where keyid>:1 and ddatetime>timestampadd(minute,-120,now())</selectsql><fieldstr>obtid,ddatetime,t,p,u,wd,wf,r,vis,keyid</fieldstr><fieldlen>10,19,8,8,8,8,8,8,8,15</fieldlen><bfilename>ZHOBTMIND</bfilename><efilename>HYCZ</efilename><outpath>/idcdata/dmindata</outpath><starttime></starttime><incfield>keyid</incfield><incfilename>/idcdata/dmining/dminingmysql_ZHOBTMIND_HYCZ.list</incfilename><timeout>30</timeout><pname>dminingmysql_ZHOBTMIND_HYCZ</pname><maxcount>1000</maxcount><connstr1>101.34.83.118,root,admin,mysql,3306</connstr1>" 

#清理/idcdata/dmindata目录下的文件
/project/tools1/bin/procctl 300 /project/tools1/bin/deletefiles /idcdata/dmindata  "*" 0.04

#把/idcdata/dmindata目录中的xml发送到/idcdata/xmltodb/vip1 交由xmltodb程序去入库
/project/tools1/bin/procctl 20 /project/tools1/bin/tcpputfiles /log/idc/tcpputfiles.log "<ip>101.34.83.118</ip><port>5005</port><ptype>1</ptype><clientpath>/idcdata/dmindata</clientpath><andchild>false</andchild><matchname>*.xml,*.csv,*.json</matchname><srvpath>/idcdata/xmltodb/vip1</srvpath><timevl>10</timevl><timeout>50</timeout><pname>tcpputfiles_dmindata</pname>"

#把/idcdata/xmltodb/vip1目录中的xml文件入库
/project/tools1/bin/procctl 10 /project/tools1/bin/xmltodb /log/idc/xmltodb_vip1.log "<connstr>101.34.83.118,root,admin,mysql,3306</connstr><charset>utf8</charset><inifilename>/project/tools1/ini/xmltodb.xml</inifilename><xmlpath>/idcdata/xmltodb/vip1</xmlpath><xmlpathbak>/idcdata/xmltodb/vip1bak</xmlpathbak><xmlpatherr>/idcdata/xmltodb/vip1err</xmlpatherr><timevl>5</timevl><timeout>50</timeout><pname>xmltodb_vip1</pname>"

#清理/idcdata/xmltodb/vip1bak 和 /idcdata/xmltodb/vip1err 目录中文件，防止把空间撑满
/project/tools1/bin/procctl 300 /project/tools1/bin/deletefiles /idcdata/xmltodb/vip1bak  "*" 0.04
/project/tools1/bin/procctl 300 /project/tools1/bin/deletefiles /idcdata/xmltodb/vip1err  "*" 0.04

#采用全表刷新同步的方法，把表T_ZHOBTCODE1中的数据同步到表T_ZHOBTCODE2中
/project/tools1/bin/procctl 10 /project/tools1/bin/syncupdate /log/idc/syncupdate_ZHOBTCODE2.log "<localconnstr>101.34.83.118,root,admin,mysql,3306</localconnstr><charset>utf8</charset><fedtname>LK_ZHOBTCODE1</fedtname><localtname>T_ZHOBTCODE2</localtname><remotecols>obtid,cityname,provname,lat,lon,height/10,upttime,keyid</remotecols><localcols>stid,cityname,provname,lat,lon,altitude,upttime,keyid</localcols><where>where obtid like '54%%'</where><synctype>2</synctype><remoteconnstr>101.34.83.118,root,admin,mysql,3306</remoteconnstr><remotetname>T_ZHOBTCODE1</remotetname><remotekeycol>obtid</remotekeycol><localkeycol>stid</localkeycol><maxcount>10</maxcount><timeout>50</timeout><pname>syncupdate_ZHOBTCODE2</pname>"

#采用分批同步的方法，把表T_ZHOBTCODE1中 obtid like '54%'的记录同步到表T_ZHOBTCODE3中
/project/tools1/bin/procctl 10 /project/tools1/bin/syncupdate /log/idc/syncupdate_ZHOBTCODE3.log "<localconnstr>101.34.83.118,root,admin,mysql,3306</localconnstr><charset>utf8</charset><fedtname>LK_ZHOBTCODE1</fedtname><localtname>T_ZHOBTCODE3</localtname><remotecols>obtid,cityname,provname,lat,lon,height/10,upttime,keyid</remotecols><localcols>stid,cityname,provname,lat,lon,altitude,upttime,keyid</localcols><where>where obtid like '54%%'</where><synctype>2</synctype><remoteconnstr>101.34.83.118,root,admin,mysql,3306</remoteconnstr><remotetname>T_ZHOBTCODE1</remotetname><remotekeycol>obtid</remotekeycol><localkeycol>stid</localkeycol><maxcount>10</maxcount><timeout>50</timeout><pname>syncupdate_ZHOBTCODE3</pname>"

#采用增量同步的方法，把T_ZHOBTMIND1中全部记录同步到表T_ZHOBTMIND2中
/project/tools1/bin/procctl 10 /project/tools1/bin/syncincrement /log/idc/syncincrement_ZHOBTMIND2.log "<localconnstr>101.34.83.118,root,admin,mysql,3306</localconnstr><charset>utf8</charset><fedtname>LK_ZHOBTMIND1</fedtname><localtname>T_ZHOBTMIND2</localtname><remotecols>obtid,ddatetime,t,p,u,wd,wf,r,vis,upttime,keyid</remotecols><localcols>stid,ddatetime,t,p,u,wd,wf,r,vis,upttime,recid</localcols><synctype>2</synctype><remoteconnstr>101.34.83.118,root,admin,mysql,3306</remoteconnstr><remotetname>T_ZHOBTMIND1</remotetname><remotekeycol>keyid</remotekeycol><localkeycol>recid</localkeycol><maxcount>300</maxcount><timevl>2</timevl><timeout>50</timeout><pname>syncincrement_ZHOBTMIND2</pname>"

#采用增量同步的方法，把T_ZHOBTMIND1中 obtid like '54'的记录同步到表T_ZHOBTMIND3中
/project/tools1/bin/procctl 10 /project/tools1/bin/syncincrement /log/idc/syncincrement_ZHOBTMIND3.log "<localconnstr>101.34.83.118,root,admin,mysql,3306</localconnstr><charset>utf8</charset><fedtname>LK_ZHOBTMIND1</fedtname><localtname>T_ZHOBTMIND3</localtname><remotecols>obtid,ddatetime,t,p,u,wd,wf,r,vis,upttime,keyid</remotecols><localcols>stid,ddatetime,t,p,u,wd,wf,r,vis,upttime,recid</localcols><where> and obtid like '54%' </where><synctype>2</synctype><remoteconnstr>101.34.83.118,root,admin,mysql,3306</remoteconnstr><remotetname>T_ZHOBTMIND1</remotetname><remotekeycol>keyid</remotekeycol><localkeycol>recid</localkeycol><maxcount>300</maxcount><timevl>2</timevl><timeout>50</timeout><pname>syncincrement_ZHOBTMIND3</pname>"

#把T_ZHOBTMIND表中120分钟之前的数据迁移到T_ZHOBTMIND_HIS表中
/project/tools1/bin/procctl 3600 /project/tools1/bin/migratetable /log/idc/migratetable_ZHOBTMIND.log "<connstr>101.34.83.118,root,admin,mysql,3306</connstr><srctname>T_ZHOBTMIND</srctname><dsttname>T_ZHOBTMIND_HIS</dsttname><keycol>keyid</keycol><where>where ddatetime<timestampadd(minute,-120,now())</where><maxcount>300</maxcount><timeout>120</timeout><pname>migratetable_ZHOBTMIND</pname>"

#清理T_ZHOBTMIND1表中120分钟之前的数据
/project/tools1/bin/procctl 3600 /project/tools1/bin/deletetable /log/idc/deletetable_ZHOBTMIND1.log "<connstr>101.34.83.118,root,admin,mysql,3306</connstr><tname>T_ZHOBTMIND1</tname><keycol>keyid</keycol><where>where ddatetime<timestampadd(minute,-120,now())</where><timeout>120</timeout><pname>deletetable_ZHOBTMIND1</pname>"

#清理T_ZHOBTMIND2表中120分钟之前的数据
/project/tools1/bin/procctl 3600 /project/tools1/bin/deletetable /log/idc/deletetable_ZHOBTMIND2.log "<connstr>101.34.83.118,root,admin,mysql,3306</connstr><tname>T_ZHOBTMIND2</tname><keycol>recid</keycol><where>where ddatetime<timestampadd(minute,-120,now())</where><timeout>120</timeout><pname>deletetable_ZHOBTMIND2</pname>"

#清理T_ZHOBTMIND3表中120分钟之前的数据
/project/tools1/bin/procctl 3600 /project/tools1/bin/deletetable /log/idc/deletetable_ZHOBTMIND3.log "<connstr>101.34.83.118,root,admin,mysql,3306</connstr><tname>T_ZHOBTMIND3</tname><keycol>recid</keycol><where>where ddatetime<timestampadd(minute,-120,now())</where><timeout>120</timeout><pname>deletetable_ZHOBTMIND3</pname>"

#清理T_ZHOBTMIND_HIS表中240分钟之前的数据
/project/tools1/bin/procctl 3600 /project/tools1/bin/deletetable /log/idc/deletetable_ZHOBTMIND_HIS.log "<connstr>101.34.83.118,root,admin,mysql,3306</connstr><tname>T_ZHOBTMIND_HIS</tname><keycol>keyid</keycol><where>where ddatetime<timestampadd(minute,-240,now())</where><timeout>120</timeout><pname>deletetable_ZHOBTMIND_HIS</pname>"
##############################################################################
#每隔一小时把数据从T_OBTCODE表中抽取出来,放在/idcdata/xmltodb/vip2目录，给xmltodb_oracle入库到oracle
/project/tools1/bin/procctl 3600 /project/tools1/bin/dminingmysql /log/idc/dminingmysql_ZHOBTCODE_vip2.log "<connstr>101.34.83.118,root,admin,mysql,3306</connstr><charset>utf8</charset><selectsql>select obtid,cityname,provname,lat,lon,height from T_ZHOBTCODE</selectsql><fieldstr> obtid,cityname,provname,lat,lon,height</fieldstr><fieldlen>10,30,30,10,10,10</fieldlen><bfilename>ZHOBTCODE</bfilename><efilename>HYCZ</efilename><outpath>/idcdata/xmltodb/vip2</outpath><timeout>10</timeout><pname>dminingmysql_ZHOBTCODE_vip2</pname>"

#每隔30秒把数据从T_OBTMIND表中抽取出来,放在/idcdata/xmltodb/vip2目录，给xmltodb_oracle入库到oracle
/project/tools1/bin/procctl 30 /project/tools1/bin/dminingmysql /log/idc/dminingmysql_ZHOBTMIND_vip2.log "<connstr>101.34.83.118,root,admin,mysql,3306</connstr><charset>utf8</charset><selectsql>select obtid,date_format(ddatetime,'%%Y-%%m-%%d %%H:%%i:%%s'),t,p,u,wd,wf,r,vis,keyid from T_ZHOBTMIND where keyid>:1 and ddatetime>timestampadd(minute,-120,now())</selectsql><fieldstr>obtid,ddatetime,t,p,u,wd,wf,r,vis,keyid</fieldstr><fieldlen>10,19,8,8,8,8,8,8,8,15</fieldlen><bfilename>ZHOBTMIND</bfilename><efilename>HYCZ</efilename><outpath>/idcdata/xmltodb/vip2</outpath><starttime></starttime><incfield>keyid</incfield><timeout>30</timeout><pname>dminingmysql_ZHOBTMIND_HYCZ_vip2</pname><maxcount>1000</maxcount><connstr1>101.34.83.118,root,admin,mysql,3306</connstr1>"

#清理/idcdata/xmltodb/vip2bak 和 /idcdata/xmltodb/vip2err 目录中文件，防止把空间撑满
/project/tools1/bin/procctl 300 /project/tools1/bin/deletefiles /idcdata/xmltodb/vip2  "*" 0.04
/project/tools1/bin/procctl 300 /project/tools1/bin/deletefiles /idcdata/xmltodb/vip2bak  "*" 0.04
/project/tools1/bin/procctl 300 /project/tools1/bin/deletefiles /idcdata/xmltodb/vip2err  "*" 0.04

#启动数据服务总线，提供对外访问oracle数据库的接口
/project/tools1/bin/procctl 10 /project/tools1/bin/webserver /log/idc/webserver.log "<connstr>qxidc/qxidcpwd@tw_oracle</connstr><charset>Simplified Chinese_China.AL32UTF8</charset><port>8080</port>"
