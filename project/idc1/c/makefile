#开发框架头文件路径
PUBINCL = -I/project/public

#开发框架cpp文件名，直接包含进来
PUBCPP = /project/public/_public.cpp

#mysql头文件存放的目录。
MYSQLINCL = -I/usr/include/mysql -I/project/public/db/mysql

#mysql库文件存放的目录。
MYSQLLIB = -L/usr/lib64/mysql

#需要链接的mysql库。
MYSQLLIBS = -lmysqlclient

#开发框架中mysql的cpp文件名
MYSQLCPP = /project/public/db/mysql/_mysql.cpp

#编译参数
CFLAGS = -g

all:crtsurfdata obtcodetodb obtmindtodb

crtsurfdata:crtsurfdata.cpp
		g++ $(CFLAGS) -o crtsurfdata crtsurfdata.cpp $(PUBINCL) $(PUBCPP) -lm -lc
		cp crtsurfdata ../bin/.

obtcodetodb:obtcodetodb.cpp
		g++ $(CFLAGS) -o obtcodetodb obtcodetodb.cpp $(PUBINCL) $(PUBCPP) $(MYSQLINCL) $(MYSQLLIB) $(MYSQLCPP) $(MYSQLLIBS) -lm -lc 
		cp obtcodetodb ../bin/.

obtmindtodb:obtmindtodb.cpp idcapp.h idcapp.cpp
		g++ $(CFLAGS) -o obtmindtodb obtmindtodb.cpp idcapp.cpp $(PUBINCL) $(PUBCPP) $(MYSQLINCL) $(MYSQLLIB) $(MYSQLCPP) $(MYSQLLIBS) -lm -lc 
		cp obtmindtodb ../bin/.

clean:
	rm -f crtsurfdata
	rm -f obtcodetodb obtmindtodb
