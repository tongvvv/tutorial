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

all:crtsurfdata1 crtsurfdata2 crtsurfdata3 crtsurfdata4 crtsurfdata5 crtsurfdata\
    obtcodetodb obtmindtodb

crtsurfdata1:crtsurfdata1.cpp
		g++ $(CFLAGS) -o crtsurfdata1 crtsurfdata1.cpp $(PUBINCL) $(PUBCPP) -lm -lc
		cp crtsurfdata1 ../bin/.

crtsurfdata2:crtsurfdata2.cpp
		g++ $(CFLAGS) -o crtsurfdata2 crtsurfdata2.cpp $(PUBINCL) $(PUBCPP) -lm -lc
		cp crtsurfdata2 ../bin/.

crtsurfdata3:crtsurfdata3.cpp
		g++ $(CFLAGS) -o crtsurfdata3 crtsurfdata3.cpp $(PUBINCL) $(PUBCPP) -lm -lc
		cp crtsurfdata3 ../bin/.

crtsurfdata4:crtsurfdata4.cpp
		g++ $(CFLAGS) -o crtsurfdata4 crtsurfdata4.cpp $(PUBINCL) $(PUBCPP) -lm -lc
		cp crtsurfdata4 ../bin/.

crtsurfdata5:crtsurfdata5.cpp
		g++ $(CFLAGS) -o crtsurfdata5 crtsurfdata5.cpp $(PUBINCL) $(PUBCPP) -lm -lc
		cp crtsurfdata5 ../bin/.

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
	rm -f crtsurfdata1  crtsurfdata2 crtsurfdata3 crtsurfdata4 crtsurfdata5 crtsurfdata
	rm -f obtcodetodb obtmindtodb
