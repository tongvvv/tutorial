/*
    程序名：crtsurfdata2.cpp  本程序用于生成全国气象站点观测的分钟数据
    作者：童伟  --仅作为练习
*/
#include"_public.h"

//站点参数结构体
struct st_stcode
{
  char provname[31];//省  
  char obtid[11];   //站号
  char obtname[31]; //站名
  double lat;       //纬度
  double lon;       //经度
  double height;    //海拔高度
};

CLogFile logfile;  //日志类

vector<struct st_stcode> vstcode;

bool LoadSTcode(const char* inifile);


int main(int argc, char* argv[])
{
  if(argc != 4) 
  {
     printf("Using: ./crtsurfdata2 infile outpath logfile\n");
     printf("Example: /project/idc1/bin/crtsurfdata2 /project/idc1/ini/stcode.ini /tmp/surfdata /log/idc/crtsurfdata2.log \n\n");
     printf("infile 全国气象站点参数文件名\n");
     printf("outpath 全国气象站点数据文件存放的目录\n");
     printf("logfile 程序运行的日志文件名\n\n");
     
     return -1;
  } 

  if(logfile.Open(argv[3]) == false)
  {
     printf("logfile Open(%s) failed.\n",argv[3]); return -1;
  }
 
  logfile.Write("crtsutfdata2 开始运行。\n");

  //这里插入业务代码  
  //把站点参数文件加载到vstcode容器中
  if(LoadSTcode(argv[1])==false){return -1;}

  logfile.Write("crtsutfdata2 结束运行。\n");

  return 0;
}


//把站点参数文件加载到vstcode容器中
bool LoadSTcode(const char* inifile)
{
   //打开站点参数文件
   CFile File;
   if(File.Open(inifile,"r")==false)
   { logfile.Write("Open file (%s) failed.\n",inifile); return false; }
 
   char strBuffer[301];  

   CCmdStr CmdStr;

   struct st_stcode stcode;
   while(true)
   {
     //从站点参数文件中读取一行数据，如果读取完，退出循环
     if(File.Fgets(strBuffer,300,true)==false) {break;}
     
     logfile.Write("=%s=\n",strBuffer);

     //把数据拆分
     CmdStr.SplitToCmd(strBuffer,",",true);   
  
     if(CmdStr.CmdCount()!=6) { continue; }  //扔掉无效行

     //把站点参数的每个数据项保存到站点参数结构体中     
     CmdStr.GetValue(0,stcode.provname,30); //省
     CmdStr.GetValue(1,stcode.obtid,10);    //站名
     CmdStr.GetValue(2,stcode.obtname,30);  //站号
     CmdStr.GetValue(3,&stcode.lat);        //纬度
     CmdStr.GetValue(4,&stcode.lon);        //经度
     CmdStr.GetValue(5,&stcode.height);     //海拔
     
     //将结构体载入结构体容器中 
     vstcode.push_back(stcode);
   }
    //析构函数自动关闭文件
  return true;
}






