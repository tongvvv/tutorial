#ifndef IDCAPP_H
#define IDCAPP_H

#include"_public.h"
#include"_mysql.h"

struct st_zhobtmind
{
  char obtid[11];
  char ddatetime[21];
  char t[11];
  char p[11];
  char u[11];
  char wd[11];
  char wf[11];
  char r[11];
  char vis[11];
};


//全国站点分钟观测数据操作类
class CZHOBTMIND
{
public:
   connection *m_conn;       //数据库连接
   CLogFile   *m_logfile;    //日志

   sqlstatement m_stmt;      //插入表操作的sql

   char m_buffer[1024];      //从文件中读到的一行
   struct st_zhobtmind m_zhobtmind; //全国站点分钟观测数据结构

   CZHOBTMIND();
   CZHOBTMIND(connection* conn,CLogFile* logfile);

  ~CZHOBTMIND();

   void BindConnLog(connection* conn,CLogFile* logfile);
   bool SplitBuffer(char* strBuffer,bool bisxml);  //把从文件读到的一行数据拆分到m_zhobtmind结构体中去
   bool InsertTable();    //把m_zhobtmind结构体中的数据插入到T_ZHOBTMIND表中
};

#endif
