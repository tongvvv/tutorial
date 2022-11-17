#include "_mysql.h"

int main(int argc,char* argv[])
{
  connection conn;

  if(conn.connecttodb("47.107.41.197,root,admin,mysql,3306","utf8") != 0 )
  {
    printf("connect database failed.\n%s\n",conn.m_cda.message); return -1;
  }

  sqlstatement stmt(&conn);
 
  stmt.prepare("create table girls(  \
                   id     bigint(10),\
                   name   varchar(10),\
                   weight decimal(8,2),\
                   btime  datetime,\
                   memo   longtext,\
                   pic    longblob,\
                   primary key(id))"\
              );

  if(stmt.execute() != 0)
  {
     printf("stmt.execute() failed .\n%s\n%d\n%s\n",stmt.m_sql,stmt.m_cda.rc,stmt.m_cda.message); return -1;
  }
  
  

  return 0;
}


