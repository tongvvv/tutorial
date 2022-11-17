delete from T_USERINFO;
create sequence SEQ_USERINFO increment by 1 minvalue 1 nocycle;
insert into T_USERINFO(username,passwd,appname,keyid) values('ty','typwd','台风网',SEQ_USERINFO.nextval);
insert into T_USERINFO(username,passwd,appname,keyid) values('sms','smspwd','短信平台',SEQ_USERINFO.nextval);

exit;
