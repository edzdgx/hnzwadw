/*
Edward0603:
	-indent修改
	-已修改表格:
		INC_S_DETAIL
		ODS_S_DETAIL
		DW_S_DETAIL
		ODS_CITY_BC
		DM_S_AAOM_SR01
		INC_MAP_MODEL
		ODS_MAP_MODEL
		DM_S_AAOM_SR

		INC_S_PERSON
		ODS_S_PERSON 
		DW_DEPT_EMP
		DM_S_AAOM_PRE
Edward0604:
	-bug fix: compiles now
Edward0605:
	-已修改表格:
		INC_LOSE_NO_COM +省份
		ODS_LOSE_NO_COM +省份
		INC_LOSE_COM +省份
		ODS_LOSE_COM +省份
		DW_CUS_LOSE +省份
		DM_S_AAOM_LOSE +省份
		DM_S_AAOM +省份
		DM_S_AAOM_COM +省份
		DM_S_AAOM_FUNNEL +省份
	-need fix:
		DM_SALESMAN_SCORE 脚本中无内容
GZY0605: 
	-已修改表格:
		dw_service_orders
		服务订单
*/
CREATE OR REPLACE PROCEDURE SP_HNZW_S_LOAD_ADW IS
	------------申明变量
	CURRENT_RPT_DATE	DATE; ----当前最新的报表时间
	CURRENT_UPLOAD_DATE	DATE; ----当前已经处理完成时间
	QTY_COM				NUMBER; ---记录数
	V_TABLE_NAME		VARCHAR2(300);
	V_OBJECT_NAME		VARCHAR2(200);
	V_MSG				VARCHAR2(300);

BEGIN
	--所有年份小行业数据
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_INDUSTRY_SMALL;

	IF QTY_COM>0 then
		BEGIN
			V_TABLE_NAME		:= 'INC_INDUSTRY_SMALL';
			V_OBJECT_NAME		:= 'ODS_INDUSTRY_SMALL';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				品牌,
				吨级,
				城市,
				地区,
				年份,
				月份,
				销量,
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_INDUSTRY_SMALL a
			WHERE exists(SELECT 1 FROM INC_INDUSTRY_SMALL WHERE 年份=a.年份 and 月份=a.月份 );
			COMMIT;

			----删除ODS_INDUSTRY_SMALL中记录；
			DELETE FROM ODS_INDUSTRY_SMALL a
			 	WHERE exists(SELECT 1 FROM INC_INDUSTRY_SMALL WHERE 年份=a.年份 and 月份=a.月份 );
			COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_INDUSTRY_SMALL (品牌,
				吨级,
				城市,
				地区,
				年份,
				月份,
				销量,
				TIME_STAMP)
			SELECT 
				品牌,
				吨级,
				城市,
				地区,
				年份,
				月份,
				销量,
				sysdate
			FROM INC_INDUSTRY_SMALL a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_industry_small' ;
			COMMIT;

			INSERT INTO
				dw_industry_small 
			SELECT
				"品牌",
				case when upper("吨级")='<6T' then '＜6T'
					 when upper("吨级")='10-19T' then '10T-19T'
					 when upper("吨级")='>=40T' then '≥40T' else upper("吨级") END as 小行业,
				"城市",
				case when "城市" is null then '长沙'
					 when "城市"='其他' then '长沙'
					 when "城市"='湘西土家族苗族自治州' then '湘西'
					 when instr("城市",'市')>0 then substr("城市",1,instr("城市",'市')-1) else "城市" END as "城市01",
				"地区",
				"年份",
				"月份",
				to_date(to_char("年份")||(case when "月份"<10 then '0'||"月份" else to_char("月份") END )||'01','YYYY-MM-DD') as 年月,
				"销量"
			FROM
				ods_industry_small;
			COMMIT;

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_industry_small数据更新完成','日志记录');
			COMMIT;
		EXCEPTION 
			WHEN OTHERS THEN
				-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录'); 
			END;
		END IF;






	--商机表
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_BUSINESS_OPP;

	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_BUSINESS_OPP';
			V_OBJECT_NAME	:= 'ODS_BUSINESS_OPP';

				----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				员工姓名,
				商机状态,
				创建时间,
				最后跟进时间,
				最后修改时间,
				商机名称,
				客户名称,
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM
				ODS_BUSINESS_OPP a
			WHERE
				exists(SELECT 1 FROM INC_BUSINESS_OPP WHERE 员工姓名=a.员工姓名 );
			COMMIT;

			----删除ODS_BUSINESS_OPP中记录；
			DELETE FROM ODS_BUSINESS_OPP a
			 	WHERE exists(SELECT 1 FROM INC_BUSINESS_OPP WHERE 员工姓名=a.员工姓名);
			COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_BUSINESS_OPP
				("员工姓名",
				"商机状态",
				"创建时间",
				"最后跟进时间",
				"最后修改时间",
				"是否有竞品参与",
				"商机名称",
				"预计成交金额",
				"客户名称",
				"团队角色",
				TIME_STAMP)
			SELECT
				"员工姓名",
				"商机状态",
				"创建时间",
				"最后跟进时间",
				"最后修改时间",
				"是否有竞品参与",
				"商机名称",
				"预计成交金额",
				"客户名称",
				"团队角色",
				SYSDATE
			FROM
				inc_business_opp a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_business_opp' ;
			COMMIT;

			INSERT INTO dw_business_opp 
			SELECT
				"员工姓名",
				"商机状态",
				"创建时间",
				"最后跟进时间",
				"最后修改时间",
				"是否有竞品参与",
				"商机名称",
				"预计成交金额",
				"客户名称",
				"团队角色",
				time_stamp
			FROM
				ods_business_opp
			WHERE
				是否有竞品参与 is not null;
			COMMIT;

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_business_opp数据更新完成','日志记录');
			COMMIT;
		EXCEPTION 
			WHEN OTHERS THEN
			 	-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;







	--新增客户报表
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_CUSTOMER_NEW;

		IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_CUSTOMER_NEW';
			V_OBJECT_NAME	:= 'ODS_CUSTOMER_NEW';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"员工姓名",
				"客户名称",
				"成交状态",
				"最后跟进时间",
				"团队角色",
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_CUSTOMER_NEW a
			WHERE exists(SELECT 1 FROM INC_CUSTOMER_NEW WHERE 员工姓名=a.员工姓名 );
			COMMIT;

			----删除ODS_CUSTOMER_NEW中记录；
			DELETE FROM ODS_CUSTOMER_NEW a
			WHERE exists(SELECT 1 FROM INC_CUSTOMER_NEW WHERE 员工姓名=a.员工姓名);
			COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_CUSTOMER_NEW ("员工姓名",
				"客户名称",
				"成交状态",
				"最后跟进时间",
				"团队角色",
				TIME_STAMP)
			SELECT
				"员工姓名",
				"客户名称",
				"成交状态",
				"最后跟进时间",
				"团队角色",
				SYSDATE
			FROM
				INC_CUSTOMER_NEW a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_customer_new' ;
			COMMIT;

			INSERT INTO
				dw_customer_new 
			SELECT
				"员工姓名",
				"客户名称",
				"成交状态",
				"最后跟进时间",
				"团队角色",
				time_stamp
			FROM
				ODS_CUSTOMER_NEW
			WHERE
				最后跟进时间 is not null;
			COMMIT;

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_customer_new数据更新完成','日志记录');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	--丢单报告对象导出结果（无竞争丢单量）
	-- Edward0604: INC_LOSE_NO_COM, ODS_LOSE_NO_COM 省份已添加
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_LOSE_NO_COM;

	IF QTY_COM>0 then
		BEGIN
			V_TABLE_NAME	:= 'INC_LOSE_NO_COM';
			V_OBJECT_NAME	:= 'ODS_LOSE_NO_COM';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"负责人",
				"序号",
				"负责人主属部门",
				"业务类型",
				"最后修改时间",
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_LOSE_NO_COM a
			WHERE exists(SELECT 1 FROM INC_LOSE_NO_COM WHERE 序号=a.序号 );
			COMMIT;

			----删除ODS_LOSE_NO_COM中记录；
			DELETE FROM ODS_LOSE_NO_COM a
				WHERE exists(SELECT 1 FROM INC_LOSE_NO_COM WHERE 序号=a.序号);
				COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_LOSE_NO_COM
				("客户名称",
				"商机名称",
				"购买品牌",
				"型号",
				"成交日期",
				"成交方式",
				"成交价格",
				"首付比例",
				"丢单原因",
				"图片",
				"外部负责人",
				"锁定状态",
				"相关团队",
				"创建人",
				"负责人",
				"序号",
				"负责人主属部门",
				"业务类型",
				"生命状态",
				"归属部门",
				"创建时间",
				"最后修改人",
				"最后修改时间",
				"省份")
			SELECT
				"客户名称",
				"商机名称",
				"购买品牌",
				"型号",
				"成交日期",
				"成交方式",
				"成交价格",
				"首付比例",
				"丢单原因",
				"图片",
				"外部负责人",
				"锁定状态",
				"相关团队",
				"创建人",
				"负责人",
				"序号",
				"负责人主属部门",
				"业务类型",
				"生命状态",
				"归属部门",
				"创建时间",
				"最后修改人",
				"最后修改时间",
				"省份"
			FROM
				INC_LOSE_NO_COM a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
				-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
			END;
	END IF;






	--丢单报告对象导出结果（有竞争丢单量）
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_LOSE_COM;

	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_LOSE_COM';
			V_OBJECT_NAME	:= 'ODS_LOSE_COM';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"负责人",
				"序号",
				"负责人主属部门",
				"业务类型",
				"最后修改时间",
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_LOSE_COM a
			WHERE exists(SELECT 1 FROM INC_LOSE_COM WHERE 序号=a.序号);
			COMMIT;

			----删除ODS_LOSE_COM中记录；
			DELETE FROM ODS_LOSE_COM a
			WHERE exists(SELECT 1 FROM INC_LOSE_COM WHERE 序号=a.序号);
			COMMIT;

			----插入增量表中数据
			-- Edward0605: INC_LOSE_COM, ODS_LOSE_COM添加省份
			INSERT INTO ODS_LOSE_COM
				("客户名称",
				"商机名称",
				"购买品牌",
				"型号",
				"成交日期",
				"成交方式",
				"成交价格",
				"首付比例",
				"丢单原因",
				"图片",
				"外部负责人",
				"锁定状态",
				"相关团队",
				"创建人",
				"负责人",
				"序号",
				"负责人主属部门",
				"业务类型",
				"生命状态",
				"归属部门",
				"创建时间",
				"最后修改人",
				"最后修改时间",
				"省份")
			SELECT
				"客户名称",
				"商机名称",
				"购买品牌",
				"型号",
				"成交日期",
				"成交方式",
				"成交价格",
				"首付比例",
				"丢单原因",
				"图片",
				"外部负责人",
				"锁定状态",
				"相关团队",
				"创建人",
				"负责人",
				"序号",
				"负责人主属部门",
				"业务类型",
				"生命状态",
				"归属部门",
				"创建时间",
				"最后修改人",
				"最后修改时间",
				"省份"
			FROM
				INC_LOSE_COM a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	--客户面访量
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_CUSTOMER_FACE;

	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_CUSTOMER_FACE';
			V_OBJECT_NAME	:= 'ODS_CUSTOMER_FACE';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"外勤类型名称",
				"负责人",
				"名称",
				"客户",
				"完成时间",
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_CUSTOMER_FACE a
			WHERE exists(SELECT 1 FROM INC_CUSTOMER_FACE WHERE 名称=a.名称);
			COMMIT;

			----删除ODS_CUSTOMER_FACE中记录；
			DELETE FROM ODS_CUSTOMER_FACE a
				WHERE exists(SELECT 1 FROM INC_CUSTOMER_FACE WHERE 名称=a.名称);
				COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_CUSTOMER_FACE
				("外勤类型名称",
				"负责人",
				"名称",
				"客户",
				"完成时间",
				time_stamp)
			SELECT
				"外勤类型名称",
				"负责人",
				"名称",
				"客户",
				"完成时间",
				SYSDATE
			FROM
				INC_CUSTOMER_FACE a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_customer_face' ;
			COMMIT;

			INSERT INTO
				dw_customer_face 
			SELECT
				"外勤类型名称",
				"负责人",
				"名称",
				"客户",
				"完成时间",
				time_stamp
			FROM
				ods_customer_face
			WHERE
				客户 is not null;
			COMMIT;

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_customer_face数据更新完成','日志记录');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
				-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	--一线人员3.9日
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_S_PERSON;
	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_S_PERSON';
			V_OBJECT_NAME	:= 'ODS_S_PERSON';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"序号",
				"工号",
				"姓名",
				"体系",
				"部门",
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_S_PERSON a
			WHERE exists(SELECT 1 FROM INC_S_PERSON WHERE 工号=a.工号);
			COMMIT;

			----删除ODS_S_PERSON中记录；
			DELETE FROM ODS_S_PERSON a
			WHERE exists(SELECT 1 FROM INC_S_PERSON WHERE 工号=a.工号);
			COMMIT;

			----插入增量表中数据
			-- Edward0601: INC_S_PERSON, ODS_S_PERSON已修改字段
			INSERT INTO ODS_S_PERSON
				("序号",
				"工号",
				"姓名",
				"体系",
				"部门",
				"科室",
				"岗位",
				"入职时间",
				"岗位类别",
				"职级",
				"省份", --更名
				"司龄", --更名
				"工作状态",
				time_stamp)
			SELECT
				"序号",
				"工号",
				"姓名",
				"体系",
				"部门",
				"科室",
				"岗位",
				"入职时间",
				"岗位类别",
				"职级",
				"省份",
				"司龄",
				"工作状态",
				SYSDATE
			FROM
				INC_S_PERSON a;
			COMMIT;



			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_DEPT_EMP' ;
			COMMIT;

			-- Edward0601: DW_DEPT_EMP, ODS_S_PERSON 字段已修改
			INSERT INTO DW_DEPT_EMP(
				工号,
				负责人,
				负责人主属部门,
				岗位,
				入职时间,
				司龄,
				所属分公司,
				工作状态,
				TIME_STAMP,
				省份 -- Edward0601: 省份字段由ODS_S_PERSON中取
			)
			SELECT
				工号,
				case when instr(姓名,'01')>0 then substr(姓名,1,instr(姓名,'01')-1) 
					 when instr(姓名,'02')>0 then substr(姓名,1,instr(姓名,'02')-1) else 姓名 END, 
				科室,
				岗位,
				入职时间,
				司龄,
				case when instr(科室,'分公司') >0 then substr(科室,1,instr(科室,'分公司')-1) 
					 when instr(科室,'办') >0 and instr(科室,'湖南') >0 then substr(科室,instr(科室,'湖南')+2,instr(科室,'办')-instr(科室,'湖南')-2) 
					 when instr(科室,'办') >0 and instr(科室,'江西')>0 then substr(科室,instr(科室,'江西')+2,instr(科室,'办')-instr(科室,'江西')-2) 
					 when instr(科室,'办事处') >0 then substr(科室,1,instr(科室,'办事处')-1) else 科室 END,
				工作状态,
				SYSDATE as time_stamp,
				省份 -- 新增的省份字段 INSERT进DW_DEPT_EMP
			FROM ODS_S_PERSON;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'DW_DEPT_EMP数据更新完成','日志记录');
			COMMIT;

		EXCEPTION
			WHEN OTHERS THEN
				-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	--营销人员里程报表
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_S_PERSON_MILEAGE;

	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_S_PERSON_MILEAGE';
			V_OBJECT_NAME	:= 'ODS_S_PERSON_MILEAGE';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"序号",
				"设备名称",
				"日期",
				"总里程KM",
				"业务员",
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_S_PERSON_MILEAGE a
			WHERE exists(SELECT 1 FROM INC_S_PERSON_MILEAGE WHERE 序号=a.序号);
			COMMIT;

			----删除ODS_S_PERSON_MILEAGE中记录；
			DELETE FROM ODS_S_PERSON_MILEAGE a
			WHERE exists(SELECT 1 FROM INC_S_PERSON_MILEAGE WHERE 序号=a.序号);
			COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_S_PERSON_MILEAGE ("序号",
				"设备名称",
				imei,
				"型号",
				"日期",
				"总里程KM",
				"业务员",
				"司机名称",
				"车牌号",
				time_stamp)
			SELECT
				"序号",
				"设备名称",
				imei,
				"型号",
				"日期",
				"总里程KM",
				"业务员",
				"司机名称",
				"车牌号",
				SYSDATE
			FROM
				inc_s_person_mileage a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_S_MILEAGE' ;
			COMMIT;

			INSERT INTO DW_S_MILEAGE(
				"销售代表",
				imei,
				"型号",
				"年度",
				"月度",
				"日期",
				"分公司",
				"地区",
				"总里程KM",
				"业务员",
				"司机名称",
				"车牌号"
			) 
			SELECT
				"设备名称" as 销售代表,
				IMEI,
				"型号",
				substr(to_char(日期,'yyyymmdd'),1,4) as 年度,
				substr(to_char(日期,'yyyymmdd'),5,2) as 月度,
				to_date (to_char(日期,'yyyymmdd'),'yyyymmdd') as 日期,
				b.负责人主属部门 as 分公司,
				case when b.所属分公司='大客户部' then '长沙'
					 when b.所属分公司='常张' then '常德'
					 when b.所属分公司='装载机事业部' then '长沙'
					 when b.所属分公司='长潭' then '长沙'
					 when b.所属分公司='九江兼景德镇' then '九江'
					 when b.所属分公司='赣州西' then '赣州'
					 when b.所属分公司 is null then '长沙'
					 else b.所属分公司 END 所属分公司,
				"总里程KM",
				"业务员",
				"司机名称",
				"车牌号"
			FROM
				ods_s_person_mileage a,DW_DEPT_EMP b
			WHERE a.设备名称 = b.负责人(+);
			COMMIT;

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'DW_S_MILEAGE数据更新完成','日志记录');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
				-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	--营销人员行程报表
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM inc_s_person_travel;

	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'inc_s_person_travel';
			V_OBJECT_NAME	:= 'ODS_S_PERSON_TRAVEL';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"设备名称",
				"总里程KM",
				"起点",
				"终点",
				"开始时间",
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_S_PERSON_TRAVEL a
			WHERE exists(SELECT 1 FROM inc_s_person_travel WHERE 序号=a.序号);
			COMMIT;

			----删除ODS_S_PERSON_TRAVEL中记录；
			DELETE FROM ODS_S_PERSON_TRAVEL a
			WHERE exists(SELECT 1 FROM inc_s_person_travel WHERE 序号=a.序号);
			COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_S_PERSON_TRAVEL ("序号",
				"设备名称",
				imei,
				"型号",
				"开始时间",
				"结束时间",
				"起点",
				"终点",
				"总里程KM",
				"总用时时间",
				"平均速度KMH",
				"业务员",
				"司机名称",
				"车牌号",
				time_stamp)
			SELECT
				"序号",
				"设备名称",
				imei,
				"型号",
				"开始时间",
				"结束时间",
				"起点",
				"终点",
				"总里程KM",
				"总用时时间",
				"平均速度KMH",
				"业务员",
				"司机名称",
				"车牌号",
				SYSDATE
			FROM
				inc_s_person_travel a;
			COMMIT;



			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;


			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_S_TRAVEL' ;
			COMMIT;

			INSERT INTO DW_S_TRAVEL( "序号",
				"设备名称",
				imei,
				"年度",
				"月度",
				"日期",
				"分公司",
				"地区",
				"开始时间",
				"结束时间",
				"起点省",
				"起点市",
				"起点区县",
				"起点",
				"终点省",
				"终点市",
				"终点区县",
				"终点",
				"总里程KM",
				"总用时时间",
				"平均速度KMH",
				"司机名称",
				"车牌号") 
			SELECT
				序号,
				"设备名称",
				imei,
				substr(to_char(开始时间,'yyyymmdd'),1,4) as 年度,
				substr(to_char(开始时间,'yyyymmdd'),5,2) as 月度,
				to_date (to_char(开始时间,'yyyymmdd'),'yyyymmdd') as 日期,
				b.负责人主属部门 as 分公司,
				case when b.所属分公司='大客户部' then '长沙'
					 when b.所属分公司='常张' then '常德'
					 when b.所属分公司='装载机事业部' then '长沙'
					 when b.所属分公司='长潭' then '长沙'
					 when b.所属分公司='九江兼景德镇' then '九江'
					 when b.所属分公司='赣州西' then '赣州'
					 when b.所属分公司 is null then '长沙'
					 else b.所属分公司 END 所属分公司,
				"开始时间",
				"结束时间",
				case when instr("起点",'省')>0 then substr( "起点",1,instr("起点",'省')-1) 
					 when instr("起点",'广西壮族自治区')>0 then '广西'
					 when instr("起点",'上海')>0 then '上海'
					 when instr("起点",'北京')>0 then '北京'
					 when instr("起点",'重庆')>0 then '重庆'
					 when instr("起点",'天津')>0 then '天津'
					 when instr("起点",'香港特别行政区')>0 then '香港'
					 when instr("起点",'澳门特别行政区')>0 then '澳门'
					 when instr("起点",'宁夏回族自治区')>0 then '宁夏'
					 when instr("起点",'内蒙古自治区')>0 then '内蒙古'
					 when instr("起点",'新疆')>0 then '新疆'
					 when instr("起点",'西藏自治区')>0 then '西藏' else "起点" END as 起点省,
				case when instr("起点",'省')>0 then 
					(case when instr("起点",'省辖县')>0 then substr( "起点",instr("起点",'省辖县')+3)
						  when instr("起点",'自治州')>0 then substr( "起点",instr("起点",'省')+1,instr("起点",'自治州')-instr("起点",'省')+2)
						  else substr( "起点",instr("起点",'省')+1,instr("起点",'市')-instr("起点",'省')-1) END)
					 when instr("起点",'广西壮族自治区')>0 then
					(case when instr("起点",'自治州')>0 then substr( "起点",instr("起点",'自治区')+3,instr("起点",'自治州')-instr("起点",'自治区')+2)
						  else substr( "起点",instr("起点",'自治区')+3,instr("起点",'市')-instr("起点",'自治区')) END)
					 when instr("起点",'上海')>0 then '上海'
					 when instr("起点",'北京')>0 then '北京'
					 when instr("起点",'重庆')>0 then '重庆'
					 when instr("起点",'天津')>0 then '天津'
					 when instr("起点",'香港特别行政区')>0 then '香港'
					 when instr("起点",'澳门特别行政区')>0 then '澳门'
					 when instr("起点",'宁夏回族自治区')>0 then
					(case when instr("起点",'自治州')>0 then substr( "起点",instr("起点",'自治区')+3,instr("起点",'自治州')-instr("起点",'自治区')+2)
						  else substr( "起点",instr("起点",'自治区')+3,instr("起点",'市')-instr("起点",'自治区')) END )
					 when instr("起点",'内蒙古自治区')>0 then
					(case when instr("起点",'自治州')>0 then substr( "起点",instr("起点",'自治区')+3,instr("起点",'自治州')-instr("起点",'自治区')+2)
						  else substr( "起点",instr("起点",'自治区')+3,instr("起点",'市')-instr("起点",'自治区'))END)
					 when instr("起点",'新疆维吾尔自治区')>0 then 
					(case when instr("起点",'自治州')>0 then substr( "起点",instr("起点",'自治区')+3,instr("起点",'自治州')-instr("起点",'自治区')+2)
						  else substr( "起点",instr("起点",'自治区')+3,instr("起点",'市')-instr("起点",'自治区'))END )
					 when instr("起点",'西藏自治区')>0 then
					(case when instr("起点",'自治州')>0 then substr( "起点",instr("起点",'自治区')+3,instr("起点",'自治州')-instr("起点",'自治区')+2)
						  else substr( "起点",instr("起点",'自治区')+3,instr("起点",'市')-instr("起点",'自治区'))END)
					 else "起点" END as 起点市,
				case when instr("起点",'省辖县')>0 then substr( "起点",instr("起点",'省辖县')+3) 
					 when instr("起点",'自治州')>0 then 
						(case when instr("起点",',')>0 then 
							 (case when instr(substr( "起点",instr("起点",'自治州')+3,instr("起点",',')-instr("起点",'自治州')-3),'县') >0 then substr(substr( "起点",instr("起点",'自治州')+3,instr("起点",',')-instr("起点",'自治州')-3),1,instr(substr( "起点",instr("起点",'自治州')+3,instr("起点",',')-instr("起点",'自治州')-3),'县',1)) 
								   when instr(substr( "起点",instr("起点",'自治州')+3,instr("起点",',')-instr("起点",'自治州')-3),'市') >0 then substr(substr( "起点",instr("起点",'自治州')+3,instr("起点",',')-instr("起点",'自治州')-3),1,instr(substr( "起点",instr("起点",'自治州')+3,instr("起点",',')-instr("起点",'自治州')-3),'市',1))
								   when instr(substr( "起点",instr("起点",'自治州')+3,instr("起点",',')-instr("起点",'自治州')-3),'区') >0 then substr(substr( "起点",instr("起点",'自治州')+3,instr("起点",',')-instr("起点",'自治州')-3),1,instr(substr( "起点",instr("起点",'自治州')+3,instr("起点",',')-instr("起点",'自治州')-3),'区',1)) 
							  END)
								   else (case when instr(substr( "起点",instr("起点",'自治州')+3),'县') >0 then substr(substr( "起点",instr("起点",'自治州')+3),1,instr(substr( "起点",instr("起点",'自治州')+3),'县',1)) 
								   when instr(substr( "起点",instr("起点",'自治州')+3),'市') >0 then substr(substr( "起点",instr("起点",'自治州')+3),1,instr(substr( "起点",instr("起点",'自治州')+3),'市',1))
								   when instr(substr( "起点",instr("起点",'自治州')+3),'区') >0 then substr(substr( "起点",instr("起点",'自治州')+3),1,instr(substr( "起点",instr("起点",'自治州')+3),'区',1)) 
							  END)
						 END)
					 when instr("起点",'市')>0 then
						(case when instr("起点",',')>0 then 
							(case when instr(substr( "起点",instr("起点",'市')+1,instr("起点",',')-instr("起点",'市')-1),'县') >0 then substr(substr( "起点",instr("起点",'市')+1,instr("起点",',')-instr("起点",'市')-1),1,instr(substr( "起点",instr("起点",'市')+1,instr("起点",',')-instr("起点",'市')-1),'县',1)) 
								  when instr(substr( "起点",instr("起点",'市')+1,instr("起点",',')-instr("起点",'市')-1),'雨花区高桥大市场') >0 then '雨花区'
								  when instr(substr( "起点",instr("起点",'市')+1,instr("起点",',')-instr("起点",'市')-1),'市') >0 then substr(substr( "起点",instr("起点",'市')+1,instr("起点",',')-instr("起点",'市')-1),1,instr(substr( "起点",instr("起点",'市')+1,instr("起点",',')-instr("起点",'市')-1),'市',1))
								  when instr(substr( "起点",instr("起点",'市')+1,instr("起点",',')-instr("起点",'市')-1),'区') >0 then substr(substr( "起点",instr("起点",'市')+1,instr("起点",',')-instr("起点",'市')-1),1,instr(substr( "起点",instr("起点",'市')+1,instr("起点",',')-instr("起点",'市')-1),'区',1)) 
							 END)
					 	else
					 		(case when instr(substr( "起点",instr("起点",'市')+1),'县') >0 then substr(substr( "起点",instr("起点",'市')+1),1,instr(substr( "起点",instr("起点",'市')+1),'县',1)) 
						 	 	  when instr(substr( "起点",instr("起点",'市')+1,instr("起点",',')-instr("起点",'市')-1),'雨花区高桥大市场') >0 then '雨花区'
						 	 	  when instr(substr( "起点",instr("起点",'市')+1),'市') >0 then substr(substr( "起点",instr("起点",'市')+1),1,instr(substr( "起点",instr("起点",'市')+1),'市',1))
						 	 	  when instr(substr( "起点",instr("起点",'市')+1),'区') >0 then substr(substr( "起点",instr("起点",'市')+1),1,instr(substr( "起点",instr("起点",'市')+1),'区',1)) 
						 	 END)
					 	 END)
					 else "起点" END as 起点区县,
				"起点",
				case when instr("终点",'省')>0 then substr( "终点",1,instr("终点",'省')-1) 
					 when instr("终点",'广西壮族自治区')>0 then '广西'
					 when instr("终点",'上海')>0 then '上海'
					 when instr("终点",'北京')>0 then '北京'
					 when instr("终点",'重庆')>0 then '重庆'
					 when instr("终点",'天津')>0 then '天津'
					 when instr("终点",'香港特别行政区')>0 then '香港'
					 when instr("终点",'澳门特别行政区')>0 then '澳门'
					 when instr("终点",'宁夏回族自治区')>0 then '宁夏'
					 when instr("终点",'内蒙古自治区')>0 then '内蒙古'
					 when instr("终点",'新疆')>0 then '新疆'
					 when instr("终点",'西藏自治区')>0 then '西藏' else "终点" END as 终点省,
				case when instr("终点",'省')>0 then 
					(case when instr("终点",'省辖县')>0 then substr( "终点",instr("终点",'省辖县')+3)
						  when instr("终点",'自治州')>0 then substr( "终点",instr("终点",'省')+1,instr("终点",'自治州')-instr("终点",'省')+2)
						  else substr( "终点",instr("终点",'省')+1,instr("终点",'市')-instr("终点",'省')-1) END)
					 when instr("终点",'广西壮族自治区')>0 then
				 	(case when instr("终点",'自治州')>0 then substr( "终点",instr("终点",'自治区')+3,instr("终点",'自治州')-instr("终点",'自治区')+2)
						  else substr( "终点",instr("终点",'自治区')+3,instr("终点",'市')-instr("终点",'自治区'))END)
					 when instr("终点",'上海')>0 then '上海'
					 when instr("终点",'北京')>0 then '北京'
					 when instr("终点",'重庆')>0 then '重庆'
					 when instr("终点",'天津')>0 then '天津'
					 when instr("终点",'香港特别行政区')>0 then '香港'
					 when instr("终点",'澳门特别行政区')>0 then '澳门'
					 when instr("终点",'宁夏回族自治区')>0 then
						 (case when instr("终点",'自治州')>0 then substr( "终点",instr("终点",'自治区')+3,instr("终点",'自治州')-instr("终点",'自治区')+2)
							   else substr( "终点",instr("终点",'自治区')+3,instr("终点",'市')-instr("终点",'自治区')) END)
					 when instr("终点",'内蒙古自治区')>0 then
						 (case when instr("终点",'自治州')>0 then substr( "终点",instr("终点",'自治区')+3,instr("终点",'自治州')-instr("终点",'自治区')+2)
							   else substr( "终点",instr("终点",'自治区')+3,instr("终点",'市')-instr("终点",'自治区')) END)
					 when instr("终点",'新疆维吾尔自治区')>0 then 
						 (case when instr("终点",'自治州')>0 then substr( "终点",instr("终点",'自治区')+3,instr("终点",'自治州')-instr("终点",'自治区')+2)
							   else substr( "终点",instr("终点",'自治区')+3,instr("终点",'市')-instr("终点",'自治区')) END)
					 when instr("终点",'西藏自治区')>0 then
						 (case when instr("终点",'自治州')>0 then substr( "终点",instr("终点",'自治区')+3,instr("终点",'自治州')-instr("终点",'自治区')+2)
							   else substr( "终点",instr("终点",'自治区')+3,instr("终点",'市')-instr("终点",'自治区')) END)
					 else "终点" END as 终点市,
				case when instr("终点",'省辖县')>0 then substr( "终点",instr("终点",'省辖县')+3) 
					 when instr("终点",'自治州')>0 then 
						(case when instr("终点",',')>0 then 
							(case when instr(substr( "终点",instr("终点",'自治州')+3,instr("终点",',')-instr("终点",'自治州')-3),'县') >0 then substr(substr( "终点",instr("终点",'自治州')+3,instr("终点",',')-instr("终点",'自治州')-3),1,instr(substr( "终点",instr("终点",'自治州')+3,instr("终点",',')-instr("终点",'自治州')-3),'县',1)) 
								  when instr(substr( "终点",instr("终点",'自治州')+3,instr("终点",',')-instr("终点",'自治州')-3),'市') >0 then substr(substr( "终点",instr("终点",'自治州')+3,instr("终点",',')-instr("终点",'自治州')-3),1,instr(substr( "终点",instr("终点",'自治州')+3,instr("终点",',')-instr("终点",'自治州')-3),'市',1))
								  when instr(substr( "终点",instr("终点",'自治州')+3,instr("终点",',')-instr("终点",'自治州')-3),'区') >0 then substr(substr( "终点",instr("终点",'自治州')+3,instr("终点",',')-instr("终点",'自治州')-3),1,instr(substr( "终点",instr("终点",'自治州')+3,instr("终点",',')-instr("终点",'自治州')-3),'区',1)) 
							 END)
						 else
						 	(case when instr(substr( "终点",instr("终点",'自治州')+3),'县') >0 then substr(substr( "终点",instr("终点",'自治州')+3),1,instr(substr( "终点",instr("终点",'自治州')+3),'县',1)) 
								  when instr(substr( "终点",instr("终点",'自治州')+3),'市') >0 then substr(substr( "终点",instr("终点",'自治州')+3),1,instr(substr( "终点",instr("终点",'自治州')+3),'市',1))
								  when instr(substr( "终点",instr("终点",'自治州')+3),'区') >0 then substr(substr( "终点",instr("终点",'自治州')+3),1,instr(substr( "终点",instr("终点",'自治州')+3),'区',1)) 
							 END)
						 END)
					 when instr("终点",'市')>0 then
						(case when instr("终点",',')>0 then 
							(case when instr(substr( "终点",instr("终点",'市')+1,instr("终点",',')-instr("终点",'市')-1),'县') >0 then substr(substr( "终点",instr("终点",'市')+1,instr("终点",',')-instr("终点",'市')-1),1,instr(substr( "终点",instr("终点",'市')+1,instr("终点",',')-instr("终点",'市')-1),'县',1)) 
								  when instr(substr( "终点",instr("终点",'市')+1,instr("终点",',')-instr("终点",'市')-1),'雨花区高桥大市场') >0 then '雨花区'
								  when instr(substr( "终点",instr("终点",'市')+1,instr("终点",',')-instr("终点",'市')-1),'市') >0 then substr(substr( "终点",instr("终点",'市')+1,instr("终点",',')-instr("终点",'市')-1),1,instr(substr( "终点",instr("终点",'市')+1,instr("终点",',')-instr("终点",'市')-1),'市',1))
								  when instr(substr( "终点",instr("终点",'市')+1,instr("终点",',')-instr("终点",'市')-1),'区') >0 then substr(substr( "终点",instr("终点",'市')+1,instr("终点",',')-instr("终点",'市')-1),1,instr(substr( "终点",instr("终点",'市')+1,instr("终点",',')-instr("终点",'市')-1),'区',1)) 
							 END)
						 else
						 	(case when instr(substr( "终点",instr("终点",'市')+1),'县') >0 then substr(substr( "终点",instr("终点",'市')+1),1,instr(substr( "终点",instr("终点",'市')+1),'县',1)) 
								  when instr(substr( "终点",instr("终点",'市')+1,instr("终点",',')-instr("终点",'市')-1),'雨花区高桥大市场') >0 then '雨花区'
								  when instr(substr( "终点",instr("终点",'市')+1),'市') >0 then substr(substr( "终点",instr("终点",'市')+1),1,instr(substr( "终点",instr("终点",'市')+1),'市',1))
								  when instr(substr( "终点",instr("终点",'市')+1),'区') >0 then substr(substr( "终点",instr("终点",'市')+1),1,instr(substr( "终点",instr("终点",'市')+1),'区',1)) 
							 END)
						 END)
					 else "终点" END as 终点区县,
				"终点",
				"总里程KM",
				"总用时时间",
				"平均速度KMH",
				"司机名称",
				"车牌号"
			FROM
				ods_s_person_travel a,
				DW_DEPT_EMP b
			WHERE
				a.设备名称 = b.负责人(+);
			COMMIT;

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'DW_S_TRAVEL数据更新完成','日志记录');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
				-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	--机型吨位对照表
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_MAP_MODEL;

	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_MAP_MODEL';
			V_OBJECT_NAME	:= 'ODS_MAP_MODEL';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				小中大挖,
				型号,
				大行业,
				小行业,
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'

			-- Edward: 已变更字段INC_MAP_MODEL, ODS_MAP_MODEL
			FROM ODS_MAP_MODEL a
			WHERE exists(SELECT 1 FROM INC_MAP_MODEL WHERE 小中大挖=a.小中大挖 and 型号=a.型号 );
			COMMIT;

			----删除ODS_MAP_MODEL中记录；
			DELETE FROM ODS_MAP_MODEL a
			WHERE exists(SELECT 1 FROM INC_MAP_MODEL WHERE 小中大挖=a.小中大挖 and 型号=a.型号);
			COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_MAP_MODEL (
				小中大挖,
				型号,
				大行业,
				小行业,
				TIME_STAMP)
			SELECT 
				小中大挖,
				型号,
				大行业,
				小行业,
				SYSDATE
			FROM INC_MAP_MODEL a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
			-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');

		END;
	END IF;






	--湖南所有年份大行业数据
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_INDUSTRY_BIG;
	IF QTY_COM>0 then 

		BEGIN
			V_TABLE_NAME	:= 'INC_INDUSTRY_BIG';
			V_OBJECT_NAME	:= 'ODS_INDUSTRY_BIG';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				品牌,
				吨级,
				年份,
				月份,
				销量,
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_INDUSTRY_BIG a
			WHERE exists(SELECT 1 FROM INC_INDUSTRY_BIG WHERE 年份=a.年份 and 月份=a.月份 );
			COMMIT;

			----删除ODS_INDUSTRY_BIG中记录；
			DELETE FROM ODS_INDUSTRY_BIG a
			WHERE exists(SELECT 1 FROM INC_INDUSTRY_BIG WHERE 年份=a.年份 and 月份=a.月份 );
			COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_INDUSTRY_BIG (品牌,
				吨级,
				年份,
				月份,
				销量,
				省份,
				TIME_STAMP)
			SELECT 品牌,
				吨级,
				年份,
				月份,
				销量,
				省份,
				SYSDATE
			FROM INC_INDUSTRY_BIG a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

			--------删除对象对应的DW表dw_industry_big
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_industry_big';
			COMMIT;
			INSERT INTO dw_industry_big 
			SELECT
				"品牌",
				case when "吨级"='0≤T＜5' then '＜5T'
					when "吨级"='5≤T＜6.5' then '5T-6T'
					when "吨级"='6.5≤T≤8' then '6T-8T'
					when "吨级"='8＜T＜11' then '8T-11T'
					when "吨级"='11≤T＜16' then '11T-16T'
					when "吨级"='16≤T＜20' then '16T-20T'
					when "吨级"='20≤T＜22' then '20T-22T'
					when "吨级"='22≤T＜24' then '22T-24T'
					when "吨级"='24≤T＜27' then '24T-28T'
					when "吨级"='27≤T＜31' then '28T-31T'
					when "吨级"='31≤T＜35' then '31T-35T'
					when "吨级"='35≤T＜40' then '35T-40T'
					when "吨级"='40≤T' then '40T以上' END as 大行业,
				"年份",
				"月份",
				"销量"
			FROM
				ods_industry_big; 
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_industry_big数据更新完成','日志记录');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
				-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	--销售明细表 Edward已修改: INC_S_DETAIL ODS_S_DETAIL DW_S_DETAIL(未修改)
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_S_DETAIL;

	IF QTY_COM>0 then
		BEGIN
			V_TABLE_NAME	:= 'INC_S_DETAIL';
			V_OBJECT_NAME	:= 'ODS_S_DETAIL';

				----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				营销代表,
				合同单位,
				销售方式,
				型号,
				机器编号,
				CRM过账交机时间,
				数量,
				事业部纸质合同号,
				销售月份ZW,
				销售年份,
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_S_DETAIL a
			WHERE exists(SELECT 1 FROM INC_S_DETAIL WHERE 机器编号=a.机器编号 );
			COMMIT;
			----删除ODS_S_DETAIL中记录；
			DELETE FROM ODS_S_DETAIL a
			WHERE exists(SELECT 1 FROM INC_S_DETAIL WHERE 机器编号=a.机器编号 );
			COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_S_DETAIL ("序号",
				dms,
				"代理商",
				"省份",
				"所属分公司",
				"营销代表",
				"客户类型",
				"合同单位",
				"销售方式",
				"型号",
				"机器编号",
				"证件号码",
				"客户住址",
				"联系电话",
				"数量",
				"CRM过账交机时间",
				"交货地址",
				"合同金额",
				"包干价",
				"终端首付货款",
				"保证金",
				"服务费",
				"公证费",
				"保险费",
				"费用合计",
				"终端贷款金额",
				"贷款成数",
				"贷款期数",
				"事业部纸质合同号",
				"标准条件应付首付",
				"终端首付不足货款",
				"包干首付",
				"包干分期",
				"终端首付不足货款还款方式",
				"终端客户延迟放款月数",
				"重机延迟放款月数",
				"分期期数",
				"重机合同编号",
				"结算金额",
				"与重机成交条件",
				"实付首付汇至重机账上",
				"小中大挖",
				"放款金额",
				"销售折让金额",
				"折后结算金额",
				"以旧换新",
				"旧机评估价",
				"旧机回收价",
				"差额",
				"展会日期",
				"备注",
				"保修期",
				"赠送配件",
				"赠送金额",
				"配件报告批复时间",
				"信息人",
				"信息人电话",
				"信息费金额",
				"信息费类型",
				"回访情况",
				"是否购买保险",
				"兑付日期",
				"销售月份SY",
				"销售月份ZW",
				"中旺买断",
				"S客户",
				"销售年份",
				TIME_STAMP)
			SELECT "序号",
				dms,
				"代理商",
				"省份",
				"所属分公司",
				"营销代表",
				"客户类型",
				"合同单位",
				"销售方式",
				"型号",
				"机器编号",
				"证件号码",
				"客户住址",
				"联系电话",
				"数量",
				"CRM过账交机时间",
				"交货地址",
				"合同金额",
				"包干价",
				"终端首付货款",
				"保证金",
				"服务费",
				"公证费",
				"保险费",
				"费用合计",
				"终端贷款金额",
				"贷款成数",
				"贷款期数",
				"事业部纸质合同号",
				"标准条件应付首付",
				"终端首付不足货款",
				"包干首付",
				"包干分期",
				"终端首付不足货款还款方式",
				"终端客户延迟放款月数",
				"重机延迟放款月数",
				"分期期数",
				"重机合同编号",
				"结算金额",
				"与重机成交条件",
				"实付首付汇至重机账上",
				"小中大挖",
				"放款金额",
				"销售折让金额",
				"折后结算金额",
				"以旧换新",
				"旧机评估价",
				"旧机回收价",
				"差额",
				"展会日期",
				"备注",
				"保修期",
				"赠送配件",
				"赠送金额",
				"配件报告批复时间",
				"信息人",
				"信息人电话",
				"信息费金额",
				"信息费类型",
				"回访情况",
				"是否购买保险",
				"兑付日期",
				"销售月份SY",
				"销售月份ZW",
				"中旺买断",
				"S客户",
				"销售年份",
				SYSDATE
			FROM INC_S_DETAIL a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;
			--------删除对象对应的DW表dw_s_detail
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_s_detail';
			COMMIT;

			----插入数据
			-- Edward: 该出DW_S_DETAIL和ODS_S_DETAIL字段已修改
			INSERT INTO dw_s_detail("订单类型",--将DW表的字段列出，不然会出现字段错位的情况；
				dms,
				"代理商",
				"省份",
				"所属分公司",
				"营销代表",
				"客户类型",
				"合同单位",
				"销售方式",
				"型号",
				"机器编号",
				"证件号码",
				"客户住址",
				"联系电话",
				"数量",
				"CRM过账交机时间",
				"交货地址",
				"合同金额",
				"包干价",
				"终端首付货款",
				"保证金",
				"服务费",
				"公证费",
				"保险费",
				"费用合计",
				"终端贷款金额",
				"贷款成数",
				"贷款期数",
				"事业部纸质合同号",
				"标准条件应付首付",
				"终端首付不足货款",
				"包干首付",
				"包干分期",
				"终端首付不足货款还款方式",
				"终端客户延迟放款月数",
				"重机延迟放款月数",
				"分期期数",
				"重机合同编号",
				"结算金额",
				"与重机成交条件",
				"实付首付汇至重机账上",
				"小中大挖",
				"放款金额",
				"销售折让金额",
				"折后结算金额",
				"以旧换新",
				"保修期",
				"赠送配件",
				"赠送金额",
				"销售月份SY",
				"销售月份" ,
				"销售年份",
				"销售月份ZW") 
			SELECT
			-- Edward: 需要对“贷款成数”“贷款期数”字段进行处理
				case when translate("序号", '0123456789', '#') is null then '正常' 
					when instr(translate("序号", '0123456789', '#'),'#')>0 then '正常'
					when translate("序号", '0123456789', '#')='C' then '正常'
					when translate("序号", '0123456789', '#')='-' then '正常'
					else "序号" END as 订单类型,
				dms,
				"代理商",
				"省份",
				"所属分公司",
				"营销代表",
				"客户类型",
				"合同单位",
				"销售方式",
				case when instr("型号",'C')>0 then substr("型号",1,instr("型号",'C')-1)
					when instr("型号",'H')>0 then substr("型号",1,instr("型号",'H')-1)
					when instr("型号",'U')>0 then substr("型号",1,instr("型号",'U')-1) else "型号" END "型号",
				trim("机器编号") as 机器编号,
				"证件号码",
				"客户住址",
				"联系电话",
				"数量",
				"CRM过账交机时间",
				"交货地址",
				"合同金额",
				"包干价",
				"终端首付货款",
				"保证金",
				"服务费",
				"公证费",
				"保险费",
				"费用合计",
				"终端贷款金额",
				"贷款成数",
				"贷款期数",
				"事业部纸质合同号",
				"标准条件应付首付",
				"终端首付不足货款",
				"包干首付",
				"包干分期",
				"终端首付不足货款还款方式",
				"终端客户延迟放款月数",
				"重机延迟放款月数",
				"分期期数",
				"重机合同编号",
				"结算金额",
				"与重机成交条件",
				"实付首付汇至重机账上",
				"小中大挖",
				"放款金额",
				"销售折让金额",
				"折后结算金额",
				"以旧换新",
				"保修期",
				"赠送配件",
				"赠送金额",
				"销售月份SY",
				case when "销售月份SY" IS NULL AND "CRM过账交机时间" IS NULL then 1
					when "销售月份SY" IS NULL AND "CRM过账交机时间" IS NOT NULL THEN to_number(SUBSTR(to_char("CRM过账交机时间",'YYYYMMDD'),5,2))
					when instr("销售月份SY",'月')>0 then to_number(substr("销售月份SY",1,instr("销售月份SY",'月')-1))
					else to_number("销售月份SY") END as "销售月份" ,
				"销售年份",
				to_date((
				case when "销售月份SY" IS NULL AND "CRM过账交机时间" IS NULL then to_char("销售年份")||'0101'
					when "销售月份SY" IS NULL AND "CRM过账交机时间" IS NOT NULL THEN SUBSTR(to_char("CRM过账交机时间",'YYYYMMDD'),1,6)||'01'
					else to_char("销售年份")||(case when instr("销售月份SY",'月')>0 and to_number(substr("销售月份SY",1,instr("销售月份SY",'月')-1)) >=10 then substr("销售月份SY",1,instr("销售月份SY",'月')-1)
					when instr("销售月份SY",'月')>0 and to_number(substr("销售月份SY",1,instr("销售月份SY",'月')-1)) <10 then '0'||substr("销售月份SY",1,instr("销售月份SY",'月')-1) 
					else "销售月份SY" END) ||'01' END),'YYYY-MM-DD') as "销售月份SY"
			FROM
				ods_s_detail;
			COMMIT;


			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE, 'dw_s_profit数据更新完成','日志记录');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
				-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	--毛利表
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_S_PROFIT;

	IF QTY_COM>0 then

		BEGIN
			V_TABLE_NAME	:= 'INC_S_PROFIT';
			V_OBJECT_NAME	:= 'ODS_S_PROFIT';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				序号,
				"机号",
				"类型",
				"客户名称",
				"营销代表",
				"分公司",
				"新机销售日期",
				"销售方式",
				"数量",
				"包干价",
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_S_PROFIT a
			WHERE exists(SELECT 1 FROM INC_S_PROFIT WHERE 机号=a.机号);
			COMMIT;
			----删除ODS_S_PROFIT中记录；
			DELETE FROM ODS_S_PROFIT a
			WHERE exists(SELECT 1 FROM INC_S_PROFIT WHERE 机号=a.机号);
			COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_S_PROFIT ("序号",
				年度,
				月度,
				"机号",
				"类型",
				"客户名称",
				"营销代表",
				"分公司",
				"新机销售日期",
				"销售方式",
				"数量",
				"包干价",
				"合同价",
				"销售收入不含税",
				"销售成本包干费用",
				"主机成本不含税",
				"折前结算价",
				"折让",
				"成本折后结算价",
				"成本一次运费",
				"成本赠送配件",
				"成本信息费",
				"包干费用管理费",
				"包干费用承担利息",
				"包干费用收客户利息",
				"销售提奖",
				"二三五八奖",
				"管理奖",
				"签单招待费",
				"二次运费",
				"奖返利",
				"置换机损益",
				"毛利1",
				"毛利2",
				"毛利3",
				"置换机销售日期",
				"置换机抵入价",
				"置换机销售价",
				"意向赠送",
				"会展赠送金额",
				"备注",
				"合计",
				"信息费",
				"是否单位客户",
				"保底基础",
				"保底奖",
				"成本化费用",
				"其他费用",
				TIME_STAMP)
			SELECT "序号",
				年度,
				月度,
				"机号",
				"类型",
				"客户名称",
				"营销代表",
				"分公司",
				"新机销售日期",
				"销售方式",
				"数量",
				"包干价",
				"合同价",
				"销售收入不含税",
				"销售成本包干费用",
				"主机成本不含税",
				"折前结算价",
				"折让",
				"成本折后结算价",
				"成本一次运费",
				"成本赠送配件",
				"成本信息费",
				"包干费用管理费",
				"包干费用承担利息",
				"包干费用收客户利息",
				"销售提奖",
				"二三五八奖",
				"管理奖",
				"签单招待费",
				"二次运费",
				"奖返利",
				"置换机损益",
				"毛利1",
				"毛利2",
				"毛利3",
				"置换机销售日期",
				"置换机抵入价",
				"置换机销售价",
				"意向赠送",
				"会展赠送金额",
				"备注",
				"合计",
				"信息费",
				"是否单位客户",
				"保底基础",
				"保底奖",
				"成本化费用",
				"其他费用",
				SYSDATE
			FROM INC_S_PROFIT a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

			--------删除对象对应的DW表dw_s_profit
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_s_profit ' ;
			COMMIT;

			INSERT INTO dw_s_profit 
			SELECT
				"机号",
				case when instr("类型",'C')>0 then substr("类型",1,instr("类型",'C')-1)
					when instr("类型",'H')>0 then substr("类型",1,instr("类型",'H')-1) 
					when instr("类型",'U')>0 then substr("类型",1,instr("类型",'U')-1) else "类型" END as 型号,
				"客户名称",
				"营销代表",
				"分公司",
				"新机销售日期",
				"销售方式",
				"数量",
				nvl("合同价",0) as 销售金额,
				nvl("销售收入不含税",0) as 销售收入不含税,
				nvl("销售成本包干费用",0)as 销售成本包干费用,
				nvl("主机成本不含税",0)as 主机成本不含税,
				nvl("折前结算价",0) as 三一结算金额,
				nvl("折让",0) as "折让",
				nvl("成本折后结算价",0) as "成本折后结算价",
				nvl("成本一次运费",0)+nvl("二次运费",0) as 运费,
				nvl("成本赠送配件",0) as 赠送配件金额,
				nvl("成本信息费",0) as 信息费,
				nvl("包干费用管理费",0) as 管理费用,
				nvl("包干费用承担利息",0)-nvl("包干费用收客户利息",0) as 融资包干利息,
				nvl("销售提奖",0)+nvl("二三五八奖",0)+nvl("管理奖",0) as 提奖,
				nvl("签单招待费",0) as 招待费,
				nvl("奖返利",0) as 奖励返利,
				nvl("毛利1",0) as 毛利1,
				nvl("毛利2",0) as"毛利2",
				nvl("毛利3",0) as"毛利3",
				"置换机销售日期",
				-- case when nvl("置换机抵入价",0)-nvl("置换机销售价",0)=0 then 0-nvl("折让",0) else nvl("置换机抵入价",0)-nvl("置换机销售价",0) END as 旧机亏损,
				nvl("置换机抵入价",0)-nvl("置换机销售价",0) as 旧机亏损,
				"是否单位客户",
				'' 日期
			FROM
				ods_s_profit;
			COMMIT; 

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE, 'dw_s_profit数据更新完成','日志记录');
			COMMIT;
		EXCEPTION 
			WHEN OTHERS THEN
			-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	--一户一册
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_EXC_LEDGER;

	IF QTY_COM>0 then 

		BEGIN
			V_TABLE_NAME	:= 'INC_EXC_LEDGER';
			V_OBJECT_NAME	:= 'ODS_EXC_LEDGER';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				机号,
				设备型号,
				省份,
				地区,
				客户名,
				销售方式,
				现营销代表,
				发货日期,
				在外货款,
				逾期款总计,
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_EXC_LEDGER a
			WHERE exists(SELECT 1 FROM INC_EXC_LEDGER WHERE 机号=a.机号 );
			COMMIT;
			----删除ODS_EXC_LEDGER中记录；
			DELETE FROM ODS_EXC_LEDGER a
			WHERE exists(SELECT 1 FROM INC_EXC_LEDGER WHERE 机号=a.机号 );
			COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_EXC_LEDGER ("机号",
				"车架号",
				"设备型号",
				"省份",
				"地区",
				"省份01",
				"地区01",
				"GPS状态",
				"GPS加装",
				"设备类型",
				"定位地点",
				"最后登录时间",
				"客户名",
				"实际管理人",
				"联系电话",
				"合同单位",
				"销售方式",
				"保证金",
				"原营销代表",
				"现营销代表",
				"发货日期",
				"放款日期",
				"客户分类",
				"催收责任人",
				"是否为价值销售",
				"管理专干",
				"流转时间",
				"备注",
				"银行按揭款贷款余额",
				"银行按揭款本月到期款",
				"银行逾期款",
				"垫付款",
				"公司货款货款余额",
				"公司货款本月到期款",
				"逾期金额",
				"旧机抵款",
				"差异",
				"差异说明",
				"总到期款",
				"总逾期款",
				"逾期罚息",
				"逾期款总计",
				"客户数",
				"回款客户数",
				"总逾期期数",
				"旧机抵款2",
				"其它抵款",
				"近12个月累计还款",
				"近6个月累计还款",
				"近3个月累计还款",
				"垫付",
				"合同价",
				"折扣金额",
				"折后价",
				"在外货款",
				"未到期",
				TIME_STAMP)
			SELECT "机号",
				"车架号",
				"设备型号",
				"省份",
				"地区",
				"省份01",
				"地区01",
				"GPS状态",
				"GPS加装",
				"设备类型",
				"定位地点",
				"最后登录时间",
				"客户名",
				"实际管理人",
				"联系电话",
				"合同单位",
				"销售方式",
				"保证金",
				"原营销代表",
				"现营销代表",
				"发货日期",
				"放款日期",
				"客户分类",
				"催收责任人",
				"是否为价值销售",
				"管理专干",
				"流转时间",
				"备注",
				"银行按揭款贷款余额",
				"银行按揭款本月到期款",
				"银行逾期款",
				"垫付款",
				"公司货款货款余额",
				"公司货款本月到期款",
				"逾期金额",
				"旧机抵款",
				"差异",
				"差异说明",
				"总到期款",
				"总逾期款",
				"逾期罚息",
				"逾期款总计",
				"客户数",
				"回款客户数",
				"总逾期期数",
				"旧机抵款2",
				"其它抵款",
				"近12个月累计还款",
				"近6个月累计还款",
				"近3个月累计还款",
				"垫付",
				"合同价",
				"折扣金额",
				"折后价",
				"在外货款",
				"未到期",
				SYSDATE
			FROM INC_EXC_LEDGER a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
			-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	--工时数据
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_HNZW1_1000;

	IF QTY_COM>0 then 

		BEGIN
			V_TABLE_NAME	:= 'INC_HNZW1_1000';
			V_OBJECT_NAME	:= 'DW_HNZW_WJGS';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				机号,
				机型,
				总工时,
				当日工时,
				当日油耗,
				发动机转速,
				燃油油位,
				定位详情,
				是否电改液控,
				更新时间,
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM DW_HNZW_WJGS a
			WHERE exists(SELECT 1 FROM INC_HNZW1_1000 WHERE 机号=a.机号 and 更新时间=a.更新时间 );
			COMMIT;
			----删除DW_HNZW_WJGS中记录；
			DELETE FROM DW_HNZW_WJGS a
			WHERE exists(SELECT 1 FROM INC_HNZW1_1000 WHERE 机号=a.机号 and 更新时间=a.更新时间);
			COMMIT;

			----插入增量表中数据
			INSERT INTO DW_HNZW_WJGS (机号,
				登录号,
				机型,
				总工时,
				当日工时,
				当日油耗,
				锁机级别,
				预埋时间,
				发动机转速,
				燃油油位,
				定位详情,
				是否电改液控,
				更新时间,
				TIME_STAMP)
			SELECT 机号,
				登录号,
				机型,
				总工时,
				当日工时,
				当日油耗,
				锁机级别,
				预埋时间,
				发动机转速,
				燃油油位,
				定位详情,
				是否电改液控,
				更新时间,SYSDATE
			FROM INC_HNZW1_1000 a;
			COMMIT;
			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;
		EXCEPTION 
			WHEN OTHERS THEN
			-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');

		END;
	END IF;






	--债权应收账
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_ACC_REC;

	IF QTY_COM>0 then 

		BEGIN
			V_TABLE_NAME	:= 'INC_ACC_REC';
			V_OBJECT_NAME	:= 'ODS_ACC_REC';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"成交方式",
				"状态",
				"机号",
				"客户名称",
				"营销代表工号",
				"型号",
				"主机编码",
				"还款计划行号",
				"还款项目",
				"应还款时间",
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM ODS_ACC_REC a
			WHERE exists(SELECT 1 FROM INC_ACC_REC WHERE 机号=a.机号 and 还款计划行号=a.还款计划行号);
			COMMIT;
			----删除ODS_ACC_REC中记录；
			delete 
			FROM ODS_ACC_REC a
			WHERE exists(SELECT 1 FROM INC_ACC_REC WHERE 机号=a.机号 and 还款计划行号=a.还款计划行号);
			COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_ACC_REC ("成交方式",
				"状态",
				"机号",
				"客户编码",
				"客户名称",
				"营销代表工号",
				"营销代表姓名",
				"型号",
				"主机编码",
				"还款计划行号",
				"还款项目",
				"应还款时间",
				"应还金额",
				"实还金额",
				"当月实际打款",
				"实际还款时间",
				"最后还款时间",
				TIME_STAMP)
			SELECT "成交方式",
				"状态",
				"机号",
				"客户编码",
				"客户名称",
				"营销代表工号",
				"营销代表姓名",
				"型号",
				"主机编码",
				"还款计划行号",
				"还款项目",
				"应还款时间",
				"应还金额",
				"实还金额",
				"当月实际打款",
				"实际还款时间",
				"最后还款时间",
				SYSDATE
			FROM
				INC_ACC_REC a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

			--------删除对象对应的DW表dw_acc_rec
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_acc_rec';
			COMMIT;
			INSERT INTO dw_acc_rec 
			SELECT
				"成交方式",
				"状态",
				"机号",
				"客户名称",
				"还款项目",
				"应还款时间",
				"应还金额",
				"实还金额",
				"当月实际打款",
				"实际还款时间",
				"最后还款时间"
			FROM
				ODS_ACC_REC;
			COMMIT;

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_acc_rec数据更新完成','日志记录');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
			-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	--服务订单
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_SERVICE_ORDERS;

	IF QTY_COM>0 then 

		BEGIN
			V_TABLE_NAME	:= 'INC_SERVICE_ORDERS';
			V_OBJECT_NAME	:= '"服务订单"';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				服务订单号,
				客户,
				订单状态,
				设备编号,
				产品组,
				设备类型,
				交机日期,
				设备型号,
				订单类型,
				服务网点,
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM "服务订单" a
			WHERE exists(SELECT 1 FROM INC_SERVICE_ORDERS WHERE 服务订单号=a.服务订单号);
			COMMIT;
			----删除ODS_SERVICE_ORDERS中记录；
			delete 
			FROM "服务订单" a
			WHERE exists(SELECT 1 FROM INC_SERVICE_ORDERS WHERE 服务订单号=a.服务订单号);
			COMMIT;

			----插入增量表中数据
			INSERT INTO "服务订单" ( "订单完工时间",
				"服务订单号",
				"客户",
				"订单状态",
				"设备编号",
				"产品组",
				"设备类型",
				"交机日期",
				"设备型号",
				"订单类型",
				"省份",-----gzy:增加字段
				"服务网点",
				"服务工程师",
				"设备联系人",
				"设备联系人电话",
				"故障描述",
				"创建人",
				"创建时间",
				"召请时间",
				"保养节点",
				"故障解决方法",
				"处理方法",
				"工程师技能等级",
				"现场完工时间",
				"累计运行时间",
				TIME_STAMP)
			SELECT "订单完工时间",
				"服务订单号",
				"客户",
				"订单状态",
				"设备编号",
				"产品组",
				"设备类型",
				"交机日期",
				"设备型号",
				"订单类型",
				"省份",-----gzy:增加字段
				"服务网点",
				"服务工程师",
				"设备联系人",
				"设备联系人电话",
				"故障描述",
				"创建人",
				"创建时间",
				"召请时间",
				"保养节点",
				"故障解决方法",
				"处理方法",
				"工程师技能等级",
				"现场完工时间",
				"累计运行时间",
				SYSDATE
			FROM
				INC_SERVICE_ORDERS a;
			COMMIT;

			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;

			--------删除对象对应的DW表dw_service_orders
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_service_orders';
			COMMIT;

			INSERT INTO dw_service_orders
				(订单完工时间,
				服务订单号,
				客户,
				订单状态,
				机号,
				订单类型,
				服务网点,
				服务工程师,
				故障描述,
				创建时间,
				保养节点,
				工程师技能等级,
				现场完工时间,
				累计运行时间,
				省份)
			SELECT
				"订单完工时间",
				"服务订单号",
				"客户",
				"订单状态",
				"设备编号" as "机号",
				"订单类型",
				"服务网点",
				"服务工程师",
				"故障描述",
				"创建时间",
				"保养节点",
				"工程师技能等级",
				"现场完工时间",
				"累计运行时间",
				"省份"-----gzy:增加字段
			FROM
				"服务订单";
			COMMIT;

			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_service_orders数据更新完成','日志记录');
			COMMIT;
				
		EXCEPTION 
			WHEN OTHERS THEN
				-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	--湖南服务促进主机销售数据
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_SEV_S_DETAIL;

	IF QTY_COM>0 then 

		BEGIN
			V_TABLE_NAME	:= 'INC_SEV_S_DETAIL';
			V_OBJECT_NAME	:= 'ODS_SEV_S_DETAIL';

			----------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新开始','日志记录');
			COMMIT;

			------插入前，先将源ODS表中存在的数据备份日志表中
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				ID,
				分公司,
				营销代表,
				工程师,
				合同主体,
				型号,
				机器编号,
				类型,
				营销管理部服务部核实情况,
				统计月份,
				SYSDATE,
				V_OBJECT_NAME || '数据备份处理',
				'数据备份记录'
			FROM
				ODS_SEV_S_DETAIL a
			WHERE exists(SELECT 1 FROM INC_SEV_S_DETAIL WHERE ID=a.ID and 机器编号=a.机器编号 and 类型=a.类型);
			COMMIT;
			
			----删除ODS_SERVICE_ORDERS中记录；
			DELETE FROM
				ODS_SEV_S_DETAIL a
			WHERE exists(SELECT 1 FROM INC_SEV_S_DETAIL WHERE ID=a.ID and 机器编号=a.机器编号 and 类型=a.类型);
			COMMIT;

			----插入增量表中数据
			INSERT INTO ODS_SEV_S_DETAIL
				(id,
				"分公司",
				"小组组长",
				"营销代表",
				"工程师",
				"合同主体",
				"型号",
				"机器编号",
				"提机时间",
				"类型",
				"营销管理部服务部核实情况",
				"小组组长奖励金额",
				"工程师奖励金额",
				"统计月份",
				TIME_STAMP)
			SELECT id,
				"分公司",
				"小组组长",
				"营销代表",
				"工程师",
				"合同主体",
				"型号",
				"机器编号",
				"提机时间",
				"类型",
				"营销管理部服务部核实情况",
				"小组组长奖励金额",
				"工程师奖励金额",
				"统计月份",SYSDATE
			FROM
				INC_SEV_S_DETAIL a;
			COMMIT;


			--------删除对象对应的INC表
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '数据更新完成','日志记录');
			COMMIT;
		EXCEPTION 
			WHEN OTHERS THEN
			-------记录报表处理的日志
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
		END;
	END IF;






	----DW_CUS_LOSE
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DW_CUS_LOSE',SYSDATE,'DW_CUS_LOSE数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DW_CUS_LOSE
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_CUS_LOSE' ;
		COMMIT;
		-- Edward0604: DW_CUS_LOSE省份已添加
		INSERT INTO DW_CUS_LOSE
			(客户名称,
			商机名称,
			购买品牌,
			型号,
			成交日期,
			成交方式,
			成交价格,
			首付比例,
			丢单原因,
			外部负责人,
			锁定状态,
			相关团队,
			创建人,
			负责人,
			序号,
			负责人主属部门,
			业务类型,
			生命状态,
			归属部门,
			创建时间,
			最后修改人,
			最后修改时间,
			竞争状态,
			省份)
		SELECT
			"客户名称",
			"商机名称",
			"购买品牌",
			"型号",
			"成交日期",
			"成交方式",
			"成交价格",
			"首付比例",
			"丢单原因",
			"外部负责人",
			"锁定状态",
			"相关团队",
			"创建人",
			"负责人",
			"序号",
			"负责人主属部门",
			"业务类型",
			"生命状态",
			"归属部门",
			"创建时间",
			"最后修改人",
			"最后修改时间",
			'有竞争' as 竞争状态,
			'省份'
		FROM
			ods_lose_com
		union all
		SELECT
			"客户名称",
			"商机名称",
			"购买品牌",
			to_char("型号") as 型号,
			"成交日期",
			"成交方式",
			"成交价格",
			"首付比例",
			"丢单原因",
			"外部负责人",
			"锁定状态",
			"相关团队",
			"创建人",
			"负责人",
			"序号",
			"负责人主属部门",
			"业务类型",
			"生命状态",
			"归属部门",
			"创建时间",
			"最后修改人",
			"最后修改时间",
			'无竞争' as 竞争状态,
			'省份'
		FROM
			ods_lose_no_com;		
		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DW_CUS_LOSE',SYSDATE,'DW_CUS_LOSE数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
		-------记录报表处理的日志
			prc_wlf_sys_writelog('DW_CUS_LOSE',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----DM_S_SALES
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_SALES',SYSDATE,'DM_S_SALES数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_SALES
		-- Edward: DW_S_DETAIL ODS_MAP_MODEL 已修改
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_SALES' ;
		COMMIT;
		INSERT INTO DM_S_SALES
			(订单类型,
			DMS,
			代理商,
			省份,
			地区,
			城市,
			销售代表,
			新老客户,
			合同单位,
			销售方式,
			类型,
			大行业,
			小行业,
			型号,
			机器编号,
			是否以旧换新,
			以旧换新数量,
			证件号码,
			住址,
			联系电话,
			数量,
			交机时间,
			交货地点,
			最终用户合同金额,
			几成几年,
			分期期数,
			结算价,
			折让金额万,
			折让后价格,
			赠送配件合计金额,
			销售月份,
			销售年份,
			销售月份ZW)
		SELECT
			"订单类型",
			dms,
			"代理商",
			"省份",
			"所属分公司",
			case when "所属分公司" ='长潭' then '长沙'
				 when "所属分公司" ='大客户部' then '长沙'
				 when "所属分公司" ='邵阳东' then '邵阳' 
				 when "所属分公司" ='邵阳西' then '邵阳' 
				 when "所属分公司" ='总部' then '湘潭' 
				 when "所属分公司" ='吉首、张家界' then '张家界'
				 when "所属分公司" ='吉首' then '张家界'
				 else "所属分公司" END AS "城市",
			a."营销代表",
			case when c.类型='老客户再次购机' then '老客户' else a."客户类型" END as "客户类型",
			a."合同单位",
			a."销售方式",
			b."小中大挖",
			b."大行业",
			b."小行业",
			a."型号",
			a."机器编号",
			"以旧换新",
			case when "以旧换新" ='是' then "数量" else 0 END as 以旧换新数量,
			"证件号码",
			"客户住址",
			"联系电话",
			"数量",
			"CRM过账交机时间",
			"交货地址",
			"合同金额",
			"贷款成数", -- Edward: 新增字段
			--"贷款期数", -- Edward: 新增字段, FIX: 等DW_S_SALES修改后添加上
			"分期期数",
			"结算金额",
			"销售折让金额",
			"折后结算金额",
			"赠送金额",
			"销售月份",
			"销售年份",
			"销售月份ZW"
		FROM
			dw_s_detail A,
			ods_map_model b,
			(SELECT distinct 机器编号,类型 FROM ods_sev_s_detail WHERE "营销管理部服务部核实情况" not like '%无记录%') c
		WHERE
			a."型号"=b."型号"(+) and a."机器编号"=c."机器编号"(+);
		COMMIT;

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_SALES',SYSDATE,'DM_S_SALES数据更新完成','日志记录');
		COMMIT;

	EXCEPTION
		WHEN OTHERS THEN
		-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_SALES',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;





	----DM_IND_SYZYL
	BEGIN
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_IND_SYZYL',SYSDATE,'DM_IND_SYZYL数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_IND_SYZYL
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_IND_SYZYL' ;
		COMMIT;
		INSERT INTO DM_IND_SYZYL 
		SELECT
			小行业,
			"小中大挖",
			"城市",
			"年份",
			"月份",
			sum("三一销量") as 三一销量,
			sum("市场容量") as 市场容量,
			case when sum("市场容量")=0 then 0 else sum(三一销量)/sum("市场容量") END as 占有率,
			sum("实际销量") as 实际销量,
			sum("销售金额") as 销售金额,
			sum("毛利") as 毛利,
			case when sum("销售金额")=0 then 0 else sum(毛利)/sum("销售金额") END as 利润率
		-- Edward: 已变更字段ODS_MAP_MODEL.类型->小中大挖
		FROM 
			(SELECT b.小中大挖,
				a.小行业,
				"城市01" as 城市,
				"年份",
				"月份",
				sum(case when "品牌"='三一' then "销量" else 0 END) "三一销量",
				sum("销量") as 市场容量,
				0 as 实际销量,
				0 as 销售金额 ,
				0 as 毛利
				FROM dw_industry_small a,ods_map_model b
				WHERE a.小行业=b.小行业(+)
				group by b.小中大挖,a.小行业,
				"城市01",
				"年份",
				"月份"
			union all
			SELECT m."类型",
				m."小行业",
				m."城市",
				m."销售年份" as 年份,
				m."销售月份" as 月份,
				0 as 三一销量,
				0 as 市场容量,
				sum(m."数量") as 实际销量,
				sum(m."最终用户合同金额") as 销售金额 ,
				sum(n."销售金额" - n."三一结算金额" + n."旧机亏损" - n."赠送配件金额" - n."信息费" - n."运费" - n."提奖" - n."招待费") as 毛利
			FROM
				DM_S_SALES m,
				DW_S_PROFIT n 
			WHERE
				m."机器编号"=n."机号"(+)
			group by
				m."类型",
				m."城市",
				m."小行业",
				m."销售月份",
				m."销售年份")
		GROUP BY
			"小行业",
			"小中大挖",
			"城市",
			"年份",
			"月份"; 
		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_IND_SYZYL',SYSDATE,'DM_IND_SYZYL数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_IND_SYZYL',SYSDATE,
			'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');

	END;




	----DM_INDUSTRY_BIG
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_INDUSTRY_BIG',SYSDATE,'DM_INDUSTRY_BIG数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_INDUSTRY_BIG
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_INDUSTRY_BIG' ;
		COMMIT;
		INSERT INTO DM_INDUSTRY_BIG
		SELECT '湖南' as 省,
			大行业,
			"年份",
			"月份",
			sum(case when "品牌"='三一' then "销量" else 0 END) "三一销量",
			sum("销量") as 市场容量,
			case when sum("销量")=0 then 0 else sum(case when "品牌"='三一' then "销量" else 0 END)/sum("销量") END as 占有率
			FROM dw_industry_big 
			group by '湖南',大行业,
			"年份",
			"月份";					
		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_INDUSTRY_BIG',SYSDATE,'DM_INDUSTRY_BIG数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_INDUSTRY_BIG',SYSDATE,
			'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;





	----dm_salesman_score01
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_salesman_score01',SYSDATE,'dm_salesman_score01数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表dm_salesman_score01
		EXECUTE IMMEDIATE 'TRUNCATE TABLE dm_salesman_score01' ;
		COMMIT;
		INSERT INTO dm_salesman_score01(
			日期,
			年度,
			月度,
			销售代表,
			序号,
			度量,
			值,
			综合得分)
		SELECT
			a.col_ as 日期,
			a.col__1 as 年度,
			a.col__1_2 as 月度,
			a.col__1_2_3 as 销售代表,
			b.序号,
			b.度量,
			case when b.度量='销量得分' then nvl(a.col__1_2_3_4 ,0)
				when b.度量='销售额得分' then nvl(a.col__1_2_3_4_5 ,0)
				when b.度量='毛利率得分' then nvl(a.col__1_2_3_4_5_6 ,0)
				when b.度量='以旧换新比例得分' then nvl(a.col__1_2_3_4_5_6_7 ,0)
				when b.度量='战胜率得分' then nvl(a.col__1_2_3_4_5_6_7_8 ,0)
				when b.度量='信息费占比得分' then nvl(a.col__1_2_3_4_5_6_7_8_9 ,0)
				when b.度量='赠送配件占比得分' then nvl(a.col__1_2_3_4_5_6_7_8_9_10,0)
				when b.度量='综合得分' then nvl(a.col__1_2_3_4_5_6_7_8_9_10_11,0) END as 值,
			nvl(a.col__1_2_3_4_5_6_7_8_9_10_11,0) as 综合得分
		FROM
			dm_salesman_score a,
			dim_salesman_score b;

		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_salesman_score01',SYSDATE,'dm_salesman_score01数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('dm_salesman_score01',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----dm_business_opp
	BEGIN
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_business_opp',SYSDATE,'dm_business_opp数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表dm_business_opp
		EXECUTE IMMEDIATE 'TRUNCATE TABLE dm_business_opp' ;
		COMMIT;
		INSERT INTO dm_business_opp
		SELECT
			substr(to_char(最后修改时间,'yyyymmdd'),1,4) as 年度,
			substr(to_char(最后修改时间,'yyyymmdd'),5,2) as 月度,
			to_date(substr(to_char(最后修改时间,'yyyymmdd'),1,6)||'01','yyyymmdd') as 日期,
			"员工姓名" as 负责人,
			count(1) as 新客量
		FROM
			dw_business_opp
		group by 
			substr(to_char(最后修改时间,'yyyymmdd'),1,4),
			substr(to_char(最后修改时间,'yyyymmdd'),5,2),
			to_date(substr(to_char(最后修改时间,'yyyymmdd'),1,6)||'01','yyyymmdd'),
			"员工姓名";						
		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_business_opp',SYSDATE,'dm_business_opp数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('dm_business_opp',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----DM_S_MILEAGE
	BEGIN
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_MILEAGE',SYSDATE,'DM_S_MILEAGE数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_MILEAGE
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_MILEAGE' ;
		COMMIT;
		INSERT INTO
			DM_S_MILEAGE
		SELECT
			年度,
			月度,
			to_date(substr(to_char(日期,'yyyymmdd'),1,6)||'01','yyyymmdd') as 日期,
			"销售代表" as 负责人,
			sum("总里程KM") as 总里程
		FROM
			DW_S_MILEAGE
		group by 
			年度,
			月度,
			to_date(substr(to_char(日期,'yyyymmdd'),1,6)||'01','yyyymmdd'),
			"销售代表";
		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_MILEAGE',SYSDATE,'DM_S_MILEAGE数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_MILEAGE',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;





	----DM_S_TRAVEL
	BEGIN
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_TRAVEL',SYSDATE,'DM_S_TRAVEL数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_TRAVEL
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_TRAVEL' ;
		COMMIT;
		INSERT INTO
			DM_S_TRAVEL
		SELECT
			年度,
			月度,
			to_date(substr(to_char(开始时间,'yyyymmdd'),1,6)||'01','yyyymmdd') as 日期,
			"设备名称" as 负责人,
			sum("总里程KM") as 总里程,
			count(1) as 总行程量
		FROM
			DW_S_TRAVEL
		group by 
			年度,
			月度,
			to_date(substr(to_char(开始时间,'yyyymmdd'),1,6)||'01','yyyymmdd') ,
			"设备名称";				
		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_TRAVEL',SYSDATE,'DM_S_TRAVEL数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_TRAVEL',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----dm_customer_new
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_customer_new',SYSDATE,'dm_customer_new数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表dm_customer_new
		EXECUTE IMMEDIATE 'TRUNCATE TABLE dm_customer_new' ;
		COMMIT;
		INSERT INTO
			dm_customer_new
		SELECT
			substr(to_char(最后跟进时间,'yyyymmdd'),1,4) as 年度,
			substr(to_char(最后跟进时间,'yyyymmdd'),5,2) as 月度,
			to_date(substr(to_char(最后跟进时间,'yyyymmdd'),1,6)||'01','yyyymmdd') as 日期,
			"员工姓名" as 负责人,
			count(1) as 新客量
		FROM
			dw_customer_new
		group by 
			substr(to_char(最后跟进时间,'yyyymmdd'),1,4),
			substr(to_char(最后跟进时间,'yyyymmdd'),5,2),
			to_date(substr(to_char(最后跟进时间,'yyyymmdd'),1,6)||'01','yyyymmdd'),
			"员工姓名";			
		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_customer_new',SYSDATE,'dm_customer_new数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('dm_customer_new',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----dm_customer_face
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_customer_face',SYSDATE,'dm_customer_face数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表dm_customer_face
		EXECUTE IMMEDIATE 'TRUNCATE TABLE dm_customer_face' ;
		COMMIT;
		INSERT INTO
			dm_customer_new
		SELECT
			substr(to_char(完成时间,'yyyymmdd'),1,4) as 年度,
			substr(to_char(完成时间,'yyyymmdd'),5,2) as 月度,
			to_date(substr(to_char(完成时间,'yyyymmdd'),1,6)||'01','yyyymmdd') as 日期,
			"负责人",
			count(1) as 面访量
		FROM
			dw_customer_face
		group by 
			substr(to_char(完成时间,'yyyymmdd'),1,4),
			substr(to_char(完成时间,'yyyymmdd'),5,2),
			to_date(substr(to_char(完成时间,'yyyymmdd'),1,6)||'01','yyyymmdd'),
			"负责人";			
		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_customer_face',SYSDATE,'dm_customer_face数据更新完成','日志记录');
		COMMIT;

	EXCEPTION
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('dm_customer_face',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----DM_S_PRESALES
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_PRESALES',SYSDATE,'DM_S_PRESALES数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_PRESALES
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_PRESALES' ;
		COMMIT;
		INSERT INTO
			DM_S_PRESALES
		SELECT "年度",
			"月度",
			"日期",
			"负责人",
			sum(nvl(商机量,0)) as 商机量,
			sum(nvl(新客量,0)) as 新客量, 
			sum(nvl(面访量,0)) as 面访量,
			sum(nvl(总里程,0)) as 总里程,
			sum(nvl(总里程,0)) as 总行程量
		FROM
			(SELECT
				"年度",
				"月度",
				"日期",
				"负责人",
				"新客量" as 商机量 ,
				0 as 新客量,
				0 as 面访量,
				0 as 总里程,
				0 as 总行程量
			FROM
				dm_business_opp 
			union all
			SELECT
				"年度",
				"月度",
				"日期",
				"负责人",
				0 as 商机量,
				"新客量" as 新客量,
				0 as 面访量,
				0 as 总里程,
				0 as 总行程量
			FROM
				dm_customer_new
			union all
			SELECT
				"年度",
				"月度",
				"日期",
				"负责人",
				0 as 商机量,
				0 as 新客量,
				"面访量" as 面访量,
				0 as 总里程,
				0 as 总行程量
			FROM
				dm_customer_face
			union all
			SELECT
				"年度",
				"月度",
				"日期",
				"负责人",
				0 as 商机量,
				0 as 新客量,
				0 as 面访量,
				总里程,
				0 as 总行程量
			FROM
				DM_S_MILEAGE
			union all
			SELECT
				"年度",
				"月度",
				"日期",
				"负责人",
				0 as 商机量,
				0 as 新客量,
				0 as 面访量,
				0 as 总里程,
				总行程量
				FROM
				DM_S_TRAVEL) a
				group by "年度",
				"月度",
				"日期",
				"负责人";			
			COMMIT;
			--------记录报表处理的日志
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values('DM_S_PRESALES',SYSDATE,'DM_S_PRESALES数据更新完成','日志记录');
			COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_PRESALES',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----DM_S_AAOM_PRE
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_PRE',SYSDATE,'DM_S_AAOM_PRE数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_AAOM_PRE
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_PRE' ;
		COMMIT;
		-- Edward0604: DM_S_AAOM_PRE省份已添加
		INSERT INTO DM_S_AAOM_PRE
			(年度,
			月度,
			日期,
			分公司,
			地区,
			销售代表,
			商机量,
			新客量,
			面访量,
			总里程,
			总行程量,
			省份) -- 添加省份
		SELECT
			a.年度,
			a.月度,
			a.日期,
			b.负责人主属部门 as 分公司,
			case when b.所属分公司='大客户部' then '长沙'
				when b.所属分公司='常张' then '常德'
				when b.所属分公司='装载机事业部' then '长沙'
				when b.所属分公司='长潭' then '长沙'
				when b.所属分公司='九江兼景德镇' then '九江'
				when b.所属分公司='赣州西' then '赣州'
				when b.所属分公司 is null then '长沙'
				else b.所属分公司 END 所属分公司,
			a.负责人 as 销售代表,
			a.商机量,
			a.新客量,
			a.面访量,
			a.总里程,
			a.总行程量,
			b.省份 -- 添加省份
		FROM
			DM_S_PRESALES a,
			DW_DEPT_EMP b
		WHERE
			a.负责人 = b.负责人(+);
		COMMIT;

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_PRE',SYSDATE,'DM_S_AAOM_PRE数据更新完成','日志记录');
		COMMIT;

	EXCEPTION
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_AAOM_PRE',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----DM_S_AAOM_LOSE
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_LOSE',SYSDATE,'DM_S_AAOM_LOSE数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_AAOM_LOSE
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_LOSE' ;
		COMMIT;

		--Edward0605: DM_S_AAOM_LOSE添加省份
		INSERT INTO DM_S_AAOM_LOSE
			(年度,
			月度,
			日期,
			分公司,
			地区,
			销售代表,
			无竞争丢单量,
			有竞争丢单量,
			丢单量,
			省份)
		SELECT
			substr(to_char(最后修改时间,'yyyymmdd'),1,4) as 年度,
			substr(to_char(最后修改时间,'yyyymmdd'),5,2) as 月度,
			to_date(substr(to_char(最后修改时间,'yyyymmdd'),1,6) ||'01','yyyymmdd') as 日期,
			"负责人主属部门" as 分公司,
			case when 负责人主属部门='长潭分公司' then '长沙'
				 when 负责人主属部门='江西中旺' then '南昌'
				 when instr(负责人主属部门,'分公司') >0 then substr(负责人主属部门,1,instr(负责人主属部门,'分公司')-1) else 负责人主属部门 END as 地区,
			负责人 as 销售代表,
			count(case when 竞争状态='无竞争' then 序号 END) as 无竞争丢单量,
			count(case when 竞争状态='有竞争' then 序号 END) as 有竞争丢单量,
			count(序号) as 丢单量,
			省份
		FROM
			dw_cus_lose
		group by
			substr(to_char(最后修改时间,'yyyymmdd'),1,4) ,
			substr(to_char(最后修改时间,'yyyymmdd'),5,2) ,
			to_date(substr(to_char(最后修改时间,'yyyymmdd'),1,6) ||'01','yyyymmdd') ,
			"负责人主属部门",
			case when 负责人主属部门='长潭分公司' then '长沙'
				 when 负责人主属部门='江西中旺' then '南昌'
				 when instr(负责人主属部门,'分公司') >0 then substr(负责人主属部门,1,instr(负责人主属部门,'分公司')-1) else 负责人主属部门 END,
			负责人;
		COMMIT;

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_LOSE',SYSDATE,'DM_S_AAOM_LOSE数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_AAOM_LOSE',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----DM_S_AAOM_SR01
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_SR01',SYSDATE,'DM_S_AAOM_SR01数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_AAOM_SR01
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_SR01';
		COMMIT;

		-- Edward: 已变更字段DM_S_AAOM_SR01
		INSERT INTO DM_S_AAOM_SR01
			(年度,
			月度,
			日期,
			省份,
			所属分公司,
			客户类型,
			合同单位,
			销售方式,
			机器编号,
			CRM过账交机时间,
			型号,
			地区,
			营销代表,
			合同金额,
			数量,
			以旧换新数量,
			旧机亏损,
			信息费,
			赠送金额,
			毛利额)
		-- Edward: DW_S_DETAIL 已修改字段
		SELECT
			a.销售年份 as 年度,
			case when instr(销售月份SY,'月')>0 then 
					(case when length(substr(销售月份SY,1,instr(销售月份SY,'月')-1))=1 then '0'||substr(销售月份SY,1,instr(销售月份SY,'月')-1)
					 else substr(销售月份SY,1,instr(销售月份SY,'月')-1) END)
				when length(销售月份SY)=1 then '0'||销售月份SY
				when 销售月份SY is null then substr(to_char(CRM过账交机时间,'yyyymmdd'),5,2)
				else 销售月份SY END as 月度,
			to_date( 销售年份|| (case when instr(销售月份SY,'月')>0 then 
					(case when length(substr(销售月份SY,1,instr(销售月份SY,'月')-1))=1 then '0'||substr(销售月份SY,1,instr(销售月份SY,'月')-1)
				else substr(销售月份SY,1,instr(销售月份SY,'月')-1) END)
				when length(销售月份SY)=1 then '0'||销售月份SY
				when 销售月份SY is null then substr(to_char(CRM过账交机时间,'yyyymmdd'),5,2)
				else 销售月份SY END)||'01','yyyymmdd') as 日期,
			a.省份,
			a.所属分公司 as 所属分公司,
			a.客户类型,
			a.合同单位,
			a.销售方式,
			a.机器编号,
			a.CRM过账交机时间,
			case when instr(a."型号",'C')>0 then substr(a."型号",1,instr(a."型号",'C')-1)
				when instr(a."型号",'H')>0 then substr(a."型号",1,instr(a."型号",'H')-1)
				when instr(a."型号",'U')>0 then substr(a."型号",1,instr(a."型号",'U')-1) else a."型号" END as 型号,
			case when a.所属分公司='大客户部' then c.城市
				when a.所属分公司='长潭' then '长沙'
				when a.所属分公司='中晟' then '长沙'
				when a.所属分公司='总部' then '长沙'
				when a.所属分公司='吉首、张家界' then '张家界'
				when a.所属分公司='邵阳西' then '邵阳'
				when a.所属分公司='邵阳东' then '邵阳'
				else a.所属分公司 END as 地区,
			a.营销代表,
			sum(nvl(a.合同金额,0)) as 合同金额,
			sum(nvl(a.数量,0)) as 数量,
			sum(case when 以旧换新 ='是' then nvl(a.数量,0) else 0 END) as 以旧换新数量,
			sum(nvl(b.旧机亏损,0)) as 旧机亏损,
			sum(nvl(b.信息费,0)) as 信息费,
			sum(nvl(b.赠送配件金额,0)) as 赠送金额,
			sum(nvl(b."销售金额",0)-nvl(b."三一结算金额",0) + nvl(b."旧机亏损",0) - nvl(b."赠送配件金额",0) - nvl(b."信息费",0)-nvl(b."运费",0) - nvl(b."提奖",0) - nvl(b."招待费",0)) as 毛利额
		FROM
			dw_s_detail a,
			dw_s_profit b,
			ods_city_bc c
		WHERE 
			a.机器编号=b.机号(+) and a.机器编号=c.机器编号(+)
		group by 
			a.销售年份,
			(case when instr(销售月份SY,'月')>0 then 
				(case when length(substr(销售月份SY,1,instr(销售月份SY,'月')-1))=1 then '0'||substr(销售月份SY,1,instr(销售月份SY,'月')-1)
					  else substr(销售月份SY,1,instr(销售月份SY,'月')-1) END)
				 when length(销售月份SY)=1 then '0'||销售月份SY
				 when 销售月份SY is null then substr(to_char(CRM过账交机时间,'yyyymmdd'),5,2)
				 else 销售月份SY END),
			to_date(a.销售年份|| (case when instr(销售月份SY,'月')>0 then
				(case when length(substr(销售月份SY,1,instr(销售月份SY,'月')-1))=1 then '0'||substr(销售月份SY,1,instr(销售月份SY,'月')-1)
					  else substr(销售月份SY,1,instr(销售月份SY,'月')-1) END )
					  when length(销售月份SY)=1 then '0'||销售月份SY
					  when 销售月份SY is null then substr(to_char(CRM过账交机时间,'yyyymmdd'),5,2)
					  else 销售月份SY END)||'01','yyyymmdd'),
			a.省份,
			a.所属分公司,
			a.客户类型,
			a.合同单位,
			a.销售方式,
			a.机器编号,
			a.CRM过账交机时间,
			case when instr(a."型号",'C')>0 then substr(a."型号",1,instr(a."型号",'C')-1)
				 when instr(a."型号",'H')>0 then substr(a."型号",1,instr(a."型号",'H')-1)
				 when instr(a."型号",'U')>0 then substr(a."型号",1,instr(a."型号",'U')-1) else a."型号" END,
			case when a.所属分公司='大客户部' then c.城市
				 when a.所属分公司='长潭' then '长沙'
				 when a.所属分公司='中晟' then '长沙'
				 when a.所属分公司='总部' then '长沙'
				 when a.所属分公司='吉首、张家界' then '张家界'
				 when a.所属分公司='邵阳西' then '邵阳'
				 when a.所属分公司='邵阳东' then '邵阳'
				 else a.所属分公司 END,
			a.营销代表;
		COMMIT;

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_SR01',SYSDATE,'DM_S_AAOM_SR01数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_AAOM_SR01',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----DM_S_AAOM_SR
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
		values('DM_S_AAOM_SR',SYSDATE,'DM_S_AAOM_SR数据更新开始','日志记录');
		COMMIT;
		--------删除对象对应的DM表DM_S_AAOM_SR
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_SR' ;
		COMMIT;
		-- Edward: 已变更字段DM_S_AAOM_SR01
		-- Edward: 已变更字段DM_S_AAOM_SR
		-- Edward: 已变更字段ODS_MAP_MODEL.类型->小中大挖
		INSERT INTO DM_S_AAOM_SR(
			"年度",
			"月度",
			"日期",
			"省份",
			"所属分公司",
			"客户类型",
			"合同单位",
			"销售方式",
			"CRM过账交机时间",
			"型号",
			"小中大挖",
			"大行业",
			"地区",
			"营销代表",
			"合同金额",
			"数量",
			"以旧换新数量",
			"旧机亏损",
			"信息费",
			"赠送金额",
			"毛利额"
			)
		SELECT
			"年度",
			"月度",
			"日期",
			"省份",
			"所属分公司",
			"客户类型",
			"合同单位",
			"销售方式",
			"CRM过账交机时间",
			a."型号",
			b."小中大挖",
			b."大行业",
			"地区",
			"营销代表",
			sum("合同金额") as 合同金额,
			sum("数量") as "数量",
			sum("以旧换新数量") as "以旧换新数量",
			sum("旧机亏损") as "旧机亏损",
			sum("信息费") as "信息费",
			sum("赠送金额") as "赠送金额",
			sum("毛利额") as "毛利额"
		FROM
			DM_S_AAOM_SR01 a,
			ods_map_model b
		WHERE
			a.型号=b.型号(+)
		group by 
			"年度",
			"月度",
			"日期",
			"省份",
			"所属分公司",
			"客户类型",
			"合同单位",
			"销售方式",
			"CRM过账交机时间",
			a."型号",
			b."小中大挖",
			b."大行业",
			"地区",
			"营销代表";
		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_SR',SYSDATE,'DM_S_AAOM_SR数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_AAOM_SR',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----DM_S_AAOM_SMALL
	BEGIN
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_SMALL',SYSDATE,'DM_S_AAOM_SMALL数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_AAOM_SMALL
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_SMALL' ;
		COMMIT;
		INSERT INTO
			DM_S_AAOM_SMALL
		SELECT
			"年份" as 年度,
			"月份" as 月度,
			"年月" as 日期,
			case when "城市01"='湘西' then '吉首' else "城市01" END as 地区,
			sum(nvl("销量",0)) as 小行业市场容量
		FROM
			dw_industry_small
		group by
			"年份",
			"月份",
			"年月",
			case when "城市01"='湘西' then '吉首' 
				 else "城市01" END;
		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_SMALL',SYSDATE,'DM_S_AAOM_SMALL数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_AAOM_SMALL',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----DM_S_AAOM
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
		values('DM_S_AAOM',SYSDATE,'DM_S_AAOM数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_AAOM
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM' ;
		COMMIT;
		-- Edward0605: DM_S_AAOM添加省份
		INSERT INTO DM_S_AAOM
			(年度,
			月度,
			日期,
			地区,
			销售代表,
			销售金额,
			数量,
			以旧换新数量,
			旧机亏损,
			信息费,
			赠送配件金额,
			毛利额,
			商机量,
			新客量,
			面访量,
			总里程,
			总行程量,
			无竞争丢单量,
			有竞争丢单量,
			参与量,
			覆盖量,
			丢单量,
			省份)
		SELECT
			a.年度,
			a.月度,
			a.日期,
			a.地区,
			a.营销代表,
			sum(a.合同金额) as 合同金额,
			sum(a.数量) as 数量,
			sum(a.以旧换新数量) as 以旧换新数量,
			sum(a.旧机亏损) as 旧机亏损,
			sum(a.信息费) as 信息费,
			sum(a.赠送金额) as 赠送金额,
			sum(a.毛利额) as 毛利额,
			sum(a.商机量) as 商机量,
			sum(a.新客量) as 新客量,
			sum(a.面访量) as 面访量,
			sum(a.总里程) as 总里程,
			sum(a.总行程量) as 总行程量,
			sum(a.无竞争丢单量) as 无竞争丢单量,
			sum(a.有竞争丢单量) as 有竞争丢单量,
			sum(nvl(a.数量,0)+nvl(a.有竞争丢单量,0)) as 参与量,
			sum(nvl(a.数量,0)+nvl(a.丢单量,0)) as 覆盖量,
			sum(a.丢单量) as 丢单量,
			a.省份 -- 添加省份
		-- Edward0605: DM_S_AAOM_SR, DM_S_AAOM_LOSE, DM_S_AAOM_PRE
		FROM
			(SELECT
				to_char("年度") as "年度",
				"月度",
				"日期",
				"地区",
				"营销代表",
				"合同金额",
				"数量",
				"以旧换新数量",
				"旧机亏损",
				"信息费",
				"赠送金额",
				"毛利额",
				0 as 商机量,
				0 as 新客量,
				0 as 面访量,
				0 as 总里程,
				0 as 总行程量,
				0 as 无竞争丢单量,
				0 as 有竞争丢单量,
				0 as 丢单量,
				"省份"
			FROM
				dm_s_aaom_sr
			UNION ALL
			SELECT
				"年度",
				"月度",
				"日期",
				"地区",
				"销售代表",
				0 as "销售金额",
				0 as "数量",
				0 as "以旧换新数量",
				0 as "旧机亏损",
				0 as "信息费",
				0 as "赠送配件金额",
				0 as "毛利额",
				"商机量",
				"新客量",
				"面访量",
				 总里程,
				总行程量,
				0 as 无竞争丢单量,
				0 as 有竞争丢单量,
				0 as 丢单量,
				"省份"
			FROM
				dm_s_aaom_pre
			UNION ALL
			SELECT
				"年度",
				"月度",
				"日期",
				"地区",
				"销售代表",
				0 as "销售金额",
				0 as "数量",
				0 as "以旧换新数量",
				0 as "旧机亏损",
				0 as "信息费",
				0 as "赠送配件金额",
				0 as "毛利额",
				0 as "商机量",
				0 as "新客量",
				0 as "面访量", 
				0 as 总里程,
				0 as 总行程量,
				"无竞争丢单量",
				"有竞争丢单量",
				"丢单量",
				"省份"
			FROM
				dm_s_aaom_lose
			) a
		group by
			a.年度,
			a.月度,
			a.日期,
			a.地区,
			a.营销代表,
			a.省份;
		COMMIT;

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM',SYSDATE,'DM_S_AAOM数据更新完成','日志记录');
		COMMIT;

	EXCEPTION
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_AAOM',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----DM_S_AAOM_COM
	BEGIN
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_COM',SYSDATE,'DM_S_AAOM_COM数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_AAOM_COM
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_COM' ;
		COMMIT;

		-- Edward0605: DM_S_AAOM_COM添加省份
		INSERT INTO DM_S_AAOM_COM
			(年度,
			月度,
			日期,
			地区,
			销售金额,
			数量,
			以旧换新数量,
			旧机亏损,
			信息费,
			赠送配件金额,
			毛利额,
			商机量,
			新客量,
			面访量,
			无竞争丢单量,
			有竞争丢单量,
			参与量,
			覆盖量,
			丢单量,
			小行业市场容量,
			省份)
		SELECT
			a."年度",
			a."月度",
			a."日期",
			a."地区",
			"销售金额",
			"数量",
			"以旧换新数量",
			"旧机亏损",
			"信息费",
			"赠送配件金额",
			"毛利额",
			"商机量",
			"新客量",
			"面访量",
			"无竞争丢单量",
			"有竞争丢单量",
			"参与量",
			"覆盖量",
			"丢单量",
			"省份", --DM_S_AAOM省份
			d.小行业市场容量
		FROM
			(SELECT
				"年度",
				"月度",
				"日期",
				"地区",
				sum("销售金额") as "销售金额",
				sum("数量") as "数量",
				sum("以旧换新数量") as "以旧换新数量",
				sum("旧机亏损") as "旧机亏损",
				sum("信息费") as "信息费",
				sum("赠送配件金额") as "赠送配件金额",
				sum("毛利额") as "毛利额",
				sum("商机量") as "商机量",
				sum("新客量") as "新客量",
				sum("面访量") as "面访量",
				sum("无竞争丢单量") as "无竞争丢单量",
				sum("有竞争丢单量") as "有竞争丢单量",
				sum("参与量") as "参与量",
				sum("覆盖量") as "覆盖量",
				sum("丢单量") as "丢单量",
				"省份"
			FROM
				dm_s_aaom
			group by 
				"年度",
				"月度",
				"日期",
				"地区",
				"省份") a,
			DM_S_AAOM_SMALL d
		WHERE
			a.年度 = d.年度 and a.月度 = d.月度 and a.地区 = d.地区;
		COMMIT;

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_COM',SYSDATE,'DM_S_AAOM_COM数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_AAOM_COM',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;







	----DM_S_AAOM_FUNNEL
	BEGIN
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_FUNNEL',SYSDATE,'DM_S_AAOM_FUNNEL数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_AAOM_FUNNEL
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_FUNNEL';
		COMMIT;
		-- Edward0605: 添加省份
		INSERT INTO DM_S_AAOM_FUNNEL
			(年度,
			月度,
			日期,
			地区,
			销售代表,
			序号,
			度量,
			值,
			省份)
		SELECT
			a."年度",
			a."月度",
			a."日期",
			a."地区",
			a.销售代表,
			b.序号,
			b.度量,
			case when b.度量='新增客户量' then a.新客量
				 when b.度量='面访量' then a.面访量
				 when b.度量='商机量' then a.商机量
				 when b.度量='成交量' then a.数量 else 0 END as 值,
			a."省份"
		FROM
			dm_s_aaom a,
			dim_funnel b
		WHERE
			a."年度">=2019;
		COMMIT;
		
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_FUNNEL',SYSDATE,'DM_S_AAOM_FUNNEL数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_AAOM_FUNNEL',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;







	----单台盈亏点分析
	----DW_S_DTYKDFX
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DW_S_DTYKDFX',SYSDATE,'DW_S_DTYKDFX数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DW表DW_S_DTYKDFX
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_S_DTYKDFX' ;
		COMMIT;
		-- Edward: ODS_MAP_MODEL无变更字段
		INSERT INTO
			DW_S_DTYKDFX
		SELECT
			a."机号",
			a."设备型号",
			d."小中大挖",
			d."大行业",
			d."小行业",
			a."省份",
			a."地区",
			a."设备类型",
			a."定位地点",
			a."最后登录时间",
			a."客户名",
			a."实际管理人",
			a."联系电话",
			a."合同单位",
			a."销售方式",
			a."保证金",
			a."原营销代表",
			a."现营销代表",
			a."发货日期",
			a."放款日期",
			a."客户分类",
			a."逾期款总计",
			a."总逾期期数",
			a."近12个月累计还款",
			a."近6个月累计还款",
			a."近3个月累计还款",
			a."合同价",
			a."折扣金额",
			a."折后价",
			a."在外货款",
			a."未到期",
			b."总工时",
			b."当年工时",
			c."销售金额"-c."三一结算金额"+c."旧机亏损"-c."赠送配件金额" as 价格,
			c."管理费用"+c."信息费"+c."运费" +c."提奖"+c."招待费"+c."融资包干利息" as 费用,
			c."奖励返利",
			c."销售金额"-c."三一结算金额"+c."奖励返利" -c."赠送配件金额" -c."管理费用"-c."信息费"-c."运费" -c."提奖"-c."招待费" +c."旧机亏损" -c."融资包干利息" as 毛利,
			(c."销售金额"-c."三一结算金额"+c."奖励返利" -c."赠送配件金额" -c."管理费用"-c."信息费"-c."运费" -c."提奖"-c."招待费" +c."旧机亏损" -c."融资包干利息")/c.销售金额 as 利润率,
			c.毛利3,
			c.毛利2
		FROM
			dw_exc_ledger a,
			dm_hnzw_zgs b,
			DW_S_PROFIT c,
			ods_map_model d
		WHERE
			a."设备型号"=d."型号"(+) and a."机号"=b."机号"(+) and a."机号"=c."机号"(+); 	
		COMMIT;

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DW_S_DTYKDFX',SYSDATE,'DW_S_DTYKDFX数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DW_S_DTYKDFX',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----单台盈亏点分析 瀑布图
	----DM_WATERFALL_PROFIT
	BEGIN
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_WATERFALL_PROFIT',SYSDATE,'DM_WATERFALL_PROFIT数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_WATERFALL_PROFIT
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_WATERFALL_PROFIT' ;
		COMMIT;

		INSERT INTO
			DM_WATERFALL_PROFIT
		SELECT
			"机号",
			b.编码,
			b.科目,
			nvl(case when b.科目='销售金额' then "销售金额" 
					 when b.科目='三一结算金额' then "销售金额"-"三一结算金额" 
					 when b.科目='旧机亏损' then "销售金额"-"三一结算金额"+"旧机亏损"
					 when b.科目='赠送配件金额' then "销售金额"-"三一结算金额"+"旧机亏损" -"赠送配件金额" 
					 when b.科目='管理费用' then "销售金额"-"三一结算金额"+"旧机亏损" -"赠送配件金额" -"管理费用" 
					 when b.科目='信息费' then "销售金额"-"三一结算金额"+"旧机亏损" -"赠送配件金额" -"管理费用"-"信息费" 
					 when b.科目='运费' then "销售金额"-"三一结算金额"+"旧机亏损" -"赠送配件金额" -"管理费用"-"信息费"-"运费" 
					 when b.科目='提奖' then "销售金额"-"三一结算金额"+"旧机亏损" -"赠送配件金额" -"管理费用"-"信息费"-"运费" -"提奖" 
					 when b.科目='招待费' then "销售金额"-"三一结算金额"+"旧机亏损" -"赠送配件金额" -"管理费用"-"信息费"-"运费" -"提奖"-"招待费" 
					 when b.科目='融资包干利息' then "销售金额"-"三一结算金额"+"旧机亏损" -"赠送配件金额" -"管理费用"-"信息费"-"运费" -"提奖"-"招待费" -"融资包干利息"
					 when b.科目='奖励返利' then "销售金额"-"三一结算金额"+"旧机亏损" -"赠送配件金额" -"管理费用"-"信息费"-"运费" -"提奖"-"招待费" +"奖励返利" -"融资包干利息" END,0) as 金额
		FROM
			dw_s_profit a,
			ODS_SUBJECTS b;
		COMMIT;

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_WATERFALL_PROFIT',SYSDATE,'DM_WATERFALL_PROFIT数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_WATERFALL_PROFIT',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----工时分析
	----DM_S_HNZW_GSQX
	BEGIN
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_HNZW_GSQX',SYSDATE,'DM_S_HNZW_GSQX数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_HNZW_GSQX
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_HNZW_GSQX' ;
		COMMIT;

		INSERT INTO DM_S_HNZW_GSQX 
		SELECT
			"机号",
			case when instr("机型",'C')>0 then substr("机型",1,instr("机型",'C')-1)
				 when instr("机型",'H')>0 then substr("机型",1,instr("机型",'H')-1)
				 when instr("机型",'W')>0 then substr("机型",1,instr("机型",'W')-1)
				 when "机型"='SY485S1I3K' then 'SY485'
				 when instr("机型",'U')>0 then substr("机型",1,instr("机型",'U')-1) else "机型" END as "机型",
			max("总工时") as 总工时,
			sum("当日工时") as 工时,
			case when instr("定位详情",'省')>0 then substr( "定位详情",1,instr("定位详情",'省')-1)
				 when instr("定位详情",'广西壮族自治区')>0 then '广西'
				 when instr("定位详情",'上海')>0 then '上海'
				 when instr("定位详情",'北京')>0 then '北京'
				 when instr("定位详情",'重庆')>0 then '重庆'
				 when instr("定位详情",'天津')>0 then '天津'
				 when instr("定位详情",'香港特别行政区')>0 then '香港'
				 when instr("定位详情",'澳门特别行政区')>0 then '澳门'
				 when instr("定位详情",'Telangana')>0 then 'Telangana'
				 when instr("定位详情",'Karnātaka')>0 then 'Karnātaka'
				 when instr("定位详情",'Tamil')>0 then 'Tamil'
				 when instr("定位详情",'Odisha')>0 then 'Odisha'
				 when instr("定位详情",'宁夏回族自治区')>0 then '宁夏'
				 when instr("定位详情",'内蒙古自治区')>0 then '内蒙古'
				 when instr("定位详情",'新疆')>0 then '新疆'
				 when instr("定位详情",'西藏自治区')>0 then '西藏' else "定位详情" END as 省,
			case when instr("定位详情",'省')>0 then 
				(case when instr("定位详情",'省辖县')>0 then substr( "定位详情",instr("定位详情",'省辖县')+3)
					  when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'省')+1,instr("定位详情",'自治州')-instr("定位详情",'省')+2)
					  else substr( "定位详情",instr("定位详情",'省')+1,instr("定位详情",'市')-instr("定位详情",'省')-1) END)
				 when instr("定位详情",'广西壮族自治区')>0 then
				(case when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'自治州')-instr("定位详情",'自治区')+2)
					  else substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'市')-instr("定位详情",'自治区'))END)
				 when instr("定位详情",'上海')>0 then '上海'
				 when instr("定位详情",'北京')>0 then '北京'
				 when instr("定位详情",'重庆')>0 then '重庆'
				 when instr("定位详情",'天津')>0 then '天津'
				 when instr("定位详情",'香港特别行政区')>0 then '香港'
				 when instr("定位详情",'澳门特别行政区')>0 then '澳门'
				 when instr("定位详情",'Telangana')>0 then 'Telangana'
				 when instr("定位详情",'Karnātaka')>0 then 'Karnātaka'
				 when instr("定位详情",'Tamil')>0 then 'Tamil'
				 when instr("定位详情",'Odisha')>0 then 'Odisha'
				 when instr("定位详情",'宁夏回族自治区')>0 then
				(case when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'自治州')-instr("定位详情",'自治区')+2)
					  else substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'市')-instr("定位详情",'自治区')) END)
				 when instr("定位详情",'内蒙古自治区')>0 then
				(case when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'自治州')-instr("定位详情",'自治区')+2)
					  else substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'市')-instr("定位详情",'自治区')) END)
				 when instr("定位详情",'新疆维吾尔自治区')>0 then 
				(case when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'自治州')-instr("定位详情",'自治区')+2)
					  else substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'市')-instr("定位详情",'自治区')) END)
				 when instr("定位详情",'西藏自治区')>0 then
				(case when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'自治州')-instr("定位详情",'自治区')+2)
					  else substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'市')-instr("定位详情",'自治区')) END)
				 else "定位详情" END as 市,
			case when instr("定位详情",'省辖县')>0 then substr( "定位详情",instr("定位详情",'省辖县')+3) 
				 when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治州')+3)
				 when instr("定位详情",'市')>0 then substr( "定位详情",instr("定位详情",'市')+1)
				 else "定位详情" END as 区县,
			"定位详情",
			to_date(substr(to_char("更新时间",'YYYYMMDD'),1,6)||'01','YYYY-MM-DD') as 更新时间,
			substr(to_char("更新时间",'YYYYMMDD'),1,4)||'W'||to_char("更新时间",'ww') as 周
		FROM
			dw_hnzw_wjgs
		group by 
			"机号",
			case when instr("机型",'C')>0 then substr("机型",1,instr("机型",'C')-1)
				 when instr("机型",'H')>0 then substr("机型",1,instr("机型",'H')-1)
				 when instr("机型",'W')>0 then substr("机型",1,instr("机型",'W')-1)
				 when "机型"='SY485S1I3K' then 'SY485'
				 when instr("机型",'U')>0 then substr("机型",1,instr("机型",'U')-1) else "机型" END,
			case when instr("定位详情",'省')>0 then substr( "定位详情",1,instr("定位详情",'省')-1)
				 when instr("定位详情",'广西壮族自治区')>0 then '广西'
				 when instr("定位详情",'上海')>0 then '上海'
				 when instr("定位详情",'北京')>0 then '北京'
				 when instr("定位详情",'重庆')>0 then '重庆'
				 when instr("定位详情",'天津')>0 then '天津'
				 when instr("定位详情",'香港特别行政区')>0 then '香港'
				 when instr("定位详情",'澳门特别行政区')>0 then '澳门'
				 when instr("定位详情",'Telangana')>0 then 'Telangana'
				 when instr("定位详情",'Karnātaka')>0 then 'Karnātaka'
				 when instr("定位详情",'Tamil')>0 then 'Tamil'
				 when instr("定位详情",'Odisha')>0 then 'Odisha'
				 when instr("定位详情",'宁夏回族自治区')>0 then '宁夏'
				 when instr("定位详情",'内蒙古自治区')>0 then '内蒙古'
				 when instr("定位详情",'新疆')>0 then '新疆'
				 when instr("定位详情",'西藏自治区')>0 then '西藏' else "定位详情" END,
			case when instr("定位详情",'省')>0 then 
				(case when instr("定位详情",'省辖县')>0 then substr( "定位详情",instr("定位详情",'省辖县')+3)
					  when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'省')+1,instr("定位详情",'自治州')-instr("定位详情",'省')+2)
					  else substr( "定位详情",instr("定位详情",'省')+1,instr("定位详情",'市')-instr("定位详情",'省')-1) END)
				 when instr("定位详情",'广西壮族自治区')>0 then
				(case when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'自治州')-instr("定位详情",'自治区')+2)
					  else substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'市')-instr("定位详情",'自治区'))END)
				 when instr("定位详情",'上海')>0 then '上海'
				 when instr("定位详情",'北京')>0 then '北京'
				 when instr("定位详情",'重庆')>0 then '重庆'
				 when instr("定位详情",'天津')>0 then '天津'
				 when instr("定位详情",'香港特别行政区')>0 then '香港'
				 when instr("定位详情",'澳门特别行政区')>0 then '澳门'
				 when instr("定位详情",'Telangana')>0 then 'Telangana'
				 when instr("定位详情",'Karnātaka')>0 then 'Karnātaka'
				 when instr("定位详情",'Tamil')>0 then 'Tamil'
				 when instr("定位详情",'Odisha')>0 then 'Odisha'
				 when instr("定位详情",'宁夏回族自治区')>0 then
				(case when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'自治州')-instr("定位详情",'自治区')+2)
					  else substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'市')-instr("定位详情",'自治区')) END)
				 when instr("定位详情",'内蒙古自治区')>0 then
				(case when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'自治州')-instr("定位详情",'自治区')+2)
					  else substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'市')-instr("定位详情",'自治区')) END)
				 when instr("定位详情",'新疆维吾尔自治区')>0 then 
				(case when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'自治州')-instr("定位详情",'自治区')+2)
					  else substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'市')-instr("定位详情",'自治区')) END)
				 when instr("定位详情",'西藏自治区')>0 then
				(case when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'自治州')-instr("定位详情",'自治区')+2)
					  else substr( "定位详情",instr("定位详情",'自治区')+3,instr("定位详情",'市')-instr("定位详情",'自治区')) END)
				 else "定位详情" END,
			case when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治区')+3)
				 when instr("定位详情",'市')>0 then substr( "定位详情",instr("定位详情",'市')+1)
				 else "定位详情" END,
			case when instr("定位详情",'省辖县')>0 then substr( "定位详情",instr("定位详情",'省辖县')+3) 
				 when instr("定位详情",'自治州')>0 then substr( "定位详情",instr("定位详情",'自治州')+3)
				 when instr("定位详情",'市')>0 then substr( "定位详情",instr("定位详情",'市')+1)
				 else "定位详情" END,
			"定位详情",
			to_date(substr(to_char("更新时间",'YYYYMMDD'),1,6)||'01','YYYY-MM-DD'),
			substr(to_char("更新时间",'YYYYMMDD'),1,4)||'W'||to_char("更新时间",'ww');

		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_HNZW_GSQX',SYSDATE,'DM_S_HNZW_GSQX数据更新完成','日志记录');
		COMMIT;

	EXCEPTION
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_HNZW_GSQX',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----工时分析,只提取湖南和江西数据
	----dm_s_hnzw_gsqx01
	BEGIN
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_HNZW_GSQX01',SYSDATE,'DM_S_HNZW_GSQX01数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DM_S_HNZW_GSQX01
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_HNZW_GSQX01' ;
		COMMIT;

		INSERT INTO
			DM_S_HNZW_GSQX01 
		SELECT
			a."机号",
			a."机型",
			case when a."机型" in('SY55','SY60','SY70','SY75','SY85','SY95','SY125','SY135','SY155','SY65','SY115','SY150','SY35') then '小挖'
				 when a."机型" in('SY195','SY205','SY215','SY225','SY245','SY265','SY285','SY305','SY230','SY240','SY235','SY330','SY335') then '中挖'
				 else '大挖' END as 类型,
			a."总工时",
			case when b."总工时"<=4000 then '4000小时以内'
				 when b."总工时">4000 and b."总工时"<=7000 then '4000~7000小时'
				 when b."总工时">7000 and b."总工时"<=10000 then '7000~10000小时'
				 when b."总工时">10000 and b."总工时"<=13000 then '10000~13000小时'
				 when b."总工时">13000 and b."总工时"<=15000 then '13000~15000小时'
				 when b."总工时">15000 and b."总工时"<=20000 then '15000~20000小时'
				 when b."总工时">20000 then '20000小时以上' END as 工时分段,
			a."工时",
			a."省",
			a."市",
			a."区县",
			a."更新时间",a.周
			FROM
				dm_s_hnzw_gsqx a,
				dm_hnzw_zgs b
			WHERE
				a."机号"=b."机号"(+) and a."省" in ('湖南','江西') and a."总工时">10; -- 过滤掉中旺库存样机
		COMMIT;

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_HNZW_GSQX01',SYSDATE,'DM_S_HNZW_GSQX01数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DM_S_HNZW_GSQX01',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----一户一册
	----DW_EXC_LEDGER
	BEGIN
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DW_EXC_LEDGER',SYSDATE,'DW_EXC_LEDGER数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的DM表DW_EXC_LEDGER
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_EXC_LEDGER' ;
		COMMIT;

		INSERT INTO
			DW_EXC_LEDGER
		SELECT
			a."机号",
			case when instr("设备型号",'C')>0 then substr("设备型号",1,instr("设备型号",'C')-1)
				 when instr("设备型号",'H')>0 then substr("设备型号",1,instr("设备型号",'H')-1)
				 when instr("设备型号",'U')>0 then substr("设备型号",1,instr("设备型号",'U')-1) else "设备型号" END as "设备型号",
			"省份",
			"地区",
			"设备类型",
			"定位地点",
			"最后登录时间",
			case when b."总工时"<=4000 then '4000小时以内'
				 when b."总工时">4000 and b."总工时"<=7000 then '4000~7000小时'
				 when b."总工时">7000 and b."总工时"<=10000 then '7000~10000小时'
				 when b."总工时">10000 and b."总工时"<=13000 then '10000~13000小时'
				 when b."总工时">13000 and b."总工时"<=15000 then '13000~15000小时'
				 when b."总工时">15000 and b."总工时"<=20000 then '15000~20000小时'
				 when b."总工时">20000 then '20000小时以上' END as 工时分段,
			b."总工时",
			b."当月工时",
			b."当年工时",
			"客户名",
			"实际管理人",
			"联系电话",
			"合同单位",
			"销售方式",
			"保证金",
			"原营销代表",
			"现营销代表",
			"发货日期",
			"放款日期",
			"客户分类",
			"催收责任人",
			"是否为价值销售",
			"管理专干",
			"流转时间",
			"备注",
			nvl("银行按揭款贷款余额",0) as "银行按揭款贷款余额",
			nvl("银行按揭款本月到期款",0) as "银行按揭款本月到期款",
			nvl("银行逾期款",0) as "银行逾期款",
			nvl("垫付款",0) as "垫付款",
			nvl("公司货款货款余额",0) as "公司货款货款余额",
			nvl("公司货款本月到期款",0) as "公司货款本月到期款",
			nvl("逾期金额",0) as "逾期金额",
			nvl("旧机抵款",0) as "旧机抵款",
			nvl("差异",0) as "差异",
			"差异说明",
			nvl("总到期款",0) as "总到期款",
			nvl("总逾期款",0) as "总逾期款",
			nvl("逾期罚息",0) as "逾期罚息",
			nvl("逾期款总计",0) as "逾期款总计",
			nvl("客户数",0) as "客户数",
			nvl("回款客户数",0) as "回款客户数",
			nvl("总逾期期数",0) as "总逾期期数",
			nvl("旧机抵款2",0) as "旧机抵款2",
			nvl("其它抵款",0) as "其它抵款",
			nvl("近12个月累计还款",0) as "近12个月累计还款",
			nvl("近6个月累计还款",0) as "近6个月累计还款",
			nvl("近3个月累计还款",0) as "近3个月累计还款",
			nvl("垫付",0) as "垫付",
			nvl("合同价",0) as "合同价",
			nvl("折扣金额",0) as "折扣金额",
			nvl("折后价",0) as "折后价",
			nvl("在外货款",0) as "在外货款",
			nvl("未到期",0) as "未到期"
		FROM
			ods_exc_ledger a,
			dm_hnzw_zgs b
		WHERE
			a."机号"=b."机号"(+);
		COMMIT;

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DW_EXC_LEDGER',SYSDATE,'DW_EXC_LEDGER1数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('DW_EXC_LEDGER',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----"湖南债权部一户一册"
	----"湖南债权部一户一册"
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('湖南债权部一户一册',SYSDATE,'湖南债权部一户一册数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的"湖南债权部一户一册"
		EXECUTE IMMEDIATE 'TRUNCATE TABLE "湖南债权部一户一册"' ;
		COMMIT;

		INSERT INTO "湖南债权部一户一册"
			("机号",
			"设备型号",
			"省份",
			"地区",
			"定位地点",
			"客户名",
			"销售方式",
			"保证金",
			"原营销代表",
			"现营销代表",
			"发货日期",
			"客户分类",
			"总逾期款",
			"总逾期期数",
			"总到期款",
			"总逾期款2",
			"逾期罚息")
		SELECT 
			"机号",
			"设备型号",
			"省份",
			"地区",
			"定位地点",
			"客户名",
			"销售方式",
			"保证金",
			"原营销代表",
			"现营销代表",
			"发货日期",
			"客户分类",
			"总逾期款",
			"总逾期期数",
			"总到期款",
			总逾期期数 as "总逾期款2",
			"逾期罚息"
		FROM
			ods_exc_ledger;
		COMMIT;

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('湖南债权部一户一册',SYSDATE,'湖南债权部一户一册数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('湖南债权部一户一册',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;






	----"应收账"
	BEGIN

		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('应收账',SYSDATE,'应收账数据更新开始','日志记录');
		COMMIT;

		--------删除对象对应的"应收账"
		EXECUTE IMMEDIATE 'TRUNCATE TABLE "应收账"' ;
		COMMIT;
		INSERT INTO "应收账"
			("成交方式",
			"状态",
			"机号",
			"客户编码",
			"客户名称",
			"营销代表工号",
			"营销代表姓名",
			"型号",
			"主机编码",
			"还款计划行号",
			"还款项目",
			"应还款时间",
			"应还金额",
			"实还金额",
			"实际还款时间",
			"最后还款时间")
		SELECT 
			"成交方式",
			"状态",
			"机号",
			"客户编码",
			"客户名称",
			"营销代表工号",
			"营销代表姓名",
			"型号",
			"主机编码",
			"还款计划行号",
			"还款项目",
			"应还款时间",
			"应还金额",
			"实还金额",
			"实际还款时间",
			"最后还款时间"
		FROM
			ods_acc_rec;
		COMMIT;
		--------记录报表处理的日志
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('应收账',SYSDATE,'应收账数据更新完成','日志记录');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------记录报表处理的日志
			prc_wlf_sys_writelog('应收账',SYSDATE,
				'发生系统错误 ： 错误代码 ' || SQLCODE( ) || '   错误信息：' ||SQLERRM( ) ,'错误日志记录');
	END;

END SP_HNZW_S_LOAD_ADW;