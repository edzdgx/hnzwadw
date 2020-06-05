/*
Edward0603:
	-indent�޸�
	-���޸ı��:
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
	-���޸ı��:
		INC_LOSE_NO_COM +ʡ��
		ODS_LOSE_NO_COM +ʡ��
		INC_LOSE_COM +ʡ��
		ODS_LOSE_COM +ʡ��
		DW_CUS_LOSE +ʡ��
		DM_S_AAOM_LOSE +ʡ��
		DM_S_AAOM +ʡ��
		DM_S_AAOM_COM +ʡ��
		DM_S_AAOM_FUNNEL +ʡ��
	-need fix:
		DM_SALESMAN_SCORE �ű���������
GZY0605: 
	-���޸ı��:
		dw_service_orders
		���񶩵�
*/
CREATE OR REPLACE PROCEDURE SP_HNZW_S_LOAD_ADW IS
	------------��������
	CURRENT_RPT_DATE	DATE; ----��ǰ���µı���ʱ��
	CURRENT_UPLOAD_DATE	DATE; ----��ǰ�Ѿ��������ʱ��
	QTY_COM				NUMBER; ---��¼��
	V_TABLE_NAME		VARCHAR2(300);
	V_OBJECT_NAME		VARCHAR2(200);
	V_MSG				VARCHAR2(300);

BEGIN
	--�������С��ҵ����
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_INDUSTRY_SMALL;

	IF QTY_COM>0 then
		BEGIN
			V_TABLE_NAME		:= 'INC_INDUSTRY_SMALL';
			V_OBJECT_NAME		:= 'ODS_INDUSTRY_SMALL';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				Ʒ��,
				�ּ�,
				����,
				����,
				���,
				�·�,
				����,
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_INDUSTRY_SMALL a
			WHERE exists(SELECT 1 FROM INC_INDUSTRY_SMALL WHERE ���=a.��� and �·�=a.�·� );
			COMMIT;

			----ɾ��ODS_INDUSTRY_SMALL�м�¼��
			DELETE FROM ODS_INDUSTRY_SMALL a
			 	WHERE exists(SELECT 1 FROM INC_INDUSTRY_SMALL WHERE ���=a.��� and �·�=a.�·� );
			COMMIT;

			----����������������
			INSERT INTO ODS_INDUSTRY_SMALL (Ʒ��,
				�ּ�,
				����,
				����,
				���,
				�·�,
				����,
				TIME_STAMP)
			SELECT 
				Ʒ��,
				�ּ�,
				����,
				����,
				���,
				�·�,
				����,
				sysdate
			FROM INC_INDUSTRY_SMALL a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_industry_small' ;
			COMMIT;

			INSERT INTO
				dw_industry_small 
			SELECT
				"Ʒ��",
				case when upper("�ּ�")='<6T' then '��6T'
					 when upper("�ּ�")='10-19T' then '10T-19T'
					 when upper("�ּ�")='>=40T' then '��40T' else upper("�ּ�") END as С��ҵ,
				"����",
				case when "����" is null then '��ɳ'
					 when "����"='����' then '��ɳ'
					 when "����"='��������������������' then '����'
					 when instr("����",'��')>0 then substr("����",1,instr("����",'��')-1) else "����" END as "����01",
				"����",
				"���",
				"�·�",
				to_date(to_char("���")||(case when "�·�"<10 then '0'||"�·�" else to_char("�·�") END )||'01','YYYY-MM-DD') as ����,
				"����"
			FROM
				ods_industry_small;
			COMMIT;

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_industry_small���ݸ������','��־��¼');
			COMMIT;
		EXCEPTION 
			WHEN OTHERS THEN
				-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼'); 
			END;
		END IF;






	--�̻���
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_BUSINESS_OPP;

	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_BUSINESS_OPP';
			V_OBJECT_NAME	:= 'ODS_BUSINESS_OPP';

				----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				Ա������,
				�̻�״̬,
				����ʱ��,
				������ʱ��,
				����޸�ʱ��,
				�̻�����,
				�ͻ�����,
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM
				ODS_BUSINESS_OPP a
			WHERE
				exists(SELECT 1 FROM INC_BUSINESS_OPP WHERE Ա������=a.Ա������ );
			COMMIT;

			----ɾ��ODS_BUSINESS_OPP�м�¼��
			DELETE FROM ODS_BUSINESS_OPP a
			 	WHERE exists(SELECT 1 FROM INC_BUSINESS_OPP WHERE Ա������=a.Ա������);
			COMMIT;

			----����������������
			INSERT INTO ODS_BUSINESS_OPP
				("Ա������",
				"�̻�״̬",
				"����ʱ��",
				"������ʱ��",
				"����޸�ʱ��",
				"�Ƿ��о�Ʒ����",
				"�̻�����",
				"Ԥ�Ƴɽ����",
				"�ͻ�����",
				"�Ŷӽ�ɫ",
				TIME_STAMP)
			SELECT
				"Ա������",
				"�̻�״̬",
				"����ʱ��",
				"������ʱ��",
				"����޸�ʱ��",
				"�Ƿ��о�Ʒ����",
				"�̻�����",
				"Ԥ�Ƴɽ����",
				"�ͻ�����",
				"�Ŷӽ�ɫ",
				SYSDATE
			FROM
				inc_business_opp a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_business_opp' ;
			COMMIT;

			INSERT INTO dw_business_opp 
			SELECT
				"Ա������",
				"�̻�״̬",
				"����ʱ��",
				"������ʱ��",
				"����޸�ʱ��",
				"�Ƿ��о�Ʒ����",
				"�̻�����",
				"Ԥ�Ƴɽ����",
				"�ͻ�����",
				"�Ŷӽ�ɫ",
				time_stamp
			FROM
				ods_business_opp
			WHERE
				�Ƿ��о�Ʒ���� is not null;
			COMMIT;

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_business_opp���ݸ������','��־��¼');
			COMMIT;
		EXCEPTION 
			WHEN OTHERS THEN
			 	-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;







	--�����ͻ�����
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_CUSTOMER_NEW;

		IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_CUSTOMER_NEW';
			V_OBJECT_NAME	:= 'ODS_CUSTOMER_NEW';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"Ա������",
				"�ͻ�����",
				"�ɽ�״̬",
				"������ʱ��",
				"�Ŷӽ�ɫ",
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_CUSTOMER_NEW a
			WHERE exists(SELECT 1 FROM INC_CUSTOMER_NEW WHERE Ա������=a.Ա������ );
			COMMIT;

			----ɾ��ODS_CUSTOMER_NEW�м�¼��
			DELETE FROM ODS_CUSTOMER_NEW a
			WHERE exists(SELECT 1 FROM INC_CUSTOMER_NEW WHERE Ա������=a.Ա������);
			COMMIT;

			----����������������
			INSERT INTO ODS_CUSTOMER_NEW ("Ա������",
				"�ͻ�����",
				"�ɽ�״̬",
				"������ʱ��",
				"�Ŷӽ�ɫ",
				TIME_STAMP)
			SELECT
				"Ա������",
				"�ͻ�����",
				"�ɽ�״̬",
				"������ʱ��",
				"�Ŷӽ�ɫ",
				SYSDATE
			FROM
				INC_CUSTOMER_NEW a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_customer_new' ;
			COMMIT;

			INSERT INTO
				dw_customer_new 
			SELECT
				"Ա������",
				"�ͻ�����",
				"�ɽ�״̬",
				"������ʱ��",
				"�Ŷӽ�ɫ",
				time_stamp
			FROM
				ODS_CUSTOMER_NEW
			WHERE
				������ʱ�� is not null;
			COMMIT;

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_customer_new���ݸ������','��־��¼');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	--����������󵼳�������޾�����������
	-- Edward0604: INC_LOSE_NO_COM, ODS_LOSE_NO_COM ʡ�������
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_LOSE_NO_COM;

	IF QTY_COM>0 then
		BEGIN
			V_TABLE_NAME	:= 'INC_LOSE_NO_COM';
			V_OBJECT_NAME	:= 'ODS_LOSE_NO_COM';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"������",
				"���",
				"��������������",
				"ҵ������",
				"����޸�ʱ��",
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_LOSE_NO_COM a
			WHERE exists(SELECT 1 FROM INC_LOSE_NO_COM WHERE ���=a.��� );
			COMMIT;

			----ɾ��ODS_LOSE_NO_COM�м�¼��
			DELETE FROM ODS_LOSE_NO_COM a
				WHERE exists(SELECT 1 FROM INC_LOSE_NO_COM WHERE ���=a.���);
				COMMIT;

			----����������������
			INSERT INTO ODS_LOSE_NO_COM
				("�ͻ�����",
				"�̻�����",
				"����Ʒ��",
				"�ͺ�",
				"�ɽ�����",
				"�ɽ���ʽ",
				"�ɽ��۸�",
				"�׸�����",
				"����ԭ��",
				"ͼƬ",
				"�ⲿ������",
				"����״̬",
				"����Ŷ�",
				"������",
				"������",
				"���",
				"��������������",
				"ҵ������",
				"����״̬",
				"��������",
				"����ʱ��",
				"����޸���",
				"����޸�ʱ��",
				"ʡ��")
			SELECT
				"�ͻ�����",
				"�̻�����",
				"����Ʒ��",
				"�ͺ�",
				"�ɽ�����",
				"�ɽ���ʽ",
				"�ɽ��۸�",
				"�׸�����",
				"����ԭ��",
				"ͼƬ",
				"�ⲿ������",
				"����״̬",
				"����Ŷ�",
				"������",
				"������",
				"���",
				"��������������",
				"ҵ������",
				"����״̬",
				"��������",
				"����ʱ��",
				"����޸���",
				"����޸�ʱ��",
				"ʡ��"
			FROM
				INC_LOSE_NO_COM a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
				-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
			END;
	END IF;






	--����������󵼳�������о�����������
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_LOSE_COM;

	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_LOSE_COM';
			V_OBJECT_NAME	:= 'ODS_LOSE_COM';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"������",
				"���",
				"��������������",
				"ҵ������",
				"����޸�ʱ��",
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_LOSE_COM a
			WHERE exists(SELECT 1 FROM INC_LOSE_COM WHERE ���=a.���);
			COMMIT;

			----ɾ��ODS_LOSE_COM�м�¼��
			DELETE FROM ODS_LOSE_COM a
			WHERE exists(SELECT 1 FROM INC_LOSE_COM WHERE ���=a.���);
			COMMIT;

			----����������������
			-- Edward0605: INC_LOSE_COM, ODS_LOSE_COM���ʡ��
			INSERT INTO ODS_LOSE_COM
				("�ͻ�����",
				"�̻�����",
				"����Ʒ��",
				"�ͺ�",
				"�ɽ�����",
				"�ɽ���ʽ",
				"�ɽ��۸�",
				"�׸�����",
				"����ԭ��",
				"ͼƬ",
				"�ⲿ������",
				"����״̬",
				"����Ŷ�",
				"������",
				"������",
				"���",
				"��������������",
				"ҵ������",
				"����״̬",
				"��������",
				"����ʱ��",
				"����޸���",
				"����޸�ʱ��",
				"ʡ��")
			SELECT
				"�ͻ�����",
				"�̻�����",
				"����Ʒ��",
				"�ͺ�",
				"�ɽ�����",
				"�ɽ���ʽ",
				"�ɽ��۸�",
				"�׸�����",
				"����ԭ��",
				"ͼƬ",
				"�ⲿ������",
				"����״̬",
				"����Ŷ�",
				"������",
				"������",
				"���",
				"��������������",
				"ҵ������",
				"����״̬",
				"��������",
				"����ʱ��",
				"����޸���",
				"����޸�ʱ��",
				"ʡ��"
			FROM
				INC_LOSE_COM a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	--�ͻ������
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_CUSTOMER_FACE;

	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_CUSTOMER_FACE';
			V_OBJECT_NAME	:= 'ODS_CUSTOMER_FACE';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"������������",
				"������",
				"����",
				"�ͻ�",
				"���ʱ��",
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_CUSTOMER_FACE a
			WHERE exists(SELECT 1 FROM INC_CUSTOMER_FACE WHERE ����=a.����);
			COMMIT;

			----ɾ��ODS_CUSTOMER_FACE�м�¼��
			DELETE FROM ODS_CUSTOMER_FACE a
				WHERE exists(SELECT 1 FROM INC_CUSTOMER_FACE WHERE ����=a.����);
				COMMIT;

			----����������������
			INSERT INTO ODS_CUSTOMER_FACE
				("������������",
				"������",
				"����",
				"�ͻ�",
				"���ʱ��",
				time_stamp)
			SELECT
				"������������",
				"������",
				"����",
				"�ͻ�",
				"���ʱ��",
				SYSDATE
			FROM
				INC_CUSTOMER_FACE a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_customer_face' ;
			COMMIT;

			INSERT INTO
				dw_customer_face 
			SELECT
				"������������",
				"������",
				"����",
				"�ͻ�",
				"���ʱ��",
				time_stamp
			FROM
				ods_customer_face
			WHERE
				�ͻ� is not null;
			COMMIT;

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_customer_face���ݸ������','��־��¼');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
				-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	--һ����Ա3.9��
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_S_PERSON;
	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_S_PERSON';
			V_OBJECT_NAME	:= 'ODS_S_PERSON';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"���",
				"����",
				"����",
				"��ϵ",
				"����",
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_S_PERSON a
			WHERE exists(SELECT 1 FROM INC_S_PERSON WHERE ����=a.����);
			COMMIT;

			----ɾ��ODS_S_PERSON�м�¼��
			DELETE FROM ODS_S_PERSON a
			WHERE exists(SELECT 1 FROM INC_S_PERSON WHERE ����=a.����);
			COMMIT;

			----����������������
			-- Edward0601: INC_S_PERSON, ODS_S_PERSON���޸��ֶ�
			INSERT INTO ODS_S_PERSON
				("���",
				"����",
				"����",
				"��ϵ",
				"����",
				"����",
				"��λ",
				"��ְʱ��",
				"��λ���",
				"ְ��",
				"ʡ��", --����
				"˾��", --����
				"����״̬",
				time_stamp)
			SELECT
				"���",
				"����",
				"����",
				"��ϵ",
				"����",
				"����",
				"��λ",
				"��ְʱ��",
				"��λ���",
				"ְ��",
				"ʡ��",
				"˾��",
				"����״̬",
				SYSDATE
			FROM
				INC_S_PERSON a;
			COMMIT;



			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_DEPT_EMP' ;
			COMMIT;

			-- Edward0601: DW_DEPT_EMP, ODS_S_PERSON �ֶ����޸�
			INSERT INTO DW_DEPT_EMP(
				����,
				������,
				��������������,
				��λ,
				��ְʱ��,
				˾��,
				�����ֹ�˾,
				����״̬,
				TIME_STAMP,
				ʡ�� -- Edward0601: ʡ���ֶ���ODS_S_PERSON��ȡ
			)
			SELECT
				����,
				case when instr(����,'01')>0 then substr(����,1,instr(����,'01')-1) 
					 when instr(����,'02')>0 then substr(����,1,instr(����,'02')-1) else ���� END, 
				����,
				��λ,
				��ְʱ��,
				˾��,
				case when instr(����,'�ֹ�˾') >0 then substr(����,1,instr(����,'�ֹ�˾')-1) 
					 when instr(����,'��') >0 and instr(����,'����') >0 then substr(����,instr(����,'����')+2,instr(����,'��')-instr(����,'����')-2) 
					 when instr(����,'��') >0 and instr(����,'����')>0 then substr(����,instr(����,'����')+2,instr(����,'��')-instr(����,'����')-2) 
					 when instr(����,'���´�') >0 then substr(����,1,instr(����,'���´�')-1) else ���� END,
				����״̬,
				SYSDATE as time_stamp,
				ʡ�� -- ������ʡ���ֶ� INSERT��DW_DEPT_EMP
			FROM ODS_S_PERSON;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'DW_DEPT_EMP���ݸ������','��־��¼');
			COMMIT;

		EXCEPTION
			WHEN OTHERS THEN
				-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	--Ӫ����Ա��̱���
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_S_PERSON_MILEAGE;

	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_S_PERSON_MILEAGE';
			V_OBJECT_NAME	:= 'ODS_S_PERSON_MILEAGE';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"���",
				"�豸����",
				"����",
				"�����KM",
				"ҵ��Ա",
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_S_PERSON_MILEAGE a
			WHERE exists(SELECT 1 FROM INC_S_PERSON_MILEAGE WHERE ���=a.���);
			COMMIT;

			----ɾ��ODS_S_PERSON_MILEAGE�м�¼��
			DELETE FROM ODS_S_PERSON_MILEAGE a
			WHERE exists(SELECT 1 FROM INC_S_PERSON_MILEAGE WHERE ���=a.���);
			COMMIT;

			----����������������
			INSERT INTO ODS_S_PERSON_MILEAGE ("���",
				"�豸����",
				imei,
				"�ͺ�",
				"����",
				"�����KM",
				"ҵ��Ա",
				"˾������",
				"���ƺ�",
				time_stamp)
			SELECT
				"���",
				"�豸����",
				imei,
				"�ͺ�",
				"����",
				"�����KM",
				"ҵ��Ա",
				"˾������",
				"���ƺ�",
				SYSDATE
			FROM
				inc_s_person_mileage a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_S_MILEAGE' ;
			COMMIT;

			INSERT INTO DW_S_MILEAGE(
				"���۴���",
				imei,
				"�ͺ�",
				"���",
				"�¶�",
				"����",
				"�ֹ�˾",
				"����",
				"�����KM",
				"ҵ��Ա",
				"˾������",
				"���ƺ�"
			) 
			SELECT
				"�豸����" as ���۴���,
				IMEI,
				"�ͺ�",
				substr(to_char(����,'yyyymmdd'),1,4) as ���,
				substr(to_char(����,'yyyymmdd'),5,2) as �¶�,
				to_date (to_char(����,'yyyymmdd'),'yyyymmdd') as ����,
				b.�������������� as �ֹ�˾,
				case when b.�����ֹ�˾='��ͻ���' then '��ɳ'
					 when b.�����ֹ�˾='����' then '����'
					 when b.�����ֹ�˾='װ�ػ���ҵ��' then '��ɳ'
					 when b.�����ֹ�˾='��̶' then '��ɳ'
					 when b.�����ֹ�˾='�Ž��澰����' then '�Ž�'
					 when b.�����ֹ�˾='������' then '����'
					 when b.�����ֹ�˾ is null then '��ɳ'
					 else b.�����ֹ�˾ END �����ֹ�˾,
				"�����KM",
				"ҵ��Ա",
				"˾������",
				"���ƺ�"
			FROM
				ods_s_person_mileage a,DW_DEPT_EMP b
			WHERE a.�豸���� = b.������(+);
			COMMIT;

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'DW_S_MILEAGE���ݸ������','��־��¼');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
				-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	--Ӫ����Ա�г̱���
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM inc_s_person_travel;

	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'inc_s_person_travel';
			V_OBJECT_NAME	:= 'ODS_S_PERSON_TRAVEL';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"�豸����",
				"�����KM",
				"���",
				"�յ�",
				"��ʼʱ��",
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_S_PERSON_TRAVEL a
			WHERE exists(SELECT 1 FROM inc_s_person_travel WHERE ���=a.���);
			COMMIT;

			----ɾ��ODS_S_PERSON_TRAVEL�м�¼��
			DELETE FROM ODS_S_PERSON_TRAVEL a
			WHERE exists(SELECT 1 FROM inc_s_person_travel WHERE ���=a.���);
			COMMIT;

			----����������������
			INSERT INTO ODS_S_PERSON_TRAVEL ("���",
				"�豸����",
				imei,
				"�ͺ�",
				"��ʼʱ��",
				"����ʱ��",
				"���",
				"�յ�",
				"�����KM",
				"����ʱʱ��",
				"ƽ���ٶ�KMH",
				"ҵ��Ա",
				"˾������",
				"���ƺ�",
				time_stamp)
			SELECT
				"���",
				"�豸����",
				imei,
				"�ͺ�",
				"��ʼʱ��",
				"����ʱ��",
				"���",
				"�յ�",
				"�����KM",
				"����ʱʱ��",
				"ƽ���ٶ�KMH",
				"ҵ��Ա",
				"˾������",
				"���ƺ�",
				SYSDATE
			FROM
				inc_s_person_travel a;
			COMMIT;



			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;


			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_S_TRAVEL' ;
			COMMIT;

			INSERT INTO DW_S_TRAVEL( "���",
				"�豸����",
				imei,
				"���",
				"�¶�",
				"����",
				"�ֹ�˾",
				"����",
				"��ʼʱ��",
				"����ʱ��",
				"���ʡ",
				"�����",
				"�������",
				"���",
				"�յ�ʡ",
				"�յ���",
				"�յ�����",
				"�յ�",
				"�����KM",
				"����ʱʱ��",
				"ƽ���ٶ�KMH",
				"˾������",
				"���ƺ�") 
			SELECT
				���,
				"�豸����",
				imei,
				substr(to_char(��ʼʱ��,'yyyymmdd'),1,4) as ���,
				substr(to_char(��ʼʱ��,'yyyymmdd'),5,2) as �¶�,
				to_date (to_char(��ʼʱ��,'yyyymmdd'),'yyyymmdd') as ����,
				b.�������������� as �ֹ�˾,
				case when b.�����ֹ�˾='��ͻ���' then '��ɳ'
					 when b.�����ֹ�˾='����' then '����'
					 when b.�����ֹ�˾='װ�ػ���ҵ��' then '��ɳ'
					 when b.�����ֹ�˾='��̶' then '��ɳ'
					 when b.�����ֹ�˾='�Ž��澰����' then '�Ž�'
					 when b.�����ֹ�˾='������' then '����'
					 when b.�����ֹ�˾ is null then '��ɳ'
					 else b.�����ֹ�˾ END �����ֹ�˾,
				"��ʼʱ��",
				"����ʱ��",
				case when instr("���",'ʡ')>0 then substr( "���",1,instr("���",'ʡ')-1) 
					 when instr("���",'����׳��������')>0 then '����'
					 when instr("���",'�Ϻ�')>0 then '�Ϻ�'
					 when instr("���",'����')>0 then '����'
					 when instr("���",'����')>0 then '����'
					 when instr("���",'���')>0 then '���'
					 when instr("���",'����ر�������')>0 then '���'
					 when instr("���",'�����ر�������')>0 then '����'
					 when instr("���",'���Ļ���������')>0 then '����'
					 when instr("���",'���ɹ�������')>0 then '���ɹ�'
					 when instr("���",'�½�')>0 then '�½�'
					 when instr("���",'����������')>0 then '����' else "���" END as ���ʡ,
				case when instr("���",'ʡ')>0 then 
					(case when instr("���",'ʡϽ��')>0 then substr( "���",instr("���",'ʡϽ��')+3)
						  when instr("���",'������')>0 then substr( "���",instr("���",'ʡ')+1,instr("���",'������')-instr("���",'ʡ')+2)
						  else substr( "���",instr("���",'ʡ')+1,instr("���",'��')-instr("���",'ʡ')-1) END)
					 when instr("���",'����׳��������')>0 then
					(case when instr("���",'������')>0 then substr( "���",instr("���",'������')+3,instr("���",'������')-instr("���",'������')+2)
						  else substr( "���",instr("���",'������')+3,instr("���",'��')-instr("���",'������')) END)
					 when instr("���",'�Ϻ�')>0 then '�Ϻ�'
					 when instr("���",'����')>0 then '����'
					 when instr("���",'����')>0 then '����'
					 when instr("���",'���')>0 then '���'
					 when instr("���",'����ر�������')>0 then '���'
					 when instr("���",'�����ر�������')>0 then '����'
					 when instr("���",'���Ļ���������')>0 then
					(case when instr("���",'������')>0 then substr( "���",instr("���",'������')+3,instr("���",'������')-instr("���",'������')+2)
						  else substr( "���",instr("���",'������')+3,instr("���",'��')-instr("���",'������')) END )
					 when instr("���",'���ɹ�������')>0 then
					(case when instr("���",'������')>0 then substr( "���",instr("���",'������')+3,instr("���",'������')-instr("���",'������')+2)
						  else substr( "���",instr("���",'������')+3,instr("���",'��')-instr("���",'������'))END)
					 when instr("���",'�½�ά���������')>0 then 
					(case when instr("���",'������')>0 then substr( "���",instr("���",'������')+3,instr("���",'������')-instr("���",'������')+2)
						  else substr( "���",instr("���",'������')+3,instr("���",'��')-instr("���",'������'))END )
					 when instr("���",'����������')>0 then
					(case when instr("���",'������')>0 then substr( "���",instr("���",'������')+3,instr("���",'������')-instr("���",'������')+2)
						  else substr( "���",instr("���",'������')+3,instr("���",'��')-instr("���",'������'))END)
					 else "���" END as �����,
				case when instr("���",'ʡϽ��')>0 then substr( "���",instr("���",'ʡϽ��')+3) 
					 when instr("���",'������')>0 then 
						(case when instr("���",',')>0 then 
							 (case when instr(substr( "���",instr("���",'������')+3,instr("���",',')-instr("���",'������')-3),'��') >0 then substr(substr( "���",instr("���",'������')+3,instr("���",',')-instr("���",'������')-3),1,instr(substr( "���",instr("���",'������')+3,instr("���",',')-instr("���",'������')-3),'��',1)) 
								   when instr(substr( "���",instr("���",'������')+3,instr("���",',')-instr("���",'������')-3),'��') >0 then substr(substr( "���",instr("���",'������')+3,instr("���",',')-instr("���",'������')-3),1,instr(substr( "���",instr("���",'������')+3,instr("���",',')-instr("���",'������')-3),'��',1))
								   when instr(substr( "���",instr("���",'������')+3,instr("���",',')-instr("���",'������')-3),'��') >0 then substr(substr( "���",instr("���",'������')+3,instr("���",',')-instr("���",'������')-3),1,instr(substr( "���",instr("���",'������')+3,instr("���",',')-instr("���",'������')-3),'��',1)) 
							  END)
								   else (case when instr(substr( "���",instr("���",'������')+3),'��') >0 then substr(substr( "���",instr("���",'������')+3),1,instr(substr( "���",instr("���",'������')+3),'��',1)) 
								   when instr(substr( "���",instr("���",'������')+3),'��') >0 then substr(substr( "���",instr("���",'������')+3),1,instr(substr( "���",instr("���",'������')+3),'��',1))
								   when instr(substr( "���",instr("���",'������')+3),'��') >0 then substr(substr( "���",instr("���",'������')+3),1,instr(substr( "���",instr("���",'������')+3),'��',1)) 
							  END)
						 END)
					 when instr("���",'��')>0 then
						(case when instr("���",',')>0 then 
							(case when instr(substr( "���",instr("���",'��')+1,instr("���",',')-instr("���",'��')-1),'��') >0 then substr(substr( "���",instr("���",'��')+1,instr("���",',')-instr("���",'��')-1),1,instr(substr( "���",instr("���",'��')+1,instr("���",',')-instr("���",'��')-1),'��',1)) 
								  when instr(substr( "���",instr("���",'��')+1,instr("���",',')-instr("���",'��')-1),'�껨�����Ŵ��г�') >0 then '�껨��'
								  when instr(substr( "���",instr("���",'��')+1,instr("���",',')-instr("���",'��')-1),'��') >0 then substr(substr( "���",instr("���",'��')+1,instr("���",',')-instr("���",'��')-1),1,instr(substr( "���",instr("���",'��')+1,instr("���",',')-instr("���",'��')-1),'��',1))
								  when instr(substr( "���",instr("���",'��')+1,instr("���",',')-instr("���",'��')-1),'��') >0 then substr(substr( "���",instr("���",'��')+1,instr("���",',')-instr("���",'��')-1),1,instr(substr( "���",instr("���",'��')+1,instr("���",',')-instr("���",'��')-1),'��',1)) 
							 END)
					 	else
					 		(case when instr(substr( "���",instr("���",'��')+1),'��') >0 then substr(substr( "���",instr("���",'��')+1),1,instr(substr( "���",instr("���",'��')+1),'��',1)) 
						 	 	  when instr(substr( "���",instr("���",'��')+1,instr("���",',')-instr("���",'��')-1),'�껨�����Ŵ��г�') >0 then '�껨��'
						 	 	  when instr(substr( "���",instr("���",'��')+1),'��') >0 then substr(substr( "���",instr("���",'��')+1),1,instr(substr( "���",instr("���",'��')+1),'��',1))
						 	 	  when instr(substr( "���",instr("���",'��')+1),'��') >0 then substr(substr( "���",instr("���",'��')+1),1,instr(substr( "���",instr("���",'��')+1),'��',1)) 
						 	 END)
					 	 END)
					 else "���" END as �������,
				"���",
				case when instr("�յ�",'ʡ')>0 then substr( "�յ�",1,instr("�յ�",'ʡ')-1) 
					 when instr("�յ�",'����׳��������')>0 then '����'
					 when instr("�յ�",'�Ϻ�')>0 then '�Ϻ�'
					 when instr("�յ�",'����')>0 then '����'
					 when instr("�յ�",'����')>0 then '����'
					 when instr("�յ�",'���')>0 then '���'
					 when instr("�յ�",'����ر�������')>0 then '���'
					 when instr("�յ�",'�����ر�������')>0 then '����'
					 when instr("�յ�",'���Ļ���������')>0 then '����'
					 when instr("�յ�",'���ɹ�������')>0 then '���ɹ�'
					 when instr("�յ�",'�½�')>0 then '�½�'
					 when instr("�յ�",'����������')>0 then '����' else "�յ�" END as �յ�ʡ,
				case when instr("�յ�",'ʡ')>0 then 
					(case when instr("�յ�",'ʡϽ��')>0 then substr( "�յ�",instr("�յ�",'ʡϽ��')+3)
						  when instr("�յ�",'������')>0 then substr( "�յ�",instr("�յ�",'ʡ')+1,instr("�յ�",'������')-instr("�յ�",'ʡ')+2)
						  else substr( "�յ�",instr("�յ�",'ʡ')+1,instr("�յ�",'��')-instr("�յ�",'ʡ')-1) END)
					 when instr("�յ�",'����׳��������')>0 then
				 	(case when instr("�յ�",'������')>0 then substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",'������')-instr("�յ�",'������')+2)
						  else substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",'��')-instr("�յ�",'������'))END)
					 when instr("�յ�",'�Ϻ�')>0 then '�Ϻ�'
					 when instr("�յ�",'����')>0 then '����'
					 when instr("�յ�",'����')>0 then '����'
					 when instr("�յ�",'���')>0 then '���'
					 when instr("�յ�",'����ر�������')>0 then '���'
					 when instr("�յ�",'�����ر�������')>0 then '����'
					 when instr("�յ�",'���Ļ���������')>0 then
						 (case when instr("�յ�",'������')>0 then substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",'������')-instr("�յ�",'������')+2)
							   else substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",'��')-instr("�յ�",'������')) END)
					 when instr("�յ�",'���ɹ�������')>0 then
						 (case when instr("�յ�",'������')>0 then substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",'������')-instr("�յ�",'������')+2)
							   else substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",'��')-instr("�յ�",'������')) END)
					 when instr("�յ�",'�½�ά���������')>0 then 
						 (case when instr("�յ�",'������')>0 then substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",'������')-instr("�յ�",'������')+2)
							   else substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",'��')-instr("�յ�",'������')) END)
					 when instr("�յ�",'����������')>0 then
						 (case when instr("�յ�",'������')>0 then substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",'������')-instr("�յ�",'������')+2)
							   else substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",'��')-instr("�յ�",'������')) END)
					 else "�յ�" END as �յ���,
				case when instr("�յ�",'ʡϽ��')>0 then substr( "�յ�",instr("�յ�",'ʡϽ��')+3) 
					 when instr("�յ�",'������')>0 then 
						(case when instr("�յ�",',')>0 then 
							(case when instr(substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",',')-instr("�յ�",'������')-3),'��') >0 then substr(substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",',')-instr("�յ�",'������')-3),1,instr(substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",',')-instr("�յ�",'������')-3),'��',1)) 
								  when instr(substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",',')-instr("�յ�",'������')-3),'��') >0 then substr(substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",',')-instr("�յ�",'������')-3),1,instr(substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",',')-instr("�յ�",'������')-3),'��',1))
								  when instr(substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",',')-instr("�յ�",'������')-3),'��') >0 then substr(substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",',')-instr("�յ�",'������')-3),1,instr(substr( "�յ�",instr("�յ�",'������')+3,instr("�յ�",',')-instr("�յ�",'������')-3),'��',1)) 
							 END)
						 else
						 	(case when instr(substr( "�յ�",instr("�յ�",'������')+3),'��') >0 then substr(substr( "�յ�",instr("�յ�",'������')+3),1,instr(substr( "�յ�",instr("�յ�",'������')+3),'��',1)) 
								  when instr(substr( "�յ�",instr("�յ�",'������')+3),'��') >0 then substr(substr( "�յ�",instr("�յ�",'������')+3),1,instr(substr( "�յ�",instr("�յ�",'������')+3),'��',1))
								  when instr(substr( "�յ�",instr("�յ�",'������')+3),'��') >0 then substr(substr( "�յ�",instr("�յ�",'������')+3),1,instr(substr( "�յ�",instr("�յ�",'������')+3),'��',1)) 
							 END)
						 END)
					 when instr("�յ�",'��')>0 then
						(case when instr("�յ�",',')>0 then 
							(case when instr(substr( "�յ�",instr("�յ�",'��')+1,instr("�յ�",',')-instr("�յ�",'��')-1),'��') >0 then substr(substr( "�յ�",instr("�յ�",'��')+1,instr("�յ�",',')-instr("�յ�",'��')-1),1,instr(substr( "�յ�",instr("�յ�",'��')+1,instr("�յ�",',')-instr("�յ�",'��')-1),'��',1)) 
								  when instr(substr( "�յ�",instr("�յ�",'��')+1,instr("�յ�",',')-instr("�յ�",'��')-1),'�껨�����Ŵ��г�') >0 then '�껨��'
								  when instr(substr( "�յ�",instr("�յ�",'��')+1,instr("�յ�",',')-instr("�յ�",'��')-1),'��') >0 then substr(substr( "�յ�",instr("�յ�",'��')+1,instr("�յ�",',')-instr("�յ�",'��')-1),1,instr(substr( "�յ�",instr("�յ�",'��')+1,instr("�յ�",',')-instr("�յ�",'��')-1),'��',1))
								  when instr(substr( "�յ�",instr("�յ�",'��')+1,instr("�յ�",',')-instr("�յ�",'��')-1),'��') >0 then substr(substr( "�յ�",instr("�յ�",'��')+1,instr("�յ�",',')-instr("�յ�",'��')-1),1,instr(substr( "�յ�",instr("�յ�",'��')+1,instr("�յ�",',')-instr("�յ�",'��')-1),'��',1)) 
							 END)
						 else
						 	(case when instr(substr( "�յ�",instr("�յ�",'��')+1),'��') >0 then substr(substr( "�յ�",instr("�յ�",'��')+1),1,instr(substr( "�յ�",instr("�յ�",'��')+1),'��',1)) 
								  when instr(substr( "�յ�",instr("�յ�",'��')+1,instr("�յ�",',')-instr("�յ�",'��')-1),'�껨�����Ŵ��г�') >0 then '�껨��'
								  when instr(substr( "�յ�",instr("�յ�",'��')+1),'��') >0 then substr(substr( "�յ�",instr("�յ�",'��')+1),1,instr(substr( "�յ�",instr("�յ�",'��')+1),'��',1))
								  when instr(substr( "�յ�",instr("�յ�",'��')+1),'��') >0 then substr(substr( "�յ�",instr("�յ�",'��')+1),1,instr(substr( "�յ�",instr("�յ�",'��')+1),'��',1)) 
							 END)
						 END)
					 else "�յ�" END as �յ�����,
				"�յ�",
				"�����KM",
				"����ʱʱ��",
				"ƽ���ٶ�KMH",
				"˾������",
				"���ƺ�"
			FROM
				ods_s_person_travel a,
				DW_DEPT_EMP b
			WHERE
				a.�豸���� = b.������(+);
			COMMIT;

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'DW_S_TRAVEL���ݸ������','��־��¼');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
				-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	--���Ͷ�λ���ձ�
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_MAP_MODEL;

	IF QTY_COM>0 then 
		BEGIN
			V_TABLE_NAME	:= 'INC_MAP_MODEL';
			V_OBJECT_NAME	:= 'ODS_MAP_MODEL';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				С�д���,
				�ͺ�,
				����ҵ,
				С��ҵ,
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'

			-- Edward: �ѱ���ֶ�INC_MAP_MODEL, ODS_MAP_MODEL
			FROM ODS_MAP_MODEL a
			WHERE exists(SELECT 1 FROM INC_MAP_MODEL WHERE С�д���=a.С�д��� and �ͺ�=a.�ͺ� );
			COMMIT;

			----ɾ��ODS_MAP_MODEL�м�¼��
			DELETE FROM ODS_MAP_MODEL a
			WHERE exists(SELECT 1 FROM INC_MAP_MODEL WHERE С�д���=a.С�д��� and �ͺ�=a.�ͺ�);
			COMMIT;

			----����������������
			INSERT INTO ODS_MAP_MODEL (
				С�д���,
				�ͺ�,
				����ҵ,
				С��ҵ,
				TIME_STAMP)
			SELECT 
				С�д���,
				�ͺ�,
				����ҵ,
				С��ҵ,
				SYSDATE
			FROM INC_MAP_MODEL a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
			-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');

		END;
	END IF;






	--����������ݴ���ҵ����
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_INDUSTRY_BIG;
	IF QTY_COM>0 then 

		BEGIN
			V_TABLE_NAME	:= 'INC_INDUSTRY_BIG';
			V_OBJECT_NAME	:= 'ODS_INDUSTRY_BIG';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				Ʒ��,
				�ּ�,
				���,
				�·�,
				����,
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_INDUSTRY_BIG a
			WHERE exists(SELECT 1 FROM INC_INDUSTRY_BIG WHERE ���=a.��� and �·�=a.�·� );
			COMMIT;

			----ɾ��ODS_INDUSTRY_BIG�м�¼��
			DELETE FROM ODS_INDUSTRY_BIG a
			WHERE exists(SELECT 1 FROM INC_INDUSTRY_BIG WHERE ���=a.��� and �·�=a.�·� );
			COMMIT;

			----����������������
			INSERT INTO ODS_INDUSTRY_BIG (Ʒ��,
				�ּ�,
				���,
				�·�,
				����,
				ʡ��,
				TIME_STAMP)
			SELECT Ʒ��,
				�ּ�,
				���,
				�·�,
				����,
				ʡ��,
				SYSDATE
			FROM INC_INDUSTRY_BIG a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

			--------ɾ�������Ӧ��DW��dw_industry_big
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_industry_big';
			COMMIT;
			INSERT INTO dw_industry_big 
			SELECT
				"Ʒ��",
				case when "�ּ�"='0��T��5' then '��5T'
					when "�ּ�"='5��T��6.5' then '5T-6T'
					when "�ּ�"='6.5��T��8' then '6T-8T'
					when "�ּ�"='8��T��11' then '8T-11T'
					when "�ּ�"='11��T��16' then '11T-16T'
					when "�ּ�"='16��T��20' then '16T-20T'
					when "�ּ�"='20��T��22' then '20T-22T'
					when "�ּ�"='22��T��24' then '22T-24T'
					when "�ּ�"='24��T��27' then '24T-28T'
					when "�ּ�"='27��T��31' then '28T-31T'
					when "�ּ�"='31��T��35' then '31T-35T'
					when "�ּ�"='35��T��40' then '35T-40T'
					when "�ּ�"='40��T' then '40T����' END as ����ҵ,
				"���",
				"�·�",
				"����"
			FROM
				ods_industry_big; 
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_industry_big���ݸ������','��־��¼');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
				-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	--������ϸ�� Edward���޸�: INC_S_DETAIL ODS_S_DETAIL DW_S_DETAIL(δ�޸�)
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_S_DETAIL;

	IF QTY_COM>0 then
		BEGIN
			V_TABLE_NAME	:= 'INC_S_DETAIL';
			V_OBJECT_NAME	:= 'ODS_S_DETAIL';

				----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				Ӫ������,
				��ͬ��λ,
				���۷�ʽ,
				�ͺ�,
				�������,
				CRM���˽���ʱ��,
				����,
				��ҵ��ֽ�ʺ�ͬ��,
				�����·�ZW,
				�������,
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_S_DETAIL a
			WHERE exists(SELECT 1 FROM INC_S_DETAIL WHERE �������=a.������� );
			COMMIT;
			----ɾ��ODS_S_DETAIL�м�¼��
			DELETE FROM ODS_S_DETAIL a
			WHERE exists(SELECT 1 FROM INC_S_DETAIL WHERE �������=a.������� );
			COMMIT;

			----����������������
			INSERT INTO ODS_S_DETAIL ("���",
				dms,
				"������",
				"ʡ��",
				"�����ֹ�˾",
				"Ӫ������",
				"�ͻ�����",
				"��ͬ��λ",
				"���۷�ʽ",
				"�ͺ�",
				"�������",
				"֤������",
				"�ͻ�סַ",
				"��ϵ�绰",
				"����",
				"CRM���˽���ʱ��",
				"������ַ",
				"��ͬ���",
				"���ɼ�",
				"�ն��׸�����",
				"��֤��",
				"�����",
				"��֤��",
				"���շ�",
				"���úϼ�",
				"�ն˴�����",
				"�������",
				"��������",
				"��ҵ��ֽ�ʺ�ͬ��",
				"��׼����Ӧ���׸�",
				"�ն��׸��������",
				"�����׸�",
				"���ɷ���",
				"�ն��׸��������ʽ",
				"�ն˿ͻ��ӳٷſ�����",
				"�ػ��ӳٷſ�����",
				"��������",
				"�ػ���ͬ���",
				"������",
				"���ػ��ɽ�����",
				"ʵ���׸������ػ�����",
				"С�д���",
				"�ſ���",
				"�������ý��",
				"�ۺ������",
				"�Ծɻ���",
				"�ɻ�������",
				"�ɻ����ռ�",
				"���",
				"չ������",
				"��ע",
				"������",
				"�������",
				"���ͽ��",
				"�����������ʱ��",
				"��Ϣ��",
				"��Ϣ�˵绰",
				"��Ϣ�ѽ��",
				"��Ϣ������",
				"�ط����",
				"�Ƿ�����",
				"�Ҹ�����",
				"�����·�SY",
				"�����·�ZW",
				"�������",
				"S�ͻ�",
				"�������",
				TIME_STAMP)
			SELECT "���",
				dms,
				"������",
				"ʡ��",
				"�����ֹ�˾",
				"Ӫ������",
				"�ͻ�����",
				"��ͬ��λ",
				"���۷�ʽ",
				"�ͺ�",
				"�������",
				"֤������",
				"�ͻ�סַ",
				"��ϵ�绰",
				"����",
				"CRM���˽���ʱ��",
				"������ַ",
				"��ͬ���",
				"���ɼ�",
				"�ն��׸�����",
				"��֤��",
				"�����",
				"��֤��",
				"���շ�",
				"���úϼ�",
				"�ն˴�����",
				"�������",
				"��������",
				"��ҵ��ֽ�ʺ�ͬ��",
				"��׼����Ӧ���׸�",
				"�ն��׸��������",
				"�����׸�",
				"���ɷ���",
				"�ն��׸��������ʽ",
				"�ն˿ͻ��ӳٷſ�����",
				"�ػ��ӳٷſ�����",
				"��������",
				"�ػ���ͬ���",
				"������",
				"���ػ��ɽ�����",
				"ʵ���׸������ػ�����",
				"С�д���",
				"�ſ���",
				"�������ý��",
				"�ۺ������",
				"�Ծɻ���",
				"�ɻ�������",
				"�ɻ����ռ�",
				"���",
				"չ������",
				"��ע",
				"������",
				"�������",
				"���ͽ��",
				"�����������ʱ��",
				"��Ϣ��",
				"��Ϣ�˵绰",
				"��Ϣ�ѽ��",
				"��Ϣ������",
				"�ط����",
				"�Ƿ�����",
				"�Ҹ�����",
				"�����·�SY",
				"�����·�ZW",
				"�������",
				"S�ͻ�",
				"�������",
				SYSDATE
			FROM INC_S_DETAIL a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;
			--------ɾ�������Ӧ��DW��dw_s_detail
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_s_detail';
			COMMIT;

			----��������
			-- Edward: �ó�DW_S_DETAIL��ODS_S_DETAIL�ֶ����޸�
			INSERT INTO dw_s_detail("��������",--��DW����ֶ��г�����Ȼ������ֶδ�λ�������
				dms,
				"������",
				"ʡ��",
				"�����ֹ�˾",
				"Ӫ������",
				"�ͻ�����",
				"��ͬ��λ",
				"���۷�ʽ",
				"�ͺ�",
				"�������",
				"֤������",
				"�ͻ�סַ",
				"��ϵ�绰",
				"����",
				"CRM���˽���ʱ��",
				"������ַ",
				"��ͬ���",
				"���ɼ�",
				"�ն��׸�����",
				"��֤��",
				"�����",
				"��֤��",
				"���շ�",
				"���úϼ�",
				"�ն˴�����",
				"�������",
				"��������",
				"��ҵ��ֽ�ʺ�ͬ��",
				"��׼����Ӧ���׸�",
				"�ն��׸��������",
				"�����׸�",
				"���ɷ���",
				"�ն��׸��������ʽ",
				"�ն˿ͻ��ӳٷſ�����",
				"�ػ��ӳٷſ�����",
				"��������",
				"�ػ���ͬ���",
				"������",
				"���ػ��ɽ�����",
				"ʵ���׸������ػ�����",
				"С�д���",
				"�ſ���",
				"�������ý��",
				"�ۺ������",
				"�Ծɻ���",
				"������",
				"�������",
				"���ͽ��",
				"�����·�SY",
				"�����·�" ,
				"�������",
				"�����·�ZW") 
			SELECT
			-- Edward: ��Ҫ�ԡ�����������������������ֶν��д���
				case when translate("���", '0123456789', '#') is null then '����' 
					when instr(translate("���", '0123456789', '#'),'#')>0 then '����'
					when translate("���", '0123456789', '#')='C' then '����'
					when translate("���", '0123456789', '#')='-' then '����'
					else "���" END as ��������,
				dms,
				"������",
				"ʡ��",
				"�����ֹ�˾",
				"Ӫ������",
				"�ͻ�����",
				"��ͬ��λ",
				"���۷�ʽ",
				case when instr("�ͺ�",'C')>0 then substr("�ͺ�",1,instr("�ͺ�",'C')-1)
					when instr("�ͺ�",'H')>0 then substr("�ͺ�",1,instr("�ͺ�",'H')-1)
					when instr("�ͺ�",'U')>0 then substr("�ͺ�",1,instr("�ͺ�",'U')-1) else "�ͺ�" END "�ͺ�",
				trim("�������") as �������,
				"֤������",
				"�ͻ�סַ",
				"��ϵ�绰",
				"����",
				"CRM���˽���ʱ��",
				"������ַ",
				"��ͬ���",
				"���ɼ�",
				"�ն��׸�����",
				"��֤��",
				"�����",
				"��֤��",
				"���շ�",
				"���úϼ�",
				"�ն˴�����",
				"�������",
				"��������",
				"��ҵ��ֽ�ʺ�ͬ��",
				"��׼����Ӧ���׸�",
				"�ն��׸��������",
				"�����׸�",
				"���ɷ���",
				"�ն��׸��������ʽ",
				"�ն˿ͻ��ӳٷſ�����",
				"�ػ��ӳٷſ�����",
				"��������",
				"�ػ���ͬ���",
				"������",
				"���ػ��ɽ�����",
				"ʵ���׸������ػ�����",
				"С�д���",
				"�ſ���",
				"�������ý��",
				"�ۺ������",
				"�Ծɻ���",
				"������",
				"�������",
				"���ͽ��",
				"�����·�SY",
				case when "�����·�SY" IS NULL AND "CRM���˽���ʱ��" IS NULL then 1
					when "�����·�SY" IS NULL AND "CRM���˽���ʱ��" IS NOT NULL THEN to_number(SUBSTR(to_char("CRM���˽���ʱ��",'YYYYMMDD'),5,2))
					when instr("�����·�SY",'��')>0 then to_number(substr("�����·�SY",1,instr("�����·�SY",'��')-1))
					else to_number("�����·�SY") END as "�����·�" ,
				"�������",
				to_date((
				case when "�����·�SY" IS NULL AND "CRM���˽���ʱ��" IS NULL then to_char("�������")||'0101'
					when "�����·�SY" IS NULL AND "CRM���˽���ʱ��" IS NOT NULL THEN SUBSTR(to_char("CRM���˽���ʱ��",'YYYYMMDD'),1,6)||'01'
					else to_char("�������")||(case when instr("�����·�SY",'��')>0 and to_number(substr("�����·�SY",1,instr("�����·�SY",'��')-1)) >=10 then substr("�����·�SY",1,instr("�����·�SY",'��')-1)
					when instr("�����·�SY",'��')>0 and to_number(substr("�����·�SY",1,instr("�����·�SY",'��')-1)) <10 then '0'||substr("�����·�SY",1,instr("�����·�SY",'��')-1) 
					else "�����·�SY" END) ||'01' END),'YYYY-MM-DD') as "�����·�SY"
			FROM
				ods_s_detail;
			COMMIT;


			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE, 'dw_s_profit���ݸ������','��־��¼');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
				-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	--ë����
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_S_PROFIT;

	IF QTY_COM>0 then

		BEGIN
			V_TABLE_NAME	:= 'INC_S_PROFIT';
			V_OBJECT_NAME	:= 'ODS_S_PROFIT';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				���,
				"����",
				"����",
				"�ͻ�����",
				"Ӫ������",
				"�ֹ�˾",
				"�»���������",
				"���۷�ʽ",
				"����",
				"���ɼ�",
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_S_PROFIT a
			WHERE exists(SELECT 1 FROM INC_S_PROFIT WHERE ����=a.����);
			COMMIT;
			----ɾ��ODS_S_PROFIT�м�¼��
			DELETE FROM ODS_S_PROFIT a
			WHERE exists(SELECT 1 FROM INC_S_PROFIT WHERE ����=a.����);
			COMMIT;

			----����������������
			INSERT INTO ODS_S_PROFIT ("���",
				���,
				�¶�,
				"����",
				"����",
				"�ͻ�����",
				"Ӫ������",
				"�ֹ�˾",
				"�»���������",
				"���۷�ʽ",
				"����",
				"���ɼ�",
				"��ͬ��",
				"�������벻��˰",
				"���۳ɱ����ɷ���",
				"�����ɱ�����˰",
				"��ǰ�����",
				"����",
				"�ɱ��ۺ�����",
				"�ɱ�һ���˷�",
				"�ɱ��������",
				"�ɱ���Ϣ��",
				"���ɷ��ù����",
				"���ɷ��óе���Ϣ",
				"���ɷ����տͻ���Ϣ",
				"�����ά",
				"������˽�",
				"����",
				"ǩ���д���",
				"�����˷�",
				"������",
				"�û�������",
				"ë��1",
				"ë��2",
				"ë��3",
				"�û�����������",
				"�û��������",
				"�û������ۼ�",
				"��������",
				"��չ���ͽ��",
				"��ע",
				"�ϼ�",
				"��Ϣ��",
				"�Ƿ�λ�ͻ�",
				"���׻���",
				"���׽�",
				"�ɱ�������",
				"��������",
				TIME_STAMP)
			SELECT "���",
				���,
				�¶�,
				"����",
				"����",
				"�ͻ�����",
				"Ӫ������",
				"�ֹ�˾",
				"�»���������",
				"���۷�ʽ",
				"����",
				"���ɼ�",
				"��ͬ��",
				"�������벻��˰",
				"���۳ɱ����ɷ���",
				"�����ɱ�����˰",
				"��ǰ�����",
				"����",
				"�ɱ��ۺ�����",
				"�ɱ�һ���˷�",
				"�ɱ��������",
				"�ɱ���Ϣ��",
				"���ɷ��ù����",
				"���ɷ��óе���Ϣ",
				"���ɷ����տͻ���Ϣ",
				"�����ά",
				"������˽�",
				"����",
				"ǩ���д���",
				"�����˷�",
				"������",
				"�û�������",
				"ë��1",
				"ë��2",
				"ë��3",
				"�û�����������",
				"�û��������",
				"�û������ۼ�",
				"��������",
				"��չ���ͽ��",
				"��ע",
				"�ϼ�",
				"��Ϣ��",
				"�Ƿ�λ�ͻ�",
				"���׻���",
				"���׽�",
				"�ɱ�������",
				"��������",
				SYSDATE
			FROM INC_S_PROFIT a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

			--------ɾ�������Ӧ��DW��dw_s_profit
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_s_profit ' ;
			COMMIT;

			INSERT INTO dw_s_profit 
			SELECT
				"����",
				case when instr("����",'C')>0 then substr("����",1,instr("����",'C')-1)
					when instr("����",'H')>0 then substr("����",1,instr("����",'H')-1) 
					when instr("����",'U')>0 then substr("����",1,instr("����",'U')-1) else "����" END as �ͺ�,
				"�ͻ�����",
				"Ӫ������",
				"�ֹ�˾",
				"�»���������",
				"���۷�ʽ",
				"����",
				nvl("��ͬ��",0) as ���۽��,
				nvl("�������벻��˰",0) as �������벻��˰,
				nvl("���۳ɱ����ɷ���",0)as ���۳ɱ����ɷ���,
				nvl("�����ɱ�����˰",0)as �����ɱ�����˰,
				nvl("��ǰ�����",0) as ��һ������,
				nvl("����",0) as "����",
				nvl("�ɱ��ۺ�����",0) as "�ɱ��ۺ�����",
				nvl("�ɱ�һ���˷�",0)+nvl("�����˷�",0) as �˷�,
				nvl("�ɱ��������",0) as ����������,
				nvl("�ɱ���Ϣ��",0) as ��Ϣ��,
				nvl("���ɷ��ù����",0) as �������,
				nvl("���ɷ��óе���Ϣ",0)-nvl("���ɷ����տͻ���Ϣ",0) as ���ʰ�����Ϣ,
				nvl("�����ά",0)+nvl("������˽�",0)+nvl("����",0) as �ά,
				nvl("ǩ���д���",0) as �д���,
				nvl("������",0) as ��������,
				nvl("ë��1",0) as ë��1,
				nvl("ë��2",0) as"ë��2",
				nvl("ë��3",0) as"ë��3",
				"�û�����������",
				-- case when nvl("�û��������",0)-nvl("�û������ۼ�",0)=0 then 0-nvl("����",0) else nvl("�û��������",0)-nvl("�û������ۼ�",0) END as �ɻ�����,
				nvl("�û��������",0)-nvl("�û������ۼ�",0) as �ɻ�����,
				"�Ƿ�λ�ͻ�",
				'' ����
			FROM
				ods_s_profit;
			COMMIT; 

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE, 'dw_s_profit���ݸ������','��־��¼');
			COMMIT;
		EXCEPTION 
			WHEN OTHERS THEN
			-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	--һ��һ��
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_EXC_LEDGER;

	IF QTY_COM>0 then 

		BEGIN
			V_TABLE_NAME	:= 'INC_EXC_LEDGER';
			V_OBJECT_NAME	:= 'ODS_EXC_LEDGER';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				����,
				�豸�ͺ�,
				ʡ��,
				����,
				�ͻ���,
				���۷�ʽ,
				��Ӫ������,
				��������,
				�������,
				���ڿ��ܼ�,
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_EXC_LEDGER a
			WHERE exists(SELECT 1 FROM INC_EXC_LEDGER WHERE ����=a.���� );
			COMMIT;
			----ɾ��ODS_EXC_LEDGER�м�¼��
			DELETE FROM ODS_EXC_LEDGER a
			WHERE exists(SELECT 1 FROM INC_EXC_LEDGER WHERE ����=a.���� );
			COMMIT;

			----����������������
			INSERT INTO ODS_EXC_LEDGER ("����",
				"���ܺ�",
				"�豸�ͺ�",
				"ʡ��",
				"����",
				"ʡ��01",
				"����01",
				"GPS״̬",
				"GPS��װ",
				"�豸����",
				"��λ�ص�",
				"����¼ʱ��",
				"�ͻ���",
				"ʵ�ʹ�����",
				"��ϵ�绰",
				"��ͬ��λ",
				"���۷�ʽ",
				"��֤��",
				"ԭӪ������",
				"��Ӫ������",
				"��������",
				"�ſ�����",
				"�ͻ�����",
				"����������",
				"�Ƿ�Ϊ��ֵ����",
				"����ר��",
				"��תʱ��",
				"��ע",
				"���а��ҿ�������",
				"���а��ҿ�µ��ڿ�",
				"�������ڿ�",
				"�渶��",
				"��˾����������",
				"��˾����µ��ڿ�",
				"���ڽ��",
				"�ɻ��ֿ�",
				"����",
				"����˵��",
				"�ܵ��ڿ�",
				"�����ڿ�",
				"���ڷ�Ϣ",
				"���ڿ��ܼ�",
				"�ͻ���",
				"�ؿ�ͻ���",
				"����������",
				"�ɻ��ֿ�2",
				"�����ֿ�",
				"��12�����ۼƻ���",
				"��6�����ۼƻ���",
				"��3�����ۼƻ���",
				"�渶",
				"��ͬ��",
				"�ۿ۽��",
				"�ۺ��",
				"�������",
				"δ����",
				TIME_STAMP)
			SELECT "����",
				"���ܺ�",
				"�豸�ͺ�",
				"ʡ��",
				"����",
				"ʡ��01",
				"����01",
				"GPS״̬",
				"GPS��װ",
				"�豸����",
				"��λ�ص�",
				"����¼ʱ��",
				"�ͻ���",
				"ʵ�ʹ�����",
				"��ϵ�绰",
				"��ͬ��λ",
				"���۷�ʽ",
				"��֤��",
				"ԭӪ������",
				"��Ӫ������",
				"��������",
				"�ſ�����",
				"�ͻ�����",
				"����������",
				"�Ƿ�Ϊ��ֵ����",
				"����ר��",
				"��תʱ��",
				"��ע",
				"���а��ҿ�������",
				"���а��ҿ�µ��ڿ�",
				"�������ڿ�",
				"�渶��",
				"��˾����������",
				"��˾����µ��ڿ�",
				"���ڽ��",
				"�ɻ��ֿ�",
				"����",
				"����˵��",
				"�ܵ��ڿ�",
				"�����ڿ�",
				"���ڷ�Ϣ",
				"���ڿ��ܼ�",
				"�ͻ���",
				"�ؿ�ͻ���",
				"����������",
				"�ɻ��ֿ�2",
				"�����ֿ�",
				"��12�����ۼƻ���",
				"��6�����ۼƻ���",
				"��3�����ۼƻ���",
				"�渶",
				"��ͬ��",
				"�ۿ۽��",
				"�ۺ��",
				"�������",
				"δ����",
				SYSDATE
			FROM INC_EXC_LEDGER a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
			-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	--��ʱ����
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_HNZW1_1000;

	IF QTY_COM>0 then 

		BEGIN
			V_TABLE_NAME	:= 'INC_HNZW1_1000';
			V_OBJECT_NAME	:= 'DW_HNZW_WJGS';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				����,
				����,
				�ܹ�ʱ,
				���չ�ʱ,
				�����ͺ�,
				������ת��,
				ȼ����λ,
				��λ����,
				�Ƿ���Һ��,
				����ʱ��,
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM DW_HNZW_WJGS a
			WHERE exists(SELECT 1 FROM INC_HNZW1_1000 WHERE ����=a.���� and ����ʱ��=a.����ʱ�� );
			COMMIT;
			----ɾ��DW_HNZW_WJGS�м�¼��
			DELETE FROM DW_HNZW_WJGS a
			WHERE exists(SELECT 1 FROM INC_HNZW1_1000 WHERE ����=a.���� and ����ʱ��=a.����ʱ��);
			COMMIT;

			----����������������
			INSERT INTO DW_HNZW_WJGS (����,
				��¼��,
				����,
				�ܹ�ʱ,
				���չ�ʱ,
				�����ͺ�,
				��������,
				Ԥ��ʱ��,
				������ת��,
				ȼ����λ,
				��λ����,
				�Ƿ���Һ��,
				����ʱ��,
				TIME_STAMP)
			SELECT ����,
				��¼��,
				����,
				�ܹ�ʱ,
				���չ�ʱ,
				�����ͺ�,
				��������,
				Ԥ��ʱ��,
				������ת��,
				ȼ����λ,
				��λ����,
				�Ƿ���Һ��,
				����ʱ��,SYSDATE
			FROM INC_HNZW1_1000 a;
			COMMIT;
			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;
		EXCEPTION 
			WHEN OTHERS THEN
			-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');

		END;
	END IF;






	--ծȨӦ����
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_ACC_REC;

	IF QTY_COM>0 then 

		BEGIN
			V_TABLE_NAME	:= 'INC_ACC_REC';
			V_OBJECT_NAME	:= 'ODS_ACC_REC';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				"�ɽ���ʽ",
				"״̬",
				"����",
				"�ͻ�����",
				"Ӫ��������",
				"�ͺ�",
				"��������",
				"����ƻ��к�",
				"������Ŀ",
				"Ӧ����ʱ��",
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM ODS_ACC_REC a
			WHERE exists(SELECT 1 FROM INC_ACC_REC WHERE ����=a.���� and ����ƻ��к�=a.����ƻ��к�);
			COMMIT;
			----ɾ��ODS_ACC_REC�м�¼��
			delete 
			FROM ODS_ACC_REC a
			WHERE exists(SELECT 1 FROM INC_ACC_REC WHERE ����=a.���� and ����ƻ��к�=a.����ƻ��к�);
			COMMIT;

			----����������������
			INSERT INTO ODS_ACC_REC ("�ɽ���ʽ",
				"״̬",
				"����",
				"�ͻ�����",
				"�ͻ�����",
				"Ӫ��������",
				"Ӫ����������",
				"�ͺ�",
				"��������",
				"����ƻ��к�",
				"������Ŀ",
				"Ӧ����ʱ��",
				"Ӧ�����",
				"ʵ�����",
				"����ʵ�ʴ��",
				"ʵ�ʻ���ʱ��",
				"��󻹿�ʱ��",
				TIME_STAMP)
			SELECT "�ɽ���ʽ",
				"״̬",
				"����",
				"�ͻ�����",
				"�ͻ�����",
				"Ӫ��������",
				"Ӫ����������",
				"�ͺ�",
				"��������",
				"����ƻ��к�",
				"������Ŀ",
				"Ӧ����ʱ��",
				"Ӧ�����",
				"ʵ�����",
				"����ʵ�ʴ��",
				"ʵ�ʻ���ʱ��",
				"��󻹿�ʱ��",
				SYSDATE
			FROM
				INC_ACC_REC a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

			--------ɾ�������Ӧ��DW��dw_acc_rec
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_acc_rec';
			COMMIT;
			INSERT INTO dw_acc_rec 
			SELECT
				"�ɽ���ʽ",
				"״̬",
				"����",
				"�ͻ�����",
				"������Ŀ",
				"Ӧ����ʱ��",
				"Ӧ�����",
				"ʵ�����",
				"����ʵ�ʴ��",
				"ʵ�ʻ���ʱ��",
				"��󻹿�ʱ��"
			FROM
				ODS_ACC_REC;
			COMMIT;

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_acc_rec���ݸ������','��־��¼');
			COMMIT;

		EXCEPTION 
			WHEN OTHERS THEN
			-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	--���񶩵�
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_SERVICE_ORDERS;

	IF QTY_COM>0 then 

		BEGIN
			V_TABLE_NAME	:= 'INC_SERVICE_ORDERS';
			V_OBJECT_NAME	:= '"���񶩵�"';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				���񶩵���,
				�ͻ�,
				����״̬,
				�豸���,
				��Ʒ��,
				�豸����,
				��������,
				�豸�ͺ�,
				��������,
				��������,
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM "���񶩵�" a
			WHERE exists(SELECT 1 FROM INC_SERVICE_ORDERS WHERE ���񶩵���=a.���񶩵���);
			COMMIT;
			----ɾ��ODS_SERVICE_ORDERS�м�¼��
			delete 
			FROM "���񶩵�" a
			WHERE exists(SELECT 1 FROM INC_SERVICE_ORDERS WHERE ���񶩵���=a.���񶩵���);
			COMMIT;

			----����������������
			INSERT INTO "���񶩵�" ( "�����깤ʱ��",
				"���񶩵���",
				"�ͻ�",
				"����״̬",
				"�豸���",
				"��Ʒ��",
				"�豸����",
				"��������",
				"�豸�ͺ�",
				"��������",
				"ʡ��",-----gzy:�����ֶ�
				"��������",
				"���񹤳�ʦ",
				"�豸��ϵ��",
				"�豸��ϵ�˵绰",
				"��������",
				"������",
				"����ʱ��",
				"����ʱ��",
				"�����ڵ�",
				"���Ͻ������",
				"������",
				"����ʦ���ܵȼ�",
				"�ֳ��깤ʱ��",
				"�ۼ�����ʱ��",
				TIME_STAMP)
			SELECT "�����깤ʱ��",
				"���񶩵���",
				"�ͻ�",
				"����״̬",
				"�豸���",
				"��Ʒ��",
				"�豸����",
				"��������",
				"�豸�ͺ�",
				"��������",
				"ʡ��",-----gzy:�����ֶ�
				"��������",
				"���񹤳�ʦ",
				"�豸��ϵ��",
				"�豸��ϵ�˵绰",
				"��������",
				"������",
				"����ʱ��",
				"����ʱ��",
				"�����ڵ�",
				"���Ͻ������",
				"������",
				"����ʦ���ܵȼ�",
				"�ֳ��깤ʱ��",
				"�ۼ�����ʱ��",
				SYSDATE
			FROM
				INC_SERVICE_ORDERS a;
			COMMIT;

			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;

			--------ɾ�������Ӧ��DW��dw_service_orders
			EXECUTE IMMEDIATE 'TRUNCATE TABLE dw_service_orders';
			COMMIT;

			INSERT INTO dw_service_orders
				(�����깤ʱ��,
				���񶩵���,
				�ͻ�,
				����״̬,
				����,
				��������,
				��������,
				���񹤳�ʦ,
				��������,
				����ʱ��,
				�����ڵ�,
				����ʦ���ܵȼ�,
				�ֳ��깤ʱ��,
				�ۼ�����ʱ��,
				ʡ��)
			SELECT
				"�����깤ʱ��",
				"���񶩵���",
				"�ͻ�",
				"����״̬",
				"�豸���" as "����",
				"��������",
				"��������",
				"���񹤳�ʦ",
				"��������",
				"����ʱ��",
				"�����ڵ�",
				"����ʦ���ܵȼ�",
				"�ֳ��깤ʱ��",
				"�ۼ�����ʱ��",
				"ʡ��"-----gzy:�����ֶ�
			FROM
				"���񶩵�";
			COMMIT;

			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values(V_TABLE_NAME,SYSDATE,'dw_service_orders���ݸ������','��־��¼');
			COMMIT;
				
		EXCEPTION 
			WHEN OTHERS THEN
				-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	--���Ϸ���ٽ�������������
	QTY_COM := 0;
	SELECT count(1)
	into QTY_COM
	FROM INC_SEV_S_DETAIL;

	IF QTY_COM>0 then 

		BEGIN
			V_TABLE_NAME	:= 'INC_SEV_S_DETAIL';
			V_OBJECT_NAME	:= 'ODS_SEV_S_DETAIL';

			----------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ��¿�ʼ','��־��¼');
			COMMIT;

			------����ǰ���Ƚ�ԴODS���д��ڵ����ݱ�����־����
			INSERT INTO ETL_LOG(TABLE_NAME,CUL01,CUL02,CUL03,CUL04,CUL05,CUL06,CUL07,CUL08,CUL09,CUL10,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			SELECT V_TABLE_NAME,
				ID,
				�ֹ�˾,
				Ӫ������,
				����ʦ,
				��ͬ����,
				�ͺ�,
				�������,
				����,
				Ӫ���������񲿺�ʵ���,
				ͳ���·�,
				SYSDATE,
				V_OBJECT_NAME || '���ݱ��ݴ���',
				'���ݱ��ݼ�¼'
			FROM
				ODS_SEV_S_DETAIL a
			WHERE exists(SELECT 1 FROM INC_SEV_S_DETAIL WHERE ID=a.ID and �������=a.������� and ����=a.����);
			COMMIT;
			
			----ɾ��ODS_SERVICE_ORDERS�м�¼��
			DELETE FROM
				ODS_SEV_S_DETAIL a
			WHERE exists(SELECT 1 FROM INC_SEV_S_DETAIL WHERE ID=a.ID and �������=a.������� and ����=a.����);
			COMMIT;

			----����������������
			INSERT INTO ODS_SEV_S_DETAIL
				(id,
				"�ֹ�˾",
				"С���鳤",
				"Ӫ������",
				"����ʦ",
				"��ͬ����",
				"�ͺ�",
				"�������",
				"���ʱ��",
				"����",
				"Ӫ���������񲿺�ʵ���",
				"С���鳤�������",
				"����ʦ�������",
				"ͳ���·�",
				TIME_STAMP)
			SELECT id,
				"�ֹ�˾",
				"С���鳤",
				"Ӫ������",
				"����ʦ",
				"��ͬ����",
				"�ͺ�",
				"�������",
				"���ʱ��",
				"����",
				"Ӫ���������񲿺�ʵ���",
				"С���鳤�������",
				"����ʦ�������",
				"ͳ���·�",SYSDATE
			FROM
				INC_SEV_S_DETAIL a;
			COMMIT;


			--------ɾ�������Ӧ��INC��
			EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || V_TABLE_NAME;
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values(V_TABLE_NAME,SYSDATE,V_OBJECT_NAME || '���ݸ������','��־��¼');
			COMMIT;
		EXCEPTION 
			WHEN OTHERS THEN
			-------��¼���������־
				prc_wlf_sys_writelog(V_TABLE_NAME,SYSDATE,
					'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
		END;
	END IF;






	----DW_CUS_LOSE
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DW_CUS_LOSE',SYSDATE,'DW_CUS_LOSE���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DW_CUS_LOSE
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_CUS_LOSE' ;
		COMMIT;
		-- Edward0604: DW_CUS_LOSEʡ�������
		INSERT INTO DW_CUS_LOSE
			(�ͻ�����,
			�̻�����,
			����Ʒ��,
			�ͺ�,
			�ɽ�����,
			�ɽ���ʽ,
			�ɽ��۸�,
			�׸�����,
			����ԭ��,
			�ⲿ������,
			����״̬,
			����Ŷ�,
			������,
			������,
			���,
			��������������,
			ҵ������,
			����״̬,
			��������,
			����ʱ��,
			����޸���,
			����޸�ʱ��,
			����״̬,
			ʡ��)
		SELECT
			"�ͻ�����",
			"�̻�����",
			"����Ʒ��",
			"�ͺ�",
			"�ɽ�����",
			"�ɽ���ʽ",
			"�ɽ��۸�",
			"�׸�����",
			"����ԭ��",
			"�ⲿ������",
			"����״̬",
			"����Ŷ�",
			"������",
			"������",
			"���",
			"��������������",
			"ҵ������",
			"����״̬",
			"��������",
			"����ʱ��",
			"����޸���",
			"����޸�ʱ��",
			'�о���' as ����״̬,
			'ʡ��'
		FROM
			ods_lose_com
		union all
		SELECT
			"�ͻ�����",
			"�̻�����",
			"����Ʒ��",
			to_char("�ͺ�") as �ͺ�,
			"�ɽ�����",
			"�ɽ���ʽ",
			"�ɽ��۸�",
			"�׸�����",
			"����ԭ��",
			"�ⲿ������",
			"����״̬",
			"����Ŷ�",
			"������",
			"������",
			"���",
			"��������������",
			"ҵ������",
			"����״̬",
			"��������",
			"����ʱ��",
			"����޸���",
			"����޸�ʱ��",
			'�޾���' as ����״̬,
			'ʡ��'
		FROM
			ods_lose_no_com;		
		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DW_CUS_LOSE',SYSDATE,'DW_CUS_LOSE���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
		-------��¼���������־
			prc_wlf_sys_writelog('DW_CUS_LOSE',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----DM_S_SALES
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_SALES',SYSDATE,'DM_S_SALES���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_SALES
		-- Edward: DW_S_DETAIL ODS_MAP_MODEL ���޸�
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_SALES' ;
		COMMIT;
		INSERT INTO DM_S_SALES
			(��������,
			DMS,
			������,
			ʡ��,
			����,
			����,
			���۴���,
			���Ͽͻ�,
			��ͬ��λ,
			���۷�ʽ,
			����,
			����ҵ,
			С��ҵ,
			�ͺ�,
			�������,
			�Ƿ��Ծɻ���,
			�Ծɻ�������,
			֤������,
			סַ,
			��ϵ�绰,
			����,
			����ʱ��,
			�����ص�,
			�����û���ͬ���,
			���ɼ���,
			��������,
			�����,
			���ý����,
			���ú�۸�,
			��������ϼƽ��,
			�����·�,
			�������,
			�����·�ZW)
		SELECT
			"��������",
			dms,
			"������",
			"ʡ��",
			"�����ֹ�˾",
			case when "�����ֹ�˾" ='��̶' then '��ɳ'
				 when "�����ֹ�˾" ='��ͻ���' then '��ɳ'
				 when "�����ֹ�˾" ='������' then '����' 
				 when "�����ֹ�˾" ='������' then '����' 
				 when "�����ֹ�˾" ='�ܲ�' then '��̶' 
				 when "�����ֹ�˾" ='���ס��żҽ�' then '�żҽ�'
				 when "�����ֹ�˾" ='����' then '�żҽ�'
				 else "�����ֹ�˾" END AS "����",
			a."Ӫ������",
			case when c.����='�Ͽͻ��ٴι���' then '�Ͽͻ�' else a."�ͻ�����" END as "�ͻ�����",
			a."��ͬ��λ",
			a."���۷�ʽ",
			b."С�д���",
			b."����ҵ",
			b."С��ҵ",
			a."�ͺ�",
			a."�������",
			"�Ծɻ���",
			case when "�Ծɻ���" ='��' then "����" else 0 END as �Ծɻ�������,
			"֤������",
			"�ͻ�סַ",
			"��ϵ�绰",
			"����",
			"CRM���˽���ʱ��",
			"������ַ",
			"��ͬ���",
			"�������", -- Edward: �����ֶ�
			--"��������", -- Edward: �����ֶ�, FIX: ��DW_S_SALES�޸ĺ������
			"��������",
			"������",
			"�������ý��",
			"�ۺ������",
			"���ͽ��",
			"�����·�",
			"�������",
			"�����·�ZW"
		FROM
			dw_s_detail A,
			ods_map_model b,
			(SELECT distinct �������,���� FROM ods_sev_s_detail WHERE "Ӫ���������񲿺�ʵ���" not like '%�޼�¼%') c
		WHERE
			a."�ͺ�"=b."�ͺ�"(+) and a."�������"=c."�������"(+);
		COMMIT;

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_SALES',SYSDATE,'DM_S_SALES���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION
		WHEN OTHERS THEN
		-------��¼���������־
			prc_wlf_sys_writelog('DM_S_SALES',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;





	----DM_IND_SYZYL
	BEGIN
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_IND_SYZYL',SYSDATE,'DM_IND_SYZYL���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_IND_SYZYL
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_IND_SYZYL' ;
		COMMIT;
		INSERT INTO DM_IND_SYZYL 
		SELECT
			С��ҵ,
			"С�д���",
			"����",
			"���",
			"�·�",
			sum("��һ����") as ��һ����,
			sum("�г�����") as �г�����,
			case when sum("�г�����")=0 then 0 else sum(��һ����)/sum("�г�����") END as ռ����,
			sum("ʵ������") as ʵ������,
			sum("���۽��") as ���۽��,
			sum("ë��") as ë��,
			case when sum("���۽��")=0 then 0 else sum(ë��)/sum("���۽��") END as ������
		-- Edward: �ѱ���ֶ�ODS_MAP_MODEL.����->С�д���
		FROM 
			(SELECT b.С�д���,
				a.С��ҵ,
				"����01" as ����,
				"���",
				"�·�",
				sum(case when "Ʒ��"='��һ' then "����" else 0 END) "��һ����",
				sum("����") as �г�����,
				0 as ʵ������,
				0 as ���۽�� ,
				0 as ë��
				FROM dw_industry_small a,ods_map_model b
				WHERE a.С��ҵ=b.С��ҵ(+)
				group by b.С�д���,a.С��ҵ,
				"����01",
				"���",
				"�·�"
			union all
			SELECT m."����",
				m."С��ҵ",
				m."����",
				m."�������" as ���,
				m."�����·�" as �·�,
				0 as ��һ����,
				0 as �г�����,
				sum(m."����") as ʵ������,
				sum(m."�����û���ͬ���") as ���۽�� ,
				sum(n."���۽��" - n."��һ������" + n."�ɻ�����" - n."����������" - n."��Ϣ��" - n."�˷�" - n."�ά" - n."�д���") as ë��
			FROM
				DM_S_SALES m,
				DW_S_PROFIT n 
			WHERE
				m."�������"=n."����"(+)
			group by
				m."����",
				m."����",
				m."С��ҵ",
				m."�����·�",
				m."�������")
		GROUP BY
			"С��ҵ",
			"С�д���",
			"����",
			"���",
			"�·�"; 
		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_IND_SYZYL',SYSDATE,'DM_IND_SYZYL���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_IND_SYZYL',SYSDATE,
			'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');

	END;




	----DM_INDUSTRY_BIG
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_INDUSTRY_BIG',SYSDATE,'DM_INDUSTRY_BIG���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_INDUSTRY_BIG
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_INDUSTRY_BIG' ;
		COMMIT;
		INSERT INTO DM_INDUSTRY_BIG
		SELECT '����' as ʡ,
			����ҵ,
			"���",
			"�·�",
			sum(case when "Ʒ��"='��һ' then "����" else 0 END) "��һ����",
			sum("����") as �г�����,
			case when sum("����")=0 then 0 else sum(case when "Ʒ��"='��һ' then "����" else 0 END)/sum("����") END as ռ����
			FROM dw_industry_big 
			group by '����',����ҵ,
			"���",
			"�·�";					
		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_INDUSTRY_BIG',SYSDATE,'DM_INDUSTRY_BIG���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_INDUSTRY_BIG',SYSDATE,
			'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;





	----dm_salesman_score01
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_salesman_score01',SYSDATE,'dm_salesman_score01���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��dm_salesman_score01
		EXECUTE IMMEDIATE 'TRUNCATE TABLE dm_salesman_score01' ;
		COMMIT;
		INSERT INTO dm_salesman_score01(
			����,
			���,
			�¶�,
			���۴���,
			���,
			����,
			ֵ,
			�ۺϵ÷�)
		SELECT
			a.col_ as ����,
			a.col__1 as ���,
			a.col__1_2 as �¶�,
			a.col__1_2_3 as ���۴���,
			b.���,
			b.����,
			case when b.����='�����÷�' then nvl(a.col__1_2_3_4 ,0)
				when b.����='���۶�÷�' then nvl(a.col__1_2_3_4_5 ,0)
				when b.����='ë���ʵ÷�' then nvl(a.col__1_2_3_4_5_6 ,0)
				when b.����='�Ծɻ��±����÷�' then nvl(a.col__1_2_3_4_5_6_7 ,0)
				when b.����='սʤ�ʵ÷�' then nvl(a.col__1_2_3_4_5_6_7_8 ,0)
				when b.����='��Ϣ��ռ�ȵ÷�' then nvl(a.col__1_2_3_4_5_6_7_8_9 ,0)
				when b.����='�������ռ�ȵ÷�' then nvl(a.col__1_2_3_4_5_6_7_8_9_10,0)
				when b.����='�ۺϵ÷�' then nvl(a.col__1_2_3_4_5_6_7_8_9_10_11,0) END as ֵ,
			nvl(a.col__1_2_3_4_5_6_7_8_9_10_11,0) as �ۺϵ÷�
		FROM
			dm_salesman_score a,
			dim_salesman_score b;

		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_salesman_score01',SYSDATE,'dm_salesman_score01���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('dm_salesman_score01',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----dm_business_opp
	BEGIN
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_business_opp',SYSDATE,'dm_business_opp���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��dm_business_opp
		EXECUTE IMMEDIATE 'TRUNCATE TABLE dm_business_opp' ;
		COMMIT;
		INSERT INTO dm_business_opp
		SELECT
			substr(to_char(����޸�ʱ��,'yyyymmdd'),1,4) as ���,
			substr(to_char(����޸�ʱ��,'yyyymmdd'),5,2) as �¶�,
			to_date(substr(to_char(����޸�ʱ��,'yyyymmdd'),1,6)||'01','yyyymmdd') as ����,
			"Ա������" as ������,
			count(1) as �¿���
		FROM
			dw_business_opp
		group by 
			substr(to_char(����޸�ʱ��,'yyyymmdd'),1,4),
			substr(to_char(����޸�ʱ��,'yyyymmdd'),5,2),
			to_date(substr(to_char(����޸�ʱ��,'yyyymmdd'),1,6)||'01','yyyymmdd'),
			"Ա������";						
		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_business_opp',SYSDATE,'dm_business_opp���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('dm_business_opp',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----DM_S_MILEAGE
	BEGIN
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_MILEAGE',SYSDATE,'DM_S_MILEAGE���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_MILEAGE
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_MILEAGE' ;
		COMMIT;
		INSERT INTO
			DM_S_MILEAGE
		SELECT
			���,
			�¶�,
			to_date(substr(to_char(����,'yyyymmdd'),1,6)||'01','yyyymmdd') as ����,
			"���۴���" as ������,
			sum("�����KM") as �����
		FROM
			DW_S_MILEAGE
		group by 
			���,
			�¶�,
			to_date(substr(to_char(����,'yyyymmdd'),1,6)||'01','yyyymmdd'),
			"���۴���";
		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_MILEAGE',SYSDATE,'DM_S_MILEAGE���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_MILEAGE',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;





	----DM_S_TRAVEL
	BEGIN
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_TRAVEL',SYSDATE,'DM_S_TRAVEL���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_TRAVEL
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_TRAVEL' ;
		COMMIT;
		INSERT INTO
			DM_S_TRAVEL
		SELECT
			���,
			�¶�,
			to_date(substr(to_char(��ʼʱ��,'yyyymmdd'),1,6)||'01','yyyymmdd') as ����,
			"�豸����" as ������,
			sum("�����KM") as �����,
			count(1) as ���г���
		FROM
			DW_S_TRAVEL
		group by 
			���,
			�¶�,
			to_date(substr(to_char(��ʼʱ��,'yyyymmdd'),1,6)||'01','yyyymmdd') ,
			"�豸����";				
		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_TRAVEL',SYSDATE,'DM_S_TRAVEL���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_TRAVEL',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----dm_customer_new
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_customer_new',SYSDATE,'dm_customer_new���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��dm_customer_new
		EXECUTE IMMEDIATE 'TRUNCATE TABLE dm_customer_new' ;
		COMMIT;
		INSERT INTO
			dm_customer_new
		SELECT
			substr(to_char(������ʱ��,'yyyymmdd'),1,4) as ���,
			substr(to_char(������ʱ��,'yyyymmdd'),5,2) as �¶�,
			to_date(substr(to_char(������ʱ��,'yyyymmdd'),1,6)||'01','yyyymmdd') as ����,
			"Ա������" as ������,
			count(1) as �¿���
		FROM
			dw_customer_new
		group by 
			substr(to_char(������ʱ��,'yyyymmdd'),1,4),
			substr(to_char(������ʱ��,'yyyymmdd'),5,2),
			to_date(substr(to_char(������ʱ��,'yyyymmdd'),1,6)||'01','yyyymmdd'),
			"Ա������";			
		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_customer_new',SYSDATE,'dm_customer_new���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('dm_customer_new',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----dm_customer_face
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_customer_face',SYSDATE,'dm_customer_face���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��dm_customer_face
		EXECUTE IMMEDIATE 'TRUNCATE TABLE dm_customer_face' ;
		COMMIT;
		INSERT INTO
			dm_customer_new
		SELECT
			substr(to_char(���ʱ��,'yyyymmdd'),1,4) as ���,
			substr(to_char(���ʱ��,'yyyymmdd'),5,2) as �¶�,
			to_date(substr(to_char(���ʱ��,'yyyymmdd'),1,6)||'01','yyyymmdd') as ����,
			"������",
			count(1) as �����
		FROM
			dw_customer_face
		group by 
			substr(to_char(���ʱ��,'yyyymmdd'),1,4),
			substr(to_char(���ʱ��,'yyyymmdd'),5,2),
			to_date(substr(to_char(���ʱ��,'yyyymmdd'),1,6)||'01','yyyymmdd'),
			"������";			
		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('dm_customer_face',SYSDATE,'dm_customer_face���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('dm_customer_face',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----DM_S_PRESALES
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_PRESALES',SYSDATE,'DM_S_PRESALES���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_PRESALES
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_PRESALES' ;
		COMMIT;
		INSERT INTO
			DM_S_PRESALES
		SELECT "���",
			"�¶�",
			"����",
			"������",
			sum(nvl(�̻���,0)) as �̻���,
			sum(nvl(�¿���,0)) as �¿���, 
			sum(nvl(�����,0)) as �����,
			sum(nvl(�����,0)) as �����,
			sum(nvl(�����,0)) as ���г���
		FROM
			(SELECT
				"���",
				"�¶�",
				"����",
				"������",
				"�¿���" as �̻��� ,
				0 as �¿���,
				0 as �����,
				0 as �����,
				0 as ���г���
			FROM
				dm_business_opp 
			union all
			SELECT
				"���",
				"�¶�",
				"����",
				"������",
				0 as �̻���,
				"�¿���" as �¿���,
				0 as �����,
				0 as �����,
				0 as ���г���
			FROM
				dm_customer_new
			union all
			SELECT
				"���",
				"�¶�",
				"����",
				"������",
				0 as �̻���,
				0 as �¿���,
				"�����" as �����,
				0 as �����,
				0 as ���г���
			FROM
				dm_customer_face
			union all
			SELECT
				"���",
				"�¶�",
				"����",
				"������",
				0 as �̻���,
				0 as �¿���,
				0 as �����,
				�����,
				0 as ���г���
			FROM
				DM_S_MILEAGE
			union all
			SELECT
				"���",
				"�¶�",
				"����",
				"������",
				0 as �̻���,
				0 as �¿���,
				0 as �����,
				0 as �����,
				���г���
				FROM
				DM_S_TRAVEL) a
				group by "���",
				"�¶�",
				"����",
				"������";			
			COMMIT;
			--------��¼���������־
			INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
				values('DM_S_PRESALES',SYSDATE,'DM_S_PRESALES���ݸ������','��־��¼');
			COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_PRESALES',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----DM_S_AAOM_PRE
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_PRE',SYSDATE,'DM_S_AAOM_PRE���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_AAOM_PRE
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_PRE' ;
		COMMIT;
		-- Edward0604: DM_S_AAOM_PREʡ�������
		INSERT INTO DM_S_AAOM_PRE
			(���,
			�¶�,
			����,
			�ֹ�˾,
			����,
			���۴���,
			�̻���,
			�¿���,
			�����,
			�����,
			���г���,
			ʡ��) -- ���ʡ��
		SELECT
			a.���,
			a.�¶�,
			a.����,
			b.�������������� as �ֹ�˾,
			case when b.�����ֹ�˾='��ͻ���' then '��ɳ'
				when b.�����ֹ�˾='����' then '����'
				when b.�����ֹ�˾='װ�ػ���ҵ��' then '��ɳ'
				when b.�����ֹ�˾='��̶' then '��ɳ'
				when b.�����ֹ�˾='�Ž��澰����' then '�Ž�'
				when b.�����ֹ�˾='������' then '����'
				when b.�����ֹ�˾ is null then '��ɳ'
				else b.�����ֹ�˾ END �����ֹ�˾,
			a.������ as ���۴���,
			a.�̻���,
			a.�¿���,
			a.�����,
			a.�����,
			a.���г���,
			b.ʡ�� -- ���ʡ��
		FROM
			DM_S_PRESALES a,
			DW_DEPT_EMP b
		WHERE
			a.������ = b.������(+);
		COMMIT;

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_PRE',SYSDATE,'DM_S_AAOM_PRE���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_AAOM_PRE',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----DM_S_AAOM_LOSE
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_LOSE',SYSDATE,'DM_S_AAOM_LOSE���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_AAOM_LOSE
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_LOSE' ;
		COMMIT;

		--Edward0605: DM_S_AAOM_LOSE���ʡ��
		INSERT INTO DM_S_AAOM_LOSE
			(���,
			�¶�,
			����,
			�ֹ�˾,
			����,
			���۴���,
			�޾���������,
			�о���������,
			������,
			ʡ��)
		SELECT
			substr(to_char(����޸�ʱ��,'yyyymmdd'),1,4) as ���,
			substr(to_char(����޸�ʱ��,'yyyymmdd'),5,2) as �¶�,
			to_date(substr(to_char(����޸�ʱ��,'yyyymmdd'),1,6) ||'01','yyyymmdd') as ����,
			"��������������" as �ֹ�˾,
			case when ��������������='��̶�ֹ�˾' then '��ɳ'
				 when ��������������='��������' then '�ϲ�'
				 when instr(��������������,'�ֹ�˾') >0 then substr(��������������,1,instr(��������������,'�ֹ�˾')-1) else �������������� END as ����,
			������ as ���۴���,
			count(case when ����״̬='�޾���' then ��� END) as �޾���������,
			count(case when ����״̬='�о���' then ��� END) as �о���������,
			count(���) as ������,
			ʡ��
		FROM
			dw_cus_lose
		group by
			substr(to_char(����޸�ʱ��,'yyyymmdd'),1,4) ,
			substr(to_char(����޸�ʱ��,'yyyymmdd'),5,2) ,
			to_date(substr(to_char(����޸�ʱ��,'yyyymmdd'),1,6) ||'01','yyyymmdd') ,
			"��������������",
			case when ��������������='��̶�ֹ�˾' then '��ɳ'
				 when ��������������='��������' then '�ϲ�'
				 when instr(��������������,'�ֹ�˾') >0 then substr(��������������,1,instr(��������������,'�ֹ�˾')-1) else �������������� END,
			������;
		COMMIT;

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_LOSE',SYSDATE,'DM_S_AAOM_LOSE���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_AAOM_LOSE',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----DM_S_AAOM_SR01
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_SR01',SYSDATE,'DM_S_AAOM_SR01���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_AAOM_SR01
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_SR01';
		COMMIT;

		-- Edward: �ѱ���ֶ�DM_S_AAOM_SR01
		INSERT INTO DM_S_AAOM_SR01
			(���,
			�¶�,
			����,
			ʡ��,
			�����ֹ�˾,
			�ͻ�����,
			��ͬ��λ,
			���۷�ʽ,
			�������,
			CRM���˽���ʱ��,
			�ͺ�,
			����,
			Ӫ������,
			��ͬ���,
			����,
			�Ծɻ�������,
			�ɻ�����,
			��Ϣ��,
			���ͽ��,
			ë����)
		-- Edward: DW_S_DETAIL ���޸��ֶ�
		SELECT
			a.������� as ���,
			case when instr(�����·�SY,'��')>0 then 
					(case when length(substr(�����·�SY,1,instr(�����·�SY,'��')-1))=1 then '0'||substr(�����·�SY,1,instr(�����·�SY,'��')-1)
					 else substr(�����·�SY,1,instr(�����·�SY,'��')-1) END)
				when length(�����·�SY)=1 then '0'||�����·�SY
				when �����·�SY is null then substr(to_char(CRM���˽���ʱ��,'yyyymmdd'),5,2)
				else �����·�SY END as �¶�,
			to_date( �������|| (case when instr(�����·�SY,'��')>0 then 
					(case when length(substr(�����·�SY,1,instr(�����·�SY,'��')-1))=1 then '0'||substr(�����·�SY,1,instr(�����·�SY,'��')-1)
				else substr(�����·�SY,1,instr(�����·�SY,'��')-1) END)
				when length(�����·�SY)=1 then '0'||�����·�SY
				when �����·�SY is null then substr(to_char(CRM���˽���ʱ��,'yyyymmdd'),5,2)
				else �����·�SY END)||'01','yyyymmdd') as ����,
			a.ʡ��,
			a.�����ֹ�˾ as �����ֹ�˾,
			a.�ͻ�����,
			a.��ͬ��λ,
			a.���۷�ʽ,
			a.�������,
			a.CRM���˽���ʱ��,
			case when instr(a."�ͺ�",'C')>0 then substr(a."�ͺ�",1,instr(a."�ͺ�",'C')-1)
				when instr(a."�ͺ�",'H')>0 then substr(a."�ͺ�",1,instr(a."�ͺ�",'H')-1)
				when instr(a."�ͺ�",'U')>0 then substr(a."�ͺ�",1,instr(a."�ͺ�",'U')-1) else a."�ͺ�" END as �ͺ�,
			case when a.�����ֹ�˾='��ͻ���' then c.����
				when a.�����ֹ�˾='��̶' then '��ɳ'
				when a.�����ֹ�˾='����' then '��ɳ'
				when a.�����ֹ�˾='�ܲ�' then '��ɳ'
				when a.�����ֹ�˾='���ס��żҽ�' then '�żҽ�'
				when a.�����ֹ�˾='������' then '����'
				when a.�����ֹ�˾='������' then '����'
				else a.�����ֹ�˾ END as ����,
			a.Ӫ������,
			sum(nvl(a.��ͬ���,0)) as ��ͬ���,
			sum(nvl(a.����,0)) as ����,
			sum(case when �Ծɻ��� ='��' then nvl(a.����,0) else 0 END) as �Ծɻ�������,
			sum(nvl(b.�ɻ�����,0)) as �ɻ�����,
			sum(nvl(b.��Ϣ��,0)) as ��Ϣ��,
			sum(nvl(b.����������,0)) as ���ͽ��,
			sum(nvl(b."���۽��",0)-nvl(b."��һ������",0) + nvl(b."�ɻ�����",0) - nvl(b."����������",0) - nvl(b."��Ϣ��",0)-nvl(b."�˷�",0) - nvl(b."�ά",0) - nvl(b."�д���",0)) as ë����
		FROM
			dw_s_detail a,
			dw_s_profit b,
			ods_city_bc c
		WHERE 
			a.�������=b.����(+) and a.�������=c.�������(+)
		group by 
			a.�������,
			(case when instr(�����·�SY,'��')>0 then 
				(case when length(substr(�����·�SY,1,instr(�����·�SY,'��')-1))=1 then '0'||substr(�����·�SY,1,instr(�����·�SY,'��')-1)
					  else substr(�����·�SY,1,instr(�����·�SY,'��')-1) END)
				 when length(�����·�SY)=1 then '0'||�����·�SY
				 when �����·�SY is null then substr(to_char(CRM���˽���ʱ��,'yyyymmdd'),5,2)
				 else �����·�SY END),
			to_date(a.�������|| (case when instr(�����·�SY,'��')>0 then
				(case when length(substr(�����·�SY,1,instr(�����·�SY,'��')-1))=1 then '0'||substr(�����·�SY,1,instr(�����·�SY,'��')-1)
					  else substr(�����·�SY,1,instr(�����·�SY,'��')-1) END )
					  when length(�����·�SY)=1 then '0'||�����·�SY
					  when �����·�SY is null then substr(to_char(CRM���˽���ʱ��,'yyyymmdd'),5,2)
					  else �����·�SY END)||'01','yyyymmdd'),
			a.ʡ��,
			a.�����ֹ�˾,
			a.�ͻ�����,
			a.��ͬ��λ,
			a.���۷�ʽ,
			a.�������,
			a.CRM���˽���ʱ��,
			case when instr(a."�ͺ�",'C')>0 then substr(a."�ͺ�",1,instr(a."�ͺ�",'C')-1)
				 when instr(a."�ͺ�",'H')>0 then substr(a."�ͺ�",1,instr(a."�ͺ�",'H')-1)
				 when instr(a."�ͺ�",'U')>0 then substr(a."�ͺ�",1,instr(a."�ͺ�",'U')-1) else a."�ͺ�" END,
			case when a.�����ֹ�˾='��ͻ���' then c.����
				 when a.�����ֹ�˾='��̶' then '��ɳ'
				 when a.�����ֹ�˾='����' then '��ɳ'
				 when a.�����ֹ�˾='�ܲ�' then '��ɳ'
				 when a.�����ֹ�˾='���ס��żҽ�' then '�żҽ�'
				 when a.�����ֹ�˾='������' then '����'
				 when a.�����ֹ�˾='������' then '����'
				 else a.�����ֹ�˾ END,
			a.Ӫ������;
		COMMIT;

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_SR01',SYSDATE,'DM_S_AAOM_SR01���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_AAOM_SR01',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----DM_S_AAOM_SR
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
		values('DM_S_AAOM_SR',SYSDATE,'DM_S_AAOM_SR���ݸ��¿�ʼ','��־��¼');
		COMMIT;
		--------ɾ�������Ӧ��DM��DM_S_AAOM_SR
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_SR' ;
		COMMIT;
		-- Edward: �ѱ���ֶ�DM_S_AAOM_SR01
		-- Edward: �ѱ���ֶ�DM_S_AAOM_SR
		-- Edward: �ѱ���ֶ�ODS_MAP_MODEL.����->С�д���
		INSERT INTO DM_S_AAOM_SR(
			"���",
			"�¶�",
			"����",
			"ʡ��",
			"�����ֹ�˾",
			"�ͻ�����",
			"��ͬ��λ",
			"���۷�ʽ",
			"CRM���˽���ʱ��",
			"�ͺ�",
			"С�д���",
			"����ҵ",
			"����",
			"Ӫ������",
			"��ͬ���",
			"����",
			"�Ծɻ�������",
			"�ɻ�����",
			"��Ϣ��",
			"���ͽ��",
			"ë����"
			)
		SELECT
			"���",
			"�¶�",
			"����",
			"ʡ��",
			"�����ֹ�˾",
			"�ͻ�����",
			"��ͬ��λ",
			"���۷�ʽ",
			"CRM���˽���ʱ��",
			a."�ͺ�",
			b."С�д���",
			b."����ҵ",
			"����",
			"Ӫ������",
			sum("��ͬ���") as ��ͬ���,
			sum("����") as "����",
			sum("�Ծɻ�������") as "�Ծɻ�������",
			sum("�ɻ�����") as "�ɻ�����",
			sum("��Ϣ��") as "��Ϣ��",
			sum("���ͽ��") as "���ͽ��",
			sum("ë����") as "ë����"
		FROM
			DM_S_AAOM_SR01 a,
			ods_map_model b
		WHERE
			a.�ͺ�=b.�ͺ�(+)
		group by 
			"���",
			"�¶�",
			"����",
			"ʡ��",
			"�����ֹ�˾",
			"�ͻ�����",
			"��ͬ��λ",
			"���۷�ʽ",
			"CRM���˽���ʱ��",
			a."�ͺ�",
			b."С�д���",
			b."����ҵ",
			"����",
			"Ӫ������";
		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_SR',SYSDATE,'DM_S_AAOM_SR���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_AAOM_SR',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----DM_S_AAOM_SMALL
	BEGIN
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_SMALL',SYSDATE,'DM_S_AAOM_SMALL���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_AAOM_SMALL
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_SMALL' ;
		COMMIT;
		INSERT INTO
			DM_S_AAOM_SMALL
		SELECT
			"���" as ���,
			"�·�" as �¶�,
			"����" as ����,
			case when "����01"='����' then '����' else "����01" END as ����,
			sum(nvl("����",0)) as С��ҵ�г�����
		FROM
			dw_industry_small
		group by
			"���",
			"�·�",
			"����",
			case when "����01"='����' then '����' 
				 else "����01" END;
		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_SMALL',SYSDATE,'DM_S_AAOM_SMALL���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_AAOM_SMALL',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----DM_S_AAOM
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
		values('DM_S_AAOM',SYSDATE,'DM_S_AAOM���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_AAOM
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM' ;
		COMMIT;
		-- Edward0605: DM_S_AAOM���ʡ��
		INSERT INTO DM_S_AAOM
			(���,
			�¶�,
			����,
			����,
			���۴���,
			���۽��,
			����,
			�Ծɻ�������,
			�ɻ�����,
			��Ϣ��,
			����������,
			ë����,
			�̻���,
			�¿���,
			�����,
			�����,
			���г���,
			�޾���������,
			�о���������,
			������,
			������,
			������,
			ʡ��)
		SELECT
			a.���,
			a.�¶�,
			a.����,
			a.����,
			a.Ӫ������,
			sum(a.��ͬ���) as ��ͬ���,
			sum(a.����) as ����,
			sum(a.�Ծɻ�������) as �Ծɻ�������,
			sum(a.�ɻ�����) as �ɻ�����,
			sum(a.��Ϣ��) as ��Ϣ��,
			sum(a.���ͽ��) as ���ͽ��,
			sum(a.ë����) as ë����,
			sum(a.�̻���) as �̻���,
			sum(a.�¿���) as �¿���,
			sum(a.�����) as �����,
			sum(a.�����) as �����,
			sum(a.���г���) as ���г���,
			sum(a.�޾���������) as �޾���������,
			sum(a.�о���������) as �о���������,
			sum(nvl(a.����,0)+nvl(a.�о���������,0)) as ������,
			sum(nvl(a.����,0)+nvl(a.������,0)) as ������,
			sum(a.������) as ������,
			a.ʡ�� -- ���ʡ��
		-- Edward0605: DM_S_AAOM_SR, DM_S_AAOM_LOSE, DM_S_AAOM_PRE
		FROM
			(SELECT
				to_char("���") as "���",
				"�¶�",
				"����",
				"����",
				"Ӫ������",
				"��ͬ���",
				"����",
				"�Ծɻ�������",
				"�ɻ�����",
				"��Ϣ��",
				"���ͽ��",
				"ë����",
				0 as �̻���,
				0 as �¿���,
				0 as �����,
				0 as �����,
				0 as ���г���,
				0 as �޾���������,
				0 as �о���������,
				0 as ������,
				"ʡ��"
			FROM
				dm_s_aaom_sr
			UNION ALL
			SELECT
				"���",
				"�¶�",
				"����",
				"����",
				"���۴���",
				0 as "���۽��",
				0 as "����",
				0 as "�Ծɻ�������",
				0 as "�ɻ�����",
				0 as "��Ϣ��",
				0 as "����������",
				0 as "ë����",
				"�̻���",
				"�¿���",
				"�����",
				 �����,
				���г���,
				0 as �޾���������,
				0 as �о���������,
				0 as ������,
				"ʡ��"
			FROM
				dm_s_aaom_pre
			UNION ALL
			SELECT
				"���",
				"�¶�",
				"����",
				"����",
				"���۴���",
				0 as "���۽��",
				0 as "����",
				0 as "�Ծɻ�������",
				0 as "�ɻ�����",
				0 as "��Ϣ��",
				0 as "����������",
				0 as "ë����",
				0 as "�̻���",
				0 as "�¿���",
				0 as "�����", 
				0 as �����,
				0 as ���г���,
				"�޾���������",
				"�о���������",
				"������",
				"ʡ��"
			FROM
				dm_s_aaom_lose
			) a
		group by
			a.���,
			a.�¶�,
			a.����,
			a.����,
			a.Ӫ������,
			a.ʡ��;
		COMMIT;

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM',SYSDATE,'DM_S_AAOM���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_AAOM',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----DM_S_AAOM_COM
	BEGIN
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_COM',SYSDATE,'DM_S_AAOM_COM���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_AAOM_COM
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_COM' ;
		COMMIT;

		-- Edward0605: DM_S_AAOM_COM���ʡ��
		INSERT INTO DM_S_AAOM_COM
			(���,
			�¶�,
			����,
			����,
			���۽��,
			����,
			�Ծɻ�������,
			�ɻ�����,
			��Ϣ��,
			����������,
			ë����,
			�̻���,
			�¿���,
			�����,
			�޾���������,
			�о���������,
			������,
			������,
			������,
			С��ҵ�г�����,
			ʡ��)
		SELECT
			a."���",
			a."�¶�",
			a."����",
			a."����",
			"���۽��",
			"����",
			"�Ծɻ�������",
			"�ɻ�����",
			"��Ϣ��",
			"����������",
			"ë����",
			"�̻���",
			"�¿���",
			"�����",
			"�޾���������",
			"�о���������",
			"������",
			"������",
			"������",
			"ʡ��", --DM_S_AAOMʡ��
			d.С��ҵ�г�����
		FROM
			(SELECT
				"���",
				"�¶�",
				"����",
				"����",
				sum("���۽��") as "���۽��",
				sum("����") as "����",
				sum("�Ծɻ�������") as "�Ծɻ�������",
				sum("�ɻ�����") as "�ɻ�����",
				sum("��Ϣ��") as "��Ϣ��",
				sum("����������") as "����������",
				sum("ë����") as "ë����",
				sum("�̻���") as "�̻���",
				sum("�¿���") as "�¿���",
				sum("�����") as "�����",
				sum("�޾���������") as "�޾���������",
				sum("�о���������") as "�о���������",
				sum("������") as "������",
				sum("������") as "������",
				sum("������") as "������",
				"ʡ��"
			FROM
				dm_s_aaom
			group by 
				"���",
				"�¶�",
				"����",
				"����",
				"ʡ��") a,
			DM_S_AAOM_SMALL d
		WHERE
			a.��� = d.��� and a.�¶� = d.�¶� and a.���� = d.����;
		COMMIT;

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_COM',SYSDATE,'DM_S_AAOM_COM���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_AAOM_COM',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;







	----DM_S_AAOM_FUNNEL
	BEGIN
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_FUNNEL',SYSDATE,'DM_S_AAOM_FUNNEL���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_AAOM_FUNNEL
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_AAOM_FUNNEL';
		COMMIT;
		-- Edward0605: ���ʡ��
		INSERT INTO DM_S_AAOM_FUNNEL
			(���,
			�¶�,
			����,
			����,
			���۴���,
			���,
			����,
			ֵ,
			ʡ��)
		SELECT
			a."���",
			a."�¶�",
			a."����",
			a."����",
			a.���۴���,
			b.���,
			b.����,
			case when b.����='�����ͻ���' then a.�¿���
				 when b.����='�����' then a.�����
				 when b.����='�̻���' then a.�̻���
				 when b.����='�ɽ���' then a.���� else 0 END as ֵ,
			a."ʡ��"
		FROM
			dm_s_aaom a,
			dim_funnel b
		WHERE
			a."���">=2019;
		COMMIT;
		
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_AAOM_FUNNEL',SYSDATE,'DM_S_AAOM_FUNNEL���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_AAOM_FUNNEL',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;







	----��̨ӯ�������
	----DW_S_DTYKDFX
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DW_S_DTYKDFX',SYSDATE,'DW_S_DTYKDFX���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DW��DW_S_DTYKDFX
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_S_DTYKDFX' ;
		COMMIT;
		-- Edward: ODS_MAP_MODEL�ޱ���ֶ�
		INSERT INTO
			DW_S_DTYKDFX
		SELECT
			a."����",
			a."�豸�ͺ�",
			d."С�д���",
			d."����ҵ",
			d."С��ҵ",
			a."ʡ��",
			a."����",
			a."�豸����",
			a."��λ�ص�",
			a."����¼ʱ��",
			a."�ͻ���",
			a."ʵ�ʹ�����",
			a."��ϵ�绰",
			a."��ͬ��λ",
			a."���۷�ʽ",
			a."��֤��",
			a."ԭӪ������",
			a."��Ӫ������",
			a."��������",
			a."�ſ�����",
			a."�ͻ�����",
			a."���ڿ��ܼ�",
			a."����������",
			a."��12�����ۼƻ���",
			a."��6�����ۼƻ���",
			a."��3�����ۼƻ���",
			a."��ͬ��",
			a."�ۿ۽��",
			a."�ۺ��",
			a."�������",
			a."δ����",
			b."�ܹ�ʱ",
			b."���깤ʱ",
			c."���۽��"-c."��һ������"+c."�ɻ�����"-c."����������" as �۸�,
			c."�������"+c."��Ϣ��"+c."�˷�" +c."�ά"+c."�д���"+c."���ʰ�����Ϣ" as ����,
			c."��������",
			c."���۽��"-c."��һ������"+c."��������" -c."����������" -c."�������"-c."��Ϣ��"-c."�˷�" -c."�ά"-c."�д���" +c."�ɻ�����" -c."���ʰ�����Ϣ" as ë��,
			(c."���۽��"-c."��һ������"+c."��������" -c."����������" -c."�������"-c."��Ϣ��"-c."�˷�" -c."�ά"-c."�д���" +c."�ɻ�����" -c."���ʰ�����Ϣ")/c.���۽�� as ������,
			c.ë��3,
			c.ë��2
		FROM
			dw_exc_ledger a,
			dm_hnzw_zgs b,
			DW_S_PROFIT c,
			ods_map_model d
		WHERE
			a."�豸�ͺ�"=d."�ͺ�"(+) and a."����"=b."����"(+) and a."����"=c."����"(+); 	
		COMMIT;

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DW_S_DTYKDFX',SYSDATE,'DW_S_DTYKDFX���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DW_S_DTYKDFX',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----��̨ӯ������� �ٲ�ͼ
	----DM_WATERFALL_PROFIT
	BEGIN
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_WATERFALL_PROFIT',SYSDATE,'DM_WATERFALL_PROFIT���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_WATERFALL_PROFIT
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_WATERFALL_PROFIT' ;
		COMMIT;

		INSERT INTO
			DM_WATERFALL_PROFIT
		SELECT
			"����",
			b.����,
			b.��Ŀ,
			nvl(case when b.��Ŀ='���۽��' then "���۽��" 
					 when b.��Ŀ='��һ������' then "���۽��"-"��һ������" 
					 when b.��Ŀ='�ɻ�����' then "���۽��"-"��һ������"+"�ɻ�����"
					 when b.��Ŀ='����������' then "���۽��"-"��һ������"+"�ɻ�����" -"����������" 
					 when b.��Ŀ='�������' then "���۽��"-"��һ������"+"�ɻ�����" -"����������" -"�������" 
					 when b.��Ŀ='��Ϣ��' then "���۽��"-"��һ������"+"�ɻ�����" -"����������" -"�������"-"��Ϣ��" 
					 when b.��Ŀ='�˷�' then "���۽��"-"��һ������"+"�ɻ�����" -"����������" -"�������"-"��Ϣ��"-"�˷�" 
					 when b.��Ŀ='�ά' then "���۽��"-"��һ������"+"�ɻ�����" -"����������" -"�������"-"��Ϣ��"-"�˷�" -"�ά" 
					 when b.��Ŀ='�д���' then "���۽��"-"��һ������"+"�ɻ�����" -"����������" -"�������"-"��Ϣ��"-"�˷�" -"�ά"-"�д���" 
					 when b.��Ŀ='���ʰ�����Ϣ' then "���۽��"-"��һ������"+"�ɻ�����" -"����������" -"�������"-"��Ϣ��"-"�˷�" -"�ά"-"�д���" -"���ʰ�����Ϣ"
					 when b.��Ŀ='��������' then "���۽��"-"��һ������"+"�ɻ�����" -"����������" -"�������"-"��Ϣ��"-"�˷�" -"�ά"-"�д���" +"��������" -"���ʰ�����Ϣ" END,0) as ���
		FROM
			dw_s_profit a,
			ODS_SUBJECTS b;
		COMMIT;

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_WATERFALL_PROFIT',SYSDATE,'DM_WATERFALL_PROFIT���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_WATERFALL_PROFIT',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----��ʱ����
	----DM_S_HNZW_GSQX
	BEGIN
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_HNZW_GSQX',SYSDATE,'DM_S_HNZW_GSQX���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_HNZW_GSQX
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_HNZW_GSQX' ;
		COMMIT;

		INSERT INTO DM_S_HNZW_GSQX 
		SELECT
			"����",
			case when instr("����",'C')>0 then substr("����",1,instr("����",'C')-1)
				 when instr("����",'H')>0 then substr("����",1,instr("����",'H')-1)
				 when instr("����",'W')>0 then substr("����",1,instr("����",'W')-1)
				 when "����"='SY485S1I3K' then 'SY485'
				 when instr("����",'U')>0 then substr("����",1,instr("����",'U')-1) else "����" END as "����",
			max("�ܹ�ʱ") as �ܹ�ʱ,
			sum("���չ�ʱ") as ��ʱ,
			case when instr("��λ����",'ʡ')>0 then substr( "��λ����",1,instr("��λ����",'ʡ')-1)
				 when instr("��λ����",'����׳��������')>0 then '����'
				 when instr("��λ����",'�Ϻ�')>0 then '�Ϻ�'
				 when instr("��λ����",'����')>0 then '����'
				 when instr("��λ����",'����')>0 then '����'
				 when instr("��λ����",'���')>0 then '���'
				 when instr("��λ����",'����ر�������')>0 then '���'
				 when instr("��λ����",'�����ر�������')>0 then '����'
				 when instr("��λ����",'Telangana')>0 then 'Telangana'
				 when instr("��λ����",'Karn��taka')>0 then 'Karn��taka'
				 when instr("��λ����",'Tamil')>0 then 'Tamil'
				 when instr("��λ����",'Odisha')>0 then 'Odisha'
				 when instr("��λ����",'���Ļ���������')>0 then '����'
				 when instr("��λ����",'���ɹ�������')>0 then '���ɹ�'
				 when instr("��λ����",'�½�')>0 then '�½�'
				 when instr("��λ����",'����������')>0 then '����' else "��λ����" END as ʡ,
			case when instr("��λ����",'ʡ')>0 then 
				(case when instr("��λ����",'ʡϽ��')>0 then substr( "��λ����",instr("��λ����",'ʡϽ��')+3)
					  when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'ʡ')+1,instr("��λ����",'������')-instr("��λ����",'ʡ')+2)
					  else substr( "��λ����",instr("��λ����",'ʡ')+1,instr("��λ����",'��')-instr("��λ����",'ʡ')-1) END)
				 when instr("��λ����",'����׳��������')>0 then
				(case when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'������')-instr("��λ����",'������')+2)
					  else substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'��')-instr("��λ����",'������'))END)
				 when instr("��λ����",'�Ϻ�')>0 then '�Ϻ�'
				 when instr("��λ����",'����')>0 then '����'
				 when instr("��λ����",'����')>0 then '����'
				 when instr("��λ����",'���')>0 then '���'
				 when instr("��λ����",'����ر�������')>0 then '���'
				 when instr("��λ����",'�����ر�������')>0 then '����'
				 when instr("��λ����",'Telangana')>0 then 'Telangana'
				 when instr("��λ����",'Karn��taka')>0 then 'Karn��taka'
				 when instr("��λ����",'Tamil')>0 then 'Tamil'
				 when instr("��λ����",'Odisha')>0 then 'Odisha'
				 when instr("��λ����",'���Ļ���������')>0 then
				(case when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'������')-instr("��λ����",'������')+2)
					  else substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'��')-instr("��λ����",'������')) END)
				 when instr("��λ����",'���ɹ�������')>0 then
				(case when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'������')-instr("��λ����",'������')+2)
					  else substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'��')-instr("��λ����",'������')) END)
				 when instr("��λ����",'�½�ά���������')>0 then 
				(case when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'������')-instr("��λ����",'������')+2)
					  else substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'��')-instr("��λ����",'������')) END)
				 when instr("��λ����",'����������')>0 then
				(case when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'������')-instr("��λ����",'������')+2)
					  else substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'��')-instr("��λ����",'������')) END)
				 else "��λ����" END as ��,
			case when instr("��λ����",'ʡϽ��')>0 then substr( "��λ����",instr("��λ����",'ʡϽ��')+3) 
				 when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3)
				 when instr("��λ����",'��')>0 then substr( "��λ����",instr("��λ����",'��')+1)
				 else "��λ����" END as ����,
			"��λ����",
			to_date(substr(to_char("����ʱ��",'YYYYMMDD'),1,6)||'01','YYYY-MM-DD') as ����ʱ��,
			substr(to_char("����ʱ��",'YYYYMMDD'),1,4)||'W'||to_char("����ʱ��",'ww') as ��
		FROM
			dw_hnzw_wjgs
		group by 
			"����",
			case when instr("����",'C')>0 then substr("����",1,instr("����",'C')-1)
				 when instr("����",'H')>0 then substr("����",1,instr("����",'H')-1)
				 when instr("����",'W')>0 then substr("����",1,instr("����",'W')-1)
				 when "����"='SY485S1I3K' then 'SY485'
				 when instr("����",'U')>0 then substr("����",1,instr("����",'U')-1) else "����" END,
			case when instr("��λ����",'ʡ')>0 then substr( "��λ����",1,instr("��λ����",'ʡ')-1)
				 when instr("��λ����",'����׳��������')>0 then '����'
				 when instr("��λ����",'�Ϻ�')>0 then '�Ϻ�'
				 when instr("��λ����",'����')>0 then '����'
				 when instr("��λ����",'����')>0 then '����'
				 when instr("��λ����",'���')>0 then '���'
				 when instr("��λ����",'����ر�������')>0 then '���'
				 when instr("��λ����",'�����ر�������')>0 then '����'
				 when instr("��λ����",'Telangana')>0 then 'Telangana'
				 when instr("��λ����",'Karn��taka')>0 then 'Karn��taka'
				 when instr("��λ����",'Tamil')>0 then 'Tamil'
				 when instr("��λ����",'Odisha')>0 then 'Odisha'
				 when instr("��λ����",'���Ļ���������')>0 then '����'
				 when instr("��λ����",'���ɹ�������')>0 then '���ɹ�'
				 when instr("��λ����",'�½�')>0 then '�½�'
				 when instr("��λ����",'����������')>0 then '����' else "��λ����" END,
			case when instr("��λ����",'ʡ')>0 then 
				(case when instr("��λ����",'ʡϽ��')>0 then substr( "��λ����",instr("��λ����",'ʡϽ��')+3)
					  when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'ʡ')+1,instr("��λ����",'������')-instr("��λ����",'ʡ')+2)
					  else substr( "��λ����",instr("��λ����",'ʡ')+1,instr("��λ����",'��')-instr("��λ����",'ʡ')-1) END)
				 when instr("��λ����",'����׳��������')>0 then
				(case when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'������')-instr("��λ����",'������')+2)
					  else substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'��')-instr("��λ����",'������'))END)
				 when instr("��λ����",'�Ϻ�')>0 then '�Ϻ�'
				 when instr("��λ����",'����')>0 then '����'
				 when instr("��λ����",'����')>0 then '����'
				 when instr("��λ����",'���')>0 then '���'
				 when instr("��λ����",'����ر�������')>0 then '���'
				 when instr("��λ����",'�����ر�������')>0 then '����'
				 when instr("��λ����",'Telangana')>0 then 'Telangana'
				 when instr("��λ����",'Karn��taka')>0 then 'Karn��taka'
				 when instr("��λ����",'Tamil')>0 then 'Tamil'
				 when instr("��λ����",'Odisha')>0 then 'Odisha'
				 when instr("��λ����",'���Ļ���������')>0 then
				(case when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'������')-instr("��λ����",'������')+2)
					  else substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'��')-instr("��λ����",'������')) END)
				 when instr("��λ����",'���ɹ�������')>0 then
				(case when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'������')-instr("��λ����",'������')+2)
					  else substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'��')-instr("��λ����",'������')) END)
				 when instr("��λ����",'�½�ά���������')>0 then 
				(case when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'������')-instr("��λ����",'������')+2)
					  else substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'��')-instr("��λ����",'������')) END)
				 when instr("��λ����",'����������')>0 then
				(case when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'������')-instr("��λ����",'������')+2)
					  else substr( "��λ����",instr("��λ����",'������')+3,instr("��λ����",'��')-instr("��λ����",'������')) END)
				 else "��λ����" END,
			case when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3)
				 when instr("��λ����",'��')>0 then substr( "��λ����",instr("��λ����",'��')+1)
				 else "��λ����" END,
			case when instr("��λ����",'ʡϽ��')>0 then substr( "��λ����",instr("��λ����",'ʡϽ��')+3) 
				 when instr("��λ����",'������')>0 then substr( "��λ����",instr("��λ����",'������')+3)
				 when instr("��λ����",'��')>0 then substr( "��λ����",instr("��λ����",'��')+1)
				 else "��λ����" END,
			"��λ����",
			to_date(substr(to_char("����ʱ��",'YYYYMMDD'),1,6)||'01','YYYY-MM-DD'),
			substr(to_char("����ʱ��",'YYYYMMDD'),1,4)||'W'||to_char("����ʱ��",'ww');

		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_HNZW_GSQX',SYSDATE,'DM_S_HNZW_GSQX���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_HNZW_GSQX',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----��ʱ����,ֻ��ȡ���Ϻͽ�������
	----dm_s_hnzw_gsqx01
	BEGIN
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_HNZW_GSQX01',SYSDATE,'DM_S_HNZW_GSQX01���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DM_S_HNZW_GSQX01
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DM_S_HNZW_GSQX01' ;
		COMMIT;

		INSERT INTO
			DM_S_HNZW_GSQX01 
		SELECT
			a."����",
			a."����",
			case when a."����" in('SY55','SY60','SY70','SY75','SY85','SY95','SY125','SY135','SY155','SY65','SY115','SY150','SY35') then 'С��'
				 when a."����" in('SY195','SY205','SY215','SY225','SY245','SY265','SY285','SY305','SY230','SY240','SY235','SY330','SY335') then '����'
				 else '����' END as ����,
			a."�ܹ�ʱ",
			case when b."�ܹ�ʱ"<=4000 then '4000Сʱ����'
				 when b."�ܹ�ʱ">4000 and b."�ܹ�ʱ"<=7000 then '4000~7000Сʱ'
				 when b."�ܹ�ʱ">7000 and b."�ܹ�ʱ"<=10000 then '7000~10000Сʱ'
				 when b."�ܹ�ʱ">10000 and b."�ܹ�ʱ"<=13000 then '10000~13000Сʱ'
				 when b."�ܹ�ʱ">13000 and b."�ܹ�ʱ"<=15000 then '13000~15000Сʱ'
				 when b."�ܹ�ʱ">15000 and b."�ܹ�ʱ"<=20000 then '15000~20000Сʱ'
				 when b."�ܹ�ʱ">20000 then '20000Сʱ����' END as ��ʱ�ֶ�,
			a."��ʱ",
			a."ʡ",
			a."��",
			a."����",
			a."����ʱ��",a.��
			FROM
				dm_s_hnzw_gsqx a,
				dm_hnzw_zgs b
			WHERE
				a."����"=b."����"(+) and a."ʡ" in ('����','����') and a."�ܹ�ʱ">10; -- ���˵������������
		COMMIT;

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DM_S_HNZW_GSQX01',SYSDATE,'DM_S_HNZW_GSQX01���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DM_S_HNZW_GSQX01',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----һ��һ��
	----DW_EXC_LEDGER
	BEGIN
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DW_EXC_LEDGER',SYSDATE,'DW_EXC_LEDGER���ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��DM��DW_EXC_LEDGER
		EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_EXC_LEDGER' ;
		COMMIT;

		INSERT INTO
			DW_EXC_LEDGER
		SELECT
			a."����",
			case when instr("�豸�ͺ�",'C')>0 then substr("�豸�ͺ�",1,instr("�豸�ͺ�",'C')-1)
				 when instr("�豸�ͺ�",'H')>0 then substr("�豸�ͺ�",1,instr("�豸�ͺ�",'H')-1)
				 when instr("�豸�ͺ�",'U')>0 then substr("�豸�ͺ�",1,instr("�豸�ͺ�",'U')-1) else "�豸�ͺ�" END as "�豸�ͺ�",
			"ʡ��",
			"����",
			"�豸����",
			"��λ�ص�",
			"����¼ʱ��",
			case when b."�ܹ�ʱ"<=4000 then '4000Сʱ����'
				 when b."�ܹ�ʱ">4000 and b."�ܹ�ʱ"<=7000 then '4000~7000Сʱ'
				 when b."�ܹ�ʱ">7000 and b."�ܹ�ʱ"<=10000 then '7000~10000Сʱ'
				 when b."�ܹ�ʱ">10000 and b."�ܹ�ʱ"<=13000 then '10000~13000Сʱ'
				 when b."�ܹ�ʱ">13000 and b."�ܹ�ʱ"<=15000 then '13000~15000Сʱ'
				 when b."�ܹ�ʱ">15000 and b."�ܹ�ʱ"<=20000 then '15000~20000Сʱ'
				 when b."�ܹ�ʱ">20000 then '20000Сʱ����' END as ��ʱ�ֶ�,
			b."�ܹ�ʱ",
			b."���¹�ʱ",
			b."���깤ʱ",
			"�ͻ���",
			"ʵ�ʹ�����",
			"��ϵ�绰",
			"��ͬ��λ",
			"���۷�ʽ",
			"��֤��",
			"ԭӪ������",
			"��Ӫ������",
			"��������",
			"�ſ�����",
			"�ͻ�����",
			"����������",
			"�Ƿ�Ϊ��ֵ����",
			"����ר��",
			"��תʱ��",
			"��ע",
			nvl("���а��ҿ�������",0) as "���а��ҿ�������",
			nvl("���а��ҿ�µ��ڿ�",0) as "���а��ҿ�µ��ڿ�",
			nvl("�������ڿ�",0) as "�������ڿ�",
			nvl("�渶��",0) as "�渶��",
			nvl("��˾����������",0) as "��˾����������",
			nvl("��˾����µ��ڿ�",0) as "��˾����µ��ڿ�",
			nvl("���ڽ��",0) as "���ڽ��",
			nvl("�ɻ��ֿ�",0) as "�ɻ��ֿ�",
			nvl("����",0) as "����",
			"����˵��",
			nvl("�ܵ��ڿ�",0) as "�ܵ��ڿ�",
			nvl("�����ڿ�",0) as "�����ڿ�",
			nvl("���ڷ�Ϣ",0) as "���ڷ�Ϣ",
			nvl("���ڿ��ܼ�",0) as "���ڿ��ܼ�",
			nvl("�ͻ���",0) as "�ͻ���",
			nvl("�ؿ�ͻ���",0) as "�ؿ�ͻ���",
			nvl("����������",0) as "����������",
			nvl("�ɻ��ֿ�2",0) as "�ɻ��ֿ�2",
			nvl("�����ֿ�",0) as "�����ֿ�",
			nvl("��12�����ۼƻ���",0) as "��12�����ۼƻ���",
			nvl("��6�����ۼƻ���",0) as "��6�����ۼƻ���",
			nvl("��3�����ۼƻ���",0) as "��3�����ۼƻ���",
			nvl("�渶",0) as "�渶",
			nvl("��ͬ��",0) as "��ͬ��",
			nvl("�ۿ۽��",0) as "�ۿ۽��",
			nvl("�ۺ��",0) as "�ۺ��",
			nvl("�������",0) as "�������",
			nvl("δ����",0) as "δ����"
		FROM
			ods_exc_ledger a,
			dm_hnzw_zgs b
		WHERE
			a."����"=b."����"(+);
		COMMIT;

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('DW_EXC_LEDGER',SYSDATE,'DW_EXC_LEDGER1���ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('DW_EXC_LEDGER',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----"����ծȨ��һ��һ��"
	----"����ծȨ��һ��һ��"
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('����ծȨ��һ��һ��',SYSDATE,'����ծȨ��һ��һ�����ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��"����ծȨ��һ��һ��"
		EXECUTE IMMEDIATE 'TRUNCATE TABLE "����ծȨ��һ��һ��"' ;
		COMMIT;

		INSERT INTO "����ծȨ��һ��һ��"
			("����",
			"�豸�ͺ�",
			"ʡ��",
			"����",
			"��λ�ص�",
			"�ͻ���",
			"���۷�ʽ",
			"��֤��",
			"ԭӪ������",
			"��Ӫ������",
			"��������",
			"�ͻ�����",
			"�����ڿ�",
			"����������",
			"�ܵ��ڿ�",
			"�����ڿ�2",
			"���ڷ�Ϣ")
		SELECT 
			"����",
			"�豸�ͺ�",
			"ʡ��",
			"����",
			"��λ�ص�",
			"�ͻ���",
			"���۷�ʽ",
			"��֤��",
			"ԭӪ������",
			"��Ӫ������",
			"��������",
			"�ͻ�����",
			"�����ڿ�",
			"����������",
			"�ܵ��ڿ�",
			���������� as "�����ڿ�2",
			"���ڷ�Ϣ"
		FROM
			ods_exc_ledger;
		COMMIT;

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('����ծȨ��һ��һ��',SYSDATE,'����ծȨ��һ��һ�����ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('����ծȨ��һ��һ��',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;






	----"Ӧ����"
	BEGIN

		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('Ӧ����',SYSDATE,'Ӧ�������ݸ��¿�ʼ','��־��¼');
		COMMIT;

		--------ɾ�������Ӧ��"Ӧ����"
		EXECUTE IMMEDIATE 'TRUNCATE TABLE "Ӧ����"' ;
		COMMIT;
		INSERT INTO "Ӧ����"
			("�ɽ���ʽ",
			"״̬",
			"����",
			"�ͻ�����",
			"�ͻ�����",
			"Ӫ��������",
			"Ӫ����������",
			"�ͺ�",
			"��������",
			"����ƻ��к�",
			"������Ŀ",
			"Ӧ����ʱ��",
			"Ӧ�����",
			"ʵ�����",
			"ʵ�ʻ���ʱ��",
			"��󻹿�ʱ��")
		SELECT 
			"�ɽ���ʽ",
			"״̬",
			"����",
			"�ͻ�����",
			"�ͻ�����",
			"Ӫ��������",
			"Ӫ����������",
			"�ͺ�",
			"��������",
			"����ƻ��к�",
			"������Ŀ",
			"Ӧ����ʱ��",
			"Ӧ�����",
			"ʵ�����",
			"ʵ�ʻ���ʱ��",
			"��󻹿�ʱ��"
		FROM
			ods_acc_rec;
		COMMIT;
		--------��¼���������־
		INSERT INTO ETL_LOG(TABLE_NAME,CURRENT_DATE,LOG_MSG,LOG_TYPE)
			values('Ӧ����',SYSDATE,'Ӧ�������ݸ������','��־��¼');
		COMMIT;

	EXCEPTION 
		WHEN OTHERS THEN
			-------��¼���������־
			prc_wlf_sys_writelog('Ӧ����',SYSDATE,
				'����ϵͳ���� �� ������� ' || SQLCODE( ) || '   ������Ϣ��' ||SQLERRM( ) ,'������־��¼');
	END;

END SP_HNZW_S_LOAD_ADW;