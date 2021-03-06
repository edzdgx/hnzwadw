--数据清洗
SELECT DISTINCT 销售月份SY FROM DW_S_DETAIL;
UPDATE ODS_S_DETAIL SET 销售月份SY = REPLACE(销售月份SY, '月', ''); --把‘月’移除
UPDATE ODS_S_DETAIL SET 销售月份SY = REPLACE(销售月份SY, '0', '') WHERE 销售月份SY<>'10'; --不是10，移除0
UPDATE DW_S_DETAIL SET 销售月份SY = REPLACE(销售月份SY, '月', '');
UPDATE DW_S_DETAIL SET 销售月份SY = REPLACE(销售月份SY, '0', '') WHERE 销售月份SY<>'10'; --不是10，移除0

--INC_S_DETAIL
ALTER TABLE INC_S_DETAIL RENAME COLUMN 地区 TO 所属分公司;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 销售代表 TO 营销代表;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 新老客户 TO 客户类型;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 住址 TO 客户住址;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 交机时间 TO CRM过账交机时间;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 交货地点 TO 交货地址;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 最终用户合同金额 TO 合同金额;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 意向约定首付 TO 终端首付货款;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 合计 TO 费用合计;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 按揭融资贷款 TO 终端贷款金额;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 几成几年 TO 贷款成数;
ALTER TABLE INC_S_DETAIL ADD 贷款期数 varchar2(50 BYTE);
ALTER TABLE INC_S_DETAIL RENAME COLUMN 合同编号 TO 事业部纸质合同号;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 首付不足部分金额 TO 终端首付不足货款;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 不足部分待付方式 TO 终端首付不足货款还款方式;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 结算价 TO 结算金额;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 中大挖 TO 小中大挖;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 折让金额万 TO 销售折让金额;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 折让后价格 TO 折后结算金额;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 是否以旧换新 TO 以旧换新;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 评估价 TO 旧机评估价;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 抵款价 TO 旧机回收价;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 展会 TO 展会日期;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 赠送配件万 TO 赠送配件;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 赠送配件合计金额 TO 赠送金额;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 信息费支付至 TO 信息人;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 联系电话2 TO 信息人电话;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 金额万 TO 信息费金额;
ALTER TABLE INC_S_DETAIL RENAME COLUMN 信息费兑付方式 TO 信息费类型;

--DW_S_DETAIL
ALTER TABLE DW_S_DETAIL RENAME COLUMN 地区 TO 所属分公司;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 销售代表 TO 营销代表;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 新老客户 TO 客户类型;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 住址 TO 客户住址;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 交机时间 TO CRM过账交机时间;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 交货地点 TO 交货地址;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 最终用户合同金额 TO 合同金额;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 意向约定首付 TO 终端首付货款;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 合计 TO 费用合计;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 按揭融资贷款 TO 终端贷款金额;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 几成几年 TO 贷款成数;
ALTER TABLE DW_S_DETAIL ADD 贷款期数 varchar2(50 BYTE);
ALTER TABLE DW_S_DETAIL RENAME COLUMN 合同编号 TO 事业部纸质合同号;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 首付不足部分金额 TO 终端首付不足货款;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 不足部分待付方式 TO 终端首付不足货款还款方式;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 结算价 TO 结算金额;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 中大挖 TO 小中大挖;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 折让金额万 TO 销售折让金额;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 折让后价格 TO 折后结算金额;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 是否以旧换新 TO 以旧换新;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 赠送配件万 TO 赠送配件;
ALTER TABLE DW_S_DETAIL RENAME COLUMN 赠送配件合计金额 TO 赠送金额;

--ODS_S_DETAIL
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 地区 TO 所属分公司;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 销售代表 TO 营销代表;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 新老客户 TO 客户类型;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 住址 TO 客户住址;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 交机时间 TO CRM过账交机时间;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 交货地点 TO 交货地址;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 最终用户合同金额 TO 合同金额;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 意向约定首付 TO 终端首付货款;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 合计 TO 费用合计;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 按揭融资贷款 TO 终端贷款金额;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 几成几年 TO 贷款成数;
ALTER TABLE ODS_S_DETAIL ADD 贷款期数 varchar2(50 BYTE);
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 合同编号 TO 事业部纸质合同号;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 首付不足部分金额 TO 终端首付不足货款;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 不足部分待付方式 TO 终端首付不足货款还款方式;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 结算价 TO 结算金额;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 中大挖 TO 小中大挖;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 折让金额万 TO 销售折让金额;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 折让后价格 TO 折后结算金额;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 是否以旧换新 TO 以旧换新;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 评估价 TO 旧机评估价;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 抵款价 TO 旧机回收价;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 展会 TO 展会日期;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 赠送配件万 TO 赠送配件;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 赠送配件合计金额 TO 赠送金额;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 信息费支付至 TO 信息人;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 联系电话2 TO 信息人电话;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 金额万 TO 信息费金额;
ALTER TABLE ODS_S_DETAIL RENAME COLUMN 信息费兑付方式 TO 信息费类型;
COMMIT;

--INC_MAP_MODEL
ALTER TABLE INC_MAP_MODEL RENAME COLUMN 类型 TO 小中大挖;
--ODS_MAP_MODEL
ALTER TABLE ODS_MAP_MODEL RENAME COLUMN 类型 TO 小中大挖;
COMMIT;

--DM_S_AAOM_SR01
ALTER TABLE DM_S_AAOM_SR01 RENAME COLUMN 分公司 TO 所属分公司;
ALTER TABLE DM_S_AAOM_SR01 RENAME COLUMN 新老客户 TO 客户类型;
ALTER TABLE DM_S_AAOM_SR01 RENAME COLUMN 交机时间 TO CRM过账交机时间;
ALTER TABLE DM_S_AAOM_SR01 RENAME COLUMN 销售代表 TO 营销代表;
ALTER TABLE DM_S_AAOM_SR01 RENAME COLUMN 销售金额 TO 合同金额;
ALTER TABLE DM_S_AAOM_SR01 RENAME COLUMN 赠送配件金额 TO 赠送金额;

--DM_S_AAOM_SR
ALTER TABLE DM_S_AAOM_SR RENAME COLUMN 分公司 TO 所属分公司;
ALTER TABLE DM_S_AAOM_SR RENAME COLUMN 新老客户 TO 客户类型;
ALTER TABLE DM_S_AAOM_SR RENAME COLUMN 交机时间 TO CRM过账交机时间;
ALTER TABLE DM_S_AAOM_SR RENAME COLUMN 销售代表 TO 营销代表;
ALTER TABLE DM_S_AAOM_SR RENAME COLUMN 销售金额 TO 合同金额;
ALTER TABLE DM_S_AAOM_SR RENAME COLUMN 赠送配件金额 TO 赠送金额;
ALTER TABLE DM_S_AAOM_SR RENAME COLUMN 类型 TO 小中大挖;
COMMIT;

--INC_S_PERSON
ALTER TABLE INC_S_PERSON RENAME COLUMN 工作区域 TO 省份;
ALTER TABLE INC_S_PERSON RENAME COLUMN 工作年限 TO 司龄;

--ODS_S_PERSON
ALTER TABLE ODS_S_PERSON RENAME COLUMN 工作区域 TO 省份;
ALTER TABLE ODS_S_PERSON RENAME COLUMN 工作年限 TO 司龄;
COMMIT;

--DW_DEPT_EMP
ALTER TABLE DW_DEPT_EMP RENAME COLUMN 工作年限 TO 司龄;
ALTER TABLE DW_DEPT_EMP RENAME COLUMN 地区 TO 所属分公司;
ALTER TABLE DW_DEPT_EMP ADD 省份 varchar2(50 BYTE);

--添加省份
ALTER TABLE INC_LOSE_NO_COM ADD 省份 varchar2(50 BYTE);
ALTER TABLE ODS_LOSE_NO_COM ADD 省份 varchar2(50 BYTE);
ALTER TABLE DW_CUS_LOSE ADD 省份 varchar2(50 BYTE);
ALTER TABLE INC_LOSE_COM ADD 省份 varchar2(50 BYTE);
ALTER TABLE ODS_LOSE_COM ADD 省份 varchar2(50 BYTE);
ALTER TABLE DM_S_AAOM_LOSE ADD 省份 varchar2(50 BYTE);
ALTER TABLE DM_S_AAOM ADD 省份 varchar2(50 BYTE);
ALTER TABLE DM_S_AAOM_COM ADD 省份 varchar2(50 BYTE);
ALTER TABLE DW_SERVICE_ORDERS ADD 省份 varchar2(50 BYTE);
ALTER TABLE DM_S_AAOM_FUNNEL ADD 省份 varchar2(50 BYTE);

--gzy添加省份
----服务
ALTER TABLE INC_SERVICE_ORDERS ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE 服务订单 ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE 配件_出库 ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE 配件_入库 ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE 车补汇总 ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE 车补每月汇总 ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE 网络满意度_2019年下半年第2轮 ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE INC_SEV_S_DETAIL ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE ODS_SEV_S_DETAIL ADD 省份 VARCHAR2(50 BYTE);
----售前
------商机表
ALTER TABLE INC_BUSINESS_OPP ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE ODS_BUSINESS_OPP ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE DW_BUSINESS_OPP ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE DM_BUSINESS_OPP ADD 省份 VARCHAR2(50 BYTE);
------新增客户报表
ALTER TABLE INC_CUSTOMER_NEW ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE ODS_CUSTOMER_NEW ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE DW_CUSTOMER_NEW ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE DM_CUSTOMER_NEW ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE DM_S_PRESALES ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE DW_S_MILEAGE ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE DM_S_MILEAGE ADD 省份 VARCHAR2(50 BYTE);
----客户面访量
ALTER TABLE INC_CUSTOMER_FACE ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE ODS_CUSTOMER_FACE ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE DW_CUSTOMER_FACE ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE DM_CUSTOMER_FACE ADD 省份 VARCHAR2(50 BYTE);

ALTER TABLE INC_S_PERSON_MILEAGE ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE ODS_S_PERSON_MILEAGE ADD 省份 VARCHAR2(50 BYTE);
ALTER TABLE DW_S_PERSON_MILEAGE ADD 省份 VARCHAR2(50 BYTE);

COMMIT;