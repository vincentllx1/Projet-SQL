USE master;
GO

DROP DATABASE IF EXISTS TIA;
GO

CREATE DATABASE TIA;
GO

USE TIA
GO

--Fact table
DROP TABLE IF EXISTS fact_WorkOrder;
GO

--Dim tables
DROP TABLE IF EXISTS dim_ProductCost;
GO

DROP TABLE IF EXISTS dim_Date;
GO

--Staging tables
DROP TABLE IF EXISTS stage_WorkOrder;
GO

DROP TABLE IF EXISTS stage_ProductCost;
GO

--Config table
DROP TABLE IF EXISTS etl_config;
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE NAME = 'ETL_CONFIG')
BEGIN
CREATE TABLE ETL_CONFIG(
	Table_Name              varchar(50)         NOT NULL CONSTRAINT PK_etl_config1 PRIMARY KEY, 
    Last_Load_Date          datetime            NOT NULL CONSTRAINT DF_etl_config_load_timestamp DEFAULT(0)
)
END
GO

INSERT INTO ETL_CONFIG(Table_Name)
	VALUES
		('work_order')
		,('product_cost')
        ,('date');
GO

DROP TABLE IF EXISTS DIM_DATE
CREATE TABLE DIM_DATE (
[DATE_ID]       INT             CONSTRAINT PK_dim_Date PRIMARY KEY CLUSTERED(Date_ID),
DATEVALUE       DATE            NOT NULL,
Jour            SMALLINT        NOT NULL,
Nom_jour        VARCHAR(25)     NOT NULL,
Mois            SMALLINT        NOT NULL,
Nom_mois        VARCHAR(25)     NOT NULL,
Annee           INT             NOT NULL
)


DROP TABLE IF EXISTS STAGE_DIM_PRODUCT;
CREATE TABLE STAGE_DIM_PRODUCT  (
    Numero_Produit  VARCHAR(50)     NOT NULL,
    Nom_Produit     VARCHAR(100)    NULL,
    Nom_SousCat     VARCHAR(50)     NULL,
    Nom_Cat         VARCHAR(50)     NULL,
    CostStartDate   DATE            NOT NULL,
    CostEndDate     DATE            NULL,
    StandardCost    MONEY           NULL,
    ModifiedDate    DATE            NULL,
    CONSTRAINT PK_stage_ProductCost PRIMARY KEY CLUSTERED(Numero_Produit, CostStartDate)
);

DROP TABLE IF EXISTS DIM_PRODUCT;
CREATE TABLE DIM_PRODUCT  (
    Produit_Id      INT             IDENTITY (1,1) CONSTRAINT PK_dim_Product PRIMARY KEY,        
    Numero_Produit  VARCHAR(50)     NOT NULL,
    Nom_Produit     VARCHAR(100)    NOT NULL,
    Nom_SousCat     VARCHAR(50)     NOT NULL DEFAULT 'NONE',
    Nom_Cat         VARCHAR(50)     NOT NULL DEFAULT 'NONE',
    CostStartDate   DATE            NOT NULL,
    CostEndDate     DATE            NOT NULL DEFAULT (CONVERT(datetime, '9999-31-12', 105)),
    StandardCost    MONEY           NOT NULL,
);


DROP TABLE IF EXISTS STAGE_FACT_BASE;
CREATE TABLE STAGE_FACT_BASE(
WorkorderID         VARCHAR(25),
--ProductID           INT,
ProductNumber       VARCHAR(50),
StepNumber          INT,
StepName            VARCHAR(50),
StartDate           DATE,
EndDate             DATE,
PlannedCost         MONEY,
ActualCost          MONEY,
ModifiedDate        DATE
)

DROP TABLE IF EXISTS FACT_WORKORDER;
CREATE TABLE FACT_WORKORDER(
RefNumber                   VARCHAR(25)         NOT NULL CONSTRAINT PK_FACT_WORKORDER PRIMARY KEY,
Product_ID                  INT                 NOT NULL CONSTRAINT FK_FACT_WORKORDER REFERENCES DIM_PRODUCT(Produit_Id),

Step1_Name                  VARCHAR(55)         NOT NULL,
Step1_StarDate              DATETIME            NOT NULL,
Step1_EndDate               DATETIME            NOT NULL,
Step1_Start_Date_ID         INT                 NOT NULL CONSTRAINT FK_fact_WorkOrder_Step1_StartDate REFERENCES DIM_DATE(DATE_ID),
Step1_End_Date_ID           INT                 NOT NULL CONSTRAINT FK_fact_WorkOrder_Step1_EndDate REFERENCES DIM_DATE(DATE_ID),
Step1_Lag                   SMALLINT            NOT NULL,
Step1_Plannedcost           MONEY               NOT NULL,
Step1_Actualcost            MONEY               NOT NULL,

Step2_Name                  VARCHAR(55)         NULL,
Step2_StarDate              DATETIME            NULL,
Step2_EndDate               DATETIME            NULL,
Step2_Start_Date_ID         INT                 NULL CONSTRAINT FK_fact_WorkOrder_Step2_StartDate REFERENCES DIM_DATE(DATE_ID),
Step2_End_Date_ID           INT                 NULL CONSTRAINT FK_fact_WorkOrder_Step2_EndDate REFERENCES DIM_DATE(DATE_ID),
Step2_Lag                   SMALLINT            NULL,
Step2_Plannedcost           MONEY               NULL,
Step2_Actualcost            MONEY               NULL,

Step3_Name                  VARCHAR(55)         NULL,
Step3_StarDate              DATETIME            NULL,
Step3_EndDate               DATETIME            NULL,
Step3_Start_Date_ID         INT                 NULL CONSTRAINT FK_fact_WorkOrder_Step3_StartDate REFERENCES DIM_DATE(DATE_ID),
Step3_End_Date_ID           INT                 NULL CONSTRAINT FK_fact_WorkOrder_Step3_EndDate REFERENCES DIM_DATE(DATE_ID),
Step3_Lag                   SMALLINT            NULL,
Step3_Plannedcost           MONEY               NULL,
Step3_Actualcost            MONEY               NULL,

Step4_Name                  VARCHAR(55)         NULL,
Step4_StarDate              DATETIME            NULL,
Step4_EndDate               DATETIME            NULL,
Step4_Start_Date_ID         INT                 NULL CONSTRAINT FK_fact_WorkOrder_Step4_StartDate REFERENCES DIM_DATE(DATE_ID),
Step4_End_Date_ID           INT                 NULL CONSTRAINT FK_fact_WorkOrder_Step4_EndDate REFERENCES DIM_DATE(DATE_ID),
Step4_Lag                   SMALLINT            NULL,
Step4_Plannedcost           MONEY               NULL,
Step4_Actualcost            MONEY               NULL,

Step5_Name                  VARCHAR(55)         NULL,
Step5_StarDate              DATETIME            NULL,
Step5_EndDate               DATETIME            NULL,
Step5_Start_Date_ID         INT                 NULL CONSTRAINT FK_fact_WorkOrder_Step5_StartDate REFERENCES DIM_DATE(DATE_ID),
Step5_End_Date_ID           INT                 NULL CONSTRAINT FK_fact_WorkOrder_Step5_EndDate REFERENCES DIM_DATE(DATE_ID),
Step5_Lag                   SMALLINT            NULL,
Step5_Plannedcost           MONEY               NULL,
Step5_Actualcost            MONEY               NULL,

Step6_Name                  VARCHAR(55)         NULL,
Step6_StarDate              DATETIME            NULL,
Step6_EndDate               DATETIME            NULL,
Step6_Start_Date_ID         INT                 NULL CONSTRAINT FK_fact_WorkOrder_Step6_StartDate REFERENCES DIM_DATE(DATE_ID),
Step6_End_Date_ID           INT                 NULL CONSTRAINT FK_fact_WorkOrder_Step6_EndDate REFERENCES DIM_DATE(DATE_ID),
Step6_Lag                   SMALLINT            NULL,
Step6_Plannedcost           MONEY               NULL,
Step6_Actualcost            MONEY               NULL,

Total_lag                   INT                 NOT NULL,
MaxsequenceNumber           TINYINT             NOT NULL
)


/************************************************************
************************************************************/
-- date dimension

IF EXISTS (SELECT * FROM sys.procedures WHERE NAME = 'sp_local_date') 
BEGIN 
    DROP PROCEDURE sp_local_date
END
GO

CREATE PROCEDURE sp_local_date 
AS
IF EXISTS (SELECT * FROM sys.tables WHERE NAME = 'DIM_DATE')
BEGIN

    SET DATEFIRST 1;

    WITH myCTE AS (
        SELECT CONVERT(DATETIME,'2010-01-01') as Datetoadd
        UNION ALL
        SELECT DATEADD(DAY,1,Datetoadd) as Datetoadd
        FROM myCTE
        WHERE Datetoadd<CONVERT(DATETIME,'2021-01-01')
    )

    INSERT INTO [DIM_DATE] ([DATE_ID],DATEVALUE,Jour,Nom_jour,Mois,Nom_mois,Annee)
    SELECT 
        FORMAT(Datetoadd,'yyyyMMdd'), 
        Datetoadd,
        DAY(Datetoadd), 
        DATENAME(weekday,Datetoadd),
        MONTH(Datetoadd),
        DATENAME(month,Datetoadd),
        YEAR(Datetoadd)
    FROM myCTE
    OPTION (MAXRECURSION 10000)

    UPDATE ETL_CONFIG
        SET Last_Load_Date=GETDATE()
        FROM ETL_CONFIG
        WHERE ETL_CONFIG.Table_Name='date'

    END
ELSE IF NOT EXISTS (SELECT * FROM sys.tables WHERE NAME = 'DIM_DATE')
BEGIN

    SET DATEFIRST 1;

    WITH myCTE AS (
        SELECT CONVERT(DATETIME,'2010-01-01') as Datetoadd
        UNION ALL
        SELECT DATEADD(DAY,1,Datetoadd) as Datetoadd
        FROM myCTE
        WHERE Datetoadd<CONVERT(DATETIME,'2021-01-01')
    )

    INSERT INTO [DIM_DATE] ([DATE_ID],DATEVALUE,Jour,Nom_jour,Mois,Nom_mois,Annee)
    SELECT 
        FORMAT(Datetoadd,'yyyyMMdd'), 
        Datetoadd,
        DAY(Datetoadd), 
        DATENAME(weekday,Datetoadd),
        MONTH(Datetoadd),
        DATENAME(month,Datetoadd),
        YEAR(Datetoadd)
    FROM myCTE
    OPTION (MAXRECURSION 10000)

    UPDATE ETL_CONFIG
        SET Last_Load_Date=GETDATE()
        FROM ETL_CONFIG
        WHERE ETL_CONFIG.Table_Name='date'
    END
RETURN 0;
GO

--EXEC sp_local_date

/************************************************************
************************************************************/
-- product dimension



IF EXISTS (SELECT * FROM sys.procedures WHERE NAME = 'sp_local_extract_dim_prod') 
BEGIN 
    DROP PROCEDURE sp_local_extract_dim_prod
END
GO

CREATE PROCEDURE sp_local_extract_dim_prod
AS
BEGIN
    INSERT INTO STAGE_DIM_PRODUCT (Numero_Produit,Nom_Produit,Nom_SousCat,Nom_Cat,CostStartDate,CostEndDate,StandardCost,ModifiedDate)
        SELECT
            p.ProductNumber, 
            p.Name, 
            psc.Name, 
            pc.Name, 
            ISNULL(pch.StartDate,0),
            pch.EndDate, 
            ISNULL(pch.StandardCost,P.StandardCost),
            ISNULL(pch.ModifiedDate,0)
        FROM AdventureWorks2019.Production.Product p
        LEFT JOIN AdventureWorks2019.Production.ProductCostHistory pch   ON p.ProductID = pch.ProductID
        LEFT JOIN AdventureWorks2019.Production.ProductSubCategory psc   ON p.ProductSubcategoryID = psc.ProductSubcategoryID
        LEFT JOIN AdventureWorks2019.Production.ProductCategory pc       ON psc.ProductCategoryID = pc.ProductCategoryID
        WHERE ISNULL(pch.ModifiedDate,1)>(SELECT Last_Load_Date FROM ETL_CONFIG WHERE Table_Name = 'product_cost')
    
    -- INSERT INTO ETL_CONFIG (Table_Name,Last_Load_Date) VALUES ('DIM_PRODUCT',(
    --     SELECT MAX(ModifiedDate) FROM AdventureWorks2019.Production.ProductCostHistory))

    UPDATE ETL_CONFIG
    SET
    Last_Load_Date=GETDATE() WHERE Table_Name='product cost';

    END
RETURN 0;
GO




IF EXISTS (SELECT * FROM sys.procedures WHERE NAME = 'sp_local_load_dim_prod') 
BEGIN 
    DROP PROCEDURE sp_local_load_dim_prod
END
GO

CREATE PROCEDURE sp_local_load_dim_prod
AS
IF NOT EXISTS (SELECT * FROM ETL_CONFIG WHERE Table_name = 'DIM_PRODUCT')
BEGIN

    INSERT INTO DIM_PRODUCT (Numero_Produit,Nom_Produit,Nom_SousCat,Nom_Cat,CostStartDate,CostEndDate,StandardCost)
        SELECT
        Numero_Produit,
        Nom_Produit,
        ISNULL(Nom_SousCat,'None'),
        ISNULL(Nom_Cat,'None'),
        CostStartDate,
        ISNULL(CostEndDate,'9999-01-01'),
        StandardCost
        FROM STAGE_DIM_PRODUCT
        WHERE STAGE_DIM_PRODUCT.Numero_Produit NOT IN (
            SELECT DIM_PRODUCT.Numero_Produit 
            FROM DIM_PRODUCT
            )
            OR STAGE_DIM_PRODUCT.CostEndDate IS NULL

    UPDATE DIM
    SET DIM.CostEndDate = STAGE.CostEndDate
    FROM DIM_PRODUCT DIM
    INNER JOIN STAGE_DIM_PRODUCT STAGE ON DIM.Numero_Produit=STAGE.Numero_Produit AND DIM.CostStartDate=STAGE.CostStartDate AND DIM.CostEndDate<>STAGE.CostEndDate

    DELETE STAGE_DIM_PRODUCT;
END
GO

    -- INSERT INTO ETL_CONFIG VALUES ('DIM_PRODUCT',(
    --     SELECT MAX(ModifiedDate) FROM STAGE_DIM_PRODUCT
    -- ))


IF EXISTS (SELECT * FROM sys.procedures WHERE NAME = 'sp_local_extract_fact') 
BEGIN 
    DROP PROCEDURE sp_local_extract_fact
END
GO

CREATE PROCEDURE sp_local_extract_fact
AS
BEGIN
    INSERT INTO STAGE_FACT_BASE(WorkorderID,/*ProductID,*/ProductNumber,StepNumber,StepName,StartDate,EndDate,PlannedCost,ActualCost,ModifiedDate)
    SELECT
        wor.WorkorderID,
        --wor.ProductID,
        p.ProductNumber,
        wor.OperationSequence,
        l.Name, 
        wor.ActualStartDate, 
        wor.ActualEndDate, 
        wor.PlannedCost, 
        wor.ActualCost,
        wor.ModifiedDate
    FROM AdventureWorks2019.Production.WorkOrderRouting wor
    INNER JOIN AdventureWorks2019.Production.WorkOrder WO           ON WOR.WorkOrderID = WO.WorkOrderID
    INNER JOIN AdventureWorks2019.Production.Product p              ON wo.ProductID=p.ProductID
    INNER JOIN AdventureWorks2019.Production.[Location] l           ON wor.LocationID=l.LocationID
    WHERE wor.ModifiedDate > (SELECT Last_Load_Date FROM ETL_CONFIG WHERE Table_Name='work_order')

    UPDATE ETL_CONFIG SET Last_Load_Date=GETDATE() WHERE Table_Name='work_order'
    END

RETURN 0;
GO



IF EXISTS (SELECT * FROM sys.procedures WHERE NAME = 'sp_local_load_fact') 
BEGIN 
    DROP PROCEDURE sp_local_load_fact
END
GO

CREATE PROCEDURE sp_local_load_fact
AS
BEGIN

		DECLARE @temp_table			
			TABLE(
					RefNumber           VARCHAR(50)     NOT NULL,
                    ProductNumber       VARCHAR(50)     NOT NULL,
                    StepNumber          INT             NOT NULL,
                    StepName            VARCHAR(50)     NOT NULL,
                    StartDate           DATE            NOT NULL,
                    EndDate             DATE            NOT NULL,
                    PlannedCost         MONEY           NOT NULL,
                    ActualCost          MONEY           NOT NULL
                    );

        WITH MyCTE (WorkOrderID,ProductNumber, SeqNumber, LocationName, StartDate, EndDate, PlannedCost, ActualCost)
        AS(
            SELECT
				STAGE.WorkorderID
				, STAGE.ProductNumber
				, ROW_NUMBER() OVER(PARTITION BY STAGE.WorkOrderID ORDER BY STAGE.StartDate ASC) + 
					COALESCE((SELECT MaxSequenceNumber FROM fact_WorkOrder WHERE		
						RefNumber = 'WO-' + LTRIM(STR(STAGE.WorkOrderID))), 0)
				, STAGE.StepName
				, STAGE.StartDate
				, STAGE.EndDate
				, STAGE.PlannedCost
				, STAGE.ActualCost
			FROM STAGE_FACT_BASE STAGE
        )
        INSERT INTO @temp_table (RefNumber,ProductNumber,StepNumber,StepName,StartDate,EndDate,PlannedCost,ActualCost)
        SELECT 
            'WO-' + LTRIM(STR(MyCTE.WorkOrderID)),
            MyCTE.ProductNumber,
            MyCTE.SeqNumber,
            MyCTE.LocationName,
            MyCTE.StartDate,
            MyCTE.EndDate,
            MyCTE.PlannedCost,
            MyCTE.ActualCost
        FROM MyCTE;

        INSERT INTO FACT_WORKORDER (RefNumber,Product_ID,Step1_Name,Step1_StarDate,Step1_EndDate,Step1_Start_Date_ID,Step1_End_Date_ID,Step1_Plannedcost,Step1_Actualcost,Step1_Lag,Total_lag,MaxsequenceNumber)
            SELECT
                TEMP.RefNumber,
                P.Produit_Id,
                TEMP.StepName,
                TEMP.StartDate,
                TEMP.EndDate,
                Ds.Date_Id,
                De.Date_Id,
                TEMP.PlannedCost,
                TEMP.ActualCost,
                DATEDIFF(DAY,TEMP.StartDate,TEMP.EndDate),
                DATEDIFF(DAY,TEMP.StartDate,TEMP.EndDate),
                1
            FROM @temp_table TEMP
            INNER JOIN DIM_PRODUCT P ON TEMP.ProductNumber=P.Numero_Produit AND TEMP.StartDate BETWEEN p.CostStartDate and p.CostEndDate
            INNER JOIN DIM_DATE Ds ON TEMP.StartDate=Ds.DATEVALUE
            INNER JOIN DIM_DATE De ON TEMP.EndDate=De.DATEVALUE
            WHERE TEMP.StepNumber=1;
        
        UPDATE F
        SET
            F.Step2_Name = TEMP.StepName,
            F.Step2_StarDate = TEMP.StartDate,
            F.Step2_EndDate = TEMP.EndDate,
            F.Step2_Start_Date_ID = Ds.Date_Id,
            F.Step2_End_Date_ID = De.Date_Id,
            F.Step2_Plannedcost = TEMP.PlannedCost,
            F.Step2_Actualcost = TEMP.ActualCost,
            F.Step2_Lag = DATEDIFF(DAY,TEMP.StartDate,TEMP.EndDate),
            F.Total_Lag = F.Total_Lag+DATEDIFF(DAY,TEMP.StartDate,TEMP.EndDate),
            F.MaxsequenceNumber = 2
            

        FROM @temp_table TEMP
            INNER JOIN FACT_WORKORDER F ON TEMP.RefNumber=F.RefNumber
            INNER JOIN DIM_DATE Ds ON TEMP.StartDate=Ds.DATEVALUE
            INNER JOIN DIM_DATE De ON TEMP.EndDate=De.DATEVALUE
        WHERE TEMP.StepNumber=2;
        
        UPDATE F
        SET
            F.Step3_Name = TEMP.StepName,
            F.Step3_StarDate = TEMP.StartDate,
            F.Step3_EndDate = TEMP.EndDate,
            F.Step3_Start_Date_ID = Ds.Date_Id,
            F.Step3_End_Date_ID = De.Date_Id,
            F.Step3_Plannedcost = TEMP.PlannedCost,
            F.Step3_Actualcost = TEMP.ActualCost,
            F.Step3_Lag = DATEDIFF(DAY,TEMP.StartDate,TEMP.EndDate),
            F.Total_Lag = F.Total_Lag+DATEDIFF(DAY,TEMP.StartDate,TEMP.EndDate),
            F.MaxsequenceNumber = 3
            

        FROM @temp_table TEMP
            INNER JOIN FACT_WORKORDER F ON TEMP.RefNumber=F.RefNumber
            INNER JOIN DIM_DATE Ds ON TEMP.StartDate=Ds.DATEVALUE
            INNER JOIN DIM_DATE De ON TEMP.EndDate=De.DATEVALUE
        WHERE TEMP.StepNumber=3;
        
        UPDATE F
        SET
            F.Step4_Name = TEMP.StepName,
            F.Step4_StarDate = TEMP.StartDate,
            F.Step4_EndDate = TEMP.EndDate,
            F.Step4_Start_Date_ID = Ds.Date_Id,
            F.Step4_End_Date_ID = De.Date_Id,
            F.Step4_Plannedcost = TEMP.PlannedCost,
            F.Step4_Actualcost = TEMP.ActualCost,
            F.Step4_Lag = DATEDIFF(DAY,TEMP.StartDate,TEMP.EndDate),
            F.Total_Lag = F.Total_Lag+DATEDIFF(DAY,TEMP.StartDate,TEMP.EndDate),
            F.MaxsequenceNumber = 4
            

        FROM @temp_table TEMP
            INNER JOIN FACT_WORKORDER F ON TEMP.RefNumber=F.RefNumber
            INNER JOIN DIM_DATE Ds ON TEMP.StartDate=Ds.DATEVALUE
            INNER JOIN DIM_DATE De ON TEMP.EndDate=De.DATEVALUE
        WHERE TEMP.StepNumber=4;
        
        UPDATE F
        SET
            F.Step5_Name = TEMP.StepName,
            F.Step5_StarDate = TEMP.StartDate,
            F.Step5_EndDate = TEMP.EndDate,
            F.Step5_Start_Date_ID = Ds.Date_Id,
            F.Step5_End_Date_ID = De.Date_Id,
            F.Step5_Plannedcost = TEMP.PlannedCost,
            F.Step5_Actualcost = TEMP.ActualCost,
            F.Step5_Lag = DATEDIFF(DAY,TEMP.StartDate,TEMP.EndDate),
            F.Total_Lag = F.Total_Lag+DATEDIFF(DAY,TEMP.StartDate,TEMP.EndDate),
            F.MaxsequenceNumber = 5
            

        FROM @temp_table TEMP
            INNER JOIN FACT_WORKORDER F ON TEMP.RefNumber=F.RefNumber
            INNER JOIN DIM_DATE Ds ON TEMP.StartDate=Ds.DATEVALUE
            INNER JOIN DIM_DATE De ON TEMP.EndDate=De.DATEVALUE
        WHERE TEMP.StepNumber=5;
        
        UPDATE F
        SET
            F.Step6_Name = TEMP.StepName,
            F.Step6_StarDate = TEMP.StartDate,
            F.Step6_EndDate = TEMP.EndDate,
            F.Step6_Start_Date_ID = Ds.Date_Id,
            F.Step6_End_Date_ID = De.Date_Id,
            F.Step6_Plannedcost = TEMP.PlannedCost,
            F.Step6_Actualcost = TEMP.ActualCost,
            F.Step6_Lag = DATEDIFF(DAY,TEMP.StartDate,TEMP.EndDate),
            F.Total_Lag = F.Total_Lag+DATEDIFF(DAY,TEMP.StartDate,TEMP.EndDate),
            F.MaxsequenceNumber = 6
            

        FROM @temp_table TEMP
            INNER JOIN FACT_WORKORDER F ON TEMP.RefNumber=F.RefNumber
            INNER JOIN DIM_DATE Ds ON TEMP.StartDate=Ds.DATEVALUE
            INNER JOIN DIM_DATE De ON TEMP.EndDate=De.DATEVALUE
        WHERE TEMP.StepNumber=6;

    DELETE STAGE_FACT_BASE;
    END
RETURN 0
GO

EXEC sp_local_date
EXEC sp_local_extract_dim_prod
EXEC sp_local_load_dim_prod
EXEC sp_local_extract_fact
EXEC sp_local_load_fact

-- SELECT * FROM ETL_CONFIG
-- SELECT * FROM DIM_PRODUCT
-- SELECT * FROM STAGE_DIM_PRODUCT

BEGIN TRAN
	-- faking an update for tomorrow
	DECLARE @now datetime = GETDATE() + 1;

	/* ProductID 707 is 'HL-U509-R' */

	-- product cost history
	UPDATE AdventureWorks2019.Production.ProductCostHistory
		SET EndDate = @now, ModifiedDate = @now WHERE ProductID = 707 AND EndDate IS NULL;

	INSERT INTO AdventureWorks2019.Production.ProductCostHistory
		VALUES(707, @now, NULL, 666.666, @now);

	EXEC sp_local_extract_dim_prod;
	EXEC sp_local_load_dim_prod;
	select * from DIM_PRODUCT WHERE Numero_Produit = 'HL-U509-R';
	

	-- work order
	DECLARE @aStartDate1 date = CONVERT(date, '2014-05-01');
	DECLARE @aStartDate2 date = CONVERT(date, '2014-06-08');
	DECLARE @aEndDate1 date = CONVERT(date, '2014-05-05');
	DECLARE @aEndDate2 date = CONVERT(date, '2014-06-10');
	INSERT INTO AdventureWorks2019.Production.WorkOrderRouting
		VALUES(16000, 811, 10, 10, @aStartDate1, @aEndDate1, @aStartDate1, @aEndDate1, 10, 100, 150, CONVERT(date, GETDATE()+1)),
				(16000, 811, 12, 10, @aStartDate2, @aEndDate2, @aStartDate2, @aEndDate2, 10, 100, 150, CONVERT(date, GETDATE()+2));

    PRINT 'TEST EXTRACT';
	EXEC sp_local_extract_fact;
	select * from FACT_WORKORDER WHERE RefNumber = 'WO-16000'
    PRINT 'TEST LOAD';
	EXEC sp_local_load_fact;
	select * from FACT_WORKORDER WHERE RefNumber = 'WO-16000'
ROLLBACK TRAN