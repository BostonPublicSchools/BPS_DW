DECLARE @lineageKey INT;

--inserting into lineage first
--select * from [Lineage]
IF NOT EXISTS(SELECT 1 FROM LongitudinalPOC.[dbo].[Lineage] WHERE TableName= 'dbo.DimeStudent')
BEGIN
    INSERT INTO LongitudinalPOC.[dbo].[Lineage]
	(
	 [TableName], 
	 [StartTime], 
	 [EndTime], 
	 [LoadType], 
	 [Status]
	)
	VALUES
	('dbo.DimStudent', 
	 GETDATE(), 
	 NULL, 
	 'F'
	 , -- full 
	 ''
	); -- Processing
	SET @lineageKey = SCOPE_IDENTITY() ;
END;
ELSE
BEGIN
     SELECT @lineageKey = LineageKey
	 FROM LongitudinalPOC.[dbo].[Lineage]	 
	 WHERE TableName= 'dbo.DimStudent'
END 

;WITH HomelessStudentsByYear AS (
--Sch year 2015:
  SELECT studentno, 2016 AS schyear
  FROM [RMUStudentBackup].[dbo].[Homeless2015Final] 
  WHERE McKinneyVento = 'Y'
  UNION ALL 
--Sch year 2016:
  SELECT studentno, 2017 AS schyear
  FROM [RMUStudentBackup].[dbo].[Homeless2016Final] 
  WHERE McKinneyVento = 'Y'  
  UNION ALL
--Sch year 2017:
  SELECT studentno, 2018 AS schyear
  FROM [RMUStudentBackup].[dbo].[Homeless2017Final] 
  WHERE McKinneyVento = 'Y'  
)
INSERT INTO LongitudinalPOC.[dbo].[DimStudent]
           ([_sourceKey]
           ,[PrimaryElectronicMailAddress]
		   ,[PrimaryElectronicMailTypeDescriptor_CodeValue]
		   ,[PrimaryElectronicMailTypeDescriptor_Description]
           ,[StudentUniqueId]
           ,[StateId]
           ,[SchoolKey]
           ,[ShortNameOfInstitution]
           ,[NameOfInstitution]
           ,[GradeLevelDescriptor_CodeValue]
           ,[GradeLevelDescriptor_Description]
           ,[FirstName]
           ,[MiddleInitial]
           ,[MiddleName]
           ,[LastSurname]
           ,[FullName]
           ,[BirthDate]
           ,[StudentAge]
           ,[GraduationSchoolYear]
           ,[Homeroom]
           ,[HomeroomTeacher]
           ,[SexType_Code]
           ,[SexType_Description]
           ,[SexType_Male_Indicator]
           ,[SexType_Female_Indicator]
           ,[SexType_NotSelected_Indicator]
           ,[RaceCode]
           ,[RaceDescription]
           ,[Race_AmericanIndianAlaskanNative_Indicator]
           ,[Race_Asian_Indicator]
           ,[Race_BlackAfricaAmerican_Indicator]
           ,[Race_NativeHawaiianPacificIslander_Indicator]
           ,[Race_White_Indicator]
           ,[Race_MultiRace_Indicator]
           ,[Race_ChooseNotRespond_Indicator]
           ,[Race_Other_Indicator]
           ,[EthnicityCode]
           ,[EthnicityDescription]
           ,[EthnicityHispanicLatino_Indicator]
           ,[Migrant_Indicator]
           ,[Homeless_Indicator]
           ,[IEP_Indicator]
           ,[LimitedEnglishProficiencyDescriptor_CodeValue]
           ,[LimitedEnglishProficiencyDescriptor_Description]
           ,[LimitedEnglishProficiency_EnglishLearner_Indicator]
           ,[LimitedEnglishProficiency_Former_Indicator]
           ,[LimitedEnglishProficiency_NotEnglisLearner_Indicator]
           ,[EconomicDisadvantage_Indicator]
           ,[EntryDate]
           ,[EntrySchoolYear]
           ,[EntryCode]
           ,[ExitWithdrawDate]
           ,[ExitWithdrawSchoolYear]
           ,[ExitWithdrawCode]
           ,[ValidFrom]
           ,[ValidTo]
           ,[IsCurrent]
           ,[LineageKey])

SELECT 
       CONCAT_WS('|','LegacyDW',Convert(NVARCHAR(MAX),s.StudentNo)) AS [_sourceKey],
	   null AS [PrimaryElectronicMailAddress],
	   null AS [PrimaryElectronicMailTypeDescriptor_CodeValue],
	   null AS [PrimaryElectronicMailTypeDescriptor_Description],

	   s.StudentNo AS [StudentUniqueId],       
       s.sasid AS StateId,
       
	   dschool.SchoolKey,
	   dschool.ShortNameOfInstitution,
	   dschool.NameOfInstitution,
	   s.Grade as GradeLevelDescriptor_CodeValue,
	   s.Grade as GradeLevelDescriptor_Description,

	   s.FirstName,
	   LEFT(LTRIM(s.MiddleName),1) AS MiddleInitial,
	   s.MiddleName,	   
       s.LastName AS LastSurname,
       LongitudinalPOC.dbo.Func_GetFullName(s.FirstName,s.MiddleName,s.LastName) AS FullName,
	   s.DOB AS BirthDate,
       DATEDIFF(YEAR, s.DOB, GetDate()) AS StudentAge,
	   s.YOG AS GraduationSchoolYear,

	   s.Homeroom,
	   NULL AS HomeroomTeacher,

	   CASE 
	        WHEN s.Sex = 'M' THEN 'M'
	        WHEN s.Sex = 'F' THEN 'F'
		    ELSE 'NS' -- not selected
	   END AS SexType_Code,
	   CASE 
	        WHEN s.Sex = 'M' THEN 'Male'
	        WHEN s.Sex = 'F' THEN 'Female'
		    ELSE 'Not Selected' -- not selected
	   END AS SexType_Description,
	   CASE WHEN s.Sex = 'M' THEN 1 ELSE 0 END AS SexType_Male_Indicator,
	   CASE WHEN s.Sex = 'F' THEN 1 ELSE 0 END AS SexType_Female_Indicator,
	   CASE WHEN s.Sex not in ( 'M','F') THEN 1 ELSE 0 END AS SexType_NotSelected_Indicator, -- NON BINARY

	   COALESCE(s.RacePrompt,'N/A') AS RaceCode,
	   COALESCE(s.RacePrompt,'N/A') AS RaceDescription,	   

	   CASE WHEN s.RacePrompt IN ('Nat. Amer.','Native American') THEN 1 ELSE 0 END AS Race_AmericanIndianAlaskanNative_Indicator,
	   CASE WHEN s.RacePrompt = 'Asian' THEN 1 ELSE 0 END AS Race_Asian_Indicator,
	   CASE WHEN s.RacePrompt = 'Black' THEN 1 ELSE 0 END AS Race_BlackAfricaAmerican_Indicator,
	   0 AS Race_NativeHawaiianPacificIslander_Indicator,
	   CASE WHEN  s.RacePrompt = 'White' THEN 1 ELSE 0 END AS Race_White_Indicator,
	   CASE WHEN  s.RacePrompt = 'Mixed or Other' THEN 1 ELSE 0 END AS Race_MultiRace_Indicator, -- did not see this in populated template
	   0 AS Race_ChooseNotRespond_Indicator,
	   CASE WHEN  s.RacePrompt = 'Other' THEN 1 ELSE 0 END AS Race_Other_Indicator,

	   CASE WHEN s.RacePrompt = 'Hispanic' THEN 'H' ELSE 'Non-H' END  AS EthnicityCode,
	   CASE WHEN s.RacePrompt = 'Hispanic' THEN 'Hispanic' ELSE 'Non Hispanic' END  AS EthnicityDescription,
	   CASE WHEN s.RacePrompt = 'Hispanic' THEN 1 ELSE 0 END AS EthnicityHispanicLatino_Indicator,
	   
	   0 AS Migrant_Indicator,
	   CASE WHEN hsby.studentno IS NULL THEN 0 ELSE 1 END AS Homeless_Indicator,
	   0 AS Homeless_Indicator,
       case WHEN COALESCE(s.SnCode,'None') <> 'None' THEN 1  ELSE 0 END  AS IEP_Indicator,
	   
	   COALESCE(s.Lep_Status,'N/A') AS LimitedEnglishProficiencyDescriptor_CodeValue,
	   COALESCE(s.Lep_Status,'N/A') AS LimitedEnglishProficiencyDescriptor_Description,
	   CASE WHEN COALESCE(s.Lep_Status,'N/A') = 'L' THEN 1 ELSE 0 END [LimitedEnglishProficiency_EnglishLearner_Indicator],
	   CASE WHEN COALESCE(s.Lep_Status,'N/A') = 'F' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_Former_Indicator,
       CASE WHEN COALESCE(s.Lep_Status,'N/A') = 'N' THEN 1 ELSE 0 END AS LimitedEnglishProficiency_NotEnglisLearner_Indicator,
	   CASE WHEN COALESCE(s.foodgroup,'None') <> 'None' THEN 1 ELSE 0 END AS EconomicDisadvantage_Indicator,
       
	   --entry
	   s.entdate AS EntryDate,
	   s.schyear AS EntrySchoolYear, 
	   COALESCE(s.entcode,'N/A') AS EntryCode,
       
	   --exit
	   case when s.sequenceno =  999999 AND s.withdate IS NULL THEN '6/30/' + CAST(s.schyear AS NVARCHAR(max)) else s.withdate END AS ExitWithdrawDate,
	   s.schyear AS ExitWithdrawSchoolYear, 
	   COALESCE(s.withcode,'N/A') AS ExitWithdrawCode              

       ,s.entdate AS ValidFrom
	   ,case when s.sequenceno =  999999 AND s.withdate IS NULL THEN '6/30/' + CAST(s.schyear AS NVARCHAR(max)) else s.withdate END AS ValidTo
	   ,0 IsCurrent
	   --,@lineageKey AS [LineageKey]
--select distinct top 100 *
FROM [BPSDW].[dbo].[student] s 
     INNER JOIN LongitudinalPOC.dbo.DimSchool dschool ON  CONCAT_WS('|','Ed-Fi',Convert(NVARCHAR(MAX),s.sch))  = dschool._sourceKey	 
	 LEFT JOIN HomelessStudentsByYear hsby ON s.StudentNo = hsby.studentno 
	                                      and s.schyear = hsby.schyear
WHERE s.schyear IN (2018,2017,2016)
	  --AND s.StudentNo = '999453'
ORDER BY s.StudentNo

  




/*
SELECT std.STD_FIELDA_024 [McKinney_Act], 
      ISNULL(std.STD_FIELDB_065,'') as [McKinney_Act/DCF],
      ISNULL(std.STD_FIELDB_037,'') [BPS_Homeless/DCF_Residence_Type],	  
      LEFT(LTRIM(std.STD_ID_LOCAL),6) [BPS_Student_ID],  
      std.STD_ID_STATE [STATE_Student_ID],
	  CASE WHEN STD_FIELDA_024 = 1 OR COALESCE(STD_FIELDB_065,'')<>'' THEN 1 ELSE 0 END AS IsHomeless
--SELECT  std.*
FROM [BPSDATA-03].[ExtractAspen].[dbo].[STUDENT] std
  --INNER JOIN [BPSDATA-03].[ExtractAspen].dbo.SCHOOL sch ON sch.SKL_OID = std.STD_SKL_OID
  --INNER JOIN [BPSDATA-03].[ExtractAspen].[dbo].[PERSON] prs ON std.STD_PSN_OID = prs.PSN_OID
 */

--updatng the lineage table
UPDATE LongitudinalPOC.[dbo].[Lineage]
  SET 
      EndTime = GETDATE(), 
      STATUS = 'S'  -- Success
WHERE LineageKey = @lineageKey;


--SELECT * FROM  LongitudinalPOC.dbo.DimStudent


/*

SELECT *
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Descriptor
WHERE Namespace LIKE '%datatype%';

SELECT *
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.ResultDatatypeType;

SELECT *
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.SexType;

SELECT count(distinct *)
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Student;

SELECT *
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.Program;

SELECT *
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.EntryGradeLevelReasonType;

SELECT *
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentSchoolAssociation;


SELECT *
FROM [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.RaceType;

select * from [EDFISQL01].[EdFi_BPS_Production_Ods].edfi.StudentElectronicMail


*/
